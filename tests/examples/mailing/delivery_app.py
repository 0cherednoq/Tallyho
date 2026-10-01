"""Per-recipient export recipe from ARCHITECTURE section 12.9 as a working application.

The domain keeps one ``mailing_delivery`` row per recipient. A task writes its
own row together with the Item outcome (``complete_in``). Outcomes produced
without user code — exhausted retries, cancellation — are exported by the
``on_finalized_task`` callback, which also closes the rows of recipients that
never became Items, sets the terminal campaign status and releases the tree,
all in one transaction.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast, final

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.states import BatchState, ItemState
from tallyho.testing import FakeClock, InlineBroker
from tests.examples.mailing.delivery_domain import (
    create_delivery_domain,
    deliveries,
    delivery_campaigns,
    recipients,
)
from tests.examples.mailing.provider import FakeMailProvider, HardBounceError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.views import BatchSummary, ItemView

__all__ = ["KIND", "CampaignStateError", "DeliveryApp", "SettleProbeError"]

KIND = "delivery_campaign"
FINAL = {
    BatchState.SUCCEEDED: "completed",
    BatchState.COMPLETED_WITH_ERRORS: "completed_with_errors",
    BatchState.FAILED: "failed",
    BatchState.CANCELLED: "cancelled",
}
EXPORTED = {ItemState.ERROR: "failed", ItemState.CANCELLED: "cancelled"}


class CampaignStateError(Exception):
    """A domain command was called in a status that does not allow it."""


class SettleProbeError(Exception):
    """Controlled failure in the middle of the export callback."""


def _normalize(email: str) -> str:
    return email.strip().casefold()


@final
class DeliveryApp:
    """Domain commands, tx-hook, tasks and the settle callback of one schema."""

    def __init__(
        self, engine: AsyncEngine, schema: str, *, page: int = 50, chunk: int = 20
    ) -> None:
        self.engine = engine.execution_options(schema_translate_map={None: schema})
        self.clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
        self.broker = InlineBroker(seed=7)
        self.th = Tallyho(engine, schema=schema, clock=self.clock, retention=timedelta(days=1))
        self.th.install(self.broker.adapter)
        self.mail = FakeMailProvider()
        self.page = page
        self.chunk = chunk
        self.settle_failures = 0
        self.settle_calls = 0
        self.finalized: list[str] = []
        self._next_campaign_id = 1

        async def expand_audience(campaign_id: int, after_id: int) -> None:
            await self._expand_audience(campaign_id, after_id)

        async def send_email(campaign_id: int, email: str) -> None:
            await self._send_email(campaign_id, email)

        async def settle_campaign(campaign_id: int) -> None:
            await self.settle(campaign_id)

        self.expand_audience: Callable[[int, int], Awaitable[None]] = expand_audience
        self.send_email: Callable[[int, str], Awaitable[None]] = send_email
        self.settle_campaign: Callable[[int], Awaitable[None]] = settle_campaign
        self._register_hooks()

    @classmethod
    async def create(
        cls, engine: AsyncEngine, schema: str, *, page: int = 50, chunk: int = 20
    ) -> DeliveryApp:
        """Create domain and tallyho tables for one isolated application."""
        app = cls(engine, schema, page=page, chunk=chunk)
        await create_delivery_domain(app.engine)
        _ = await app.th.migrate()
        return app

    async def close(self) -> None:
        """Flush and stop the in-process worker runtime."""
        await self.broker.close()

    async def drain(self) -> int:
        """Run the in-process worker until the application is idle."""
        return await self.broker.drain()

    # --- domain commands -----------------------------------------------------

    async def create_campaign(self, emails: Sequence[str], *, tenant: str = "acme") -> int:
        """Insert a draft campaign, its audience and one pending row per address."""
        campaign_id = self._next_campaign_id
        self._next_campaign_id += 1
        async with self.engine.begin() as connection:
            _ = await connection.execute(
                insert(delivery_campaigns).values(
                    id=campaign_id, tenant=tenant, status="draft", sent=0, failed=0, cancelled=0
                )
            )
            _ = await connection.execute(
                insert(recipients),
                [
                    {"id": index, "campaign_id": campaign_id, "email": email}
                    for index, email in enumerate(emails, start=1)
                ],
            )
            rows = [
                {"campaign_id": campaign_id, "email": email, "status": "pending"}
                for email in dict.fromkeys(_normalize(value) for value in emails)
            ]
            _ = await connection.execute(pg_insert(deliveries).on_conflict_do_nothing(), rows)
        return campaign_id

    async def start(self, campaign_id: int, *, retries: int = 1) -> UUID:
        """Create the tree atomically with the domain transition draft → running."""
        async with AsyncSession(self.engine, expire_on_commit=False) as session, session.begin():
            campaign = await self._locked(session, campaign_id)
            if campaign["status"] != "draft":
                raise CampaignStateError(campaign["status"])
            async with self.th.batch(
                KIND,
                key=f"campaign:{campaign_id}",
                attributes={"campaign_id": campaign_id, "tenant": cast("str", campaign["tenant"])},
                memo={"started_by": "delivery-example"},
                release_required=True,  # дерево ждёт экспорта исходов
                on_finalized_task=self.th.call(self.settle_campaign, campaign_id).opts(
                    max_retries=3
                ),
                session=session,
            ) as root:
                expand = root.sub_batch("expand")
                _ = root.sub_batch("send", fed_by=[expand])
                await expand.add_calls(
                    [self.th.call(self.expand_audience, campaign_id, 0).opts(max_retries=retries)]
                )
            _ = await session.execute(
                update(delivery_campaigns)
                .where(delivery_campaigns.c.id == campaign_id)
                .values(status="running", batch_id=root.handle.id)
            )
            return root.handle.id

    async def cancel(self, campaign_id: int) -> None:
        """Request cancellation; the terminal status arrives through settle."""
        async with AsyncSession(self.engine) as session, session.begin():
            campaign = await self._locked(session, campaign_id)
            if campaign["status"] != "running":
                raise CampaignStateError(campaign["status"])
            await self.th.handle(cast("UUID", campaign["batch_id"])).cancel(session=session)

    async def retry_failed(self, campaign_id: int) -> int:
        """Reopen the pipeline for failed recipients; the cycle repeats from running."""
        async with AsyncSession(self.engine) as session, session.begin():
            campaign = await self._locked(session, campaign_id)
            if campaign["status"] not in {"completed_with_errors", "failed"}:
                raise CampaignStateError(campaign["status"])
            retried = await self.th.handle(cast("UUID", campaign["batch_id"])).retry_failed(
                session=session
            )
            _ = await session.execute(
                update(delivery_campaigns)
                .where(delivery_campaigns.c.id == campaign_id)
                .values(status="running", outcome=None, finished_at=None)
            )
            return retried

    async def settle(self, campaign_id: int) -> bool:
        """Export infrastructure outcomes and finish the campaign in one transaction.

        Idempotent: a repeated delivery of the callback finds the campaign out
        of ``settling`` and does nothing.

        Returns:
            Whether this call settled the campaign.
        """
        self.settle_calls += 1
        async with AsyncSession(self.engine) as session, session.begin():
            campaign = await self._locked(session, campaign_id)
            if campaign["status"] != "settling":
                return False
            root = self.th.handle(cast("UUID", campaign["batch_id"]))
            send = await root.child("send")  # Items лежат в этапе, не в корне
            chunk: list[ItemView] = []
            async for entry in send.items(states=set(EXPORTED)):
                chunk.append(entry)
                if len(chunk) == self.chunk:
                    await self._export(session, campaign_id, chunk)
                    chunk = []
                    self._probe()
            await self._export(session, campaign_id, chunk)
            # Получатели, которые так и не стали Items: отмена посреди разворачивания.
            _ = await session.execute(
                update(deliveries)
                .where(deliveries.c.campaign_id == campaign_id, deliveries.c.status == "pending")
                .values(status="cancelled", reason="not_dispatched")
            )
            _ = await session.execute(
                update(delivery_campaigns)
                .where(delivery_campaigns.c.id == campaign_id)
                .values(status=campaign["outcome"])
            )
            await root.release(session=session)  # release — у корня, в той же транзакции
        return True

    # --- reads used by scenarios ---------------------------------------------

    async def campaign(self, campaign_id: int) -> Mapping[str, object]:
        """Read the campaign row."""
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(delivery_campaigns).where(delivery_campaigns.c.id == campaign_id)
                    )
                )
                .mappings()
                .one()
            )
        return cast("Mapping[str, object]", dict(row))

    async def delivery_statuses(self, campaign_id: int) -> Counter[tuple[str, str | None]]:
        """Count delivery rows by ``(status, reason)``."""
        async with self.engine.connect() as connection:
            rows = await connection.execute(
                select(deliveries.c.status, deliveries.c.reason, func.count())
                .where(deliveries.c.campaign_id == campaign_id)
                .group_by(deliveries.c.status, deliveries.c.reason)
            )
            return Counter({(status, reason): count for status, reason, count in rows})

    async def delivery(self, campaign_id: int, email: str) -> tuple[str, str | None]:
        """Read ``(status, reason)`` of one recipient."""
        async with self.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(deliveries.c.status, deliveries.c.reason).where(
                        deliveries.c.campaign_id == campaign_id,
                        deliveries.c.email == _normalize(email),
                    )
                )
            ).one()
        return cast("str", row[0]), cast("str | None", row[1])

    # --- tasks -----------------------------------------------------------------

    async def _expand_audience(self, campaign_id: int, after_id: int) -> None:
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(recipients.c.id, recipients.c.email)
                    .where(recipients.c.campaign_id == campaign_id, recipients.c.id > after_id)
                    .order_by(recipients.c.id)
                    .limit(self.page)
                )
            ).all()
        for _recipient_id, email in rows:
            call = self.th.call(self.send_email, campaign_id, cast("str", email))
            item.spawn_call(
                call.opts(key=_normalize(cast("str", email)), max_retries=1), into="send"
            )
        if len(rows) == self.page:
            last_id = cast("int", rows[-1][0])
            item.spawn_call(
                self.th.call(self.expand_audience, campaign_id, last_id).opts(key=f"page:{last_id}")
            )

    async def _send_email(self, campaign_id: int, email: str) -> None:
        if item.cancelled():
            return
        try:
            _ = await self.mail.send(from_="mailbox@example.test", to=email)
        except HardBounceError:
            # Исход, который знает код задачи: строка и итог Item — один commit.
            await self._complete(campaign_id, email, status="failed", reason="hard_bounce")
            return
        # TemporaryMailError пробрасывается: ретрай брокера, затем error("exhausted")
        # без участия кода задачи. Такую строку обновит только экспорт.
        await self._complete(campaign_id, email, status="sent", reason=None)

    async def _complete(
        self, campaign_id: int, email: str, *, status: str, reason: str | None
    ) -> None:
        async with self.engine.begin() as connection:
            _ = await connection.execute(
                update(deliveries)
                .where(
                    deliveries.c.campaign_id == campaign_id,
                    deliveries.c.email == _normalize(email),
                )
                .values(status=status, reason=reason)
            )
            if reason is None:
                item.ok("sent")
            else:
                item.error(reason)
            await item.complete_in(connection)

    # --- internals -------------------------------------------------------------

    async def _export(
        self, session: AsyncSession, campaign_id: int, chunk: Sequence[ItemView]
    ) -> None:
        for state, status in EXPORTED.items():
            by_reason: dict[str | None, list[str]] = {}
            for entry in chunk:
                if entry.state is state and entry.key is not None:
                    by_reason.setdefault(entry.label, []).append(entry.key)
            for reason, emails in by_reason.items():
                _ = await session.execute(
                    update(deliveries)
                    .where(deliveries.c.campaign_id == campaign_id, deliveries.c.email.in_(emails))
                    .values(status=status, reason=reason)
                )

    def _probe(self) -> None:
        if self.settle_failures > 0:
            self.settle_failures -= 1
            raise SettleProbeError

    @staticmethod
    async def _locked(session: AsyncSession, campaign_id: int) -> Mapping[str, object]:
        row = (
            (
                await session.execute(
                    select(delivery_campaigns)
                    .where(delivery_campaigns.c.id == campaign_id)
                    .with_for_update()  # домен → tallyho: порядок блокировок как у tx-хука
                )
            )
            .mappings()
            .one()
        )
        return cast("Mapping[str, object]", dict(row))

    def _register_hooks(self) -> None:
        @self.th.on_finalized(KIND)
        async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
            send = summary.children["send"].progress
            outcome = FINAL[summary.state]
            self.finalized.append(outcome)
            # Счётчики точные уже здесь; терминальный статус поставит settle.
            _ = await session.execute(
                update(delivery_campaigns)
                .where(
                    delivery_campaigns.c.batch_id == summary.id,
                    delivery_campaigns.c.status.in_(("running", "settling")),
                )
                .values(
                    status="settling",
                    outcome=outcome,
                    finished_at=summary.finished_at,
                    sent=send.ok,
                    failed=send.error,
                    cancelled=send.cancelled,
                )
            )

        _ = save_result
