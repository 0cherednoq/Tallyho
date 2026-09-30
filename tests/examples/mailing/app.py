"""Working user application for the mailing example in ARCHITECTURE section 12."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast, final

from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker
from tests.examples.mailing.domain import (
    campaigns,
    contacts,
    create_domain,
    record,
    suppressions,
)
from tests.examples.mailing.provider import FakeMailProvider, HardBounceError, RejectedError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.policy import PolicyBreach
    from tallyho.model.views import BatchSummary
    from tests.examples.mailing.dataset import ContactSeed
    from tests.examples.mailing.domain import CampaignRecord

__all__ = ["KIND", "MailingApp", "normalize_email"]

KIND = "campaign_deliveries"
PAGE = 1_000
ACTIVE = ("scheduled", "running", "paused")
FINAL = {
    BatchState.SUCCEEDED: "completed",
    BatchState.COMPLETED_WITH_ERRORS: "completed_with_errors",
    BatchState.FAILED: "failed",
    BatchState.CANCELLED: "cancelled",
}


class FinalizationProbeError(Exception):
    """Controlled failure used to prove hook retry semantics."""


def normalize_email(value: str) -> str:
    """Normalize the address used as the idempotency key."""
    return value.strip().casefold()


def _valid_email(value: str) -> bool:
    local, separator, domain = value.rpartition("@")
    return bool(local and separator and domain and "." in domain)


def _figures(summary: BatchSummary) -> dict[str, object]:
    send = summary.children["send"]
    return {
        "sent": send.labels.get("sent", 0),
        "skipped": send.progress.skip,
        "failed": send.progress.error,
        "duplicates": send.progress.duplicates,
        "breakdown": dict(send.labels),
        "progress": send.progress.ratio or 0.0,
        "progress_seq": summary.seq,
    }


@final
class MailingApp:
    """Domain commands, tx-hooks and tracked tasks wired to one schema."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        duplicate_delivery_rate: float = 0.05,
    ) -> None:
        self.engine = engine.execution_options(schema_translate_map={None: schema})
        self.clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
        self.broker = InlineBroker(
            duplicate_delivery_rate=duplicate_delivery_rate,
            seed=42,
        )
        self.th = Tallyho(
            engine,
            schema=schema,
            clock=self.clock,
            lease_ttl=timedelta(seconds=60),
            heartbeat_every=timedelta(seconds=20),
            retention=timedelta(days=14),
        )
        self.th.install(self.broker.adapter)
        self.mail = FakeMailProvider()
        self.fail_finalized = False
        self.finalized_calls = 0
        self.snapshot_progress: list[float] = []
        self._next_campaign_id = 1
        self._contacts: dict[tuple[int, int], ContactSeed] = {}
        self._suppressed: set[str] = set()

        async def expand_audience(campaign_id: int, after_id: int) -> None:
            await self._expand_audience(campaign_id, after_id)

        async def send_email(campaign_id: int, contact_id: int, mailbox_id: int) -> None:
            await self._send_email(campaign_id, contact_id, mailbox_id)

        self.expand_audience: Callable[[int, int], Awaitable[None]] = expand_audience
        self.send_email: Callable[[int, int, int], Awaitable[None]] = send_email
        self._register_hooks()

    @classmethod
    async def create(
        cls,
        engine: AsyncEngine,
        schema: str,
        *,
        duplicate_delivery_rate: float = 0.05,
    ) -> MailingApp:
        """Create domain and tallyho tables for one isolated application."""
        app = cls(engine, schema, duplicate_delivery_rate=duplicate_delivery_rate)
        await create_domain(app.engine)
        _ = await app.th.migrate()
        return app

    async def close(self) -> None:
        """Flush and stop the in-process worker runtime."""
        await self.broker.close()

    async def drain(self) -> int:
        """Run a bounded in-process worker pool until the application is idle."""
        return await self.broker.drain(concurrency=100)

    async def create_campaign(self, audience: Sequence[ContactSeed]) -> int:
        """Insert one draft campaign and its complete audience."""
        campaign_id = self._next_campaign_id
        self._next_campaign_id += 1
        contact_rows: list[dict[str, object]] = []
        suppression_rows: dict[str, dict[str, object]] = {}
        for contact_id, value in enumerate(audience, start=1):
            self._contacts[campaign_id, contact_id] = value
            contact_rows.append(
                {
                    "id": contact_id,
                    "audience_id": campaign_id,
                    "email": value.email,
                    "unsubscribed": value.unsubscribed,
                    "deleted": value.deleted,
                }
            )
            if value.suppressed:
                normalized = normalize_email(value.email)
                self._suppressed.add(normalized)
                suppression_rows[normalized] = {
                    "email": normalized,
                    "reason": "seeded",
                }
        async with self.engine.begin() as connection:
            _ = await connection.execute(
                insert(campaigns).values(
                    id=campaign_id,
                    title="October mailing",
                    subject="Hello",
                    html_template="<p>Hello</p>",
                    audience_id=campaign_id,
                    mailbox_ids=[11, 12],
                    status="draft",
                    sent=0,
                    skipped=0,
                    failed=0,
                    duplicates=0,
                    breakdown={},
                    progress=0.0,
                    progress_seq=0,
                )
            )
            if contact_rows:
                for offset in range(0, len(contact_rows), PAGE):
                    _ = await connection.execute(
                        insert(contacts).values(contact_rows[offset : offset + PAGE])
                    )
            if suppression_rows:
                _ = await connection.execute(
                    insert(suppressions).values(list(suppression_rows.values()))
                )
        return campaign_id

    async def schedule(self, campaign_id: int, at: datetime) -> UUID:
        """Atomically schedule the domain row and its two-stage tree."""
        async with AsyncSession(self.engine, expire_on_commit=False) as session, session.begin():
            current = (
                (
                    await session.execute(
                        select(campaigns).where(campaigns.c.id == campaign_id).with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            if current["status"] != "draft":
                message = f"campaign is {current['status']}"
                raise FinalizationProbeError(message)
            audience_id = cast("int", current["audience_id"])
            size = cast(
                "int",
                await session.scalar(
                    select(func.count(func.distinct(func.lower(contacts.c.email)))).where(
                        contacts.c.audience_id == audience_id
                    )
                ),
            )
            async with self.th.batch(
                KIND,
                key=f"campaign:{campaign_id}",
                start_at=at,
                session=session,
            ) as root:
                expand = root.sub_batch("expand")
                _ = root.sub_batch(
                    "send",
                    fed_by=[expand],
                    expected_total=size,
                    max_in_flight=500,
                    failure_policy=self.th.FailurePolicy.threshold(
                        ratio=0.05,
                        min_processed=500,
                        labels=["hard_bounce"],
                        action="pause",
                    ),
                )
                await expand.add_calls(
                    [self.th.call(self.expand_audience, campaign_id, 0).opts(max_retries=5)]
                )
            _ = await session.execute(
                update(campaigns)
                .where(campaigns.c.id == campaign_id)
                .values(
                    status="scheduled",
                    scheduled_at=at,
                    batch_id=root.handle.id,
                    audience_size=size,
                )
            )
            return root.handle.id

    async def start_now(self, campaign_id: int) -> UUID:
        """Schedule a draft campaign at the fake clock's current time."""
        return await self.schedule(campaign_id, self.clock.now())

    async def get(self, campaign_id: int) -> CampaignRecord:
        """Read the domain projection used by the UI."""
        async with self.engine.connect() as connection:
            row = (
                (await connection.execute(select(campaigns).where(campaigns.c.id == campaign_id)))
                .mappings()
                .one()
            )
        return record(cast("Mapping[str, object]", row))

    async def pause(self, campaign_id: int) -> None:
        """Pause domain status and the complete tallyho tree atomically."""
        async with AsyncSession(self.engine) as session, session.begin():
            current = (
                (
                    await session.execute(
                        select(campaigns).where(campaigns.c.id == campaign_id).with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            batch_id = cast("UUID", current["batch_id"])
            await self.th.handle(batch_id).pause(session=session)
            _ = await session.execute(
                update(campaigns)
                .where(campaigns.c.id == campaign_id)
                .values(status="paused", pause_reason="manual")
            )

    async def resume(self, campaign_id: int) -> None:
        """Resume domain status and the complete tallyho tree atomically."""
        async with AsyncSession(self.engine) as session, session.begin():
            current = (
                (
                    await session.execute(
                        select(campaigns).where(campaigns.c.id == campaign_id).with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            batch_id = cast("UUID", current["batch_id"])
            await self.th.handle(batch_id).resume(session=session)
            _ = await session.execute(
                update(campaigns)
                .where(campaigns.c.id == campaign_id)
                .values(status="running", pause_reason=None)
            )

    async def suppression_count(self, reason: str) -> int:
        """Count durable suppression rows written by hard-bounce completion."""
        async with self.engine.connect() as connection:
            value = await connection.scalar(
                select(func.count())
                .select_from(suppressions)
                .where(suppressions.c.reason == reason)
            )
        return cast("int", value)

    async def _expand_audience(self, campaign_id: int, after_id: int) -> None:
        async with self.engine.begin() as connection:
            campaign = (
                (await connection.execute(select(campaigns).where(campaigns.c.id == campaign_id)))
                .mappings()
                .one()
            )
            if after_id == 0:
                _ = await connection.execute(
                    update(campaigns)
                    .where(campaigns.c.id == campaign_id, campaigns.c.status == "scheduled")
                    .values(status="running")
                )
            audience_id = cast("int", campaign["audience_id"])
            rows = (
                (
                    await connection.execute(
                        select(contacts)
                        .where(
                            contacts.c.audience_id == audience_id,
                            contacts.c.id > after_id,
                        )
                        .order_by(contacts.c.id)
                        .limit(PAGE)
                    )
                )
                .mappings()
                .all()
            )
        mailboxes = cast("list[int]", campaign["mailbox_ids"])
        for row in rows:
            contact_id = cast("int", row["id"])
            email = cast("str", row["email"])
            call = self.th.call(
                self.send_email,
                campaign_id,
                contact_id,
                mailboxes[contact_id % len(mailboxes)],
            ).opts(key=normalize_email(email), max_retries=4)
            item.spawn_call(call, into="send")
        if len(rows) == PAGE:
            last_id = cast("int", rows[-1]["id"])
            item.spawn_call(
                self.th.call(self.expand_audience, campaign_id, last_id).opts(
                    key=f"page:{last_id}",
                    max_retries=5,
                )
            )

    async def _send_email(  # ruff: ignore[too-many-return-statements]  # each domain outcome exits immediately and maps to one tallyho label
        self, campaign_id: int, contact_id: int, mailbox_id: int
    ) -> None:
        contact = self._contacts.get((campaign_id, contact_id))
        if contact is None or contact.deleted:
            item.skip("recipient_not_found")
            return
        if contact.unsubscribed:
            item.skip("unsubscribed")
            return
        email = contact.email
        normalized = normalize_email(email)
        if normalized in self._suppressed:
            item.skip("suppressed")
            return
        if not _valid_email(email):
            item.error("invalid_address")
            return
        if item.cancelled():
            return
        try:
            message_id = await self.mail.send(from_=f"mailbox-{mailbox_id}@example.test", to=email)
        except HardBounceError as exc:
            async with self.engine.begin() as connection:
                statement = pg_insert(suppressions).values(
                    email=normalized,
                    reason="hard_bounce",
                )
                _ = await connection.execute(statement.on_conflict_do_nothing())
                item.error("hard_bounce", detail=str(exc))
                await item.complete_in(connection)
            self._suppressed.add(normalized)
            return
        except RejectedError as exc:
            item.error("rejected", detail=str(exc))
            return
        item.ok("sent", result={"message_id": message_id})

    def _register_hooks(self) -> None:
        @self.th.on_finalized(KIND)
        async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
            if self.fail_finalized:
                message = "controlled finalized hook failure"
                raise FinalizationProbeError(message)
            self.finalized_calls += 1
            _ = await session.execute(
                update(campaigns)
                .where(campaigns.c.batch_id == summary.id, campaigns.c.status.in_(ACTIVE))
                .values(
                    status=FINAL[summary.state],
                    finished_at=summary.finished_at,
                    **_figures(summary),
                )
            )

        @self.th.on_progress(KIND, every=timedelta(seconds=2))
        async def save_progress(session: AsyncSession, summary: BatchSummary) -> None:
            figures = _figures(summary)
            progress = cast("float", figures["progress"])
            self.snapshot_progress.append(progress)
            _ = await session.execute(
                update(campaigns)
                .where(
                    campaigns.c.batch_id == summary.id,
                    campaigns.c.progress_seq < summary.seq,
                )
                .values(
                    **figures,
                    progress=func.greatest(campaigns.c.progress, progress),
                )
            )

        @self.th.on_policy_breach(KIND)
        async def auto_pause(
            session: AsyncSession,
            summary: BatchSummary,
            breach: PolicyBreach,
        ) -> None:
            reason = f"{breach.labels} rate {breach.ratio:.1%}"
            _ = await session.execute(
                update(campaigns)
                .where(campaigns.c.batch_id == summary.id, campaigns.c.status == "running")
                .values(status="paused", pause_reason=reason)
            )
