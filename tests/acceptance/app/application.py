"""Shared real-Flexiq reference application for acceptance scenarios S1/S2/S3."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import aiohttp
from flexiq import Queue, current_job
from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.model.states import BatchState
from tests.acceptance.app.common import (
    FaultPlan,
    PermanentError,
    TransientError,
    hold_transaction,
    network,
)
from tests.acceptance.app.domain import build_domain
from tests.acceptance.app.usecases import HookMissingPrinter, register_usecases

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.model.policy import PolicyBreach
    from tallyho.model.views import BatchSummary
    from tests.acceptance.app.domain import DomainTable, DomainTables
    from tests.acceptance.app.usecases import UcTasks

__all__ = [
    "AUDIENCE_ID_SPAN",
    "PAGE",
    "AcceptanceApp",
    "AcceptanceTasks",
    "StandTuning",
    "build_app",
    "flexiq_dsn",
]

S1_KIND = "acceptance.s1"
S2_KIND = "acceptance.s2"
S3_KIND = "acceptance.s3"
PAGE = 10
AUDIENCE_ID_SPAN = 1_000_000


@dataclass(frozen=True, slots=True, kw_only=True)
class StandTuning:
    """Process-level knobs the chaos stand overrides; defaults keep the smoke harness as is.

    Attributes:
        application_name: PostgreSQL ``application_name`` of this process' SQLAlchemy
            connections; the chaos controller finds processes by it in ``pg_stat_activity``.
        lease_ttl: Tallyho ``lease_ttl``.
        heartbeat_every: Tallyho ``heartbeat_every``.
        sweep_interval: Tallyho ``sweep_interval``.
        drain_timeout: FlexIQ graceful-shutdown budget for running jobs, seconds.
        hook_delay: Seconds ``on_finalized`` keeps its transaction open after the domain
            write, so A-CH-03 can observe the hook in ``pg_stat_activity``.
    """

    application_name: str | None = None
    lease_ttl: timedelta = timedelta(seconds=60)
    heartbeat_every: timedelta = timedelta(seconds=20)
    sweep_interval: timedelta = timedelta(milliseconds=100)
    drain_timeout: int = 1
    hook_delay: float = 0.0

    @property
    def job_timeout(self) -> int:
        """FlexIQ ``timeout`` of every stand task, seconds: ``lease_ttl``, not the 300 s default.

        A result FlexIQ could not record during a PostgreSQL outage leaves the job
        ``running`` until this timeout, and a non-holding attempt leaves its Item without
        lease and outbox until then (ARCHITECTURE §11.3, Fix-19). Tying it to ``lease_ttl``
        keeps that recovery inside ``T_rec`` (ACCEPTANCE §6); stand tasks take seconds.
        """
        return max(1, math.ceil(self.lease_ttl.total_seconds()))


@dataclass(frozen=True, slots=True)
class AcceptanceTasks:
    """Registered task functions shared by producer and subprocess workers."""

    render_invoice: Callable[[int], Awaitable[None]]
    expand_audience: Callable[[int, int, int], Awaitable[None]]
    send_email: Callable[[int, int], Awaitable[None]]
    parse_page: Callable[[int, int], Awaitable[None]]
    parse_card: Callable[[int, str], Awaitable[None]]
    download_pdf: Callable[[int, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class AcceptanceApp:
    """Process-local reference app connected to shared PostgreSQL and services."""

    dsn: str
    tallyho_schema: str
    flexiq_schema: str
    domain_schema: str
    engine: AsyncEngine
    queue: Queue
    adapter: FlexiqAdapter
    th: Tallyho
    tasks: AcceptanceTasks
    domain: DomainTables
    seed: int
    site_url: str
    mail_url: str
    uc: UcTasks

    async def migrate(self) -> None:
        """Create domain and tallyho tables; FlexIQ initializes its own schema."""
        async with self.engine.begin() as connection:
            await connection.run_sync(self.domain.metadata.create_all, checkfirst=True)
        _ = await self.th.migrate()

    async def close(self) -> None:
        """Close process-local clients; call it in the loop that used them (API, tests)."""
        # Сначала установка: фоновые задачи дожидаются, relay не шлёт через закрытый адаптер.
        await self.th.aclose()
        self.queue.close()
        await self.adapter.close()
        await self.engine.dispose()

    async def start_s1(self, invoice_ids: Sequence[int]) -> UUID:
        """Create known-size invoice work and its domain launch atomically."""
        engine = self.engine.execution_options(schema_translate_map={None: self.tallyho_schema})
        invoices = self.domain.invoices
        async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
            _ = await session.execute(
                insert(invoices),
                [{"id": invoice_id, "status": "queued"} for invoice_id in invoice_ids],
            )
            async with self.th.batch(
                S1_KIND,
                key=f"invoices:{min(invoice_ids)}",
                expected_total=len(invoice_ids),
                session=session,
            ) as batch:
                await batch.add_calls(
                    [
                        self.th.call(self.tasks.render_invoice, invoice_id).opts(
                            key=f"invoice:{invoice_id}"
                        )
                        for invoice_id in invoice_ids
                    ]
                )
            _ = await session.execute(
                update(invoices)
                .where(invoices.c.id.in_(invoice_ids))
                .values(batch_id=batch.handle.id)
            )
        return batch.handle.id

    async def start_s2(
        self,
        campaign_id: int,
        addresses: Sequence[str],
        *,
        expected_total: int | None = None,
        page: int = PAGE,
    ) -> UUID:
        """Create the unknown-size expand -> send campaign.

        ``expected_total`` is the audience size announced to the ``send`` stage (A-UC-02);
        ``page`` is how many contacts one ``expand_audience`` Item reads.
        """
        engine = self.engine.execution_options(schema_translate_map={None: self.tallyho_schema})
        campaigns = self.domain.campaigns
        audience = self.domain.audience
        async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
            _ = await session.execute(insert(campaigns).values(id=campaign_id, status="running"))
            _ = await session.execute(
                insert(audience),
                [
                    {
                        "id": campaign_id * AUDIENCE_ID_SPAN + index,
                        "campaign_id": campaign_id,
                        "email": address,
                    }
                    for index, address in enumerate(addresses, start=1)
                ],
            )
            async with self.th.batch(
                S2_KIND, key=f"campaign:{campaign_id}", session=session
            ) as root:
                expand = root.sub_batch("expand")
                _ = root.sub_batch("send", fed_by=[expand], expected_total=expected_total)
                await expand.add_calls(
                    [
                        self.th.call(self.tasks.expand_audience, campaign_id, 0, page).opts(
                            key="page:0", max_retries=3
                        )
                    ]
                )
            _ = await session.execute(
                update(campaigns)
                .where(campaigns.c.id == campaign_id)
                .values(batch_id=root.handle.id)
            )
        return root.handle.id

    async def start_s3(self, run_id: int, *, pages: int, max_items: int = 100_000) -> UUID:
        """Create the pages -> cards -> pdfs pipeline."""
        engine = self.engine.execution_options(schema_translate_map={None: self.tallyho_schema})
        catalog_runs = self.domain.catalog_runs
        async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
            _ = await session.execute(insert(catalog_runs).values(id=run_id, status="running"))
            async with self.th.batch(
                S3_KIND,
                key=f"catalog:{run_id}",
                max_items=max_items,
                session=session,
            ) as root:
                page_stage = root.sub_batch("pages", max_depth=1)
                card_stage = root.sub_batch("cards", fed_by=[page_stage])
                _ = root.sub_batch("pdfs", fed_by=[card_stage])
                await page_stage.add_calls(
                    [
                        self.th.call(self.tasks.parse_page, run_id, page).opts(
                            key=f"page:{page}", max_retries=3
                        )
                        for page in range(1, pages + 1)
                    ]
                )
            _ = await session.execute(
                update(catalog_runs)
                .where(catalog_runs.c.id == run_id)
                .values(batch_id=root.handle.id)
            )
        return root.handle.id


def flexiq_dsn(dsn: str) -> str:
    """Return the driver-neutral URL required by FlexIQ."""
    return dsn.replace("postgresql+asyncpg://", "postgresql://", 1)


def build_app(  # ruff: ignore[complex-structure, too-many-statements, too-many-arguments, too-many-locals]  # shared factory must register one identical task graph in API and workers
    *,
    dsn: str,
    tallyho_schema: str,
    flexiq_schema: str,
    domain_schema: str,
    site_url: str,
    mail_url: str,
    seed: int,
    network_scale: float = 1.0,
    transient_rate: float = 0.05,
    permanent_rate: float = 0.01,
    worker_count: int = 4,
    tuning: StandTuning | None = None,
    worker: bool = False,
) -> AcceptanceApp:
    """Build one producer/worker process with identical task registrations.

    ``worker=True`` marks a FlexIQ worker process: it does not register the hooks of
    ``UC_NOHOOK_KIND``, so those batches are finalized by maintenance (A-UC-17).
    """
    knobs = tuning or StandTuning()
    engine = (
        create_async_engine(dsn)
        if knobs.application_name is None
        else create_async_engine(
            dsn, connect_args={"server_settings": {"application_name": knobs.application_name}}
        )
    )
    domain = build_domain(domain_schema)
    domain_engine = engine.execution_options(schema_translate_map={None: tallyho_schema})
    audience = domain.audience
    campaigns = domain.campaigns
    cards = domain.cards
    catalog_runs = domain.catalog_runs
    deliveries = domain.deliveries
    hook_log = domain.hook_log
    invoice_files = domain.invoice_files
    invoices = domain.invoices
    pdf_files = domain.pdf_files
    task_log = domain.task_log
    queue = Queue(
        backend="postgres",
        db_url=flexiq_dsn(dsn),
        schema=flexiq_schema,
        workers=worker_count,
        async_concurrency=worker_count,
        drain_timeout=knobs.drain_timeout,
        scheduler_poll_interval_ms=20,
        scheduler_reap_interval=1,
    )
    adapter = FlexiqAdapter(queue)
    job_timeout = knobs.job_timeout
    th = Tallyho(
        engine,
        schema=tallyho_schema,
        relay_grace=timedelta(0),
        finalize_grace=timedelta(0),
        sweep_interval=knobs.sweep_interval,
        lease_ttl=knobs.lease_ttl,
        heartbeat_every=knobs.heartbeat_every,
        snapshot_tick=timedelta(milliseconds=50),
        watch_throttle=timedelta(milliseconds=10),
        observer=HookMissingPrinter(),
    )
    th.install(adapter)
    faults = FaultPlan(seed, transient_rate, permanent_rate)

    async def prepare(task_name: str) -> UUID:
        item_id = item.id()
        if item_id is None:
            message = f"{task_name} executed outside tallyho item context"
            raise PermanentError(message)
        await network(seed, item_id, namespace=task_name, scale=network_scale)
        faults.raise_for(item_id, namespace=task_name, attempt=current_job.retry_count)
        return item_id

    async def log_task(
        connection: AsyncConnection,
        *,
        item_id: UUID,
        scenario: str,
        task_name: str,
        status: str,
    ) -> None:
        statement = pg_insert(task_log).values(
            item_id=item_id,
            scenario=scenario,
            task=task_name,
            status=status,
        )
        _ = await connection.execute(statement.on_conflict_do_nothing())

    @adapter.task(
        max_retries=3,
        timeout=job_timeout,
        retry_delays=[0.01, 0.02, 0.03],
        retry_on=[TransientError],
        dont_retry_on=[PermanentError],
    )
    async def render_invoice(invoice_id: int) -> None:
        item_id = await prepare("render_invoice")
        async with domain_engine.begin() as connection:
            _ = await hold_transaction(
                seed, item_id, namespace="render_invoice", scale=network_scale
            )
            statement = pg_insert(invoice_files).values(
                item_id=item_id,
                invoice_id=invoice_id,
                bytes=f"invoice:{invoice_id}".encode(),
            )
            _ = await connection.execute(statement.on_conflict_do_nothing())
            _ = await connection.execute(
                update(invoices).where(invoices.c.id == invoice_id).values(status="rendered")
            )
            await log_task(
                connection,
                item_id=item_id,
                scenario="S1",
                task_name="render_invoice",
                status="ok",
            )
            item.ok("rendered")
            await item.complete_in(connection)

    @adapter.task(
        max_retries=3,
        timeout=job_timeout,
        retry_delays=[0.01, 0.02, 0.03],
        retry_on=[TransientError],
        dont_retry_on=[PermanentError],
    )
    async def expand_audience(campaign_id: int, after_id: int, page: int = PAGE) -> None:
        item_id = await prepare("expand_audience")
        async with domain_engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(audience.c.id, audience.c.email)
                    .where(audience.c.campaign_id == campaign_id, audience.c.id > after_id)
                    .order_by(audience.c.id)
                    .limit(page)
                )
            ).all()
        for raw_contact_id, raw_address in rows:
            contact_id = cast("int", raw_contact_id)
            address = cast("str", raw_address)
            item.spawn_call(
                th.call(send_email, campaign_id, contact_id).opts(
                    key=address.strip().casefold(), max_retries=3
                ),
                into="send",
            )
        if len(rows) == page:
            item.spawn_call(
                th.call(expand_audience, campaign_id, rows[-1].id, page).opts(
                    key=f"page:{rows[-1].id}", max_retries=3
                )
            )
        async with domain_engine.begin() as connection:
            _ = await hold_transaction(
                seed, item_id, namespace="expand_audience", scale=network_scale
            )
            await log_task(
                connection,
                item_id=item_id,
                scenario="S2",
                task_name="expand_audience",
                status="ok",
            )
            item.ok("expanded")
            await item.complete_in(connection)

    @adapter.task(
        max_retries=3,
        timeout=job_timeout,
        retry_delays=[0.01, 0.02, 0.03],
        retry_on=[TransientError],
        dont_retry_on=[PermanentError],
    )
    async def send_email(campaign_id: int, contact_id: int) -> None:
        item_id = await prepare("send_email")
        async with domain_engine.connect() as connection:
            address = cast(
                "str",
                await connection.scalar(
                    select(audience.c.email).where(
                        audience.c.campaign_id == campaign_id,
                        audience.c.id == contact_id,
                    )
                ),
            )
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                f"{mail_url}/send",
                json={"item_id": str(item_id), "address": address},
            )
            async with response:
                result = cast("dict[str, object]", await response.json())
                status = response.status
        if status == 503:
            message = f"mail provider unavailable for {address}"
            raise TransientError(message)
        label = "sent" if status == 202 else "rejected"
        async with domain_engine.begin() as connection:
            _ = await hold_transaction(seed, item_id, namespace="send_email", scale=network_scale)
            statement = pg_insert(deliveries).values(
                item_id=item_id,
                campaign_id=campaign_id,
                email=address,
                label=label,
            )
            _ = await connection.execute(statement.on_conflict_do_nothing())
            await log_task(
                connection,
                item_id=item_id,
                scenario="S2",
                task_name="send_email",
                status=label,
            )
            item.ok(label, result=result)
            await item.complete_in(connection)

    async def get_json(path: str) -> tuple[int, dict[str, object]]:
        async with aiohttp.ClientSession() as client:
            response = await client.get(f"{site_url}{path}")
            async with response:
                status = response.status
                body = cast("dict[str, object]", await response.json())
        return status, body

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def parse_page(run_id: int, page: int) -> None:
        item_id = await prepare("parse_page")
        status, body = await get_json(f"/pages/{page}")
        if status == 503:
            message = "catalog page unavailable"
            raise TransientError(message)
        if status == 200:
            for raw_card in cast("list[object]", body["cards"]):
                card_url = str(raw_card)
                item.spawn_call(
                    th.call(parse_card, run_id, card_url).opts(key=card_url, max_retries=3),
                    into="cards",
                )
            for raw_page in cast("list[object]", body["pages"]):
                linked_page = int(cast("int", raw_page))
                item.spawn_call(
                    th.call(parse_page, run_id, linked_page).opts(
                        key=f"page:{linked_page}", max_retries=3
                    )
                )
        async with domain_engine.begin() as connection:
            _ = await hold_transaction(seed, item_id, namespace="parse_page", scale=network_scale)
            await log_task(
                connection,
                item_id=item_id,
                scenario="S3",
                task_name="parse_page",
                status=str(status),
            )
            if status == 200:
                item.ok("parsed")
            else:
                item.error("not_found")
            await item.complete_in(connection)

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def parse_card(run_id: int, url: str) -> None:
        item_id = await prepare("parse_card")
        status, body = await get_json(url)
        if status == 503:
            message = "catalog card unavailable"
            raise TransientError(message)
        if status == 200:
            for raw_pdf in cast("list[object]", body["pdfs"]):
                pdf_url = str(raw_pdf)
                item.spawn_call(
                    th.call(download_pdf, run_id, pdf_url).opts(key=pdf_url, max_retries=3),
                    into="pdfs",
                )
        async with domain_engine.begin() as connection:
            _ = await hold_transaction(seed, item_id, namespace="parse_card", scale=network_scale)
            if status == 200:
                statement = pg_insert(cards).values(
                    item_id=item_id,
                    run_id=run_id,
                    url=url,
                    status=status,
                )
                _ = await connection.execute(statement.on_conflict_do_nothing())
            await log_task(
                connection,
                item_id=item_id,
                scenario="S3",
                task_name="parse_card",
                status=str(status),
            )
            if status == 200:
                item.ok("parsed")
            else:
                item.error("not_found")
            await item.complete_in(connection)

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def download_pdf(run_id: int, url: str) -> None:
        item_id = await prepare("download_pdf")
        item.progress(1, 2)
        async with aiohttp.ClientSession() as client:
            response = await client.get(f"{site_url}{url}")
            async with response:
                status = response.status
                body = await response.read()
        if status == 503:
            message = "catalog PDF unavailable"
            raise TransientError(message)
        async with domain_engine.begin() as connection:
            _ = await hold_transaction(seed, item_id, namespace="download_pdf", scale=network_scale)
            if status == 200:
                statement = pg_insert(pdf_files).values(
                    item_id=item_id,
                    run_id=run_id,
                    url=url,
                    bytes=body,
                )
                _ = await connection.execute(statement.on_conflict_do_nothing())
            await log_task(
                connection,
                item_id=item_id,
                scenario="S3",
                task_name="download_pdf",
                status=str(status),
            )
            if status == 200:
                item.ok("downloaded")
            else:
                item.error("not_found")
            await item.complete_in(connection)

    def register_hooks(kind: str, domain_table: DomainTable) -> None:
        @th.on_finalized(kind)
        async def finalized(session: AsyncSession, summary: BatchSummary) -> None:
            inserted = await _write_hook(session, summary, "on_finalized")
            if not inserted:
                return
            value = (
                "completed" if summary.state is BatchState.SUCCEEDED else "completed_with_errors"
            )
            _ = await session.execute(
                update(domain_table)
                .where(domain_table.c.batch_id == summary.id)
                .values(
                    batch_status=value,
                    progress_done=summary.progress.done,
                    progress_found=summary.progress.found,
                )
            )
            if knobs.hook_delay > 0:
                await asyncio.sleep(knobs.hook_delay)

        @th.on_progress(kind, every=timedelta(seconds=1))
        async def progress(session: AsyncSession, summary: BatchSummary) -> None:
            inserted = await _write_hook(session, summary, "on_progress")
            if inserted:
                _ = await session.execute(
                    update(domain_table)
                    .where(domain_table.c.batch_id == summary.id)
                    .values(
                        progress_done=summary.progress.done,
                        progress_found=summary.progress.found,
                    )
                )

        @th.on_policy_breach(kind)
        async def policy(
            session: AsyncSession,
            summary: BatchSummary,
            _breach: PolicyBreach,
        ) -> None:
            inserted = await _write_hook(session, summary, "on_policy_breach")
            if inserted:
                _ = await session.execute(
                    update(domain_table)
                    .where(domain_table.c.batch_id == summary.id)
                    .values(policy_breaches=domain_table.c.policy_breaches + 1)
                )

    async def _write_hook(session: AsyncSession, summary: BatchSummary, name: str) -> bool:
        statement = pg_insert(hook_log).values(
            batch_id=summary.id,
            hook=name,
            seq=summary.seq,
            state=int(summary.state),
            progress_done=summary.progress.done,
            progress_found=summary.progress.found,
            txid=func.txid_current(),
        )
        result = await session.execute(statement.on_conflict_do_nothing().returning(hook_log.c.id))
        return result.scalar_one_or_none() is not None

    register_hooks(S1_KIND, invoices)
    register_hooks(S2_KIND, campaigns)
    register_hooks(S3_KIND, catalog_runs)
    uc = register_usecases(
        th,
        adapter,
        engine=domain_engine,
        domain=domain,
        seed=seed,
        network_scale=network_scale,
        job_timeout=job_timeout,
        worker=worker,
    )
    tasks = AcceptanceTasks(
        render_invoice=render_invoice,
        expand_audience=expand_audience,
        send_email=send_email,
        parse_page=parse_page,
        parse_card=parse_card,
        download_pdf=download_pdf,
    )
    return AcceptanceApp(
        dsn=dsn,
        tallyho_schema=tallyho_schema,
        flexiq_schema=flexiq_schema,
        domain_schema=domain_schema,
        engine=engine,
        queue=queue,
        adapter=adapter,
        th=th,
        tasks=tasks,
        domain=domain,
        seed=seed,
        site_url=site_url,
        mail_url=mail_url,
        uc=uc,
    )
