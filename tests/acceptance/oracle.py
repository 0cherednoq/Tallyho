"""Numeric consistency oracle for ACCEPTANCE invariants I-01 through I-14."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from sqlalchemy import func, or_, select, text

from tallyho.model.states import TERMINAL_THRESHOLD, BatchState, ItemState
from tallyho.storage.counters import read_counters

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.model.views import BatchView
    from tallyho.storage.tables import Tables
    from tests.acceptance.app.domain import DomainTable, DomainTables
    from tests.acceptance.app.site import CatalogTruth

__all__ = [
    "InvariantReport",
    "PurgedBatch",
    "check_i01_terminal_batches",
    "check_i02_no_tails",
    "check_i03_single_finalization",
    "check_i04_exact_domain_effects",
    "check_i05_counter_truth",
    "check_i06_domain_matches_tallyho",
    "check_i07_generator_truth",
    "check_i08_monotonic_snapshots",
    "check_i09_tree_order",
    "check_i10_broker_alignment",
    "check_i11_external_effects",
    "check_i12_exact_after_seal",
    "check_i13_tree_consistency",
    "check_i14_retention",
    "recovery_timeout",
]


@dataclass(frozen=True, slots=True)
class InvariantReport:
    """One machine-readable invariant result with a numeric violation count."""

    invariant: str
    checked: int
    violations: int
    evidence: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """Whether the invariant held for every checked entity."""
        return self.violations == 0


@dataclass(frozen=True, slots=True)
class PurgedBatch:
    """Observable retention outcome retained outside Tallyho storage."""

    batch_id: UUID
    release_required: bool
    released: bool
    retention_elapsed: bool
    domain_intact: bool
    raised_batch_purged: bool


def recovery_timeout(lease_ttl: timedelta, sweep_interval: timedelta) -> timedelta:
    """Return the mandatory quiet period ``lease_ttl + 2*sweep + 30s``."""
    return lease_ttl + 2 * sweep_interval + timedelta(seconds=30)


def _report(code: str, checked: int, evidence: Sequence[str]) -> InvariantReport:
    rows = tuple(evidence)
    return InvariantReport(code, checked, len(rows), rows)


async def check_i01_terminal_batches(
    connection: AsyncConnection, tables: Tables
) -> InvariantReport:
    """I-01: every retained batch is terminal."""
    rows = (
        await connection.execute(
            select(tables.batch.c.id, tables.batch.c.state).where(
                tables.batch.c.state < TERMINAL_THRESHOLD
            )
        )
    ).all()
    evidence = [f"{batch_id}:{state}" for batch_id, state in rows]
    checked = int(await connection.scalar(select(func.count()).select_from(tables.batch)) or 0)
    return _report("I-01", checked, evidence)


async def check_i02_no_tails(connection: AsyncConnection, tables: Tables) -> InvariantReport:
    """I-02: outbox, leases, and counter deltas are empty after recovery."""
    evidence: list[str] = []
    for name, table in (
        ("outbox", tables.outbox),
        ("lease", tables.lease),
        ("counter_delta", tables.counter_delta),
    ):
        count = int(await connection.scalar(select(func.count()).select_from(table)) or 0)
        if count:
            evidence.append(f"{name}={count}")
    return _report("I-02", 3, evidence)


async def check_i03_single_finalization(
    connection: AsyncConnection,
    tables: Tables,
    domain: DomainTables,
    *,
    retry_failed: Mapping[UUID, int] | None = None,
) -> InvariantReport:
    """I-03: each batch has one final hook plus explicit retry_failed runs."""
    expected_extra = retry_failed or {}
    hook = domain.hook_log
    counts = (
        select(hook.c.batch_id, func.count().label("n"))
        .where(hook.c.hook == "on_finalized")
        .group_by(hook.c.batch_id)
        .subquery()
    )
    rows = (
        await connection.execute(
            select(tables.batch.c.id, func.coalesce(counts.c.n, 0))
            .outerjoin(counts, counts.c.batch_id == tables.batch.c.id)
            .where(tables.batch.c.hooks.contains(["finalized"]))
        )
    ).all()
    evidence = [
        f"{batch_id}:finalized={count},expected={1 + expected_extra.get(batch_id, 0)}"
        for batch_id, count in rows
        if int(count) != 1 + expected_extra.get(batch_id, 0)
    ]
    return _report("I-03", len(rows), evidence)


async def _domain_effects(
    connection: AsyncConnection, domain: DomainTables
) -> dict[str, set[UUID]]:
    """Item ids with a domain row, by the short name of the leaf task that writes it."""
    effects: dict[str, set[UUID]] = {}
    for task_name, table in (
        ("render_invoice", domain.invoice_files),
        ("send_email", domain.deliveries),
        ("parse_card", domain.cards),
        ("download_pdf", domain.pdf_files),
    ):
        values = list(await connection.scalars(select(table.c.item_id)))
        effects[task_name] = {cast("UUID", value) for value in values}
    return effects


def _effect_mismatch(
    effects: Mapping[str, set[UUID]], item_id: UUID, *, task_name: str, state: int
) -> bool | None:
    """Whether one Item breaks I-04; ``None`` when its task writes no domain row."""
    short_name = task_name.rsplit(".", 1)[-1]
    if short_name not in effects:
        return None
    present = item_id in effects[short_name]
    return present != (state == int(ItemState.OK))


async def check_i04_exact_domain_effects(
    connection: AsyncConnection, tables: Tables, domain: DomainTables
) -> InvariantReport:
    """I-04: leaf-task effects exist exactly once for OK Items and never otherwise."""
    effects = await _domain_effects(connection, domain)
    rows = (
        await connection.execute(
            select(tables.item.c.id, tables.item.c.task_name, tables.item.c.state)
        )
    ).all()
    evidence: list[str] = []
    checked = 0
    for item_id, task_name, state in rows:
        mismatch = _effect_mismatch(effects, item_id, task_name=str(task_name), state=int(state))
        if mismatch is None:
            continue
        checked += 1
        if mismatch:
            present = item_id in effects[str(task_name).rsplit(".", 1)[-1]]
            evidence.append(f"{item_id}:{task_name}:state={state}:effect={present}")
    return _report("I-04", checked, evidence)


async def check_i05_counter_truth(connection: AsyncConnection, tables: Tables) -> InvariantReport:
    """I-05: folded counters and label metrics equal the Item rows."""
    batch_ids = list(await connection.scalars(select(tables.batch.c.id)))
    counters = await read_counters(connection, tables, batch_ids)
    evidence: list[str] = []
    checked = 0
    for batch_id in batch_ids:
        row = (
            await connection.execute(
                select(
                    func.count(),
                    func.count().filter(tables.item.c.state == int(ItemState.OK)),
                    func.count().filter(tables.item.c.state == int(ItemState.SKIP)),
                    func.count().filter(tables.item.c.state == int(ItemState.ERROR)),
                    func.count().filter(tables.item.c.state == int(ItemState.CANCELLED)),
                ).where(tables.item.c.batch_id == batch_id)
            )
        ).one()
        seen = counters.get(batch_id)
        actual = (int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]))
        observed = (
            (seen.total, seen.ok, seen.skip, seen.error, seen.cancelled)
            if seen is not None
            else (0, 0, 0, 0, 0)
        )
        checked += 1
        if observed != actual:
            evidence.append(f"{batch_id}:counter={observed}:items={actual}")
    label_rows = (
        await connection.execute(
            select(tables.item.c.batch_id, tables.item.c.label, func.count())
            .where(tables.item.c.label.is_not(None))
            .group_by(tables.item.c.batch_id, tables.item.c.label)
        )
    ).all()
    for batch_id, label, count in label_rows:
        metric = int(
            await connection.scalar(
                select(func.coalesce(func.sum(tables.metric.c.value), 0)).where(
                    tables.metric.c.batch_id == batch_id,
                    tables.metric.c.name == label,
                )
            )
            or 0
        )
        checked += 1
        if metric != int(count):
            evidence.append(f"{batch_id}:{label}:metric={metric}:items={count}")
    return _report("I-05", checked, evidence)


async def check_i06_domain_matches_tallyho(
    connection: AsyncConnection,
    roots: Mapping[UUID, tuple[DomainTable, BatchView]],
) -> InvariantReport:
    """I-06: finalized domain totals equal the public final view."""
    evidence: list[str] = []
    for batch_id, (table, view) in roots.items():
        rows = (
            await connection.execute(
                select(table.c.progress_done, table.c.progress_found).where(
                    table.c.batch_id == batch_id
                )
            )
        ).all()
        expected = (view.progress.done, view.progress.found)
        observed = [(int(cast("int", done)), int(cast("int", found))) for done, found in rows]
        if not observed or any(value != expected for value in observed):
            evidence.append(f"{batch_id}:domain={rows}:view={expected}")
    return _report("I-06", len(roots), evidence)


def check_i07_generator_truth(
    s2: BatchView,
    s3: BatchView,
    truth: CatalogTruth,
    *,
    unique_addresses: int,
    duplicate_addresses: int,
) -> InvariantReport:
    """I-07: dynamic fan-out exactly matches mail and catalog generators."""
    send = s2.children["send"].progress
    cards = s3.children["cards"].progress
    pdfs = s3.children["pdfs"].progress
    comparisons = {
        "s2.found": (send.found, unique_addresses),
        "s2.duplicates": (send.duplicates, duplicate_addresses),
        "cards.found": (cards.found, truth.cards),
        "cards.duplicates": (cards.duplicates, truth.card_duplicates),
        "cards.404": (cards.error, truth.card_not_found),
        "pdfs.found": (pdfs.found, truth.pdfs),
        "pdfs.duplicates": (pdfs.duplicates, truth.pdf_duplicates),
        "pdfs.404": (pdfs.error, truth.pdf_not_found),
        "skipped_by_limit": (
            cards.skipped_by_limit + pdfs.skipped_by_limit,
            0,
        ),
    }
    evidence = [
        f"{name}:actual={actual}:expected={expected}"
        for name, (actual, expected) in comparisons.items()
        if actual != expected
    ]
    return _report("I-07", len(comparisons), evidence)


async def check_i08_monotonic_snapshots(
    connection: AsyncConnection, domain: DomainTables
) -> InvariantReport:
    """I-08: progress seq/totals grow and no progress commits after finalization.

    ``hook_log.at`` is the hook transaction's start time. A snapshot transaction that
    starts after the finalization transaction started may still legitimately commit first
    (the finalization CAS then wins), so start time misorders such pairs. When PostgreSQL
    runs with ``track_commit_timestamp=on`` (the compose stand does), rows are ordered by
    the real commit time of ``txid``; otherwise by start time, as before.
    """
    hook = domain.hook_log
    tracked = await connection.scalar(text("SELECT current_setting('track_commit_timestamp')"))
    statement = select(
        hook.c.batch_id,
        hook.c.hook,
        hook.c.seq,
        hook.c.progress_done,
        hook.c.progress_found,
        hook.c.at,
        hook.c.txid,
    )
    if tracked == "on":
        committed = text("pg_xact_commit_timestamp((txid % 4294967296)::text::xid)")
        statement = statement.order_by(hook.c.batch_id, committed, hook.c.txid)
    else:
        statement = statement.order_by(hook.c.batch_id, hook.c.at, hook.c.txid)
    rows = (await connection.execute(statement)).all()
    evidence: list[str] = []
    previous: dict[UUID, tuple[int, int, int]] = {}
    finalized: set[UUID] = set()
    for raw_batch_id, name, raw_seq, raw_done, raw_found, _at, _txid in rows:
        batch_id = cast("UUID", raw_batch_id)
        seq = int(cast("int", raw_seq))
        done = int(cast("int", raw_done))
        found = int(cast("int", raw_found))
        if name == "on_progress":
            old = previous.get(batch_id)
            if batch_id in finalized:
                evidence.append(f"{batch_id}:progress-after-finalized")
            if old is not None and (seq <= old[0] or done < old[1] or found < old[2]):
                evidence.append(f"{batch_id}:non-monotonic={old}->{(seq, done, found)}")
            previous[batch_id] = (seq, done, found)
        elif name == "on_finalized":
            finalized.add(batch_id)
    return _report("I-08", len(rows), evidence)


async def check_i09_tree_order(
    connection: AsyncConnection, tables: Tables, domain: DomainTables
) -> InvariantReport:
    """I-09: fed stages and child final hooks precede their dependants/parents."""
    feeder = tables.batch.alias("feeder")
    fed = tables.batch.alias("fed")
    feed_rows = (
        await connection.execute(
            select(
                tables.feed.c.feeder_id,
                tables.feed.c.fed_id,
                feeder.c.finished_at,
                fed.c.finished_at,
            )
            .join(feeder, feeder.c.id == tables.feed.c.feeder_id)
            .join(fed, fed.c.id == tables.feed.c.fed_id)
        )
    ).all()
    evidence: list[str] = []
    for feeder_id, fed_id, raw_feeder_at, raw_fed_at in feed_rows:
        # An unfinished stage (I-01 reports it after chaos) does not break the order; a stage
        # finished before its feeder, or while the feeder is still unfinished, does.
        feeder_at = cast("datetime | None", raw_feeder_at)
        fed_at = cast("datetime | None", raw_fed_at)
        if fed_at is not None and (feeder_at is None or fed_at < feeder_at):
            evidence.append(f"feed:{feeder_id}->{fed_id}")
    hooks = domain.hook_log.alias("hooks")
    parent_hooks = domain.hook_log.alias("parent_hooks")
    child_rows = (
        await connection.execute(
            select(tables.batch.c.id, tables.batch.c.parent_id, hooks.c.at, parent_hooks.c.at)
            .join(hooks, hooks.c.batch_id == tables.batch.c.id)
            .join(parent_hooks, parent_hooks.c.batch_id == tables.batch.c.parent_id)
            .where(
                tables.batch.c.parent_id.is_not(None),
                hooks.c.hook == "on_finalized",
                parent_hooks.c.hook == "on_finalized",
            )
        )
    ).all()
    evidence.extend(
        f"hook:{child_id}->{parent_id}"
        for child_id, parent_id, child_at, parent_at in child_rows
        if cast("datetime", child_at) > cast("datetime", parent_at)
    )
    return _report("I-09", len(feed_rows) + len(child_rows), evidence)


async def check_i10_broker_alignment(
    connection: AsyncConnection,
    tables: Tables,
    domain: DomainTables,
    *,
    dead_item_ids: Sequence[UUID],
) -> InvariantReport:
    """I-10: an Item whose current-generation job is in the FlexIQ DLQ is settled (D-056).

    ``dead_item_ids`` are Items with a dead job of their current dispatch generation.
    Such an Item is an ``error`` (or ``cancelled``: the DLQ reconciler settles it so when
    its batch is being cancelled, D-051), or it is terminal ``ok``/``skip`` and its domain
    effect obeys I-04: the attempt committed, the broker lost the answer and its
    redeliveries exhausted the retries on claim. The truth about the Item is in Tallyho,
    not in the broker DLQ. An Item still ``active`` (the oracle runs after ``T_rec``), a
    missing Item and an Item whose effect breaks I-04 are violations.
    """
    if not dead_item_ids:
        return _report("I-10", 0, ())
    rows = {
        item_id: (str(task_name), int(state))
        for item_id, task_name, state in (
            await connection.execute(
                select(tables.item.c.id, tables.item.c.task_name, tables.item.c.state).where(
                    tables.item.c.id.in_(tuple(dead_item_ids))
                )
            )
        ).all()
    }
    effects = await _domain_effects(connection, domain)
    evidence: list[str] = []
    for item_id in dead_item_ids:
        row = rows.get(item_id)
        if row is None:
            evidence.append(f"{item_id}:missing")
            continue
        task_name, state = row
        if state < TERMINAL_THRESHOLD:
            evidence.append(f"{item_id}:{task_name}:state={state}:active-with-dead-job")
        elif _effect_mismatch(effects, item_id, task_name=task_name, state=state):
            evidence.append(f"{item_id}:{task_name}:state={state}:effect-breaks-I-04")
    return _report("I-10", len(dead_item_ids), evidence)


async def check_i11_external_effects(
    connection: AsyncConnection,
    tables: Tables,
    *,
    observed_calls: int,
    retry_calls: int = 0,
    killed_after_effect: int = 0,
) -> InvariantReport:
    """I-11: HTTP calls do not exceed executions, retries, and recorded kill windows."""
    external_tasks = ("send_email", "parse_page", "parse_card", "download_pdf")
    row = (
        await connection.execute(
            select(func.count(), func.coalesce(func.sum(tables.item.c.attempt), 0)).where(
                or_(*(tables.item.c.task_name.endswith(task_name) for task_name in external_tasks))
            )
        )
    ).one()
    items = int(row[0])
    attempts = int(row[1])
    allowed = max(items, attempts) + retry_calls + killed_after_effect
    evidence = () if observed_calls <= allowed else (f"calls={observed_calls}:allowed={allowed}",)
    return _report("I-11", items, evidence)


def check_i12_exact_after_seal(views: Sequence[BatchView]) -> InvariantReport:
    """I-12: every closed batch exposes exact expected == found."""
    evidence = [
        f"{view.id}:expected={view.progress.expected}:found={view.progress.found}:estimate={view.progress.expected_is_estimate}"
        for view in views
        if view.state is not BatchState.OPEN
        and (view.progress.expected != view.progress.found or view.progress.expected_is_estimate)
    ]
    return _report("I-12", len(views), evidence)


async def check_i13_tree_consistency(
    connection: AsyncConnection, tables: Tables
) -> InvariantReport:
    """I-13: no open fed stage has only terminal feeders; cancelled batches have no active Items."""
    feeder = tables.batch.alias("feeder")
    fed = tables.batch.alias("fed")
    open_rows = (
        await connection.execute(
            select(fed.c.id)
            .join(tables.feed, tables.feed.c.fed_id == fed.c.id)
            .join(feeder, feeder.c.id == tables.feed.c.feeder_id)
            .where(fed.c.state == int(BatchState.OPEN))
            .group_by(fed.c.id)
            .having(func.bool_and(feeder.c.state >= TERMINAL_THRESHOLD))
        )
    ).all()
    active_cancelled = (
        await connection.execute(
            select(tables.item.c.id)
            .join(tables.batch, tables.batch.c.id == tables.item.c.batch_id)
            .where(
                tables.batch.c.state == int(BatchState.CANCELLED),
                tables.item.c.state == int(ItemState.ACTIVE),
            )
        )
    ).all()
    evidence = [f"open-fed:{row[0]}" for row in open_rows]
    evidence.extend(f"active-in-cancelled:{row[0]}" for row in active_cancelled)
    return _report("I-13", len(open_rows) + len(active_cancelled), evidence)


def check_i14_retention(records: Sequence[PurgedBatch]) -> InvariantReport:
    """I-14: purge requires elapsed retention/release and preserves domain totals."""
    evidence: list[str] = []
    for record in records:
        if not record.domain_intact:
            evidence.append(f"{record.batch_id}:domain-lost")
        if record.raised_batch_purged and not record.retention_elapsed:
            evidence.append(f"{record.batch_id}:purged-before-retention")
        if record.raised_batch_purged and record.release_required and not record.released:
            evidence.append(f"{record.batch_id}:purged-before-release")
    return _report("I-14", len(records), evidence)
