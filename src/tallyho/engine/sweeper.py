"""Короткие восстановительные проходы Sweeper (ARCHITECTURE UC-15)."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from itertools import starmap
from typing import TYPE_CHECKING, Protocol, TypeVar, cast

from sqlalchemy import (
    DateTime,
    SmallInteger,
    Uuid,
    delete,
    func,
    literal,
    literal_column,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tallyho.engine.operations import Operations
from tallyho.engine.relay import release_window
from tallyho.engine.retry_limits import effective_max_retries
from tallyho.model.errors import ConfigurationError, InvalidStateError
from tallyho.model.states import (
    TERMINAL_THRESHOLD,
    BatchState,
    CancelReason,
    ItemState,
    OnFeederFailed,
    OutboxKind,
)
from tallyho.protocols.observer import NullObserver
from tallyho.storage.attributes import delete_batch_attributes
from tallyho.storage.counters import (
    CounterDelta,
    fold_delta_ids,
    reconcile,
    upsert_metrics,
    upsert_slots,
)
from tallyho.storage.now import sql_now
from tallyho.storage.tx import RetryPolicy, TxSettings, run_transaction

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.protocols.broker import RetryLimits
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.tables import Tables

__all__ = ["FinishRow", "SweepResult", "Sweeper", "SweeperSettings", "finish_active"]

_ACTIVE = literal_column(str(int(ItemState.ACTIVE)), SmallInteger())
_ITEM = literal_column(str(int(OutboxKind.ITEM)), SmallInteger())
_INFINITY = literal_column("'infinity'::timestamptz", DateTime(timezone=True))
_POSITIVE_BATCH = "sweeper batch_size должен быть положительным"
_POSITIVE_GRACE = "finalize_grace должен быть неотрицательным"
_NO_DATABASE_TIME = "БД не вернула текущее время"
_PARENT_CYCLE = "цикл parent_id в дереве батчей"

T = TypeVar("T")
_log = logging.getLogger(__name__)


def _pending(tables: Tables, batch_id: ColumnElement[UUID]) -> ColumnElement[int]:
    """Точный pending из слотов и ещё не свёрнутых дельт одним выражением.

    Returns:
        Коррелированное SQL-выражение для числа незавершённых Items.
    """
    counter = tables.counter
    delta = tables.counter_delta
    stored = (
        select(
            func.coalesce(
                func.sum(
                    counter.c.total
                    - counter.c.ok
                    - counter.c.skip
                    - counter.c.error
                    - counter.c.cancelled
                ),
                0,
            )
        )
        .where(counter.c.batch_id == batch_id)
        .scalar_subquery()
    )
    pending_delta = (
        select(
            func.coalesce(
                func.sum(
                    delta.c.d_total
                    - delta.c.d_ok
                    - delta.c.d_skip
                    - delta.c.d_error
                    - delta.c.d_cancelled
                ),
                0,
            )
        )
        .where(delta.c.batch_id == batch_id)
        .scalar_subquery()
    )
    return stored + pending_delta


class _Relay(Protocol):
    def kick(self, batch_ids: Iterable[UUID]) -> None: ...


class _Finalizer(Protocol):
    async def try_finalize(self, batch_id: UUID) -> bool: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class SweeperSettings:
    """Размер прохода, grace-периоды и транзакционные ретраи."""

    batch_size: int = 1000
    slot: int = 0
    finalize_grace: timedelta = timedelta(seconds=30)
    hook_backoff_max: timedelta = timedelta(minutes=5)
    lease_ttl: timedelta = timedelta(seconds=60)
    tx: TxSettings = field(default_factory=TxSettings)
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        """Проверить границы настроек.

        Raises:
            ConfigurationError: Значение настройки недопустимо.
        """
        if self.batch_size <= 0:
            raise ConfigurationError(_POSITIVE_BATCH)
        if self.slot < 0:
            message = "sweeper slot должен быть >= 0"
            raise ConfigurationError(message)
        if (
            self.finalize_grace < timedelta(0)
            or self.hook_backoff_max <= timedelta(0)
            or self.lease_ttl <= timedelta(0)
        ):
            raise ConfigurationError(_POSITIVE_GRACE)


@dataclass(frozen=True, slots=True)
class SweepResult:
    """Число исправлений каждого вида за один полный цикл."""

    leases: int = 0
    finalized: int = 0
    deadlines: int = 0
    stages: int = 0
    reconciled: int = 0
    deltas: int = 0
    expired: int = 0
    retained: int = 0


@dataclass(frozen=True, slots=True)
class _LeaseRow:
    item_id: UUID
    batch_id: UUID
    item_state: ItemState
    attempt: int
    task_name: str
    payload: bytes
    options: dict[str, object]
    weight: int
    paused: bool
    cancelled: bool
    start_at: datetime | None


@dataclass(frozen=True, slots=True)
class FinishRow:
    """Item, который восстановительный проход завершает сам, без задачи."""

    item_id: UUID
    batch_id: UUID
    weight: int


async def finish_active(  # ruff: ignore[too-many-arguments]  # итог и учёт Item задаются именованными параметрами
    conn: AsyncConnection,
    tables: Tables,
    rows: Sequence[FinishRow],
    *,
    slot: int,
    state: ItemState,
    label: str,
    now: datetime,
    errors: Mapping[UUID, object] | None = None,
) -> tuple[int, tuple[UUID, ...]]:
    """Завершить активные Items идемпотентным CAS и учесть только изменённые.

    Общий путь восстановительных проходов: истёкший lease и срок ``expires``
    (Sweeper), мёртвая джоба (сверка с DLQ). Вызывающий держит блокировки
    строк ``th_item``.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        rows: Кандидаты; уже терминальные Items пропускаются.
        slot: Слот ``th_counter`` и ``th_metric``.
        state: Терминальное состояние.
        label: Метка итога.
        now: Время завершения.
        errors: ``th_item.error`` по id Item; ``None`` — колонку не менять.

    Returns:
        Число завершённых Items и батчи, в окне которых освободились места.
    """
    if not rows:
        return 0, ()
    item = tables.item
    item_ids = [row.item_id for row in rows]
    values: dict[str, object] = {"state": int(state), "label": label, "finished_at": now}
    statement = update(item).where(item.c.state == _ACTIVE)
    if errors is None:
        statement = statement.where(item.c.id.in_(item_ids))
    else:
        details = (
            func.unnest(
                literal(item_ids, ARRAY(Uuid())),
                literal([errors.get(item_id) for item_id in item_ids], ARRAY(JSONB())),
            )
            .table_valued("id", "error")
            .render_derived("u")
        )
        statement = statement.where(item.c.id == details.c.id)
        values["error"] = details.c.error
    changed_result = await conn.execute(
        statement.values(**values).returning(item.c.id, item.c.batch_id, item.c.weight)
    )
    changed = list(starmap(FinishRow, cast("Iterable[tuple[UUID, UUID, int]]", changed_result)))
    changed_ids = [row.item_id for row in changed]
    if not changed:
        return 0, ()
    for side in (tables.lease, tables.expiry):
        _ = await conn.execute(delete(side).where(side.c.item_id.in_(changed_ids)))
    deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
    metrics: defaultdict[tuple[UUID, str, int], int] = defaultdict(int)
    counter_name = state.name.lower()
    for row in changed:
        deltas[row.batch_id] += CounterDelta(**{counter_name: 1, "w_done": row.weight})
        metrics[row.batch_id, label, slot] += 1
    await upsert_slots(
        conn,
        tables,
        {(batch_id, slot): delta for batch_id, delta in deltas.items()},
    )
    await upsert_metrics(conn, tables, metrics)
    if state is ItemState.ERROR:
        mark = tables.item_mark
        mark_stmt = pg_insert(mark).values(
            [{"batch_id": row.batch_id, "label": label, "item_id": row.item_id} for row in changed]
        )
        _ = await conn.execute(
            mark_stmt.on_conflict_do_nothing(
                index_elements=[mark.c.batch_id, mark.c.label, mark.c.item_id]
            )
        )
    kick = tuple(await release_window(conn, tables, changed_ids))
    return len(changed), kick


@dataclass(eq=False, kw_only=True)
class Sweeper:
    """Восстановить пропущенную работу небольшими независимыми транзакциями."""

    tables: Tables
    engine: AsyncEngine
    clock: Clock
    finalizer: _Finalizer
    relay: _Relay | None = None
    limits: RetryLimits | None = None
    """Умолчания ``max_retries`` задач у адаптера; без него умолчание — 0."""
    settings: SweeperSettings = field(default_factory=SweeperSettings)
    observer: Observer = field(default_factory=NullObserver)

    async def sweep(self) -> SweepResult:
        """Выполнить по одному ограниченному проходу каждого вида.

        Returns:
            Сводка числа исправлений.
        """
        self._notify_oldest_lease(await self._run(self._oldest_lease_age))
        leases = await self.expire_leases()
        finalized = await self.finalize_stuck()
        deadlines = await self.enforce_deadlines()
        stages = await self.seal_orphan_stages()
        reconciled = await self.reconcile_drift()
        deltas = await self.fold_stale_deltas()
        expired = await self.expire_unclaimed()
        retained = await self.retention()
        return SweepResult(
            leases=leases,
            finalized=finalized,
            deadlines=deadlines,
            stages=stages,
            reconciled=reconciled,
            deltas=deltas,
            expired=expired,
            retained=retained,
        )

    async def expire_leases(self) -> int:
        """Переотправить Items с истёкшим lease или завершить исчерпанные.

        Returns:
            Число обработанных lease.
        """
        changed, kick, finalize = await self._run(self._expire_leases_in)
        self._kick(kick)
        await self._finalize(finalize)
        return changed

    async def finalize_stuck(self) -> int:
        """Повторить пропущенную или упавшую финализацию с backoff.

        Returns:
            Число успешно финализированных батчей.
        """
        candidates = await self._run(self._finalize_candidates)
        changed = 0
        for batch_id in candidates:
            changed += int(await self.finalizer.try_finalize(batch_id))
        return changed

    async def enforce_deadlines(self) -> int:
        """Поставить просроченным батчам и их поддеревьям запрос отмены ``deadline``.

        Батчи, у которых флаг уже есть, сохраняют свою причину (§6.1).

        Returns:
            Число немедленно отменённых Items.
        """
        candidates = await self._run(self._deadline_candidates)
        changed = 0
        operations = Operations(tables=self.tables, clock=self.clock, slot=self.settings.slot)
        for batch_id in candidates:
            changed += await self._run(
                partial(operations.cancel, batch_id=batch_id, reason=CancelReason.DEADLINE)
            )
            _ = await self.finalizer.try_finalize(batch_id)
        return changed

    async def seal_orphan_stages(self) -> int:
        """Закрыть open-этапы, все источники которых терминальны.

        Returns:
            Число закрытых или отменённых этапов.
        """
        changed, finalize = await self._run(self._seal_orphans_in)
        await self._finalize(finalize)
        return changed

    async def reconcile_drift(self) -> int:
        """Пересчитать подозрительные sealed-батчи по фактическим Items.

        Returns:
            Число проверенных батчей.
        """
        candidates = await self._run(self._drift_candidates)
        changed = 0
        for batch_id in candidates:
            found = await self._run(partial(reconcile, tables=self.tables, batch_id=batch_id))
            if found is not None:
                changed += 1
                _ = await self.finalizer.try_finalize(batch_id)
        return changed

    async def fold_stale_deltas(self) -> int:
        """Свернуть append-only дельты старше ``finalize_grace``.

        Returns:
            Число свёрнутых строк дельт.
        """
        return await self._run(self._fold_deltas_in)

    async def expire_unclaimed(self) -> int:
        """Завершить как ``expired`` отправленные, но не захваченные Items.

        Returns:
            Число завершённых Items.
        """
        changed, kick, finalize = await self._run(self._expire_unclaimed_in)
        self._kick(kick)
        await self._finalize(finalize)
        return changed

    async def retention(self) -> int:
        """Удалить один истёкший терминальный корень вместе с деревом.

        Returns:
            Число полностью удалённых деревьев (0 или 1).
        """
        root_id = await self._run(self._retention_candidate)
        if root_id is None:
            return 0
        while await self._run(partial(self._purge_items_in, root_id=root_id)):
            pass
        while await self._run(partial(self._purge_batches_in, root_id=root_id)):
            pass
        return 1

    async def _run(self, work: Callable[[AsyncConnection], Awaitable[T]]) -> T:
        return await run_transaction(
            self.engine,
            work,
            settings=self.settings.tx,
            policy=self.settings.retry,
        )

    async def _oldest_lease_age(self, conn: AsyncConnection) -> float:
        lease_until = await conn.scalar(select(func.min(self.tables.lease.c.lease_until)))
        if lease_until is None:
            return 0.0
        now = await self._now(conn)
        acquired_at = lease_until - self.settings.lease_ttl
        return float(max(0.0, (now - acquired_at).total_seconds()))

    def _notify_oldest_lease(self, seconds: float) -> None:
        try:
            self.observer.oldest_lease(seconds=seconds)
        except Exception:  # ruff: ignore[blind-except]  # observer must not affect recovery
            _log.exception("Observer.oldest_lease failed")

    async def _expire_leases_in(  # ruff: ignore[too-many-locals]  # один атомарный проход классифицирует lease по четырём исходам
        self, conn: AsyncConnection
    ) -> tuple[int, tuple[UUID, ...], tuple[UUID, ...]]:
        now = await self._now(conn)
        lease = self.tables.lease
        candidate_ids = list(
            await conn.scalars(
                select(lease.c.item_id)
                .where(lease.c.lease_until < now)
                .order_by(lease.c.lease_until, lease.c.item_id)
                .limit(self.settings.batch_size)
            )
        )
        rows = await self._lock_lease_rows(conn, candidate_ids)
        requeue: list[_LeaseRow] = []
        finishes: list[FinishRow] = []
        terminal: list[UUID] = []
        cancelled: list[FinishRow] = []
        for row in rows:
            if row.item_state.is_terminal:
                terminal.append(row.item_id)
            elif row.cancelled:
                cancelled.append(FinishRow(row.item_id, row.batch_id, row.weight))
            elif row.attempt < self._max_retries(row):
                requeue.append(row)
            else:
                finishes.append(FinishRow(row.item_id, row.batch_id, row.weight))
        handled_ids = [row.item_id for row in requeue] + terminal
        if handled_ids:
            _ = await conn.execute(delete(lease).where(lease.c.item_id.in_(handled_ids)))
        if requeue:
            await self._requeue(conn, requeue, now)
            # Истёкший lease тратит попытку, как перехват lease в claim (UC-15).
            # Возврат в outbox — новое поколение отправки: мёртвую джобу
            # прошлой отправки сверка с DLQ к Item уже не отнесёт.
            item = self.tables.item
            _ = await conn.execute(
                update(item)
                .where(item.c.id.in_([row.item_id for row in requeue]))
                .values(attempt=item.c.attempt + 1, generation=item.c.generation + 1)
            )
            deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
            for row in requeue:
                deltas[row.batch_id] += CounterDelta(dispatched=-1)
            await upsert_slots(
                conn,
                self.tables,
                {(batch_id, self.settings.slot): delta for batch_id, delta in deltas.items()},
            )
        changed_error, kick_error = await self._finish_rows(
            conn, finishes, state=ItemState.ERROR, label="lease_expired", now=now
        )
        changed_cancel, kick_cancel = await self._finish_rows(
            conn, cancelled, state=ItemState.CANCELLED, label="cancelled", now=now
        )
        terminal_kick = await release_window(conn, self.tables, terminal)
        kick = tuple(
            sorted(
                {row.batch_id for row in requeue}
                | set(kick_error)
                | set(kick_cancel)
                | set(terminal_kick)
            )
        )
        finalize = tuple(sorted({row.batch_id for row in finishes + cancelled}))
        return len(terminal) + len(requeue) + changed_error + changed_cancel, kick, finalize

    async def _lock_lease_rows(
        self, conn: AsyncConnection, item_ids: Sequence[UUID]
    ) -> list[_LeaseRow]:
        if not item_ids:
            return []
        batch = self.tables.batch
        item = self.tables.item
        lease = self.tables.lease
        result = await conn.execute(
            select(
                item.c.id,
                item.c.batch_id,
                item.c.state,
                item.c.attempt,
                item.c.task_name,
                item.c.payload,
                item.c.options,
                item.c.weight,
                batch.c.paused_at,
                batch.c.cancel_requested_at,
                batch.c.start_at,
            )
            .join(batch, batch.c.id == item.c.batch_id)
            .join(lease, lease.c.item_id == item.c.id)
            .where(item.c.id.in_(item_ids), lease.c.lease_until < sql_now(self.clock))
            .order_by(batch.c.id, item.c.id)
            .with_for_update(of=(batch, item, lease), skip_locked=True)
        )
        return [self._lease_row(row) for row in result]

    async def _requeue(
        self, conn: AsyncConnection, rows: Sequence[_LeaseRow], now: datetime
    ) -> None:
        outbox = self.tables.outbox
        stmt = pg_insert(outbox).values(
            [
                {
                    "id": row.item_id,
                    "kind": int(OutboxKind.ITEM),
                    "batch_id": row.batch_id,
                    "item_id": row.item_id,
                    "task_name": row.task_name,
                    "payload": row.payload,
                    "options": row.options,
                    "available_at": row.start_at or now,
                }
                for row in rows
            ]
        )
        _ = await conn.execute(stmt.on_conflict_do_nothing(index_elements=[outbox.c.id]))
        paused = [row.item_id for row in rows if row.paused]
        if paused:
            _ = await conn.execute(
                update(outbox).where(outbox.c.item_id.in_(paused)).values(available_at=_INFINITY)
            )

    async def _finish_rows(
        self,
        conn: AsyncConnection,
        rows: Sequence[FinishRow],
        *,
        state: ItemState,
        label: str,
        now: datetime,
    ) -> tuple[int, tuple[UUID, ...]]:
        return await finish_active(
            conn, self.tables, rows, slot=self.settings.slot, state=state, label=label, now=now
        )

    async def _finalize_candidates(self, conn: AsyncConnection) -> tuple[UUID, ...]:
        now = await self._now(conn)
        batch = self.tables.batch
        result = await conn.execute(
            select(
                batch.c.id,
                batch.c.updated_at,
                batch.c.hook_attempts,
                batch.c.hook_error,
            )
            .where(
                or_(
                    batch.c.state == int(BatchState.SEALED),
                    batch.c.cancel_requested_at.is_not(None),
                ),
                _pending(self.tables, batch.c.id) == 0,
            )
            .order_by(batch.c.updated_at, batch.c.id)
            .limit(self.settings.batch_size)
        )
        candidates: list[UUID] = []
        for row in result:
            values = tuple(cast("Iterable[object]", row))
            batch_id = cast("UUID", values[0])
            updated_at = cast("datetime", values[1])
            attempts = cast("int", values[2])
            hook_error = cast("str | None", values[3])
            delay = self.settings.finalize_grace
            if hook_error is not None:
                seconds = min(
                    self.settings.hook_backoff_max.total_seconds(), 2.0 ** max(0, attempts - 1)
                )
                delay = timedelta(seconds=seconds)
            if updated_at + delay <= now:
                candidates.append(batch_id)
        return tuple(candidates)

    async def _deadline_candidates(self, conn: AsyncConnection) -> tuple[UUID, ...]:
        batch = self.tables.batch
        now = await self._now(conn)
        result = await conn.scalars(
            select(batch.c.id)
            # Дедлайн есть у любого узла (§6.1): под-батч отменяется со своим поддеревом.
            .where(
                batch.c.state < TERMINAL_THRESHOLD,
                batch.c.cancel_requested_at.is_(None),
                batch.c.deadline_at.is_not(None),
                batch.c.deadline_at <= now,
            )
            .order_by(batch.c.deadline_at, batch.c.id)
            .limit(self.settings.batch_size)
            .with_for_update(skip_locked=True)
        )
        return tuple(result)

    async def _seal_orphans_in(  # ruff: ignore[too-many-locals]  # условия feed и два исхода считаются в одной транзакции
        self, conn: AsyncConnection
    ) -> tuple[int, tuple[UUID, ...]]:
        batch = self.tables.batch
        feed = self.tables.feed
        feeder = batch.alias("feeder")
        has_feed = select(feed.c.feeder_id).where(feed.c.fed_id == batch.c.id).exists()
        live_feed = (
            select(feed.c.feeder_id)
            .join(feeder, feeder.c.id == feed.c.feeder_id)
            .where(feed.c.fed_id == batch.c.id, feeder.c.state < TERMINAL_THRESHOLD)
            .exists()
        )
        failed_feed = (
            select(feed.c.feeder_id)
            .join(feeder, feeder.c.id == feed.c.feeder_id)
            .where(
                feed.c.fed_id == batch.c.id,
                feeder.c.state.in_([int(BatchState.FAILED), int(BatchState.CANCELLED)]),
            )
            .exists()
        )
        rows = list(
            await conn.execute(
                select(batch.c.id, batch.c.on_feeder_failed, failed_feed.label("failed"))
                .where(batch.c.state == int(BatchState.OPEN), has_feed, ~live_feed)
                .order_by(batch.c.id)
                .limit(self.settings.batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        if not rows:
            return 0, ()
        now = await self._now(conn)
        sealed: list[UUID] = []
        cancelled: list[UUID] = []
        for row in rows:
            values = tuple(cast("Iterable[object]", row))
            batch_id = cast("UUID", values[0])
            mode = OnFeederFailed(cast("int", values[1]))
            failed = cast("bool", values[2])
            (cancelled if failed and mode is OnFeederFailed.CANCEL else sealed).append(batch_id)
        if sealed:
            _ = await conn.execute(
                update(batch)
                .where(batch.c.id.in_(sealed), batch.c.state == int(BatchState.OPEN))
                .values(state=int(BatchState.SEALED), updated_at=now)
            )
        if cancelled:
            operations = Operations(tables=self.tables, clock=self.clock, slot=self.settings.slot)
            for batch_id in cancelled:
                _ = await operations.cancel(conn, batch_id, reason=CancelReason.CANCEL)
        ids = tuple(sorted([*sealed, *cancelled]))
        return len(ids), ids

    async def _drift_candidates(self, conn: AsyncConnection) -> tuple[UUID, ...]:
        batch = self.tables.batch
        lease = self.tables.lease
        outbox = self.tables.outbox
        has_lease = select(lease.c.item_id).where(lease.c.batch_id == batch.c.id).exists()
        has_outbox = (
            select(outbox.c.id)
            .where(outbox.c.batch_id == batch.c.id, outbox.c.kind == _ITEM)
            .exists()
        )
        result = await conn.scalars(
            select(batch.c.id)
            .where(
                batch.c.state == int(BatchState.SEALED),
                batch.c.updated_at <= sql_now(self.clock) - self.settings.finalize_grace,
                _pending(self.tables, batch.c.id) > 0,
                ~has_lease,
                ~has_outbox,
            )
            .order_by(batch.c.updated_at, batch.c.id)
            .limit(self.settings.batch_size)
        )
        return tuple(result)

    async def _fold_deltas_in(self, conn: AsyncConnection) -> int:
        delta = self.tables.counter_delta
        now = await self._now(conn)
        ids = list(
            await conn.scalars(
                select(delta.c.id)
                .where(delta.c.created_at <= now - self.settings.finalize_grace)
                .order_by(delta.c.created_at, delta.c.id)
                .limit(self.settings.batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        folded = await fold_delta_ids(conn, self.tables, ids)
        await upsert_slots(
            conn,
            self.tables,
            {(batch_id, self.settings.slot): value for batch_id, value in folded.items()},
        )
        return len(ids)

    async def _expire_unclaimed_in(
        self, conn: AsyncConnection
    ) -> tuple[int, tuple[UUID, ...], tuple[UUID, ...]]:
        expiry = self.tables.expiry
        item = self.tables.item
        lease = self.tables.lease
        now = await self._now(conn)
        rows = list(
            await conn.execute(
                select(item.c.id, item.c.batch_id, item.c.weight)
                .join(expiry, expiry.c.item_id == item.c.id)
                .outerjoin(lease, lease.c.item_id == item.c.id)
                .where(
                    expiry.c.expires_at <= now,
                    item.c.state == _ACTIVE,
                    lease.c.item_id.is_(None),
                )
                .order_by(expiry.c.expires_at, item.c.id)
                .limit(self.settings.batch_size)
                .with_for_update(of=(item, expiry), skip_locked=True)
            )
        )
        candidates = [self._finish_row(row) for row in rows]
        changed, kick = await self._finish_rows(
            conn, candidates, state=ItemState.ERROR, label="expired", now=now
        )
        finalize = tuple(sorted({row.batch_id for row in candidates}))
        return changed, kick, finalize

    async def _retention_candidate(self, conn: AsyncConnection) -> UUID | None:
        batch = self.tables.batch
        now = await self._now(conn)
        return await conn.scalar(
            select(batch.c.id)
            .where(
                batch.c.id == batch.c.root_id,
                batch.c.state >= TERMINAL_THRESHOLD,
                batch.c.finished_at.is_not(None),
                batch.c.retention.is_not(None),
                batch.c.finished_at + batch.c.retention <= now,
                or_(~batch.c.release_required, batch.c.released_at.is_not(None)),
            )
            .order_by(batch.c.finished_at, batch.c.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )

    async def _purge_items_in(self, conn: AsyncConnection, *, root_id: UUID) -> int:
        batch = self.tables.batch
        item = self.tables.item
        item_ids = list(
            await conn.scalars(
                select(item.c.id)
                .where(item.c.batch_id.in_(select(batch.c.id).where(batch.c.root_id == root_id)))
                .order_by(item.c.id)
                .limit(self.settings.batch_size)
                .with_for_update()
            )
        )
        if not item_ids:
            return 0
        for side in (
            self.tables.lease,
            self.tables.expiry,
            self.tables.window,
            self.tables.outbox,
            self.tables.item_mark,
        ):
            _ = await conn.execute(delete(side).where(side.c.item_id.in_(item_ids)))
        _ = await conn.execute(delete(item).where(item.c.id.in_(item_ids)))
        return len(item_ids)

    async def _purge_batches_in(self, conn: AsyncConnection, *, root_id: UUID) -> int:
        batch = self.tables.batch
        rows = list(
            await conn.execute(
                select(batch.c.id, batch.c.parent_id)
                .where(batch.c.root_id == root_id)
                .order_by(batch.c.id)
                .with_for_update()
            )
        )
        parents = {
            cast("UUID", values[0]): cast("UUID | None", values[1])
            for row in rows
            if (values := tuple(cast("Iterable[object]", row)))
        }
        remaining = set(parents)
        if not remaining:
            return 0
        leaves = [
            value for value in remaining if not any(parents[other] == value for other in remaining)
        ][: self.settings.batch_size]
        if not leaves:
            raise InvalidStateError(_PARENT_CYCLE)
        for table in (
            self.tables.counter_delta,
            self.tables.counter,
            self.tables.metric,
            self.tables.outbox,
        ):
            _ = await conn.execute(delete(table).where(table.c.batch_id.in_(leaves)))
        _ = await conn.execute(
            delete(self.tables.feed).where(
                or_(
                    self.tables.feed.c.feeder_id.in_(leaves),
                    self.tables.feed.c.fed_id.in_(leaves),
                )
            )
        )
        await delete_batch_attributes(conn, self.tables, leaves)
        _ = await conn.execute(delete(batch).where(batch.c.id.in_(leaves)))
        return len(leaves)

    async def _now(self, conn: AsyncConnection) -> datetime:
        value: datetime | None = await conn.scalar(select(sql_now(self.clock)))
        if value is None:
            raise InvalidStateError(_NO_DATABASE_TIME)
        return value

    async def _finalize(self, batch_ids: Iterable[UUID]) -> None:
        for batch_id in sorted(set(batch_ids)):
            _ = await self.finalizer.try_finalize(batch_id)

    def _kick(self, batch_ids: Iterable[UUID]) -> None:
        ids = tuple(sorted(set(batch_ids)))
        if self.relay is not None and ids:
            self.relay.kick(ids)

    def _max_retries(self, row: _LeaseRow) -> int:
        # Тот же лимит, с которым relay ставит задачу в брокер (D-012): опция
        # вызова, иначе умолчание задачи, известное только адаптеру.
        return effective_max_retries(row.options, row.task_name, self.limits)

    @staticmethod
    def _lease_row(row: object) -> _LeaseRow:
        values = tuple(cast("Iterable[object]", row))
        options = values[6]
        raw = cast("dict[object, object]", options) if isinstance(options, dict) else {}
        return _LeaseRow(
            item_id=cast("UUID", values[0]),
            batch_id=cast("UUID", values[1]),
            item_state=ItemState(cast("int", values[2])),
            attempt=cast("int", values[3]),
            task_name=cast("str", values[4]),
            payload=cast("bytes", values[5]),
            options={key: value for key, value in raw.items() if isinstance(key, str)},
            weight=cast("int", values[7]),
            paused=values[8] is not None,
            cancelled=values[9] is not None,
            start_at=cast("datetime | None", values[10]),
        )

    @staticmethod
    def _finish_row(row: object) -> FinishRow:
        values = tuple(cast("Iterable[object]", row))
        return FinishRow(
            item_id=cast("UUID", values[0]),
            batch_id=cast("UUID", values[1]),
            weight=cast("int", values[2]),
        )
