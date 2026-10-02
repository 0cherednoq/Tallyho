"""Финализация батчей: tx-хук, CAS, колбэки и каскад дерева.

ARCHITECTURE §6.1, §7.3, §7.5 и §8.1. Финализация выполняется в собственной
транзакции. Пользовательский хук вызывается до CAS: проигравшая конкурентная
попытка откатывает и доменные изменения хука вместе с транзакцией tallyho.

Порядок блокировок в транзакции (§9.2): доменные строки хука → ``th_batch``
(сам батч и этапы, которые он наполняет, одним ``FOR UPDATE`` по id) →
``th_outbox`` → ``th_item`` (виртуальный Item родителя) → ``th_counter`` →
``th_metric``. Тот же порядок у Completer и операций над поддеревом.

Итог выбирается дважды: без блокировок — чтобы показать его хуку, и под
блокировкой строки батча — по ней и по счётчикам, прочитанным заново. Запись
делается только если оба результата совпали: запрос отмены, дедлайн, политика
или ``retry_failed`` потомка, закоммиченные между чтениями, повторяют попытку
или откладывают финализацию.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final, Protocol, cast

from sqlalchemy import SmallInteger, func, insert, literal_column, select, update

from tallyho.engine.producer import CallbackName, StoredCallback
from tallyho.model.errors import ConfigurationError, HookMissingError, TallyhoError
from tallyho.model.policy import FailurePolicy, PolicyAction
from tallyho.model.progress import NodeCounters, compute_progress
from tallyho.model.states import (
    TERMINAL_THRESHOLD,
    BatchState,
    CancelReason,
    ItemState,
    OnFeederFailed,
    OutboxKind,
)
from tallyho.model.views import BatchSummary
from tallyho.protocols.observer import NullObserver
from tallyho.storage.attributes import read_batch_attributes
from tallyho.storage.counters import (
    CounterDelta,
    read_counters,
    upsert_metrics,
    upsert_slots,
)
from tallyho.storage.now import sql_now
from tallyho.storage.tx import (
    RetryPolicy,
    TxSettings,
    hook_session,
    own_transaction,
    run_transaction,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql.elements import ColumnElement

    from tallyho.hooks.registry import HookRegistry
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.observer import Observer
    from tallyho.storage.counters import CounterTotals
    from tallyho.storage.tables import Tables

__all__ = ["Finalizer", "FinalizerSettings"]

_log = logging.getLogger(__name__)

_OPEN = literal_column(str(int(BatchState.OPEN)), SmallInteger())
_SEALED = literal_column(str(int(BatchState.SEALED)), SmallInteger())
_ACTIVE = literal_column(str(int(ItemState.ACTIVE)), SmallInteger())
_OK = literal_column(str(int(ItemState.OK)), SmallInteger())
_HOOK_FAILED = "on_finalized"
_SNAPSHOT_RETRIES: Final = 5


class _Relay(Protocol):
    def kick(self, batch_ids: Iterable[UUID]) -> None:
        """Подтолкнуть Relay после commit callback-записей."""
        ...


class _Progress(Protocol):
    async def notify(self, batch_ids: Iterable[UUID], *, final: bool = False) -> int:
        """Опубликовать изменившиеся батчи для ``watch()``."""
        ...


class _RollbackError(TallyhoError):
    """Внутренний сигнал: транзакцию нужно откатить без ошибки наружу."""


class _CasLostError(_RollbackError):
    """Другой процесс уже финализировал батч."""


class _SnapshotChangedError(_RollbackError):
    """Снимок прогресса изменил ``snap_seq`` между чтением и CAS."""


class _HookCallError(_RollbackError):
    """Пользовательский ``on_finalized`` завершился исключением."""

    error: Exception
    batch_id: UUID
    kind: str

    def __init__(self, *, error: Exception, batch_id: UUID, kind: str) -> None:
        super().__init__(str(error))
        self.error = error
        self.batch_id = batch_id
        self.kind = kind


@dataclass(frozen=True, slots=True, kw_only=True)
class FinalizerSettings:
    """Настройки Finalizer.

    ``slot`` — отдельный слот для завершения виртуальных Items родителей.
    ``hook_timeout`` ограничивает и Python-хук, и SQL statement_timeout.
    """

    slot: int = 0
    hook_timeout: timedelta = timedelta(seconds=10)
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        """Проверить диапазоны настроек.

        Raises:
            ConfigurationError: слот отрицательный или таймаут неположительный.
        """
        if self.slot < 0:
            message = f"slot finalizer должен быть >= 0, получено {self.slot}"
            raise ConfigurationError(message)
        if self.hook_timeout <= timedelta(0):
            message = "hook_timeout должен быть положительным"
            raise ConfigurationError(message)


@dataclass(frozen=True, slots=True)
class _Batch:
    id: UUID
    root_id: UUID
    parent_id: UUID | None
    parent_item_id: UUID | None
    kind: str
    key: str | None
    state: BatchState
    cancel_requested_at: datetime | None
    cancel_reason: CancelReason | None
    options: Mapping[str, object]
    hooks: tuple[str, ...]
    expected_total: int | None
    on_feeder_failed: OnFeederFailed
    snap_seq: int
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class _Stage:
    """Этап, который наполняет финализируемый батч; строка уже под ``FOR UPDATE``."""

    id: UUID
    state: BatchState
    on_feeder_failed: OnFeederFailed


@dataclass(frozen=True, slots=True)
class _Verdict:
    """Итог готового к финализации батча: ``summary.state`` и ``summary.reason`` хука.

    Счётчики в сравнение не входят: при ``pending = 0`` у незавершённого батча
    меняется только ``dispatched`` (relay пишет его после отправки), а на итог
    он не влияет.
    """

    state: BatchState
    reason: CancelReason | None


@dataclass(frozen=True, slots=True)
class _Committed:
    batch_id: UUID
    kind: str
    state: BatchState
    parent_id: UUID | None
    cascade: tuple[UUID, ...]
    callbacks: bool


@dataclass(eq=False, kw_only=True)
class Finalizer:
    """Атомарно финализирует батч и продолжает каскад дерева после commit."""

    tables: Tables
    engine: AsyncEngine
    clock: Clock
    ids: IdFactory
    hooks: HookRegistry
    settings: FinalizerSettings = field(default_factory=FinalizerSettings)
    observer: Observer = field(default_factory=NullObserver)
    relay: _Relay | None = None
    progress: _Progress | None = None

    async def try_finalize(self, batch_id: UUID) -> bool:
        """Финализировать готовый батч, если условие всё ещё истинно.

        Returns:
            ``True``, если эта попытка закоммитила терминальный переход.
            ``False``, если батч не готов, уже финализирован, хук отсутствует
            или хук завершился ошибкой.
        """
        settings = TxSettings(statement_timeout=self.settings.hook_timeout)
        for _ in range(_SNAPSHOT_RETRIES):
            try:
                committed = await run_transaction(
                    self.engine,
                    lambda conn: self._attempt(conn, batch_id),
                    settings=settings,
                    policy=self.settings.retry,
                )
            except _SnapshotChangedError:
                continue
            except _CasLostError:
                return False
            except HookMissingError as exc:
                self._notify_missing(batch_id, exc)
                return False
            except _HookCallError as exc:
                attempt = await self._record_hook_failure(exc)
                self._notify_failed(exc, attempt)
                return False
            await self._after_commit(committed)
            for candidate in committed.cascade:
                _ = await self.try_finalize(candidate)
            return True
        return False

    async def _attempt(self, conn: AsyncConnection, batch_id: UUID) -> _Committed:
        target = await self._read_batch(conn, batch_id)
        verdict = None if target is None else await self._verdict(conn, target)
        if target is None or verdict is None:
            raise _CasLostError
        self.hooks.ensure(target.kind, target.hooks)
        now = await conn.scalar(select(sql_now(self.clock)))
        if now is None:
            raise _CasLostError
        summary = await self._summary(conn, target=target, state=verdict.state, now=now)
        hook = self.hooks.finalized(target.kind)
        if hook is not None:
            try:
                async with asyncio.timeout(self.settings.hook_timeout.total_seconds()):
                    async with hook_session(conn) as session:
                        await hook(session, summary)
            except Exception as exc:
                raise _HookCallError(error=exc, batch_id=batch_id, kind=target.kind) from exc
        return await self._apply(conn, target, verdict=verdict, now=now)

    async def _verdict(self, conn: AsyncConnection, batch: _Batch) -> _Verdict | None:
        """Выбрать итог по прочитанной строке батча и текущим счётчикам.

        Returns:
            Итог или ``None``, если батч уже терминален либо ещё не готов.
        """
        if batch.state.is_terminal:
            return None
        totals = (await read_counters(conn, self.tables, [batch.id]))[batch.id]
        if totals.pending != 0 or not self._closable(batch):
            return None
        values = (await self._metrics(conn, [batch.id])).get(batch.id, {})
        child_errors = await self._child_errors(conn, batch.id)
        state = self._final_state(batch, totals, values, child_errors=child_errors)
        return _Verdict(state=state, reason=batch.cancel_reason)

    async def _apply(
        self, conn: AsyncConnection, target: _Batch, *, verdict: _Verdict, now: datetime
    ) -> _Committed:
        # Порядок блокировок общий с Completer (§9.2, §10): сначала все строки
        # th_batch в порядке id, потом th_outbox, th_item и th_counter. Хук уже
        # отработал: под блокировкой батча его вызывать нельзя (A-DB-08).
        stages = await self._lock(conn, target.id)
        # Итог, показанный хуку, выбран по раннему снимку. Решает строка под
        # блокировкой: всё, что меняет итог (cancel, дедлайн, fail_fast, политика,
        # retry_failed), блокирует её же и после этой точки ждёт нашего commit.
        locked = await self._read_batch(conn, target.id)
        if locked is None:
            raise _CasLostError
        if not locked.state.is_terminal:
            current = await self._verdict(conn, locked)
            if current is None:
                # Батч снова не готов: retry_failed потомка вернул Item в работу.
                raise _CasLostError
            if current != verdict:
                raise _SnapshotChangedError
        state = verdict.state
        if not await self._cas(conn, batch=target, state=state, now=now):
            # Терминальную строку отсекает сам CAS; иначе изменился только snap_seq.
            if locked.state.is_terminal:
                raise _CasLostError
            raise _SnapshotChangedError
        if await self._fed_ids(conn, target.id) != {stage.id for stage in stages}:
            # add_feed успел добавить этап до блокировки батча: повторить с начала.
            raise _SnapshotChangedError
        candidate_ids = set(await self._seal_downstream(conn, stages, now=now))
        callbacks = await self._write_callbacks(conn, batch=target, state=state, now=now)
        parent_id = await self._finish_virtual(conn, target, now)
        if parent_id is not None:
            candidate_ids.add(parent_id)
        return _Committed(
            batch_id=target.id,
            kind=target.kind,
            state=state,
            parent_id=parent_id,
            cascade=tuple(sorted(candidate_ids)),
            callbacks=callbacks,
        )

    @staticmethod
    def _closable(batch: _Batch) -> bool:
        return batch.state is BatchState.SEALED or batch.cancel_requested_at is not None

    @staticmethod
    def _final_state(
        batch: _Batch,
        totals: CounterTotals,
        labels: Mapping[str, int],
        *,
        child_errors: bool = False,
    ) -> BatchState:
        if batch.cancel_reason is not None:
            return batch.cancel_reason.terminal_state
        if batch.cancel_requested_at is not None:
            return BatchState.CANCELLED
        policy_data = batch.options.get("failure_policy")
        policy = (
            FailurePolicy.from_json(_string_mapping(policy_data))
            if _string_mapping(policy_data)
            else FailurePolicy.continue_()
        )
        verdict = policy.evaluate(totals, labels)
        if verdict.action is PolicyAction.FAIL:
            return BatchState.FAILED
        if totals.error or child_errors:
            return BatchState.COMPLETED_WITH_ERRORS
        return BatchState.SUCCEEDED

    async def _child_errors(self, conn: AsyncConnection, batch_id: UUID) -> bool:
        batch = self.tables.batch
        failed_states = (
            int(BatchState.COMPLETED_WITH_ERRORS),
            int(BatchState.FAILED),
            int(BatchState.CANCELLED),
        )
        count = await conn.scalar(
            select(func.count())
            .select_from(batch)
            .where(batch.c.parent_id == batch_id, batch.c.state.in_(failed_states))
        )
        return bool(count)

    async def _cas(
        self,
        conn: AsyncConnection,
        *,
        batch: _Batch,
        state: BatchState,
        now: datetime,
    ) -> bool:
        table = self.tables.batch
        result = await conn.execute(
            update(table)
            .where(
                table.c.id == batch.id,
                table.c.state.in_((_OPEN, _SEALED)),
                table.c.snap_seq == batch.snap_seq,
            )
            .values(
                state=int(state),
                finished_at=now,
                updated_at=now,
                snap_seq=table.c.snap_seq + 1,
                hook_error=None,
            )
            .returning(table.c.id)
        )
        return result.scalar_one_or_none() is not None

    async def _write_callbacks(
        self,
        conn: AsyncConnection,
        *,
        batch: _Batch,
        state: BatchState,
        now: datetime,
    ) -> bool:
        callbacks = _string_mapping(batch.options.get("callbacks"))
        if not callbacks:
            return False
        names = (_callback_for(state), CallbackName.ON_FINALIZED_TASK)
        rows: list[dict[str, object]] = []
        for name in names:
            raw = _string_mapping(callbacks.get(name.value))
            if not raw:
                continue
            stored = StoredCallback.from_json(raw)
            options = dict(stored.options)
            if stored.queue is not None:
                options["queue"] = stored.queue
            rows.append(
                {
                    "id": self.ids.new_id(),
                    "kind": int(OutboxKind.CALLBACK),
                    "batch_id": batch.id,
                    "task_name": stored.task_name,
                    "payload": stored.payload,
                    "options": options or None,
                    "available_at": now,
                }
            )
        if rows:
            _ = await conn.execute(insert(self.tables.outbox).values(rows))
        return bool(rows)

    async def _finish_virtual(
        self, conn: AsyncConnection, batch: _Batch, now: datetime
    ) -> UUID | None:
        if batch.parent_item_id is None or batch.parent_id is None:
            return None
        item = self.tables.item
        result = await conn.execute(
            update(item)
            .where(item.c.id == batch.parent_item_id, item.c.state == _ACTIVE)
            .values(
                state=_OK,
                label="ok",
                finished_at=now,
            )
            .returning(item.c.batch_id, item.c.weight)
        )
        row = result.one_or_none()
        if row is None:
            return batch.parent_id
        parent_id, weight = row
        await upsert_slots(
            conn,
            self.tables,
            {(parent_id, self.settings.slot): CounterDelta(ok=1, w_done=weight)},
        )
        await upsert_metrics(conn, self.tables, {(parent_id, "ok", self.settings.slot): 1})
        return parent_id

    async def _fed_ids(self, conn: AsyncConnection, feeder_id: UUID) -> set[UUID]:
        feed = self.tables.feed
        return set(await conn.scalars(select(feed.c.fed_id).where(feed.c.feeder_id == feeder_id)))

    async def _lock(self, conn: AsyncConnection, batch_id: UUID) -> list[_Stage]:
        """Заблокировать батч вместе с этапами, которые он наполняет.

        Один ``FOR UPDATE`` в порядке id — глобальный порядок блокировок
        ``th_batch``. Строка батча блокируется всегда: по ней под блокировкой
        перепроверяется итог.

        Returns:
            Этапы, которые наполняет батч, по возрастанию id.
        """
        fed_ids = await self._fed_ids(conn, batch_id)
        batch = self.tables.batch
        rows = await conn.execute(
            select(batch.c.id, batch.c.state, batch.c.on_feeder_failed)
            .where(batch.c.id.in_(sorted({batch_id, *fed_ids})))
            .order_by(batch.c.id)
            .with_for_update()
        )
        return [
            _Stage(id=row_id, state=BatchState(state), on_feeder_failed=OnFeederFailed(behavior))
            for row_id, state, behavior in rows
            if row_id != batch_id
        ]

    async def _seal_downstream(
        self,
        conn: AsyncConnection,
        stages: list[_Stage],
        *,
        now: datetime,
    ) -> list[UUID]:
        batch = self.tables.batch
        closed: list[UUID] = []
        for stage in stages:
            all_terminal, failed = await self._feeder_status(conn, stage.id)
            if stage.state.is_terminal or not all_terminal:
                continue
            values: dict[str, object] = {"updated_at": now}
            if failed and stage.on_feeder_failed is OnFeederFailed.CANCEL:
                values |= {
                    "cancel_requested_at": now,
                    "cancel_reason": CancelReason.CANCEL.value,
                }
            else:
                values["state"] = int(BatchState.SEALED)
            _ = await conn.execute(
                update(batch).where(batch.c.id == stage.id, batch.c.state == _OPEN).values(values)
            )
            closed.append(stage.id)
        return closed

    async def _feeder_status(self, conn: AsyncConnection, fed_id: UUID) -> tuple[bool, bool]:
        feed = self.tables.feed
        batch = self.tables.batch
        active, failed = (
            await conn.execute(
                select(
                    func.count().filter(batch.c.state < TERMINAL_THRESHOLD).label("active"),
                    func.count()
                    .filter(
                        batch.c.state.in_(
                            (
                                int(BatchState.COMPLETED_WITH_ERRORS),
                                int(BatchState.FAILED),
                                int(BatchState.CANCELLED),
                            )
                        )
                    )
                    .label("failed"),
                )
                .select_from(feed.join(batch, batch.c.id == feed.c.feeder_id))
                .where(feed.c.fed_id == fed_id)
            )
        ).one()
        return active == 0, failed > 0

    async def _summary(
        self,
        conn: AsyncConnection,
        *,
        target: _Batch,
        state: BatchState,
        now: datetime,
    ) -> BatchSummary:
        # Строка самого батча — та же, по которой выбран итог: ``state`` и
        # ``reason`` в сводке хука согласованы и сверяются под блокировкой.
        tree = await self._tree(conn, target.root_id)
        rows = [target if row.id == target.id else row for row in tree]
        ids = [row.id for row in rows]
        attributes = await read_batch_attributes(conn, self.tables, target.root_id)
        totals = await read_counters(conn, self.tables, ids)
        feeds = await self._feeds(conn, ids)
        in_flight = await self._in_flight(conn, ids)
        metrics = await self._metrics(conn, ids)
        nodes = [
            NodeCounters(
                id=row.id,
                parent_id=row.parent_id,
                state=state if row.id == target.id else row.state,
                total=totals[row.id].total,
                ok=totals[row.id].ok,
                skip=totals[row.id].skip,
                error=totals[row.id].error,
                cancelled=totals[row.id].cancelled,
                w_total=totals[row.id].w_total,
                w_done=totals[row.id].w_done,
                duplicates=totals[row.id].duplicates,
                skipped_by_limit=totals[row.id].skipped_by_limit,
                in_flight=in_flight.get(row.id, 0),
                expected_total=row.expected_total,
                fed_by=feeds.get(row.id, ()),
            )
            for row in rows
        ]
        progress = compute_progress(nodes)
        children: defaultdict[UUID, list[_Batch]] = defaultdict(list)
        for row in rows:
            if row.parent_id is not None:
                children[row.parent_id].append(row)

        def build(row: _Batch) -> BatchSummary:
            finalizing = row.id == target.id
            row_state = state if finalizing else row.state
            values = metrics.get(row.id, {})
            return BatchSummary(
                id=row.id,
                kind=row.kind,
                key=row.key,
                state=row_state,
                progress=progress[row.id],
                labels=values,
                metrics=values,
                children={
                    child.key or str(child.id): build(child)
                    for child in sorted(children[row.id], key=lambda value: value.id)
                },
                seq=row.snap_seq + 1 if finalizing else row.snap_seq,
                reason=row.cancel_reason,
                finished_at=now if finalizing else row.finished_at,
                attributes=attributes,
            )

        by_id = {row.id: row for row in rows}
        return build(by_id[target.id])

    async def _tree(self, conn: AsyncConnection, root_id: UUID) -> list[_Batch]:
        return await self._batch_rows(conn, self.tables.batch.c.root_id == root_id)

    async def _read_batch(self, conn: AsyncConnection, batch_id: UUID) -> _Batch | None:
        rows = await self._batch_rows(conn, self.tables.batch.c.id == batch_id)
        return rows[0] if rows else None

    async def _batch_rows(self, conn: AsyncConnection, where: ColumnElement[bool]) -> list[_Batch]:
        batch = self.tables.batch
        core = await conn.execute(
            select(
                batch.c.id,
                batch.c.root_id,
                batch.c.parent_id,
                batch.c.parent_item_id,
                batch.c.kind,
                batch.c.key,
                batch.c.state,
                batch.c.cancel_requested_at,
                batch.c.cancel_reason,
            ).where(where)
        )
        extra_result = await conn.execute(
            select(
                batch.c.id,
                batch.c.options,
                batch.c.hooks,
                batch.c.expected_total,
                batch.c.on_feeder_failed,
                batch.c.snap_seq,
                batch.c.finished_at,
            ).where(where)
        )
        extra = {
            row_id: (options, hooks, expected, behavior, snap_seq, finished_at)
            for row_id, options, hooks, expected, behavior, snap_seq, finished_at in extra_result
        }
        rows: list[_Batch] = []
        for (
            row_id,
            root_id,
            parent_id,
            parent_item_id,
            kind,
            key,
            state,
            cancel_requested_at,
            cancel_reason,
        ) in core:
            options, hooks, expected, behavior, snap_seq, finished_at = extra[row_id]
            rows.append(
                _Batch(
                    id=row_id,
                    root_id=root_id,
                    parent_id=parent_id,
                    parent_item_id=parent_item_id,
                    kind=kind,
                    key=key,
                    state=BatchState(state),
                    cancel_requested_at=cancel_requested_at,
                    cancel_reason=_cancel_reason(cast("object", cancel_reason)),
                    options=_string_mapping(options),
                    hooks=tuple(hooks),
                    expected_total=expected,
                    on_feeder_failed=OnFeederFailed(behavior),
                    snap_seq=snap_seq,
                    finished_at=finished_at,
                )
            )
        return rows

    async def _feeds(self, conn: AsyncConnection, ids: list[UUID]) -> dict[UUID, tuple[UUID, ...]]:
        feed = self.tables.feed
        values: defaultdict[UUID, list[UUID]] = defaultdict(list)
        for feeder_id, fed_id in await conn.execute(
            select(feed.c.feeder_id, feed.c.fed_id)
            .where(feed.c.fed_id.in_(ids))
            .order_by(feed.c.fed_id, feed.c.feeder_id)
        ):
            values[fed_id].append(feeder_id)
        return {batch_id: tuple(feeder_ids) for batch_id, feeder_ids in values.items()}

    async def _in_flight(self, conn: AsyncConnection, ids: list[UUID]) -> dict[UUID, int]:
        lease = self.tables.lease
        result = await conn.execute(
            select(lease.c.batch_id, func.count().label("n"))
            .where(lease.c.batch_id.in_(ids))
            .group_by(lease.c.batch_id)
        )
        return {batch_id: int(n) for batch_id, n in result}

    async def _metrics(self, conn: AsyncConnection, ids: list[UUID]) -> dict[UUID, dict[str, int]]:
        metric = self.tables.metric
        result = await conn.execute(
            select(metric.c.batch_id, metric.c.name, func.sum(metric.c.value).label("value"))
            .where(metric.c.batch_id.in_(ids))
            .group_by(metric.c.batch_id, metric.c.name)
        )
        values: defaultdict[UUID, dict[str, int]] = defaultdict(dict)
        for batch_id, name, value in result:
            values[batch_id][name] = int(value)
        return dict(values)

    async def _record_hook_failure(self, failure: _HookCallError) -> int:
        batch = self.tables.batch
        async with own_transaction(self.engine) as conn:
            result = await conn.execute(
                update(batch)
                .where(batch.c.id == failure.batch_id, batch.c.state.in_((_OPEN, _SEALED)))
                .values(
                    hook_attempts=batch.c.hook_attempts + 1,
                    hook_error=str(failure.error),
                    updated_at=sql_now(self.clock),
                )
                .returning(batch.c.hook_attempts)
            )
            attempt = result.scalar_one_or_none()
        return int(attempt or 0)

    async def _after_commit(self, committed: _Committed) -> None:
        try:
            self.observer.batch_finalized(
                batch_id=committed.batch_id,
                kind=committed.kind,
                state=committed.state,
            )
        except Exception:  # ruff: ignore[blind-except]  # commit уже состоялся
            _log.exception("Observer.batch_finalized упал")
        if committed.callbacks and self.relay is not None:
            try:
                self.relay.kick([committed.batch_id])
            except Exception:  # ruff: ignore[blind-except]  # relay scan страхует fast-path
                _log.exception("Relay.kick после финализации упал")
        if self.progress is not None:
            try:
                _ = await self.progress.notify([committed.batch_id], final=True)
            except Exception:  # ruff: ignore[blind-except]  # watch перечитает финал по таймауту
                _log.exception("Публикация финального прогресса упала")

    def _notify_missing(self, batch_id: UUID, exc: HookMissingError) -> None:
        _log.error(
            "Tx hook missing batch_id=%s kind=%s hook=%s attempt=0",
            batch_id,
            exc.kind,
            exc.hook,
        )
        try:
            self.observer.hook_missing(batch_id=batch_id, kind=exc.kind, hook=exc.hook)
        except Exception:  # ruff: ignore[blind-except]  # наблюдаемость не влияет на учёт
            _log.exception("Observer.hook_missing упал")

    def _notify_failed(self, failure: _HookCallError, attempt: int) -> None:
        _log.error(
            "Tx hook failed batch_id=%s kind=%s hook=%s attempt=%d error_type=%s",
            failure.batch_id,
            failure.kind,
            _HOOK_FAILED,
            attempt,
            type(failure.error).__name__,
        )
        try:
            self.observer.hook_failed(
                batch_id=failure.batch_id,
                kind=failure.kind,
                hook=_HOOK_FAILED,
                attempt=attempt,
                error=failure.error,
            )
        except Exception:  # ruff: ignore[blind-except]  # наблюдаемость не влияет на учёт
            _log.exception("Observer.hook_failed упал")


def _callback_for(state: BatchState) -> CallbackName:
    return {
        BatchState.SUCCEEDED: CallbackName.ON_SUCCEEDED,
        BatchState.COMPLETED_WITH_ERRORS: CallbackName.ON_COMPLETED_WITH_ERRORS,
        BatchState.FAILED: CallbackName.ON_FAILED,
        BatchState.CANCELLED: CallbackName.ON_CANCELLED,
    }[state]


def _string_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    raw = cast("dict[object, object]", value)
    return {key: item for key, item in raw.items() if isinstance(key, str)}


def _cancel_reason(value: object) -> CancelReason | None:
    return CancelReason(value) if isinstance(value, str) else None
