"""Применение политик ошибок после flush Completer (ARCHITECTURE UC-11).

Срабатывание и tx-хук составляют одну транзакцию. Условный переход по
``paused_at`` / ``cancel_requested_at`` обеспечивает единственный успешный
commit даже при нескольких конкурентных Completer.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING, cast

from sqlalchemy import DateTime, SmallInteger, delete, func, literal_column, select, update

from tallyho.model.errors import ConfigurationError, HookMissingError, TallyhoError
from tallyho.model.policy import FailurePolicy, PolicyAction
from tallyho.model.progress import NodeCounters, compute_progress
from tallyho.model.states import BatchState, CancelReason, ItemState, OutboxKind
from tallyho.model.views import BatchSummary
from tallyho.protocols.observer import NullObserver
from tallyho.storage.attributes import read_batch_attributes
from tallyho.storage.counters import CounterDelta, read_counters, upsert_metrics, upsert_slots
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

    from tallyho.hooks.registry import HookRegistry, PolicyBreachHook
    from tallyho.model.policy import PolicyBreach
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.tables import Tables

__all__ = ["PolicyEnforcer", "PolicyEnforcerSettings"]

_log = logging.getLogger(__name__)
_ACTIVE = literal_column(str(int(ItemState.ACTIVE)), SmallInteger())
_ITEM_OUTBOX = literal_column(str(int(OutboxKind.ITEM)), SmallInteger())
_INFINITY = literal_column("'infinity'::timestamptz", DateTime(timezone=True))
_HOOK = "on_policy_breach"


class _NoBreachError(TallyhoError):
    """Политика не сработала или уже была применена."""


class _HookCallError(TallyhoError):
    error: Exception
    batch_id: UUID
    kind: str

    def __init__(self, *, error: Exception, batch_id: UUID, kind: str) -> None:
        super().__init__(str(error))
        self.error = error
        self.batch_id = batch_id
        self.kind = kind


@dataclass(frozen=True, slots=True, kw_only=True)
class PolicyEnforcerSettings:
    """Слот отмен и предел выполнения tx-хука политики."""

    slot: int = 0
    hook_timeout: timedelta = timedelta(seconds=10)
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        """Проверить слот и таймаут.

        Raises:
            ConfigurationError: Настройка вне допустимого диапазона.
        """
        if self.slot < 0:
            message = "slot policy enforcer должен быть >= 0"
            raise ConfigurationError(message)
        if self.hook_timeout <= timedelta(0):
            message = "hook_timeout policy enforcer должен быть положительным"
            raise ConfigurationError(message)


@dataclass(frozen=True, slots=True)
class _Batch:
    id: UUID
    root_id: UUID
    parent_id: UUID | None
    kind: str
    key: str | None
    state: BatchState
    paused_at: datetime | None
    cancel_requested_at: datetime | None
    cancel_reason: CancelReason | None
    options: Mapping[str, object]
    hooks: tuple[str, ...]
    expected_total: int | None
    snap_seq: int
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class _Applied:
    finalize: tuple[UUID, ...]


@dataclass(eq=False, kw_only=True)
class PolicyEnforcer:
    """Оценить политики затронутых батчей и атомарно применить первую breach."""

    tables: Tables
    engine: AsyncEngine
    clock: Clock
    hooks: HookRegistry
    settings: PolicyEnforcerSettings = field(default_factory=PolicyEnforcerSettings)
    observer: Observer = field(default_factory=NullObserver)

    async def evaluate(self, batch_ids: Iterable[UUID]) -> tuple[UUID, ...]:
        """Применить политики.

        Returns:
            Батчи, которым после отмены Items нужна финализация.
        """
        finalize: set[UUID] = set()
        settings = TxSettings(statement_timeout=self.settings.hook_timeout)
        for batch_id in sorted(set(batch_ids)):
            try:
                applied = await run_transaction(
                    self.engine,
                    partial(self._attempt, batch_id=batch_id),
                    settings=settings,
                    policy=self.settings.retry,
                )
            except (_NoBreachError, HookMissingError):
                continue
            except _HookCallError as exc:
                hook_attempt = await self._record_hook_failure(exc)
                self._notify_failed(exc, hook_attempt)
                continue
            finalize.update(applied.finalize)
        return tuple(sorted(finalize))

    async def _attempt(self, conn: AsyncConnection, batch_id: UUID) -> _Applied:
        target = await self._read_batch(conn, batch_id)
        if target is None or target.state.is_terminal:
            raise _NoBreachError
        policy_data = _string_mapping(target.options.get("failure_policy"))
        if not policy_data:
            raise _NoBreachError
        policy = FailurePolicy.from_json(policy_data)
        totals = (await read_counters(conn, self.tables, [batch_id]))[batch_id]
        labels = (await self._metrics(conn, [batch_id])).get(batch_id, {})
        verdict = policy.evaluate(totals, labels)
        if not verdict.breached:
            raise _NoBreachError
        if verdict.action is PolicyAction.PAUSE and target.paused_at is not None:
            raise _NoBreachError
        if verdict.action is PolicyAction.FAIL and target.cancel_requested_at is not None:
            raise _NoBreachError
        tree = await self._tree(conn, target.root_id)
        root = next(row for row in tree if row.id == row.root_id)
        hook = self._policy_hook(target, root)
        now = await conn.scalar(select(sql_now(self.clock)))
        if now is None:
            raise _NoBreachError
        breach = verdict.breach(batch_key=target.key, labels=policy.labels or ())
        if hook is not None:
            summary = await self._summary(conn, rows=tree, root=root)
            await self._call_hook(
                conn=conn, hook=hook, summary=summary, breach=breach, target=target
            )
        won = await self._claim_breach(conn, target=target, breach=breach, now=now)
        if not won:
            raise _NoBreachError
        if breach.action is PolicyAction.PAUSE:
            await self._pause_tree(conn, root_id=target.root_id, now=now)
            return _Applied(())
        finalize = await self._fail_tree(
            conn, root_id=target.root_id, reason=verdict.reason or CancelReason.POLICY, now=now
        )
        return _Applied(finalize)

    def _policy_hook(self, target: _Batch, root: _Batch) -> PolicyBreachHook | None:
        required = _HOOK.removeprefix("on_")
        expects = required in target.hooks or required in root.hooks
        hook = self.hooks.policy_breach(target.kind, root_kind=root.kind)
        if expects and hook is None:
            kind = target.kind if required in target.hooks else root.kind
            raise HookMissingError(kind, _HOOK)
        return hook

    async def _call_hook(
        self,
        *,
        conn: AsyncConnection,
        hook: PolicyBreachHook,
        summary: BatchSummary,
        breach: PolicyBreach,
        target: _Batch,
    ) -> None:
        try:
            async with asyncio.timeout(self.settings.hook_timeout.total_seconds()):
                async with hook_session(conn) as session:
                    await hook(session, summary, breach)
        except Exception as exc:
            raise _HookCallError(error=exc, batch_id=target.id, kind=target.kind) from exc

    async def _claim_breach(
        self, conn: AsyncConnection, *, target: _Batch, breach: PolicyBreach, now: datetime
    ) -> bool:
        batch = self.tables.batch
        predicate = (
            batch.c.paused_at.is_(None)
            if breach.action is PolicyAction.PAUSE
            else batch.c.cancel_requested_at.is_(None)
        )
        result = await conn.execute(
            update(batch)
            .where(batch.c.id == target.id, predicate, batch.c.state < int(BatchState.SUCCEEDED))
            .values(updated_at=now)
            .returning(batch.c.id)
        )
        return result.scalar_one_or_none() is not None

    async def _pause_tree(self, conn: AsyncConnection, *, root_id: UUID, now: datetime) -> None:
        batch = self.tables.batch
        outbox = self.tables.outbox
        _ = await conn.execute(
            update(batch)
            .where(batch.c.root_id == root_id, batch.c.state < int(BatchState.SUCCEEDED))
            .values(paused_at=now, updated_at=now)
        )
        _ = await conn.execute(
            update(outbox)
            .where(outbox.c.batch_id.in_(select(batch.c.id).where(batch.c.root_id == root_id)))
            .values(available_at=_INFINITY)
        )

    async def _fail_tree(
        self,
        conn: AsyncConnection,
        *,
        root_id: UUID,
        reason: CancelReason,
        now: datetime,
    ) -> tuple[UUID, ...]:
        batch = self.tables.batch
        item = self.tables.item
        lease = self.tables.lease
        ids = select(batch.c.id).where(batch.c.root_id == root_id)
        _ = await conn.execute(
            update(batch)
            .where(batch.c.root_id == root_id, batch.c.state < int(BatchState.SUCCEEDED))
            .values(cancel_requested_at=now, cancel_reason=reason.value, updated_at=now)
        )
        cancellable = (
            select(item.c.id)
            .outerjoin(lease, lease.c.item_id == item.c.id)
            .where(item.c.batch_id.in_(ids), item.c.state == _ACTIVE, lease.c.item_id.is_(None))
        )
        changed = await conn.execute(
            update(item)
            .where(item.c.id.in_(cancellable))
            .values(state=int(ItemState.CANCELLED), label="cancelled", finished_at=now)
            .returning(item.c.id, item.c.batch_id, item.c.weight)
        )
        deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
        metrics: defaultdict[tuple[UUID, str, int], int] = defaultdict(int)
        item_ids: list[UUID] = []
        for item_id, batch_id, weight in changed:
            item_ids.append(item_id)
            deltas[batch_id] += CounterDelta(cancelled=1, w_done=weight)
            metrics[batch_id, "cancelled", self.settings.slot] += 1
        await upsert_slots(
            conn, self.tables, {(key, self.settings.slot): value for key, value in deltas.items()}
        )
        await upsert_metrics(conn, self.tables, metrics)
        if item_ids:
            for table in (self.tables.outbox, self.tables.expiry, self.tables.window):
                column = table.c.item_id
                _ = await conn.execute(delete(table).where(column.in_(item_ids)))
        return tuple(sorted(deltas))

    async def _summary(
        self, conn: AsyncConnection, *, rows: list[_Batch], root: _Batch
    ) -> BatchSummary:
        ids = [row.id for row in rows]
        attributes = await read_batch_attributes(conn, self.tables, root.root_id)
        totals = await read_counters(conn, self.tables, ids)
        metrics = await self._metrics(conn, ids)
        in_flight_result = await conn.execute(
            select(self.tables.lease.c.batch_id, func.count())
            .where(self.tables.lease.c.batch_id.in_(ids))
            .group_by(self.tables.lease.c.batch_id)
        )
        in_flight = {batch_id: int(count) for batch_id, count in in_flight_result}
        feeds: defaultdict[UUID, list[UUID]] = defaultdict(list)
        for feeder_id, fed_id in await conn.execute(
            select(self.tables.feed.c.feeder_id, self.tables.feed.c.fed_id).where(
                self.tables.feed.c.fed_id.in_(ids)
            )
        ):
            feeds[fed_id].append(feeder_id)
        nodes = [
            NodeCounters(
                id=row.id,
                parent_id=row.parent_id,
                state=row.state,
                total=totals[row.id].total,
                ok=totals[row.id].ok,
                skip=totals[row.id].skip,
                error=totals[row.id].error,
                cancelled=totals[row.id].cancelled,
                w_total=totals[row.id].w_total,
                w_done=totals[row.id].w_done,
                duplicates=totals[row.id].duplicates,
                skipped_by_limit=totals[row.id].skipped_by_limit,
                in_flight=int(in_flight.get(row.id, 0)),
                expected_total=row.expected_total,
                fed_by=tuple(feeds[row.id]),
            )
            for row in rows
        ]
        progress = compute_progress(nodes)
        children: defaultdict[UUID, list[_Batch]] = defaultdict(list)
        for row in rows:
            if row.parent_id is not None:
                children[row.parent_id].append(row)

        def build(row: _Batch) -> BatchSummary:
            values = metrics.get(row.id, {})
            return BatchSummary(
                id=row.id,
                kind=row.kind,
                key=row.key,
                state=row.state,
                progress=progress[row.id],
                labels=values,
                metrics=values,
                children={
                    child.key or str(child.id): build(child)
                    for child in sorted(children[row.id], key=lambda value: value.id)
                },
                seq=row.snap_seq,
                reason=row.cancel_reason,
                finished_at=row.finished_at,
                attributes=attributes,
            )

        return build(root)

    async def _tree(self, conn: AsyncConnection, root_id: UUID) -> list[_Batch]:
        batch = self.tables.batch
        result = await conn.execute(
            select(
                batch.c.id,
                batch.c.root_id,
                batch.c.parent_id,
                batch.c.kind,
                batch.c.key,
                batch.c.state,
                batch.c.paused_at,
                batch.c.cancel_requested_at,
                batch.c.cancel_reason,
                batch.c.options,
                batch.c.hooks,
                batch.c.expected_total,
                batch.c.snap_seq,
                batch.c.finished_at,
            )
            .where(batch.c.root_id == root_id)
            .order_by(batch.c.id)
        )
        return [self._batch_from_row(row) for row in result]

    async def _read_batch(self, conn: AsyncConnection, batch_id: UUID) -> _Batch | None:
        rows = await self._tree_for_id(conn, batch_id)
        return rows[0] if rows else None

    async def _tree_for_id(self, conn: AsyncConnection, batch_id: UUID) -> list[_Batch]:
        batch = self.tables.batch
        result = await conn.execute(
            select(
                batch.c.id,
                batch.c.root_id,
                batch.c.parent_id,
                batch.c.kind,
                batch.c.key,
                batch.c.state,
                batch.c.paused_at,
                batch.c.cancel_requested_at,
                batch.c.cancel_reason,
                batch.c.options,
                batch.c.hooks,
                batch.c.expected_total,
                batch.c.snap_seq,
                batch.c.finished_at,
            ).where(batch.c.id == batch_id)
        )
        return [self._batch_from_row(row) for row in result]

    @staticmethod
    def _batch_from_row(row: object) -> _Batch:
        values = tuple(cast("Iterable[object]", row))
        return _Batch(
            id=cast("UUID", values[0]),
            root_id=cast("UUID", values[1]),
            parent_id=cast("UUID | None", values[2]),
            kind=cast("str", values[3]),
            key=cast("str | None", values[4]),
            state=BatchState(cast("int", values[5])),
            paused_at=cast("datetime | None", values[6]),
            cancel_requested_at=cast("datetime | None", values[7]),
            cancel_reason=_cancel_reason(values[8]),
            options=_string_mapping(values[9]),
            hooks=tuple(cast("list[str]", values[10])),
            expected_total=cast("int | None", values[11]),
            snap_seq=cast("int", values[12]),
            finished_at=cast("datetime | None", values[13]),
        )

    async def _metrics(self, conn: AsyncConnection, ids: list[UUID]) -> dict[UUID, dict[str, int]]:
        result = await conn.execute(
            select(
                self.tables.metric.c.batch_id,
                self.tables.metric.c.name,
                func.sum(self.tables.metric.c.value),
            )
            .where(self.tables.metric.c.batch_id.in_(ids))
            .group_by(self.tables.metric.c.batch_id, self.tables.metric.c.name)
        )
        values: defaultdict[UUID, dict[str, int]] = defaultdict(dict)
        for batch_id, name, value in result:
            values[batch_id][name] = int(value)
        return dict(values)

    async def _record_hook_failure(self, failure: _HookCallError) -> int:
        batch = self.tables.batch
        async with own_transaction(self.engine) as conn:
            attempt = await conn.scalar(
                update(batch)
                .where(batch.c.id == failure.batch_id)
                .values(hook_attempts=batch.c.hook_attempts + 1, hook_error=str(failure.error))
                .returning(batch.c.hook_attempts)
            )
        return int(attempt or 0)

    def _notify_failed(self, failure: _HookCallError, attempt: int) -> None:
        _log.error(
            "Tx hook failed batch_id=%s kind=%s hook=%s attempt=%d error_type=%s",
            failure.batch_id,
            failure.kind,
            _HOOK,
            attempt,
            type(failure.error).__name__,
        )
        try:
            self.observer.hook_failed(
                batch_id=failure.batch_id,
                kind=failure.kind,
                hook=_HOOK,
                attempt=attempt,
                error=failure.error,
            )
        except Exception:  # ruff: ignore[blind-except]  # наблюдаемость не влияет на учёт
            _log.exception("Observer.hook_failed упал")


def _string_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    raw = cast("dict[object, object]", value)
    return {key: item for key, item in raw.items() if isinstance(key, str)}


def _cancel_reason(value: object) -> CancelReason | None:
    return CancelReason(value) if isinstance(value, str) else None
