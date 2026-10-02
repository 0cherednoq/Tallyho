"""Транзакционные операции над деревом батчей (ARCHITECTURE UC-10 - UC-16)."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

from sqlalchemy import (
    DateTime,
    SmallInteger,
    case,
    delete,
    func,
    insert,
    literal,
    literal_column,
    select,
    update,
)

from tallyho.model.errors import DownstreamFinalized, InvalidStateError, NotFoundError
from tallyho.model.states import TERMINAL_THRESHOLD, BatchState, CancelReason, ItemState, OutboxKind
from tallyho.storage.counters import CounterDelta, insert_delta, upsert_metrics
from tallyho.storage.now import sql_now
from tallyho.storage.tx import after_commit, resolve_connection

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from tallyho.protocols.clock import Clock
    from tallyho.storage.tables import Tables

__all__ = ["OperationTriggers", "Operations"]

_log = logging.getLogger(__name__)

_ACTIVE = literal_column(str(int(ItemState.ACTIVE)), SmallInteger())
_ERROR = literal_column(str(int(ItemState.ERROR)), SmallInteger())
_ITEM = literal_column(str(int(OutboxKind.ITEM)), SmallInteger())
_INFINITY = literal_column("'infinity'::timestamptz", DateTime(timezone=True))
_CHUNK = 1000
_NOT_FOUND = "батч не найден"
_NOT_TERMINAL = "операция допустима только для терминального батча"
_NO_DATABASE_TIME = "БД не вернула текущее время"


class _Relay(Protocol):
    def kick(self, batch_ids: Iterable[UUID]) -> None: ...


class _Finalizer(Protocol):
    async def try_finalize(self, batch_id: UUID) -> bool: ...


class _Progress(Protocol):
    async def notify(self, batch_ids: Iterable[UUID], *, final: bool = False) -> int: ...


@dataclass(frozen=True, slots=True)
class _BatchRow:
    id: UUID
    root_id: UUID
    parent_id: UUID | None
    state: BatchState


@dataclass(frozen=True, slots=True)
class _ItemRow:
    id: UUID
    batch_id: UUID
    task_name: str
    payload: bytes
    options: object | None
    weight: int
    label: str | None


@dataclass(frozen=True, slots=True)
class _Schedule:
    paused_at: datetime | None
    start_at: datetime | None


@dataclass(frozen=True, slots=True)
class _VirtualRow:
    id: UUID
    batch_id: UUID
    state: ItemState
    label: str | None
    weight: int


@dataclass(frozen=True, slots=True, kw_only=True)
class OperationTriggers:
    """Необязательные fast-path получатели, вызываемые только после commit."""

    relay: _Relay | None = None
    finalizer: _Finalizer | None = None
    progress: _Progress | None = None


@dataclass(eq=False, kw_only=True)
class Operations:
    """Изменить батч и его потомков внутри транзакции пользователя."""

    tables: Tables
    clock: Clock
    triggers: OperationTriggers = field(default_factory=OperationTriggers)
    slot: int = 0
    _background: set[asyncio.Task[None]] = field(default_factory=set, init=False)
    _closed: bool = field(default=False, init=False)

    def shut(self) -> tuple[asyncio.Task[None], ...]:
        """Перестать создавать после-коммитные задачи (закрытие установки, §11.1).

        Returns:
            Ещё не завершённые задачи: их дожидается тот, кто закрывает установку.
        """
        self._closed = True
        return tuple(self._background)

    async def close(self) -> None:
        """Перестать создавать после-коммитные задачи и дождаться уже созданных.

        Без ограничения по времени и только для задач текущего event loop;
        закрытие установки с бюджетом — ``Tallyho.aclose()``.
        """
        tasks = self.shut()
        if tasks:
            _ = await asyncio.wait(tasks)

    async def pause(self, target: AsyncSession | AsyncConnection, batch_id: UUID) -> None:
        """Поставить батч и его активных потомков на паузу."""
        conn = await resolve_connection(target)
        ids = await self._lock_subtree(conn, batch_id)
        now = await self._now(conn)
        batch = self.tables.batch
        _ = await conn.execute(
            update(batch)
            .where(batch.c.id.in_(ids), batch.c.state < TERMINAL_THRESHOLD)
            .values(paused_at=now, updated_at=now)
        )
        _ = await conn.execute(
            update(self.tables.outbox)
            .where(self.tables.outbox.c.batch_id.in_(ids), self.tables.outbox.c.kind == _ITEM)
            .values(available_at=_INFINITY)
        )

    async def resume(self, target: AsyncSession | AsyncConnection, batch_id: UUID) -> None:
        """Снять паузу и вернуть припаркованные Items в очередь."""
        conn = await resolve_connection(target)
        ids = await self._lock_subtree(conn, batch_id)
        now = await self._now(conn)
        batch = self.tables.batch
        outbox = self.tables.outbox
        _ = await conn.execute(
            update(batch)
            .where(batch.c.id.in_(ids), batch.c.state < TERMINAL_THRESHOLD)
            .values(paused_at=None, updated_at=now)
        )
        available = func.greatest(func.coalesce(batch.c.start_at, now), now)
        _ = await conn.execute(
            update(outbox)
            .where(outbox.c.batch_id == batch.c.id, batch.c.id.in_(ids), outbox.c.kind == _ITEM)
            .values(available_at=available)
        )
        await self._after(target, relay=ids)

    async def reschedule(
        self, target: AsyncSession | AsyncConnection, batch_id: UUID, start_at: datetime
    ) -> int:
        """Перенести неотправленные Items.

        Returns:
            Число уже отправленных и потому не перенесённых Items.
        """
        conn = await resolve_connection(target)
        ids = await self._lock_subtree(conn, batch_id)
        now = await self._now(conn)
        batch = self.tables.batch
        outbox = self.tables.outbox
        _ = await conn.execute(
            update(batch)
            .where(batch.c.id.in_(ids), batch.c.state < TERMINAL_THRESHOLD)
            .values(start_at=start_at, updated_at=now)
        )
        _ = await conn.execute(
            update(outbox)
            .where(
                outbox.c.batch_id == batch.c.id,
                batch.c.id.in_(ids),
                batch.c.state < TERMINAL_THRESHOLD,
                outbox.c.kind == _ITEM,
            )
            .values(
                available_at=case(
                    (batch.c.paused_at.is_not(None), _INFINITY), else_=literal(start_at)
                )
            )
        )
        total = await conn.scalar(
            select(func.count())
            .select_from(self.tables.item)
            .where(
                self.tables.item.c.batch_id.in_(ids),
                self.tables.item.c.state == _ACTIVE,
                self.tables.item.c.child_batch_id.is_(None),
            )
        )
        queued = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id.in_(ids), outbox.c.kind == _ITEM)
        )
        await self._after(target, relay=ids)
        return int(total or 0) - int(queued or 0)

    async def cancel(
        self,
        target: AsyncSession | AsyncConnection,
        batch_id: UUID,
        *,
        reason: CancelReason = CancelReason.CANCEL,
    ) -> int:
        """Запросить отмену дерева и сразу завершить все Items в outbox.

        Returns:
            Число немедленно отменённых Items.
        """
        conn = await resolve_connection(target)
        ids = await self._lock_subtree(conn, batch_id)
        now = await self._now(conn)
        batch = self.tables.batch
        _ = await conn.execute(
            update(batch)
            .where(batch.c.id.in_(ids), batch.c.state < TERMINAL_THRESHOLD)
            .values(cancel_requested_at=now, cancel_reason=reason.value, updated_at=now)
        )
        changed = 0
        while True:
            raw_rows = list(
                await conn.execute(
                    select(
                        self.tables.item.c.id,
                        self.tables.item.c.batch_id,
                        self.tables.item.c.weight,
                    )
                    .join(
                        self.tables.outbox,
                        self.tables.outbox.c.item_id == self.tables.item.c.id,
                    )
                    .where(
                        self.tables.item.c.batch_id.in_(ids),
                        self.tables.item.c.state == _ACTIVE,
                        self.tables.outbox.c.kind == _ITEM,
                    )
                    .order_by(self.tables.item.c.id)
                    .limit(_CHUNK)
                    .with_for_update(of=self.tables.item)
                )
            )
            rows = [self._cancel_row(row) for row in raw_rows]
            if not rows:
                break
            item_ids = [row.id for row in rows]
            _ = await conn.execute(
                update(self.tables.item)
                .where(self.tables.item.c.id.in_(item_ids), self.tables.item.c.state == _ACTIVE)
                .values(state=int(ItemState.CANCELLED), label="cancelled", finished_at=now)
            )
            for side in (self.tables.outbox, self.tables.expiry, self.tables.window):
                _ = await conn.execute(delete(side).where(side.c.item_id.in_(item_ids)))
            deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
            metrics: defaultdict[tuple[UUID, str, int], int] = defaultdict(int)
            for row in rows:
                deltas[row.batch_id] += CounterDelta(cancelled=1, w_done=row.weight)
                metrics[row.batch_id, "cancelled", self.slot] += 1
            _ = await insert_delta(conn, self.tables, deltas, created_at=now)
            await upsert_metrics(conn, self.tables, metrics)
            changed += len(rows)
        await self._after(target, finalize=ids)
        return changed

    async def retry_finalize(self, target: AsyncSession | AsyncConnection, batch_id: UUID) -> None:
        """Сбросить backoff упавшего tx-хука и подтолкнуть финализацию."""
        conn = await resolve_connection(target)
        ids = await self._lock_subtree(conn, batch_id)
        now = await self._now(conn)
        _ = await conn.execute(
            update(self.tables.batch)
            .where(self.tables.batch.c.id.in_(ids), self.tables.batch.c.state < TERMINAL_THRESHOLD)
            .values(hook_attempts=0, hook_error=None, updated_at=now)
        )
        await self._after(target, finalize=ids)

    async def release(self, target: AsyncSession | AsyncConnection, batch_id: UUID) -> None:
        """Разрешить retention удалить терминальное дерево.

        Raises:
            InvalidStateError: Батч ещё не терминален.
        """
        conn = await resolve_connection(target)
        rows = await self._lock_rows(conn, [batch_id])
        if not rows[0].state.is_terminal:
            raise InvalidStateError(_NOT_TERMINAL)
        now = await self._now(conn)
        _ = await conn.execute(
            update(self.tables.batch)
            .where(
                self.tables.batch.c.id == batch_id,
                self.tables.batch.c.state >= TERMINAL_THRESHOLD,
            )
            .values(released_at=now, updated_at=now)
        )

    async def retry_failed(
        self,
        target: AsyncSession | AsyncConnection,
        batch_id: UUID,
        *,
        labels: Sequence[str] | None = None,
    ) -> int:
        """Вернуть выбранные error Items в active и повторно поставить их в outbox.

        Returns:
            Число поставленных на повтор Items.

        Raises:
            NotFoundError: Батч не существует.
            InvalidStateError: Батч не завершён с ошибками.
            DownstreamFinalized: Получающий этап уже финализирован.
        """
        conn = await resolve_connection(target)
        root_id = await conn.scalar(
            select(self.tables.batch.c.root_id).where(self.tables.batch.c.id == batch_id)
        )
        if root_id is None:
            raise NotFoundError(_NOT_FOUND)
        tree_ids: list[UUID] = list(
            await conn.scalars(
                select(self.tables.batch.c.id)
                .where(self.tables.batch.c.root_id == root_id)
                .order_by(self.tables.batch.c.id)
            )
        )
        tree = await self._lock_rows(conn, tree_ids)
        selected = next(row for row in tree if row.id == batch_id)
        if selected.state not in {BatchState.COMPLETED_WITH_ERRORS, BatchState.FAILED}:
            raise InvalidStateError(_NOT_TERMINAL)
        if selected.id != selected.root_id:
            terminal_downstream = await conn.scalar(
                select(func.count())
                .select_from(
                    self.tables.feed.join(
                        self.tables.batch, self.tables.batch.c.id == self.tables.feed.c.fed_id
                    )
                )
                .where(
                    self.tables.feed.c.feeder_id == batch_id,
                    self.tables.batch.c.state >= TERMINAL_THRESHOLD,
                )
            )
            if terminal_downstream:
                raise DownstreamFinalized
            ids = [batch_id]
            reopen_ids = {batch_id}
            parent_by_id = {row.id: row.parent_id for row in tree}
            parent_id = parent_by_id[batch_id]
            while parent_id is not None:
                reopen_ids.add(parent_id)
                parent_id = parent_by_id[parent_id]
        else:
            ids = tree_ids
            reopen_ids = set(ids)
        now = await self._now(conn)
        fed_ids = set(
            await conn.scalars(
                select(self.tables.feed.c.fed_id).where(self.tables.feed.c.fed_id.in_(ids))
            )
        )
        _ = await conn.execute(
            update(self.tables.batch)
            .where(
                self.tables.batch.c.id.in_(reopen_ids),
                self.tables.batch.c.state.in_(
                    [
                        int(BatchState.SUCCEEDED),
                        int(BatchState.COMPLETED_WITH_ERRORS),
                        int(BatchState.FAILED),
                    ]
                ),
            )
            .values(
                state=case(
                    (self.tables.batch.c.id.in_(fed_ids), int(BatchState.OPEN)),
                    else_=int(BatchState.SEALED),
                ),
                finished_at=None,
                # release() относится к последней финализации: после переоткрытия
                # его нужно вызвать заново, иначе retention удалит дерево (§7.6).
                released_at=None,
                hook_error=None,
                updated_at=now,
            )
        )
        await self._reactivate_virtuals(conn, child_ids=set(ids), now=now)
        changed = await self._retry_items(conn, ids=ids, labels=labels, now=now)
        await self._after(target, relay=ids, finalize=reopen_ids)
        return changed

    async def _reactivate_virtuals(
        self, conn: AsyncConnection, *, child_ids: set[UUID], now: datetime
    ) -> None:
        item = self.tables.item
        raw_rows = list(
            await conn.execute(
                select(item.c.id, item.c.batch_id, item.c.state, item.c.label, item.c.weight)
                .where(item.c.child_batch_id.in_(child_ids), item.c.state >= TERMINAL_THRESHOLD)
                .order_by(item.c.id)
                .with_for_update()
            )
        )
        rows = [self._virtual_row(row) for row in raw_rows]
        if not rows:
            return
        _ = await conn.execute(
            update(item)
            .where(item.c.id.in_([row.id for row in rows]))
            .values(
                state=int(ItemState.ACTIVE), label=None, result=None, error=None, finished_at=None
            )
        )
        deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
        metrics: defaultdict[tuple[UUID, str, int], int] = defaultdict(int)
        for row in rows:
            values = {"w_done": -row.weight, row.state.name.lower(): -1}
            deltas[row.batch_id] += CounterDelta(**values)
            if row.label is not None:
                metrics[row.batch_id, row.label, self.slot] -= 1
        _ = await insert_delta(conn, self.tables, deltas, created_at=now)
        await upsert_metrics(conn, self.tables, metrics)

    async def _retry_items(
        self,
        conn: AsyncConnection,
        *,
        ids: Sequence[UUID],
        labels: Sequence[str] | None,
        now: datetime,
    ) -> int:
        item = self.tables.item
        mark = self.tables.item_mark
        predicate = item.c.label.is_not(None)
        if labels:
            # item_id глобально уникален, а batch_id Items ниже всё равно ограничен ids.
            predicate = item.c.id.in_(select(mark.c.item_id).where(mark.c.label.in_(labels)))
        total = 0
        while True:
            raw_rows = list(
                await conn.execute(
                    select(
                        item.c.id,
                        item.c.batch_id,
                        item.c.task_name,
                        item.c.payload,
                        item.c.options,
                        item.c.weight,
                        item.c.label,
                    )
                    .where(
                        item.c.batch_id.in_(ids),
                        item.c.state == _ERROR,
                        predicate,
                    )
                    # Порядок и размер чанка проверяют stress/EXPLAIN.
                    .order_by(item.c.id)
                    .limit(_CHUNK)
                    .with_for_update()
                )
            )
            rows = [self._retry_row(row) for row in raw_rows]
            if not rows:
                break
            item_ids = [row.id for row in rows]
            _ = await conn.execute(
                update(item)
                .where(item.c.id.in_(item_ids))
                .values(
                    state=int(ItemState.ACTIVE),
                    label=None,
                    attempt=0,
                    # Новая отправка: записи DLQ прошлых джоб этот Item больше не завершают (UC-15).
                    generation=item.c.generation + 1,
                    result=None,
                    error=None,
                    finished_at=None,
                )
            )
            batches = {row.batch_id for row in rows}
            schedules = await self._batch_schedule(conn, batches)
            _ = await conn.execute(
                insert(self.tables.outbox),
                [
                    {
                        "id": row.id,
                        "kind": int(OutboxKind.ITEM),
                        "batch_id": row.batch_id,
                        "item_id": row.id,
                        "task_name": row.task_name,
                        "payload": row.payload,
                        "options": row.options,
                        "available_at": schedules[row.batch_id].start_at or now,
                    }
                    for row in rows
                ],
            )
            paused = [batch for batch, value in schedules.items() if value.paused_at is not None]
            if paused:
                _ = await conn.execute(
                    update(self.tables.outbox)
                    .where(
                        self.tables.outbox.c.item_id.in_(item_ids),
                        self.tables.outbox.c.batch_id.in_(paused),
                    )
                    .values(available_at=_INFINITY)
                )
            _ = await conn.execute(delete(mark).where(mark.c.item_id.in_(item_ids)))
            deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
            metrics: defaultdict[tuple[UUID, str, int], int] = defaultdict(int)
            for row in rows:
                deltas[row.batch_id] += CounterDelta(error=-1, w_done=-row.weight)
                if row.label is not None:
                    metrics[row.batch_id, row.label, self.slot] -= 1
            _ = await insert_delta(conn, self.tables, deltas, created_at=now)
            await upsert_metrics(conn, self.tables, metrics)
            total += len(
                rows
            )  # pragma: no mutate  # арифметика нескольких чанков не меняет SQL CAS
        return total

    async def _batch_schedule(self, conn: AsyncConnection, ids: set[UUID]) -> dict[UUID, _Schedule]:
        rows = await conn.execute(
            select(
                self.tables.batch.c.id,
                self.tables.batch.c.paused_at,
                self.tables.batch.c.start_at,
            ).where(self.tables.batch.c.id.in_(ids))
        )
        result: dict[UUID, _Schedule] = {}
        for row in rows:
            values = tuple(cast("Iterable[object]", row))
            result[cast("UUID", values[0])] = _Schedule(
                paused_at=cast("datetime | None", values[1]),
                start_at=cast("datetime | None", values[2]),
            )
        return result

    async def _lock_subtree(self, conn: AsyncConnection, batch_id: UUID) -> list[UUID]:
        batch = self.tables.batch
        root = select(batch.c.id).where(batch.c.id == batch_id).cte("subtree", recursive=True)
        root = root.union_all(select(batch.c.id).where(batch.c.parent_id == root.c.id))
        ids: list[UUID] = list(await conn.scalars(select(root.c.id).order_by(root.c.id)))
        if not ids:
            raise NotFoundError(_NOT_FOUND)
        _ = await self._lock_rows(conn, ids)
        return ids

    async def _lock_rows(self, conn: AsyncConnection, ids: Sequence[UUID]) -> list[_BatchRow]:
        raw_rows = list(
            await conn.execute(
                select(
                    self.tables.batch.c.id,
                    self.tables.batch.c.root_id,
                    self.tables.batch.c.parent_id,
                    self.tables.batch.c.state,
                )
                .where(self.tables.batch.c.id.in_(ids))
                .order_by(self.tables.batch.c.id)
                .with_for_update()
            )
        )
        rows = [self._batch_row(row) for row in raw_rows]
        if not rows:
            raise NotFoundError(_NOT_FOUND)
        return rows

    async def _now(self, conn: AsyncConnection) -> datetime:
        value: datetime | None = await conn.scalar(select(sql_now(self.clock)))
        if value is None:
            raise InvalidStateError(_NO_DATABASE_TIME)
        return value

    @staticmethod
    def _batch_row(row: object) -> _BatchRow:
        values = tuple(cast("Iterable[object]", row))
        return _BatchRow(
            id=cast("UUID", values[0]),
            root_id=cast("UUID", values[1]),
            parent_id=cast("UUID | None", values[2]),
            state=BatchState(cast("int", values[3])),
        )

    @staticmethod
    def _cancel_row(row: object) -> _ItemRow:
        values = tuple(cast("Iterable[object]", row))
        return _ItemRow(
            id=cast("UUID", values[0]),
            batch_id=cast("UUID", values[1]),
            task_name="",
            payload=b"",
            options=None,
            weight=cast("int", values[2]),
            label=None,
        )

    @staticmethod
    def _retry_row(row: object) -> _ItemRow:
        values = tuple(cast("Iterable[object]", row))
        return _ItemRow(
            id=cast("UUID", values[0]),
            batch_id=cast("UUID", values[1]),
            task_name=cast("str", values[2]),
            payload=cast("bytes", values[3]),
            options=values[4],
            weight=cast("int", values[5]),
            label=cast("str | None", values[6]),
        )

    @staticmethod
    def _virtual_row(row: object) -> _VirtualRow:
        values = tuple(cast("Iterable[object]", row))
        return _VirtualRow(
            id=cast("UUID", values[0]),
            batch_id=cast("UUID", values[1]),
            state=ItemState(cast("int", values[2])),
            label=cast("str | None", values[3]),
            weight=cast("int", values[4]),
        )

    async def _after(
        self,
        target: AsyncSession | AsyncConnection,
        *,
        relay: Iterable[UUID] = (),
        finalize: Iterable[UUID] = (),
    ) -> None:
        relay_ids = tuple(sorted(set(relay)))
        finalize_ids = tuple(sorted(set(finalize)))
        progress_ids = tuple(sorted({*relay_ids, *finalize_ids}))

        def callback() -> None:
            if self.triggers.relay is not None and relay_ids:
                self.triggers.relay.kick(relay_ids)
            if self._closed:
                # Установка закрыта: финализацию выполнит sweeper, watch перечитает сам.
                return
            if (self.triggers.progress is not None and progress_ids) or (
                self.triggers.finalizer is not None and finalize_ids
            ):
                task = asyncio.get_running_loop().create_task(
                    self._post_commit(progress_ids, finalize_ids),
                    name="tallyho-operation-post-commit",
                )
                self._background.add(task)
                task.add_done_callback(self._background.discard)

        await after_commit(target, callback)

    async def _post_commit(
        self,
        progress_ids: Sequence[UUID],
        finalize_ids: Sequence[UUID],
    ) -> None:
        if self.triggers.progress is not None and progress_ids:
            try:
                _ = await self.triggers.progress.notify(progress_ids)
            except Exception:  # ruff: ignore[blind-except]  # watch перечитает состояние по таймауту
                _log.exception("Публикация прогресса операции упала")
        if self.triggers.finalizer is not None and finalize_ids:
            await self._finalize(self.triggers.finalizer, finalize_ids)

    @staticmethod
    async def _finalize(finalizer: _Finalizer, ids: Sequence[UUID]) -> None:
        for batch_id in ids:
            try:
                _ = await finalizer.try_finalize(batch_id)
            except Exception:  # ruff: ignore[blind-except]  # commit состоялся; финализацию повторит sweeper
                _log.exception("try_finalize(%s) после операции упал", batch_id)
