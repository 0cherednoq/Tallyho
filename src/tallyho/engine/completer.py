"""Completer: групповой коммит операций воркера (ARCHITECTURE UC-03, UC-04, §9.2).

Обёртка задачи не пишет в БД сама: она отдаёт операцию (claim, heartbeat,
release, позже finish) в буфер процесса и ждёт future. Completer раз в тик
(20 мс) или при наборе ``max_batch`` (500) операций выполняет **одну** короткую
транзакцию (:func:`~tallyho.storage.tx.run_transaction`, повтор на дедлоке и
``lock_timeout``) и резолвит futures только после commit. Когда в буфере
``backpressure`` операций (10 000), новые ждут места (как River).

Completer привязывается к event loop при первой операции (лениво создаёт
задачу сброса): он живёт в loop исполнителя async-задач брокера (§11.3).

Порядок блокировок в транзакции (§9.2, COUNTERS §3.5):
``th_batch`` (``FOR SHARE``, по id) → ``th_item`` (``FOR UPDATE``, по id) →
``th_lease`` (``FOR UPDATE``, по item_id) → ``th_outbox`` / ``th_expiry`` →
``th_counter`` (по ``(batch_id, slot)``). Все блокировки строк берутся в
начале транзакции отсортированными пачками, запись идёт потом.

Исходы claim (§6.2, §11.3, UC-03, UC-11, UC-12, §11.4):

* ``CLAIMED`` — lease взят, задачу надо выполнить;
* ``DUPLICATE`` — у Item живой lease (чужой или свой): дубль доставки или
  ретрай брокера при живом lease (FLEXIQ_SPIKE факт 9a) — успех без выполнения,
  Item остаётся за lease;
* ``TERMINAL`` — Item уже завершён или не найден (удалён retention);
* ``PARKED`` — батч на паузе: Item возвращается в outbox с
  ``available_at = infinity``, ``dispatched -= 1``;
* ``CANCELLED`` — у батча запрошена отмена: ленивая отмена Item
  (CAS ``active → cancelled``, ``cancelled += 1``), после commit —
  ``try_finalize`` батча;
* ``EXPIRED`` — срок ``expires`` (``th_expiry``) истёк: задача не
  выполняется, строку ``th_expiry`` claim не трогает — Item завершит sweeper
  как ``error("expired")``.

Lease, истёкший у другого воркера (воркер умер, sweeper ещё не дошёл),
перехватывается: ``attempt += 1``, новый lease — ``CLAIMED``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, final

from sqlalchemy import (
    DateTime,
    Interval,
    SmallInteger,
    Text,
    Uuid,
    any_,
    delete,
    literal,
    literal_column,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert

from tallyho.model.errors import CompleterError, ConfigurationError, InvalidStateError
from tallyho.model.states import ItemState, OutboxKind, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.storage.counters import CounterDelta, upsert_slots
from tallyho.storage.now import sql_now
from tallyho.storage.tx import RetryPolicy, TxSettings, run_transaction

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.tables import Tables

__all__ = [
    "CANCELLED_LABEL",
    "ClaimOutcome",
    "ClaimResult",
    "Completer",
    "CompleterSettings",
    "FinalizeTrigger",
    "ItemRef",
]

_log = logging.getLogger(__name__)

_T = TypeVar("_T")

CANCELLED_LABEL: Final = "cancelled"
"""Метка Item, отменённого лениво при claim (ARCHITECTURE §11.2: «отмена → cancelled»)."""

_CLOSED = "Completer закрыт: новые операции не принимаются"
_OTHER_LOOP = "Completer привязан к другому event loop: создайте свой экземпляр на loop"
_FLUSH_FAILED = "групповая транзакция Completer не прошла"


class ClaimOutcome(StrEnum):
    """Исход claim: выполнять ли задачу (см. docstring модуля)."""

    CLAIMED = "claimed"
    DUPLICATE = "duplicate"
    TERMINAL = "terminal"
    PARKED = "parked"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class ItemRef:
    """Item из служебного ``_th`` сообщения брокера: ``{"i": id, "b": batch_id}``."""

    id: UUID
    batch_id: UUID


@dataclass(frozen=True, slots=True, kw_only=True)
class ClaimResult:
    """Итог claim.

    Attributes:
        outcome: Исход.
        attempt: Номер попытки Item (``th_item.attempt``) после claim; для
            ``TERMINAL`` у ненайденного Item — 0.
        depth: Глубина самоподпитки Item (для ``ItemContext.depth``).
    """

    outcome: ClaimOutcome
    attempt: int = 0
    depth: int = 0

    @property
    def run(self) -> bool:
        """Задачу нужно выполнить: lease взят этим claim."""
        return self.outcome is ClaimOutcome.CLAIMED


def _positive(name: str, value: float) -> None:
    if value <= 0:
        message = f"{name} должен быть > 0, получено {value!r}"
        raise ConfigurationError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class CompleterSettings:
    """Настройки Completer (ARCHITECTURE §15).

    Attributes:
        worker_id: Идентификатор процесса-воркера в ``th_lease.worker_id``.
        slot: Слот ``th_counter`` процесса.
        tick: Сколько ждать новые операции после первой в буфере.
        max_batch: Операций в одной транзакции.
        backpressure: Операций в буфере, после которого новые ждут места.
        lease_ttl: Срок lease от claim и от каждого heartbeat.
        tx: Таймауты транзакции.
        retry: Повтор транзакции на дедлоке, конфликте и ``lock_timeout``.
    """

    worker_id: str
    slot: int = 0
    tick: timedelta = timedelta(milliseconds=20)
    max_batch: int = 500
    backpressure: int = 10_000
    lease_ttl: timedelta = timedelta(seconds=60)
    tx: TxSettings = field(default_factory=TxSettings)
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        """Проверить значения.

        Raises:
            ConfigurationError: пустой ``worker_id``, неположительные интервалы
                или размеры, ``backpressure < max_batch``, отрицательный слот.
        """
        if not self.worker_id:
            message = "worker_id не может быть пустым"
            raise ConfigurationError(message)
        if self.slot < 0:
            message = f"slot должен быть >= 0, получено {self.slot!r}"
            raise ConfigurationError(message)
        _positive("tick", self.tick.total_seconds())
        _positive("lease_ttl", self.lease_ttl.total_seconds())
        _positive("max_batch", self.max_batch)
        if self.backpressure < self.max_batch:
            message = (
                f"backpressure ({self.backpressure}) должен быть >= max_batch ({self.max_batch})"
            )
            raise ConfigurationError(message)


class FinalizeTrigger(Protocol):
    """Кого Completer зовёт после commit для батчей, где Items стали терминальными.

    Реализует Finalizer (T4.4); до него — любая заглушка.
    """

    async def try_finalize(self, batch_id: UUID) -> bool:
        """Финализировать батч, если его ``pending`` стал 0.

        Args:
            batch_id: Батч.

        Returns:
            ``True``, если батч финализирован этим вызовом.
        """
        ...


# --- операции буфера -----------------------------------------------------------------


@dataclass(eq=False, slots=True)
class _Claim:
    item: ItemRef
    future: asyncio.Future[ClaimResult]


_Op = _Claim


def _pending(ops: Sequence[_Op]) -> list[_Op]:
    # Операцию, чей вызывающий уже отменён, не выполняем.
    return [op for op in ops if not op.future.done()]


# --- строки, прочитанные под блокировкой ---------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class _BatchFlags:
    paused: bool
    cancel_requested: bool


@dataclass(slots=True, kw_only=True)
class _ItemRow:
    batch_id: UUID
    state: ItemState
    attempt: int
    depth: int
    weight: int


@dataclass(slots=True, kw_only=True)
class _LeaseRow:
    worker_id: str
    live: bool


@dataclass(slots=True)
class _Applied:
    """Результат транзакции: что вернуть в futures и что сделать после commit."""

    claims: dict[UUID, ClaimResult] = field(default_factory=dict["UUID", ClaimResult])
    finalize: set[UUID] = field(default_factory=set["UUID"])
    cancelled: list[tuple[UUID, UUID, int]] = field(
        default_factory=list[tuple["UUID", "UUID", int]]
    )
    """``(batch_id, item_id, attempt)`` лениво отменённых Items — для Observer."""


@final
class _Tx:
    """Одна групповая транзакция: блокировки по порядку, потом запись.

    Создаётся заново на каждую попытку :func:`run_transaction`, поэтому
    побочных эффектов вне БД не имеет.
    """

    def __init__(self, completer: Completer, conn: AsyncConnection) -> None:
        self.c = completer
        self.conn = conn
        self.tables = completer.tables
        self.now = sql_now(completer.clock)
        self.batches: dict[UUID, _BatchFlags] = {}
        self.items: dict[UUID, _ItemRow] = {}
        self.leases: dict[UUID, _LeaseRow] = {}
        self.deltas: defaultdict[UUID, CounterDelta] = defaultdict(CounterDelta)
        self.applied = _Applied()

    # --- блокировки ----------------------------------------------------------------

    async def lock_batches(self, batch_ids: Iterable[UUID]) -> None:
        ids = sorted(set(batch_ids))
        if not ids:
            return
        batch = self.tables.batch
        result = await self.conn.execute(
            select(
                batch.c.id,
                batch.c.paused_at.is_not(None),
                batch.c.cancel_requested_at.is_not(None),
            )
            .where(batch.c.id == any_(_uuids(ids)))
            .order_by(batch.c.id)
            .with_for_update(read=True)
        )
        for batch_id, paused, cancel_requested in result:
            self.batches[batch_id] = _BatchFlags(paused=paused, cancel_requested=cancel_requested)

    async def lock_items(self, item_ids: Iterable[UUID]) -> None:
        ids = sorted(set(item_ids))
        if not ids:
            return
        item = self.tables.item
        result = await self.conn.execute(
            select(
                item.c.id,
                item.c.batch_id,
                item.c.state,
                item.c.attempt,
                item.c.depth,
                item.c.weight,
            )
            .where(item.c.id == any_(_uuids(ids)))
            .order_by(item.c.id)
            .with_for_update()
        )
        for item_id, batch_id, state, attempt, depth, weight in result:
            self.items[item_id] = _ItemRow(
                batch_id=batch_id,
                state=ItemState(state),
                attempt=attempt,
                depth=depth,
                weight=weight,
            )

    async def lock_leases(self, item_ids: Iterable[UUID]) -> None:
        ids = sorted(set(item_ids))
        if not ids:
            return
        lease = self.tables.lease
        result = await self.conn.execute(
            select(lease.c.item_id, lease.c.worker_id, lease.c.lease_until > self.now)
            .where(lease.c.item_id == any_(_uuids(ids)))
            .order_by(lease.c.item_id)
            .with_for_update()
        )
        for item_id, worker_id, live in result:
            self.leases[item_id] = _LeaseRow(worker_id=worker_id, live=live)

    async def expired(self, item_ids: Iterable[UUID]) -> set[UUID]:
        ids = sorted(set(item_ids))
        if not ids:
            return set()
        expiry = self.tables.expiry
        found = await self.conn.scalars(
            select(expiry.c.item_id).where(
                expiry.c.item_id == any_(_uuids(ids)), expiry.c.expires_at <= self.now
            )
        )
        return set(found)

    # --- claim ---------------------------------------------------------------------

    async def claim(self, refs: dict[UUID, ItemRef]) -> None:
        expired = await self.expired(refs)
        take: list[UUID] = []
        bump: list[UUID] = []
        cancel: list[UUID] = []
        park: list[UUID] = []
        drop_lease: list[UUID] = []
        for item_id, ref in refs.items():
            outcome = self._classify(ref, expired=item_id in expired)
            lease = self.leases.get(item_id)
            if outcome is ClaimOutcome.CLAIMED:
                take.append(item_id)
                if lease is not None:
                    # Перехват истёкшего lease: воркер умер, это новая попытка.
                    bump.append(item_id)
            elif outcome in {ClaimOutcome.CANCELLED, ClaimOutcome.PARKED}:
                (cancel if outcome is ClaimOutcome.CANCELLED else park).append(item_id)
                if lease is not None:
                    drop_lease.append(item_id)
            else:
                self._result(item_id, outcome)
        await self._bump_attempts(bump)
        await self._cancel(cancel)
        await self._delete_leases(drop_lease)
        await self._take_leases(take)
        await self._park(park)
        await self._delete_expiry([*take, *cancel])

    def _classify(self, ref: ItemRef, *, expired: bool) -> ClaimOutcome:
        row = self.items.get(ref.id)
        batch = self.batches.get(ref.batch_id)
        if row is None or batch is None or row.batch_id != ref.batch_id or row.state.is_terminal:
            return ClaimOutcome.TERMINAL
        lease = self.leases.get(ref.id)
        if lease is not None and lease.live:
            return ClaimOutcome.DUPLICATE
        if batch.cancel_requested:
            return ClaimOutcome.CANCELLED
        if batch.paused:
            return ClaimOutcome.PARKED
        if expired:
            return ClaimOutcome.EXPIRED
        return ClaimOutcome.CLAIMED

    def _result(self, item_id: UUID, outcome: ClaimOutcome) -> None:
        row = self.items.get(item_id)
        if row is None:
            self.applied.claims[item_id] = ClaimResult(outcome=outcome)
            return
        self.applied.claims[item_id] = ClaimResult(
            outcome=outcome, attempt=row.attempt, depth=row.depth
        )

    async def _bump_attempts(self, item_ids: list[UUID]) -> None:
        if not item_ids:
            return
        item = self.tables.item
        _ = await self.conn.execute(
            update(item)
            .where(item.c.id == any_(_uuids(sorted(item_ids))), item.c.state == _ACTIVE)
            .values(attempt=item.c.attempt + 1)
        )
        for item_id in item_ids:
            self.items[item_id].attempt += 1

    async def _cancel(self, item_ids: list[UUID]) -> None:
        # Ленивая отмена (§6.1, UC-12): CAS active → cancelled, счётчики по вернувшимся.
        if not item_ids:
            return
        item = self.tables.item
        ids = _uuids(sorted(item_ids))
        result = await self.conn.execute(
            update(item)
            .where(item.c.id == any_(ids), item.c.state == _ACTIVE)
            .values(state=_small(ItemState.CANCELLED), label=CANCELLED_LABEL, finished_at=self.now)
            .returning(item.c.id, item.c.batch_id, item.c.weight, item.c.attempt)
        )
        for item_id, batch_id, weight, attempt in result:
            self.deltas[batch_id] += CounterDelta(cancelled=1, w_done=weight)
            self.applied.finalize.add(batch_id)
            self.applied.cancelled.append((batch_id, item_id, attempt))
        # Запись outbox Item (id = item_id, D-031), если дубль пришёл раньше relay DELETE.
        outbox = self.tables.outbox
        _ = await self.conn.execute(delete(outbox).where(outbox.c.id == any_(ids)))
        for item_id in item_ids:
            self._result(item_id, ClaimOutcome.CANCELLED)

    async def _delete_leases(self, item_ids: list[UUID]) -> None:
        if not item_ids:
            return
        lease = self.tables.lease
        _ = await self.conn.execute(
            delete(lease).where(lease.c.item_id == any_(_uuids(sorted(item_ids))))
        )
        for item_id in item_ids:
            _ = self.leases.pop(item_id, None)

    async def _take_leases(self, item_ids: list[UUID]) -> None:
        if not item_ids:
            return
        item = self.tables.item
        lease = self.tables.lease
        until = self.now + literal(self.c.settings.lease_ttl, Interval())
        source = select(
            item.c.id,
            item.c.batch_id,
            until,
            literal(self.c.settings.worker_id, Text()),
            item.c.attempt,
        ).where(item.c.id == any_(_uuids(sorted(item_ids))))
        stmt = insert(lease).from_select(
            ["item_id", "batch_id", "lease_until", "worker_id", "attempt"], source
        )
        # Конфликт возможен только с истёкшим lease, заблокированным в lock_leases.
        stmt = stmt.on_conflict_do_update(
            index_elements=[lease.c.item_id],
            set_={
                "lease_until": stmt.excluded.lease_until,
                "worker_id": stmt.excluded.worker_id,
                "attempt": stmt.excluded.attempt,
                "progress_done": None,
                "progress_total": None,
            },
        )
        _ = await self.conn.execute(stmt)
        for item_id in item_ids:
            self.leases[item_id] = _LeaseRow(worker_id=self.c.settings.worker_id, live=True)
            self._result(item_id, ClaimOutcome.CLAIMED)

    async def _park(self, item_ids: list[UUID]) -> None:
        # Пауза (UC-11): Item обратно в outbox до resume; relay его снова отправит.
        if not item_ids:
            return
        item = self.tables.item
        outbox = self.tables.outbox
        source = select(
            item.c.id,
            _small(OutboxKind.ITEM),
            item.c.batch_id,
            item.c.id,
            item.c.task_name,
            _INFINITY,
        ).where(item.c.id == any_(_uuids(sorted(item_ids))))
        result = await self.conn.execute(
            insert(outbox)
            .from_select(["id", "kind", "batch_id", "item_id", "task_name", "available_at"], source)
            .on_conflict_do_nothing(index_elements=[outbox.c.id])
            .returning(outbox.c.batch_id)
        )
        for (batch_id,) in result:
            # Item больше не у брокера: окно max_in_flight считает dispatched - done.
            self.deltas[batch_id] += CounterDelta(dispatched=-1)
        for item_id in item_ids:
            self._result(item_id, ClaimOutcome.PARKED)

    async def _delete_expiry(self, item_ids: list[UUID]) -> None:
        if not item_ids:
            return
        expiry = self.tables.expiry
        _ = await self.conn.execute(
            delete(expiry).where(expiry.c.item_id == any_(_uuids(sorted(item_ids))))
        )

    # --- счётчики ------------------------------------------------------------------

    async def write_counters(self) -> None:
        slot = self.c.settings.slot
        deltas = {(batch_id, slot): delta for batch_id, delta in self.deltas.items()}
        await upsert_slots(self.conn, self.tables, deltas)


_ACTIVE: Final = literal_column(str(int(ItemState.ACTIVE)), SmallInteger())
_INFINITY: Final = literal_column("'infinity'::timestamptz", DateTime(timezone=True))


def _small(value: int) -> ColumnElement[int]:
    # Коды состояний — литералами, а не bind-параметрами (D-020).
    return literal_column(str(int(value)), SmallInteger())


def _uuids(ids: list[UUID]) -> ColumnElement[Sequence[UUID]]:
    # Один параметр-массив вместо IN (...): план не зависит от размера пачки.
    return literal(ids, ARRAY(Uuid()))


# --- Completer -----------------------------------------------------------------------


@final
class Completer:
    """Буфер операций воркера и групповой коммит (ARCHITECTURE §9.2).

    Один экземпляр на event loop процесса. Методы — корутины, которые ждут
    commit своей операции.
    """

    tables: Tables
    engine: AsyncEngine
    clock: Clock
    settings: CompleterSettings
    observer: Observer
    finalizer: FinalizeTrigger | None

    def __init__(
        self,
        *,
        tables: Tables,
        engine: AsyncEngine,
        clock: Clock,
        settings: CompleterSettings,
        observer: Observer | None = None,
        finalizer: FinalizeTrigger | None = None,
    ) -> None:
        """Completer поверх ``engine`` (со ``schema_translate_map`` установки).

        Args:
            tables: Таблицы установки.
            engine: Движок БД; свои транзакции Completer открывает сам.
            clock: Часы: «сейчас» в SQL (D-002).
            settings: Настройки.
            observer: Получатель событий; по умолчанию пустой.
            finalizer: Кого звать после commit для батчей, где Items стали
                терминальными (Finalizer, T4.4).
        """
        self.tables = tables
        self.engine = engine
        self.clock = clock
        self.settings = settings
        self.observer = observer or NullObserver()
        self.finalizer = finalizer
        self._buffer: list[_Op] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._wakeup = asyncio.Event()
        self._full = asyncio.Event()
        self._capacity = asyncio.Semaphore(settings.backpressure)
        self._closing = False
        self._held: set[UUID] = set()

    # --- публичные операции ------------------------------------------------------------

    @property
    def buffered(self) -> int:
        """Операций в буфере, ещё не взятых в транзакцию."""
        return len(self._buffer)

    @property
    def held(self) -> frozenset[UUID]:
        """Items, lease которых этот процесс взял и ещё не отпустил."""
        return frozenset(self._held)

    async def claim(self, item: ItemRef) -> ClaimResult:
        """Захватить Item перед выполнением задачи (UC-03).

        Args:
            item: Item из ``_th`` сообщения.

        Returns:
            Исход claim; задачу выполнять только при ``result.run``.
        """
        future = self._new_future(ClaimResult)
        return await self._submit(_Claim(item, future), future)

    async def close(self) -> None:
        """Мягкая остановка: дослать буфер и остановить задачу сброса.

        Новые операции после вызова бросают ``InvalidStateError``. Повторный
        вызов ничего не делает.
        """
        self._closing = True
        self._wakeup.set()
        self._full.set()
        if self._task is not None:
            await self._task

    # --- буфер -------------------------------------------------------------------------

    def _new_future(self, _kind: type[_T]) -> asyncio.Future[_T]:
        return self._bind().create_future()

    def _bind(self) -> asyncio.AbstractEventLoop:
        if self._closing:
            raise InvalidStateError(_CLOSED)
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
            self._task = loop.create_task(self._run(), name="tallyho-completer")
        elif self._loop is not loop:
            raise InvalidStateError(_OTHER_LOOP)
        return loop

    async def _submit(self, op: _Op, future: asyncio.Future[_T]) -> _T:
        await self._capacity.acquire()
        future.add_done_callback(lambda _: self._capacity.release())
        if self._closing:
            future.cancel()
            raise InvalidStateError(_CLOSED)
        self._buffer.append(op)
        self._wakeup.set()
        if len(self._buffer) >= self.settings.max_batch:
            self._full.set()
        return await future

    async def _run(self) -> None:
        tick = self.settings.tick.total_seconds()
        while True:
            if not self._buffer:
                if self._closing:
                    return
                self._wakeup.clear()
                _ = await self._wakeup.wait()
                continue
            if len(self._buffer) < self.settings.max_batch and not self._closing:
                self._full.clear()
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(tick):
                        _ = await self._full.wait()
            ops = self._buffer[: self.settings.max_batch]
            del self._buffer[: self.settings.max_batch]
            await self._flush(ops)

    # --- транзакция --------------------------------------------------------------------

    async def _flush(self, ops: Sequence[_Op]) -> None:
        ops = _pending(ops)
        if not ops:
            return
        started = self.clock.monotonic()
        try:
            applied = await run_transaction(
                self.engine,
                lambda conn: self._apply(conn, ops),
                settings=self.settings.tx,
                policy=self.settings.retry,
            )
        except Exception as exc:  # ruff: ignore[blind-except]  # ошибка не глотается: уходит в futures операций
            error = CompleterError(_FLUSH_FAILED)
            error.__cause__ = exc
            for op in ops:
                if not op.future.done():
                    op.future.set_exception(error)
            return
        self._resolve(ops, applied)
        self._notify(len(ops), self.clock.monotonic() - started, applied)
        await self._after_commit(applied)

    async def _apply(self, conn: AsyncConnection, ops: Sequence[_Op]) -> _Applied:
        tx = _Tx(self, conn)
        claims: dict[UUID, ItemRef] = {}
        for op in ops:
            _ = claims.setdefault(op.item.id, op.item)
        await tx.lock_batches(ref.batch_id for ref in claims.values())
        await tx.lock_items(claims)
        await tx.lock_leases(claims)
        await tx.claim(claims)
        await tx.write_counters()
        return tx.applied

    def _resolve(self, ops: Sequence[_Op], applied: _Applied) -> None:
        seen: set[UUID] = set()
        for op in ops:
            result = applied.claims[op.item.id]
            if op.item.id in seen and result.run:
                # Второй claim того же Item в одной транзакции — дубль доставки.
                result = ClaimResult(
                    outcome=ClaimOutcome.DUPLICATE, attempt=result.attempt, depth=result.depth
                )
            elif result.run:
                self._held.add(op.item.id)
            seen.add(op.item.id)
            if not op.future.done():
                op.future.set_result(result)

    def _notify(self, items: int, duration: float, applied: _Applied) -> None:
        # Исключение наблюдателя не должно ломать учёт: только лог.
        try:
            self.observer.completer_flush(items=items, duration=duration)
            for batch_id, item_id, attempt in applied.cancelled:
                self.observer.item_finished(
                    batch_id=batch_id,
                    item_id=item_id,
                    result=ResultClass.CANCELLED,
                    label=CANCELLED_LABEL,
                    attempt=attempt,
                )
        except Exception:  # ruff: ignore[blind-except]  # сбой наблюдателя не влияет на учёт
            _log.exception("Observer упал на событии Completer")

    async def _after_commit(self, applied: _Applied) -> None:
        if self.finalizer is None:
            return
        for batch_id in sorted(applied.finalize):
            try:
                _ = await self.finalizer.try_finalize(batch_id)
            except Exception:  # ruff: ignore[blind-except]  # финализацию подхватит sweeper
                _log.exception("try_finalize(%s) после flush упал", batch_id)
