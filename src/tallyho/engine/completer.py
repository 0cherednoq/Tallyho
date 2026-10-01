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
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, final

from sqlalchemy import (
    BigInteger,
    DateTime,
    Interval,
    SmallInteger,
    Text,
    Uuid,
    any_,
    delete,
    func,
    literal,
    literal_column,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, insert

from tallyho.engine.relay import release_window
from tallyho.model.errors import (
    CompleterError,
    ConfigurationError,
    InvalidStateError,
    SpawnTargetError,
)
from tallyho.model.states import BatchState, ItemState, OutboxKind, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.storage.counters import (
    CounterDelta,
    fold_delta_ids,
    fold_deltas,
    insert_delta,
    read_counters,
    upsert_metrics,
    upsert_slots,
)
from tallyho.storage.now import sql_now
from tallyho.storage.tx import (
    RetryPolicy,
    TxSettings,
    after_commit,
    resolve_connection,
    run_transaction,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

    from tallyho.engine.producer import Producer, SubBatchSpec
    from tallyho.engine.spawn import SpawnRoute, TreeCache
    from tallyho.model.calls import TaskCall
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.counters import CounterTotals
    from tallyho.storage.tables import Tables

__all__ = [
    "CANCELLED_LABEL",
    "ClaimOutcome",
    "ClaimResult",
    "Completer",
    "CompleterSettings",
    "CompleterTriggers",
    "ExpectRequest",
    "FinalizeTrigger",
    "FinishResult",
    "ItemRef",
    "PolicyTrigger",
    "ProgressTrigger",
    "RelayTrigger",
    "SpawnRequest",
    "SubBatchRequest",
]

_log = logging.getLogger(__name__)

_T = TypeVar("_T")

CANCELLED_LABEL: Final = "cancelled"
"""Метка Item, отменённого лениво при claim (ARCHITECTURE §11.2: «отмена → cancelled»)."""

_CLOSED = "Completer закрыт: новые операции не принимаются"
_OTHER_LOOP = "Completer привязан к другому event loop: создайте свой экземпляр на loop"
_FLUSH_FAILED = "групповая транзакция Completer не прошла"
_SPAWN_SERVICES = "Completer не настроен для spawn: передайте Producer в CompleterTriggers"
_SPAWN_ROUTE = "маршрут spawn не соответствует завершаемому Item или дереву"
_SPAWN_TARGET_CLOSED = "целевой этап spawn уже закрыт"


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


class PolicyTrigger(Protocol):
    """Оценка политик для батчей, чьи счётчики изменил flush."""

    async def evaluate(self, batch_ids: Iterable[UUID]) -> tuple[UUID, ...]:
        """Применить breach и вернуть кандидатов на финализацию."""
        ...


class RelayTrigger(Protocol):
    """Минимальный интерфейс Relay для fast-path после commit."""

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        """Разбудить Relay для затронутых батчей."""
        ...


class ProgressTrigger(Protocol):
    """Минимальный интерфейс публикации изменений для ``watch()``."""

    async def notify(self, batch_ids: Iterable[UUID], *, final: bool = False) -> int:
        """Опубликовать изменившиеся батчи с троттлингом."""
        ...


@dataclass(frozen=True, slots=True, kw_only=True)
class CompleterTriggers:
    """Получатели действий Completer после успешного commit."""

    finalizer: FinalizeTrigger | None = None
    policy: PolicyTrigger | None = None
    relay: RelayTrigger | None = None
    producer: Producer | None = None
    tree_cache: TreeCache | None = None
    progress: ProgressTrigger | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SpawnRequest:
    """Вызов, чей маршрут проверен :class:`~tallyho.engine.spawn.TreeSnapshot`."""

    route: SpawnRoute
    call: TaskCall


@dataclass(frozen=True, slots=True, kw_only=True)
class ExpectRequest:
    """Монотонное обновление expected целевого батча."""

    route: SpawnRoute
    total: int

    def __post_init__(self) -> None:
        """Проверить неотрицательное ожидаемое число.

        Raises:
            ConfigurationError: ``total`` отрицателен или является bool.
        """
        if isinstance(self.total, bool) or self.total < 0:
            message = f"expected должен быть целым >= 0, получено {self.total!r}"
            raise ConfigurationError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class SubBatchRequest:
    """Динамический под-батч и его начальные Items из одной задачи."""

    spec: SubBatchSpec
    calls: Sequence[TaskCall] = ()
    seal: bool = True

    def __post_init__(self) -> None:
        """Заморозить начальные вызовы."""
        object.__setattr__(self, "calls", tuple(self.calls))


@dataclass(frozen=True, slots=True, kw_only=True)
class FinishResult:
    """Итог, который путь A атомарно записывает в Item."""

    result_class: ResultClass
    label: str | None = None
    result: object = None
    error: object = None
    metrics: Mapping[str, int] = field(default_factory=dict[str, int])
    mark: bool | None = None
    spawns: Sequence[SpawnRequest] = ()
    expects: Sequence[ExpectRequest] = ()
    sub_batches: Sequence[SubBatchRequest] = ()

    def __post_init__(self) -> None:
        """Заморозить накопленные во время задачи буферы."""
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))
        object.__setattr__(self, "spawns", tuple(self.spawns))
        object.__setattr__(self, "expects", tuple(self.expects))
        object.__setattr__(self, "sub_batches", tuple(self.sub_batches))

    @property
    def effective_label(self) -> str:
        """Метка по умолчанию совпадает с техническим классом итога."""
        return self.label or self.result_class.name.lower()

    @property
    def effective_mark(self) -> bool:
        """Ошибки помечаются по умолчанию, остальные классы — только явно."""
        if self.mark is not None:
            return self.mark
        return self.result_class is ResultClass.ERROR


# --- операции буфера -----------------------------------------------------------------


@dataclass(eq=False, slots=True)
class _Claim:
    item: ItemRef
    future: asyncio.Future[ClaimResult]


_Progress = tuple[int | None, int | None]
"""``(progress_done, progress_total)`` из ``th.item.progress``; ``None`` — не менять."""


@dataclass(eq=False, slots=True)
class _Heartbeat:
    item: ItemRef
    progress: _Progress
    future: asyncio.Future[bool]


@dataclass(eq=False, slots=True)
class _Release:
    item: ItemRef
    future: asyncio.Future[bool]


@dataclass(eq=False, slots=True)
class _Finish:
    item: ItemRef
    value: FinishResult
    future: asyncio.Future[bool]


_Op = _Claim | _Heartbeat | _Release | _Finish


def _pending(ops: Sequence[_Op]) -> list[_Op]:
    # Операцию, чей вызывающий уже отменён, не выполняем.
    return [op for op in ops if not op.future.done()]


# --- строки, прочитанные под блокировкой ---------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class _BatchFlags:
    root_id: UUID
    state: BatchState
    paused: bool
    cancel_requested: bool
    start_at: datetime | None
    max_items: int | None
    max_depth: int | None


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
    claimed: list[tuple[UUID, UUID, int]] = field(default_factory=list[tuple["UUID", "UUID", int]])
    created: list[tuple[UUID, str]] = field(default_factory=list[tuple["UUID", str]])
    beating: set[UUID] = field(default_factory=set["UUID"])
    """Items, чей lease продлён heartbeat'ом: lease всё ещё у этого процесса."""
    released: set[UUID] = field(default_factory=set["UUID"])
    finalize: set[UUID] = field(default_factory=set["UUID"])
    kick: set[UUID] = field(default_factory=set["UUID"])
    invalidate_trees: set[UUID] = field(default_factory=set["UUID"])
    progress: set[UUID] = field(default_factory=set["UUID"])
    finished: dict[UUID, tuple[UUID, FinishResult, int]] = field(
        default_factory=dict["UUID", tuple["UUID", FinishResult, int]]
    )
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
        self.metrics: defaultdict[tuple[UUID, str, int], int] = defaultdict(int)
        self.applied = _Applied()

    # --- блокировки ----------------------------------------------------------------

    async def lock_batches(self, batch_ids: Iterable[UUID], *, write: bool = False) -> None:
        ids = sorted(set(batch_ids))
        if not ids:
            return
        batch = self.tables.batch
        result = await self.conn.execute(
            select(
                batch.c.id,
                batch.c.root_id,
                batch.c.state,
                batch.c.paused_at.is_not(None),
                batch.c.cancel_requested_at.is_not(None),
                batch.c.start_at,
                batch.c.max_items,
                batch.c.max_depth,
            )
            .where(batch.c.id == any_(_uuids(ids)))
            .order_by(batch.c.id)
            .with_for_update(read=not write)
        )
        for (
            batch_id,
            root_id,
            state,
            paused,
            cancel_requested,
            start_at,
            max_items,
            max_depth,
        ) in result:
            self.batches[batch_id] = _BatchFlags(
                root_id=root_id,
                state=BatchState(state),
                paused=paused,
                cancel_requested=cancel_requested,
                start_at=start_at,
                max_items=max_items,
                max_depth=max_depth,
            )

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
        released = await self._release_window([*cancel, *park])
        self.applied.kick.update(released)

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
        source = select(
            item.c.id,
            item.c.batch_id,
            self._lease_until(),
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
            row = self.items[item_id]
            self.applied.claimed.append((row.batch_id, item_id, row.attempt))

    async def _park(self, item_ids: list[UUID]) -> None:
        # Пауза (UC-11): Item обратно в outbox до resume; relay его снова отправит.
        await self._to_outbox(item_ids, _INFINITY)
        for item_id in item_ids:
            self._result(item_id, ClaimOutcome.PARKED)

    async def _to_outbox(self, item_ids: list[UUID], available_at: ColumnElement[datetime]) -> None:
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
            available_at,
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

    async def requeue(self, refs: Iterable[ItemRef]) -> None:
        """Свои lease — сразу обратно в outbox, как после их истечения (A-CH-08).

        Попытка не тратится: задача не упала, процесс останавливается.
        """
        refs = list(refs)
        await self.lock_batches(ref.batch_id for ref in refs)
        await self.lock_items(ref.id for ref in refs)
        await self.lock_leases(ref.id for ref in refs)
        own = self._owned(ref.id for ref in refs)
        await self._delete_leases(own)
        waiting: list[UUID] = []
        paused: list[UUID] = []
        for item_id in own:
            row = self.items.get(item_id)
            if row is None or row.state.is_terminal:
                continue
            flags = self.batches.get(row.batch_id)
            (paused if flags is not None and flags.paused else waiting).append(item_id)
        await self._to_outbox(waiting, self.now)
        await self._to_outbox(paused, _INFINITY)
        self.applied.released.update(own)

    async def _delete_expiry(self, item_ids: list[UUID]) -> None:
        if not item_ids:
            return
        expiry = self.tables.expiry
        _ = await self.conn.execute(
            delete(expiry).where(expiry.c.item_id == any_(_uuids(sorted(item_ids))))
        )

    # --- release и heartbeat -------------------------------------------------------

    def _owned(self, item_ids: Iterable[UUID]) -> list[UUID]:
        worker = self.c.settings.worker_id
        return sorted(
            item_id
            for item_id in item_ids
            if (lease := self.leases.get(item_id)) is not None and lease.worker_id == worker
        )

    async def release(self, item_ids: Iterable[UUID]) -> None:
        # UC-04, вердикт RETRY: брокер повторит задачу, lease отпускаем, attempt += 1.
        own = self._owned(item_ids)
        active = [
            item_id
            for item_id in own
            if (row := self.items.get(item_id)) is not None and not row.state.is_terminal
        ]
        await self._bump_attempts(active)
        await self._delete_leases(own)
        self.applied.released.update(own)

    async def heartbeat(self, beats: dict[UUID, _Progress]) -> None:
        own = self._owned(beats)
        if not own:
            return
        lease = self.tables.lease
        rows = (
            func.unnest(
                _uuids(own),
                literal([beats[item_id][0] for item_id in own], ARRAY(BigInteger())),
                literal([beats[item_id][1] for item_id in own], ARRAY(BigInteger())),
            )
            .table_valued("item_id", "done", "total")
            .render_derived("u")
        )
        done: ColumnElement[int] = rows.c.done
        total: ColumnElement[int] = rows.c.total
        _ = await self.conn.execute(
            update(lease)
            .where(lease.c.item_id == rows.c.item_id)
            .values(
                lease_until=self._lease_until(),
                progress_done=func.coalesce(done, lease.c.progress_done),
                progress_total=func.coalesce(total, lease.c.progress_total),
            )
        )
        self.applied.beating.update(own)

    # --- finish -------------------------------------------------------------------

    async def finish(
        self,
        values: dict[UUID, tuple[ItemRef, FinishResult]],
        *,
        scalar: bool = False,
    ) -> None:
        """CAS ``active -> terminal`` и учёт только действительно изменённых Items."""
        if not values:
            return
        item = self.tables.item
        ids = sorted(values)
        if scalar:
            item_id = ids[0]
            ref, value = values[item_id]
            statement = (
                update(item)
                .where(
                    item.c.id == item_id,
                    item.c.batch_id == ref.batch_id,
                    item.c.state == _ACTIVE,
                )
                .values(
                    state=int(value.result_class),
                    label=value.effective_label,
                    result=value.result,
                    error=value.error,
                    finished_at=self.now,
                )
            )
        else:
            rows = (
                func.unnest(
                    _uuids(ids),
                    literal([values[item_id][0].batch_id for item_id in ids], ARRAY(Uuid())),
                    literal(
                        [int(values[item_id][1].result_class) for item_id in ids],
                        ARRAY(SmallInteger()),
                    ),
                    literal([values[item_id][1].effective_label for item_id in ids], ARRAY(Text())),
                    literal([values[item_id][1].result for item_id in ids], ARRAY(JSONB())),
                    literal([values[item_id][1].error for item_id in ids], ARRAY(JSONB())),
                )
                .table_valued("id", "batch_id", "state", "label", "result", "error")
                .render_derived("u")
            )
            statement = (
                update(item)
                .where(
                    item.c.id == rows.c.id,
                    item.c.batch_id == rows.c.batch_id,
                    item.c.state == _ACTIVE,
                )
                .values(
                    state=rows.c.state,
                    label=rows.c.label,
                    result=rows.c.result,
                    error=rows.c.error,
                    finished_at=self.now,
                )
            )
        changed = await self.conn.execute(
            statement.returning(
                item.c.id,
                item.c.batch_id,
                item.c.weight,
                item.c.attempt,
                item.c.depth,
            )
        )
        successful: list[UUID] = []
        plain_values = {item_id: value for item_id, (_, value) in values.items()}
        for finished_id, batch_id, weight, attempt, depth in changed:
            value = plain_values[finished_id]
            if scalar:
                row = _ItemRow(
                    batch_id=batch_id,
                    state=ItemState(value.result_class),
                    attempt=attempt,
                    depth=depth,
                    weight=weight,
                )
                self.items[finished_id] = row
            else:
                row = self.items[finished_id]
            successful.append(finished_id)
            counter = value.result_class.name.lower()
            self.deltas[row.batch_id] += CounterDelta(**{counter: 1, "w_done": row.weight})
            self.metrics[row.batch_id, value.effective_label, self.c.settings.slot] += 1
            for name, increment in value.metrics.items():
                self.metrics[row.batch_id, name, self.c.settings.slot] += increment
            self.applied.finished[finished_id] = (row.batch_id, value, row.attempt)
            self.applied.finalize.add(row.batch_id)
        await self._delete_leases(list(values))
        await self._delete_expiry(successful)
        await self._write_marks(successful, plain_values)
        released = await self._release_window(successful)
        self.applied.kick.update(released)

    async def expand(self, values: dict[UUID, tuple[ItemRef, FinishResult]]) -> None:
        """Записать spawn/expect только для Items, чей CAS finish был успешен."""
        successful = set(self.applied.finished)
        if not successful:
            return
        spawns: list[tuple[UUID, SpawnRequest]] = []
        expects: list[tuple[UUID, ExpectRequest]] = []
        for item_id in sorted(successful):
            _, finish = values[item_id]
            spawns.extend((item_id, request) for request in finish.spawns)
            expects.extend((item_id, request) for request in finish.expects)
        await self._spawn(spawns)
        await self._expect(expects)
        await self._sub_batches(values, successful)

    async def _sub_batches(
        self,
        values: dict[UUID, tuple[ItemRef, FinishResult]],
        successful: set[UUID],
    ) -> None:
        requests = [
            (item_id, request)
            for item_id in sorted(successful)
            for request in values[item_id][1].sub_batches
        ]
        if not requests:
            return
        producer = self.c.triggers.producer
        if producer is None:
            raise ConfigurationError(_SPAWN_SERVICES)
        root_ids = {self.items[item_id].batch_id for item_id, _ in requests}
        roots = {self.batches[batch_id].root_id for batch_id in root_ids}
        totals = await read_counters(self.conn, self.tables, roots)
        for item_id, request in requests:
            parent_id = self.items[item_id].batch_id
            root_id = self.batches[parent_id].root_id
            await self._sub_batch(
                producer,
                parent_id=parent_id,
                request=request,
                total=totals[root_id],
            )

    async def _sub_batch(
        self,
        producer: Producer,
        *,
        parent_id: UUID,
        request: SubBatchRequest,
        total: CounterTotals,
    ) -> None:
        parent = self.batches[parent_id]
        ref = await producer.create_sub_batch_unaccounted(
            self.conn, parent_id, request.spec, from_task=True
        )
        if ref.created:
            self.deltas[parent_id] += CounterDelta(total=1)
            self.applied.invalidate_trees.add(ref.root_id)
            self.applied.created.append((ref.id, ref.kind))
        calls = list(request.calls)
        over_items = parent.max_items is not None and total.tree_total >= parent.max_items
        accepted = [] if over_items else calls
        found = weight = duplicates = 0
        for start in range(0, len(accepted), 1000):
            chunk = accepted[start : start + 1000]
            inserted = await producer.insert_items(
                self.conn,
                ref.id,
                calls=chunk,
                depths=[0] * len(chunk),
                available_at=producer.available_at(
                    paused=parent.paused,
                    start_at=request.spec.start_at or parent.start_at,
                ),
            )
            found += inserted.found
            weight += inserted.weight
            duplicates += len(chunk) - inserted.found
        self.deltas[ref.id] += CounterDelta(
            total=found,
            w_total=weight,
            duplicates=duplicates,
            skipped_by_limit=len(calls) - len(accepted),
        )
        self.deltas[ref.root_id] += CounterDelta(tree_total=found)
        if found:
            self.applied.kick.add(ref.id)
        if request.seal and not request.spec.fed_by:
            batch = self.tables.batch
            _ = await self.conn.execute(
                update(batch)
                .where(batch.c.id == ref.id, batch.c.state == _small(BatchState.OPEN))
                .values(state=_small(BatchState.SEALED), updated_at=self.now)
            )
            self.applied.finalize.add(ref.id)

    async def _spawn(self, requests: list[tuple[UUID, SpawnRequest]]) -> None:
        if not requests:
            return
        producer = self.c.triggers.producer
        if producer is None:
            raise ConfigurationError(_SPAWN_SERVICES)
        roots = {request.route.root_id for _, request in requests}
        totals = await read_counters(self.conn, self.tables, roots)
        accepted: defaultdict[UUID, list[tuple[TaskCall, int]]] = defaultdict(list)
        skipped: defaultdict[UUID, int] = defaultdict(int)
        for item_id, request in requests:
            parent = self.items[item_id]
            target = self._validate_route(parent.batch_id, request.route)
            depth = parent.depth + 1 if request.route.into_self else 0
            over_depth = target.max_depth is not None and depth > target.max_depth
            root_total = totals[request.route.root_id].tree_total
            over_items = target.max_items is not None and root_total >= target.max_items
            if over_depth or over_items:
                skipped[request.route.target_id] += 1
                continue
            accepted[request.route.target_id].append((request.call, depth))
        for target_id in sorted(set(accepted) | set(skipped)):
            await self._spawn_target(
                producer,
                target_id=target_id,
                rows=accepted[target_id],
                skipped=skipped[target_id],
            )

    async def _spawn_target(
        self,
        producer: Producer,
        *,
        target_id: UUID,
        rows: list[tuple[TaskCall, int]],
        skipped: int,
    ) -> None:
        target = self.batches[target_id]
        found = weight = duplicates = 0
        for start in range(0, len(rows), 1000):
            chunk = rows[start : start + 1000]
            inserted = await producer.insert_items(
                self.conn,
                target_id,
                calls=[call for call, _ in chunk],
                depths=[depth for _, depth in chunk],
                available_at=producer.available_at(
                    paused=target.paused,
                    start_at=target.start_at,
                ),
            )
            found += inserted.found
            weight += inserted.weight
            duplicates += len(chunk) - inserted.found
        self.deltas[target_id] += CounterDelta(
            total=found,
            w_total=weight,
            duplicates=duplicates,
            skipped_by_limit=skipped,
        )
        self.deltas[target.root_id] += CounterDelta(tree_total=found)
        if found:
            self.applied.kick.add(target_id)

    async def _expect(self, requests: list[tuple[UUID, ExpectRequest]]) -> None:
        if not requests:
            return
        expected: dict[UUID, int] = {}
        for item_id, request in requests:
            source_id = self.items[item_id].batch_id
            _ = self._validate_route(source_id, request.route)
            expected[request.route.target_id] = max(
                expected.get(request.route.target_id, 0), request.total
            )
        batch = self.tables.batch
        for target_id in sorted(expected):
            _ = await self.conn.execute(
                update(batch)
                .where(batch.c.id == target_id)
                .values(
                    expected_total=func.greatest(
                        batch.c.expected_total, literal(expected[target_id], BigInteger())
                    )
                )
            )

    def _validate_route(self, source_id: UUID, route: SpawnRoute) -> _BatchFlags:
        source = self.batches.get(source_id)
        target = self.batches.get(route.target_id)
        if source is None or target is None:
            raise SpawnTargetError(_SPAWN_ROUTE)
        if (
            route.source_id != source_id
            or route.root_id != source.root_id
            or target.root_id != source.root_id
        ):
            raise SpawnTargetError(_SPAWN_ROUTE)
        if not route.into_self and target.state is not BatchState.OPEN:
            raise SpawnTargetError(_SPAWN_TARGET_CLOSED)
        return target

    async def _write_marks(self, item_ids: list[UUID], values: dict[UUID, FinishResult]) -> None:
        rows = [
            {
                "batch_id": self.items[item_id].batch_id,
                "label": values[item_id].effective_label,
                "item_id": item_id,
            }
            for item_id in sorted(item_ids)
            if values[item_id].effective_mark
        ]
        if rows:
            _ = await self.conn.execute(insert(self.tables.item_mark).values(rows))

    async def _release_window(self, item_ids: Iterable[UUID]) -> list[UUID]:
        return await release_window(self.conn, self.tables, item_ids)

    def _lease_until(self) -> ColumnElement[datetime]:
        return self.now + literal(self.c.settings.lease_ttl, Interval())

    # --- счётчики ------------------------------------------------------------------

    async def write_counters(self) -> None:
        slot = self.c.settings.slot
        deltas = {(batch_id, slot): delta for batch_id, delta in self.deltas.items()}
        await upsert_slots(self.conn, self.tables, deltas)
        await upsert_metrics(self.conn, self.tables, self.metrics)


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
    triggers: CompleterTriggers

    def __init__(
        self,
        *,
        tables: Tables,
        engine: AsyncEngine,
        clock: Clock,
        settings: CompleterSettings,
        observer: Observer | None = None,
        triggers: CompleterTriggers | None = None,
    ) -> None:
        """Completer поверх ``engine`` (со ``schema_translate_map`` установки).

        Args:
            tables: Таблицы установки.
            engine: Движок БД; свои транзакции Completer открывает сам.
            clock: Часы: «сейчас» в SQL (D-002).
            settings: Настройки.
            observer: Получатель событий; по умолчанию пустой.
            triggers: Получатели действий после commit: Finalizer и Relay.
        """
        self.tables = tables
        self.engine = engine
        self.clock = clock
        self.settings = settings
        self.observer = observer or NullObserver()
        self.triggers = triggers or CompleterTriggers()
        self._buffer: list[_Op] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._wakeup = asyncio.Event()
        self._full = asyncio.Event()
        self._capacity = asyncio.Semaphore(settings.backpressure)
        self._closing = False
        self._held: dict[UUID, ItemRef] = {}
        self._background: set[asyncio.Task[None]] = set()

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

    async def heartbeat(
        self,
        item: ItemRef,
        *,
        progress_done: int | None = None,
        progress_total: int | None = None,
    ) -> bool:
        """Продлить lease на ``lease_ttl`` и записать прогресс задачи (§9.4).

        Прогресс ``th.item.progress(done, total)`` пишется в ``th_lease`` вместе
        с продлением — отдельных транзакций нет. ``None`` оставляет прежнее
        значение.

        Args:
            item: Item, захваченный этим процессом.
            progress_done: Сколько сделано внутри задачи.
            progress_total: Сколько всего внутри задачи.

        Returns:
            ``False``, если lease уже не у этого процесса (истёк и перехвачен,
            отпущен или удалён sweeper'ом): результат задачи всё равно
            запишет finish, CAS сделает его идемпотентным.
        """
        future = self._new_future(bool)
        op = _Heartbeat(item, (progress_done, progress_total), future)
        return await self._submit(op, future)

    async def release(self, item: ItemRef) -> bool:
        """Отпустить lease перед ретраем брокера: ``attempt += 1`` (UC-04, вердикт RETRY).

        Args:
            item: Item, захваченный этим процессом.

        Returns:
            ``True``, если lease был у этого процесса и удалён.
        """
        future = self._new_future(bool)
        return await self._submit(_Release(item, future), future)

    async def finish(
        self,
        item: ItemRef,
        value: FinishResult,
    ) -> bool:
        """Завершить Item идемпотентным CAS и дождаться commit.

        Returns:
            ``True``, если этот вызов перевёл Item из active в терминальное
            состояние; ``False`` для повторного или уже завершённого Item.

        Raises:
            ConfigurationError: есть динамические операции, но Completer
                создан без Producer.
        """
        if (value.spawns or value.sub_batches) and self.triggers.producer is None:
            raise ConfigurationError(_SPAWN_SERVICES)
        future = self._new_future(bool)
        return await self._submit(_Finish(item, value, future), future)

    async def complete_in(
        self,
        target: AsyncSession | AsyncConnection,
        item: ItemRef,
        value: FinishResult,
    ) -> bool:
        """Завершить Item внутри внешней транзакции пользователя (путь B).

        Внешняя транзакция меняет Item и добавляет append-only дельты, но не
        касается горячих строк ``th_counter``. После её commit Completer
        сворачивает дельты в своей короткой транзакции и проверяет
        финализацию. Откат транзакции или savepoint отменяет и записи, и
        зарегистрированное действие после commit.

        Args:
            target: Открытая пользовательская сессия или соединение.
            item: Завершаемый Item.
            value: Итог и накопленные динамические операции.

        Returns:
            ``True``, если CAS завершил Item; ``False``, если он уже был
            терминальным. Это флаг для middleware, чтобы не писать finish
            повторно после возврата задачи.

        Raises:
            ConfigurationError: Нужен spawn, но Completer создан без Producer.
        """
        if (value.spawns or value.sub_batches) and self.triggers.producer is None:
            raise ConfigurationError(_SPAWN_SERVICES)
        conn = await resolve_connection(target)
        tx = _Tx(self, conn)
        batch_ids: list[UUID] = []
        writes_structure = bool(value.sub_batches)
        for spawn_request in value.spawns:
            batch_ids.extend(
                [
                    spawn_request.route.source_id,
                    spawn_request.route.target_id,
                    spawn_request.route.root_id,
                ]
            )
        for expect_request in value.expects:
            batch_ids.extend(
                [
                    expect_request.route.source_id,
                    expect_request.route.target_id,
                    expect_request.route.root_id,
                ]
            )
        for sub_batch in value.sub_batches:
            batch_ids.extend(sub_batch.spec.fed_by)
        if batch_ids:
            await tx.lock_batches(batch_ids, write=writes_structure)
        values = {item.id: (item, value)}
        await tx.finish(values, scalar=True)
        await tx.expand(values)
        inserted = await insert_delta(conn, self.tables, tx.deltas, created_at=tx.now)
        await upsert_metrics(conn, self.tables, tx.metrics)
        tx.applied.progress.update(tx.deltas)
        changed = item.id in tx.applied.finished
        if changed:
            delta_ids = {delta_id for ids in inserted.values() for delta_id in ids}
            await after_commit(
                target,
                lambda: self._schedule_external(tx.applied, delta_ids),
            )
        return changed

    async def fold(self, batch_id: UUID) -> bool:
        """Свернуть закоммиченные дельты батча и проверить финализацию.

        Args:
            batch_id: Батч, чьи append-only дельты надо перенести в слот
                процесса Completer.

        Returns:
            ``True``, если была перенесена хотя бы одна ненулевая дельта.
        """
        folded = await run_transaction(
            self.engine,
            lambda conn: self._fold_in(conn, [batch_id]),
            settings=self.settings.tx,
            policy=self.settings.retry,
        )
        if self.triggers.finalizer is not None:
            await self.triggers.finalizer.try_finalize(batch_id)
        return batch_id in folded

    async def close(self, *, requeue_held: bool = False) -> None:
        """Мягкая остановка: дослать буфер и остановить задачу сброса.

        Новые операции после вызова бросают ``InvalidStateError``; операции,
        принятые раньше, выполняются. Повторный вызов только досылает
        ``requeue_held``.

        Args:
            requeue_held: Путь ``SIGTERM`` (A-CH-08): lease, которые процесс
                ещё держит (задачи не успели доработать), сразу вернуть в
                outbox, не дожидаясь ``lease_ttl``. Попытка не тратится.

        Raises:
            CompleterError: транзакция возврата lease не прошла; их вернёт
                sweeper по истечении.
        """
        self._closing = True
        self._wakeup.set()
        self._full.set()
        if self._task is not None:
            await self._task
        if self._background:
            await asyncio.gather(*tuple(self._background))
        if not (requeue_held and self._held):
            return
        refs = list(self._held.values())
        self._held.clear()
        try:
            _ = await run_transaction(
                self.engine,
                lambda conn: self._requeue(conn, refs),
                settings=self.settings.tx,
                policy=self.settings.retry,
            )
        except Exception as exc:
            raise CompleterError(_FLUSH_FAILED) from exc

    async def _requeue(self, conn: AsyncConnection, refs: list[ItemRef]) -> None:
        tx = _Tx(self, conn)
        await tx.requeue(refs)
        await tx.write_counters()

    async def _fold_in(
        self, conn: AsyncConnection, batch_ids: Iterable[UUID]
    ) -> dict[UUID, CounterDelta]:
        folded = await fold_deltas(conn, self.tables, batch_ids)
        await upsert_slots(
            conn,
            self.tables,
            {(batch_id, self.settings.slot): delta for batch_id, delta in folded.items()},
        )
        return folded

    def _schedule_external(self, applied: _Applied, delta_ids: set[int]) -> None:
        loop = self._bind()
        task = loop.create_task(
            self._after_external_commit(applied, delta_ids),
            name="tallyho-complete-in",
        )
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _after_external_commit(self, applied: _Applied, delta_ids: set[int]) -> None:
        try:
            _ = await run_transaction(
                self.engine,
                lambda conn: self._fold_ids_in(conn, delta_ids),
                settings=self.settings.tx,
                policy=self.settings.retry,
            )
            self._notify(len(applied.finished), 0.0, applied)
            await self._after_commit(applied)
        except Exception:  # ruff: ignore[blind-except]  # commit уже состоялся; sweeper повторит fold/finalize
            _log.exception("обработка complete_in после commit упала")

    async def _fold_ids_in(
        self, conn: AsyncConnection, delta_ids: Iterable[int]
    ) -> dict[UUID, CounterDelta]:
        folded = await fold_delta_ids(conn, self.tables, delta_ids)
        await upsert_slots(
            conn,
            self.tables,
            {(batch_id, self.settings.slot): delta for batch_id, delta in folded.items()},
        )
        return folded

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
        self._notify_buffer()
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
            self._notify_buffer()
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
        beats: dict[UUID, _Progress] = {}
        releases: set[UUID] = set()
        finishes: dict[UUID, tuple[ItemRef, FinishResult]] = {}
        for op in ops:
            if isinstance(op, _Claim):
                _ = claims.setdefault(op.item.id, op.item)
            elif isinstance(op, _Heartbeat):
                done, total = beats.get(op.item.id, (None, None))
                new_done, new_total = op.progress
                beats[op.item.id] = (
                    done if new_done is None else new_done,
                    total if new_total is None else new_total,
                )
            elif isinstance(op, _Release):
                releases.add(op.item.id)
            else:
                _ = finishes.setdefault(op.item.id, (op.item, op.value))
        # Все блокировки строк — в начале и по порядку: batch → item → lease.
        batch_ids = [ref.batch_id for ref in claims.values()]
        batch_ids.extend(op.item.batch_id for op in ops if isinstance(op, _Finish))
        writes_structure = False
        for _, value in finishes.values():
            for spawn_request in value.spawns:
                batch_ids.extend(
                    [
                        spawn_request.route.source_id,
                        spawn_request.route.target_id,
                        spawn_request.route.root_id,
                    ]
                )
            for expect_request in value.expects:
                batch_ids.extend(
                    [
                        expect_request.route.source_id,
                        expect_request.route.target_id,
                        expect_request.route.root_id,
                    ]
                )
            for sub_batch in value.sub_batches:
                writes_structure = True
                batch_ids.extend(sub_batch.spec.fed_by)
        await tx.lock_batches(batch_ids, write=writes_structure)
        await tx.lock_items([*releases, *claims, *finishes])
        await tx.lock_leases([*releases, *claims, *beats, *finishes])
        # release раньше claim: ретрай брокера мог прийти в ту же пачку.
        await tx.release(releases)
        await tx.claim(claims)
        await tx.heartbeat(beats)
        await tx.finish(finishes)
        await tx.expand(finishes)
        await tx.write_counters()
        tx.applied.progress.update(tx.deltas)
        return tx.applied

    def _resolve(self, ops: Sequence[_Op], applied: _Applied) -> None:
        seen: set[UUID] = set()
        released: set[UUID] = set()
        finished: set[UUID] = set()
        for op in ops:
            if isinstance(op, _Claim):
                self._resolve_claim(op, applied.claims[op.item.id], again=op.item.id in seen)
                seen.add(op.item.id)
                continue
            if isinstance(op, _Heartbeat):
                value = op.item.id in applied.beating
            elif isinstance(op, _Release):
                # Повторный release того же Item в пачке уже ничего не отпускает.
                value = op.item.id in applied.released and op.item.id not in released
                released.add(op.item.id)
            else:
                value = op.item.id in applied.finished and op.item.id not in finished
                finished.add(op.item.id)
            if not value or isinstance(op, _Release | _Finish):
                _ = self._held.pop(op.item.id, None)
            if not op.future.done():
                op.future.set_result(value)

    def _resolve_claim(self, op: _Claim, result: ClaimResult, *, again: bool) -> None:
        if again and result.run:
            # Второй claim того же Item в одной транзакции — дубль доставки.
            result = ClaimResult(
                outcome=ClaimOutcome.DUPLICATE, attempt=result.attempt, depth=result.depth
            )
        elif result.run:
            self._held[op.item.id] = op.item
        if not op.future.done():
            op.future.set_result(result)

    def _notify(self, items: int, duration: float, applied: _Applied) -> None:
        # Исключение наблюдателя не должно ломать учёт: только лог.
        try:
            self.observer.completer_flush(items=items, duration=duration)
            for batch_id, kind in applied.created:
                self.observer.batch_created(batch_id=batch_id, kind=kind)
            for batch_id, item_id, attempt in applied.claimed:
                self.observer.item_claimed(
                    batch_id=batch_id,
                    item_id=item_id,
                    attempt=attempt,
                )
            for batch_id, item_id, attempt in applied.cancelled:
                self.observer.item_finished(
                    batch_id=batch_id,
                    item_id=item_id,
                    result=ResultClass.CANCELLED,
                    label=CANCELLED_LABEL,
                    attempt=attempt,
                )
            for item_id, (batch_id, value, attempt) in applied.finished.items():
                self.observer.item_finished(
                    batch_id=batch_id,
                    item_id=item_id,
                    result=value.result_class,
                    label=value.effective_label,
                    attempt=attempt,
                )
        except Exception:  # ruff: ignore[blind-except]  # сбой наблюдателя не влияет на учёт
            _log.exception("Observer упал на событии Completer")

    def _notify_buffer(self) -> None:
        try:
            self.observer.completer_buffer(items=len(self._buffer))
        except Exception:  # ruff: ignore[blind-except]  # observer must not affect accounting
            _log.exception("Observer.completer_buffer failed")

    async def _after_commit(self, applied: _Applied) -> None:
        if self.triggers.tree_cache is not None:
            for root_id in applied.invalidate_trees:
                self.triggers.tree_cache.invalidate(root_id)
        if self.triggers.relay is not None and applied.kick:
            self.triggers.relay.kick(sorted(applied.kick))
        await self._publish_progress(applied.progress)
        candidates = set(applied.finalize)
        if self.triggers.policy is not None and candidates:
            try:
                candidates.update(await self.triggers.policy.evaluate(candidates))
            except Exception:  # ruff: ignore[blind-except]  # sweeper повторит оценку политики
                _log.exception("Оценка политики после flush упала")
        if self.triggers.finalizer is None:
            return
        for batch_id in sorted(candidates):
            try:
                _ = await self.triggers.finalizer.try_finalize(batch_id)
            except Exception:  # ruff: ignore[blind-except]  # финализацию подхватит sweeper
                _log.exception("try_finalize(%s) после flush упал", batch_id)

    async def _publish_progress(self, batch_ids: Iterable[UUID]) -> None:
        if self.triggers.progress is None:
            return
        ids = tuple(batch_ids)
        if not ids:
            return
        try:
            _ = await self.triggers.progress.notify(ids)
        except Exception:  # ruff: ignore[blind-except]  # polling watch страхует потерянную подсказку
            _log.exception("Публикация прогресса после flush упала")
