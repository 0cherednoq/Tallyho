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
  Item остаётся за lease. Брокер после такого успеха считает джобу завершённой,
  поэтому lease помечается ``redelivered``: если его владелец потом отпустит
  Item по вердикту ``RETRY``, повторять будет некому, и release сам вернёт
  Item в outbox (UC-04);
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
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast, final

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
from tallyho.engine.retry_limits import effective_max_retries
from tallyho.model.errors import (
    ClosedError,
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
    take_metric_slot,
    upsert_metrics,
    upsert_slots,
)
from tallyho.storage.metric_names import metric_rows
from tallyho.storage.now import sql_now
from tallyho.storage.tx import (
    RetryPolicy,
    TxSettings,
    after_commit,
    deliver_committed,
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
    from tallyho.protocols.broker import RetryLimits
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
_ABORTED = "Completer остановлен до commit операции: закрытие не уложилось в срок"
_SPAWN_SERVICES = "Completer не настроен для spawn: передайте Producer в CompleterTriggers"
_SPAWN_ROUTE = "маршрут spawn не соответствует завершаемому Item или дереву"
_SPAWN_TARGET_CLOSED = "целевой этап spawn уже закрыт"
_REDELIVERY_EXHAUSTED_MESSAGE = "дубль доставки закрыл джобу, попытка упала с RETRY, попыток нет"


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
    limits: RetryLimits | None = None
    """Умолчания ``max_retries`` задач от адаптера: ими ``release`` ограничивает
    возврат Item в outbox после дубля доставки (UC-04); без них — 0."""


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
    """Итог, который путь A атомарно записывает в Item.

    ``metrics`` хранит приращения уже под именами строк ``th_metric``
    (:func:`~tallyho.storage.metric_names.metric_rows`): метрика не
    смешивается с меткой итога того же имени.
    """

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
        object.__setattr__(self, "metrics", MappingProxyType(metric_rows(self.metrics)))
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
    attempt: int | None = None
    """Попытка-владелец lease (UC-03); ``None`` — проверка только по ``worker_id``."""


@dataclass(eq=False, slots=True)
class _Release:
    item: ItemRef
    future: asyncio.Future[bool]
    attempt: int | None = None
    """Попытка-владелец lease (UC-03); ``None`` — проверка только по ``worker_id``."""


@dataclass(eq=False, slots=True)
class _Finish:
    item: ItemRef
    value: FinishResult
    future: asyncio.Future[bool]
    attempt: int | None = None
    """Попытка-владелец lease (UC-03); ``None`` — без проверки lease, только CAS."""


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
    attempt: int
    """Попытка Item, для которой взят lease (``th_item.attempt`` на момент claim)."""
    redelivered: bool = False


@dataclass(slots=True)
class _Applied:
    """Результат транзакции: что вернуть в futures и что сделать после commit."""

    claims: dict[UUID, ClaimResult] = field(default_factory=dict["UUID", ClaimResult])
    claimed: list[tuple[UUID, UUID, int]] = field(default_factory=list[tuple["UUID", "UUID", int]])
    created: list[tuple[UUID, str]] = field(default_factory=list[tuple["UUID", str]])
    heartbeats: set[_Heartbeat] = field(default_factory=set[_Heartbeat])
    """Операции heartbeat, продлившие lease: он всё ещё у этого процесса и попытки."""
    released: set[UUID] = field(default_factory=set["UUID"])
    releasers: dict[UUID, _Release] = field(default_factory=dict["UUID", _Release])
    """Операция release, применённая к Item: другие release того же Item в пачке — ``False``."""
    finishers: dict[UUID, _Finish] = field(default_factory=dict["UUID", _Finish])
    """Операция finish, переданная в CAS: другие finish того же Item в пачке — ``False``."""
    requeued: set[UUID] = field(default_factory=set["UUID"])
    """Items, которые release вернул в outbox: дубль доставки уже закрыл джобу брокера."""
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

    def __init__(
        self, completer: Completer, conn: AsyncConnection, *, user_tx: bool = False
    ) -> None:
        self.c = completer
        self.conn = conn
        self.user_tx = user_tx
        """Транзакция пользователя (путь B): строку ``th_lease`` не меняем (UC-08)."""
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

    async def lock_leases(self, item_ids: Iterable[UUID], *, lock: bool = True) -> None:
        """Прочитать строки ``th_lease``; с ``lock`` — под ``FOR UPDATE`` по порядку.

        Без блокировки читает путь B (UC-08): владельца lease меняют только
        операции, которые раньше блокируют строку ``th_item``, а её путь B уже
        держит. Так транзакция пользователя не ждёт heartbeat и не мешает ему.
        """
        ids = sorted(set(item_ids))
        if not ids:
            return
        lease = self.tables.lease
        statement = (
            select(
                lease.c.item_id,
                lease.c.worker_id,
                lease.c.lease_until > self.now,
                lease.c.attempt,
                lease.c.redelivered,
            )
            .where(lease.c.item_id == any_(_uuids(ids)))
            .order_by(lease.c.item_id)
        )
        if lock:
            statement = statement.with_for_update()
        result = await self.conn.execute(statement)
        for item_id, worker_id, live, attempt, redelivered in result:
            self.leases[item_id] = _LeaseRow(
                worker_id=worker_id, live=live, attempt=attempt, redelivered=redelivered
            )

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
        # Ленивая отмена (§6.1, UC-12): CAS active → cancelled, счётчики и метка
        # итога по вернувшимся — как у немедленной отмены и у Sweeper (Fix-22).
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
            self.metrics[batch_id, CANCELLED_LABEL, self.c.settings.slot] += 1
            self.applied.finalize.add(batch_id)
            self.applied.cancelled.append((batch_id, item_id, attempt))
        # Запись outbox Item (id = item_id, D-031), если дубль пришёл раньше relay DELETE.
        outbox = self.tables.outbox
        _ = await self.conn.execute(delete(outbox).where(outbox.c.id == any_(ids)))
        for item_id in item_ids:
            self._result(item_id, ClaimOutcome.CANCELLED)

    async def _delete_leases(self, item_ids: list[UUID]) -> None:
        if not item_ids or self.user_tx:
            # Путь B lease не удаляет: это сделает Completer после commit (UC-08),
            # иначе heartbeat ждал бы транзакцию пользователя, а та ловила 40001.
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
                # Новая попытка: дубли, подтверждённые прежнему владельцу, к ней не относятся.
                "redelivered": False,
            },
        )
        _ = await self.conn.execute(stmt)
        for item_id in item_ids:
            row = self.items[item_id]
            self.leases[item_id] = _LeaseRow(
                worker_id=self.c.settings.worker_id, live=True, attempt=row.attempt
            )
            self._result(item_id, ClaimOutcome.CLAIMED)
            self.applied.claimed.append((row.batch_id, item_id, row.attempt))

    async def mark_redelivered(self, repeated: Iterable[UUID]) -> None:
        """Запомнить в lease, что брокеру подтверждён дубль доставки (UC-04).

        Дубль — claim с исходом ``DUPLICATE`` при живом lease и повторный claim
        того же Item в этой же пачке (``repeated``): в обоих случаях обёртка
        вернёт брокеру успех, и джоба будет закрыта, хотя Item ещё выполняется.
        Строки lease уже заблокированы в :meth:`lock_leases`.
        """
        candidates = set(repeated)
        candidates.update(
            item_id
            for item_id, result in self.applied.claims.items()
            if result.outcome is ClaimOutcome.DUPLICATE
        )
        ids = sorted(
            item_id
            for item_id in candidates
            if (row := self.leases.get(item_id)) is not None and row.live and not row.redelivered
        )
        if not ids:
            return
        lease = self.tables.lease
        _ = await self.conn.execute(
            update(lease).where(lease.c.item_id == any_(_uuids(ids))).values(redelivered=True)
        )
        for item_id in ids:
            self.leases[item_id].redelivered = True

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
            .returning(outbox.c.id, outbox.c.batch_id)
        )
        returned: list[UUID] = []
        for item_id, batch_id in result:
            # Item больше не у брокера: окно max_in_flight считает dispatched - done.
            self.deltas[batch_id] += CounterDelta(dispatched=-1)
            returned.append(item_id)
        if returned:
            # Новая запись outbox — новая отправка: мёртвые джобы прошлого
            # поколения сверка с DLQ к этому Item уже не относит (UC-15).
            _ = await self.conn.execute(
                update(item)
                .where(item.c.id == any_(_uuids(sorted(returned))))
                .values(generation=item.c.generation + 1)
            )

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
        _ = await self._back_to_outbox(own)
        self.applied.released.update(own)

    async def _back_to_outbox(self, item_ids: Iterable[UUID]) -> set[UUID]:
        """Вернуть активные Items в outbox; у батча на паузе — запаркованными.

        Returns:
            Батчи, чьи Items готовы к отправке сразу: их стоит передать relay.
        """
        waiting: list[UUID] = []
        paused: list[UUID] = []
        ready: set[UUID] = set()
        for item_id in item_ids:
            row = self.items.get(item_id)
            if row is None or row.state.is_terminal:
                continue
            flags = self.batches.get(row.batch_id)
            if flags is not None and flags.paused:
                paused.append(item_id)
            else:
                waiting.append(item_id)
                ready.add(row.batch_id)
        await self._to_outbox(waiting, self.now)
        await self._to_outbox(paused, _INFINITY)
        return ready

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

    def owns(self, item_id: UUID, attempt: int) -> bool:
        """Взят ли lease Item этим процессом для попытки ``attempt`` (UC-03, D-053).

        Читает строку, заблокированную :meth:`lock_leases`. Срок lease не
        проверяется: истёкший, но никем не перехваченный lease всё ещё
        принадлежит попытке. Перехват (claim другого процесса или этого же)
        меняет ``worker_id`` или ``attempt``, sweeper и ``release`` удаляют строку.

        Returns:
            ``True``, если lease принадлежит этой попытке этого процесса.
        """
        lease = self.leases.get(item_id)
        return (
            lease is not None
            and lease.worker_id == self.c.settings.worker_id
            and lease.attempt == attempt
        )

    def holds(self, item: ItemRef, attempt: int) -> bool:
        """Владеет ли попытка ``attempt`` этого процесса активным Item (UC-08).

        Читает строки, заблокированные :meth:`lock_items` и :meth:`lock_leases`:
        Item ``active``, а lease взят этим процессом для этой попытки. Срок
        lease не проверяется: истёкший, но никем не перехваченный lease всё
        ещё принадлежит попытке, а строка Item заблокирована.

        Returns:
            ``True``, если завершить Item вправе эта попытка.
        """
        row = self.items.get(item.id)
        return (
            row is not None
            and row.batch_id == item.batch_id
            and not row.state.is_terminal
            and self.owns(item.id, attempt)
        )

    async def release(self, item_ids: Iterable[UUID]) -> None:
        # UC-04, вердикт RETRY: брокер повторит задачу, lease отпускаем, attempt += 1.
        own = self._owned(item_ids)
        active = [
            item_id
            for item_id in own
            if (row := self.items.get(item_id)) is not None and not row.state.is_terminal
        ]
        # Дубль доставки при живом lease уже вернул брокеру успех: джоба закрыта,
        # ретрая не будет. Такой Item возвращаем в outbox сами, иначе он останется
        # active без lease, outbox и джобы.
        orphaned = [item_id for item_id in active if self.leases[item_id].redelivered]
        # Новая джоба начинает ретраи брокера с нуля: круг «дубль → RETRY → outbox»
        # обрывает только лимит попыток Item, как у sweeper (UC-04, D-012).
        exhausted = await self._out_of_attempts(orphaned)
        await self.finish(
            {
                item_id: (ItemRef(item_id, self.items[item_id].batch_id), _REDELIVERY_EXHAUSTED)
                for item_id in exhausted
            }
        )
        requeued = [item_id for item_id in orphaned if item_id not in exhausted]
        await self._bump_attempts([item_id for item_id in active if item_id not in exhausted])
        await self._delete_leases(own)
        self.applied.kick.update(await self._back_to_outbox(requeued))
        self.applied.requeued.update(requeued)
        self.applied.released.update(own)

    async def _out_of_attempts(self, item_ids: list[UUID]) -> set[UUID]:
        """Items, чей возврат в outbox превысил бы эффективный лимит повторов.

        Returns:
            Items с ``attempt`` не меньше лимита.
        """
        if not item_ids:
            return set()
        item = self.tables.item
        rows = await self.conn.execute(
            select(item.c.id, item.c.task_name, item.c.options).where(
                item.c.id == any_(_uuids(sorted(item_ids)))
            )
        )
        exhausted: set[UUID] = set()
        for item_id, task_name, options in rows:
            raw = cast("dict[object, object]", options) if isinstance(options, dict) else {}
            values = {key: value for key, value in raw.items() if isinstance(key, str)}
            limit = effective_max_retries(values, task_name, self.c.triggers.limits)
            if self.items[item_id].attempt >= limit:
                exhausted.add(item_id)
        return exhausted

    def _beats(self, op: _Heartbeat) -> bool:
        # Без attempt — прежняя проверка только по worker_id.
        if op.attempt is not None:
            return self.owns(op.item.id, op.attempt)
        lease = self.leases.get(op.item.id)
        return lease is not None and lease.worker_id == self.c.settings.worker_id

    async def heartbeat(self, ops: Sequence[_Heartbeat]) -> None:
        """Продлить lease, которые принадлежат попыткам операций (UC-03).

        Операция с ``attempt`` продлевает lease, только если он взят этим
        процессом для этой попытки: устаревшая попытка того же процесса lease
        новой не продлевает. Прогресс нескольких операций Item сливается.
        """
        beats: dict[UUID, _Progress] = {}
        for op in ops:
            if not self._beats(op):
                continue
            self.applied.heartbeats.add(op)
            old_done, old_total = beats.get(op.item.id, (None, None))
            new_done, new_total = op.progress
            beats[op.item.id] = (
                old_done if new_done is None else new_done,
                old_total if new_total is None else new_total,
            )
        own = sorted(beats)
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
        # Items, завершённые release по исчерпанным попыткам, ничего не порождают.
        successful = set(self.applied.finished).intersection(values)
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
_REDELIVERY_EXHAUSTED: Final = FinishResult(
    result_class=ResultClass.ERROR,
    label="exhausted",
    error={
        "type": "RedeliveryExhausted",
        "message": _REDELIVERY_EXHAUSTED_MESSAGE,
    },
)
"""Итог Item, которому ``release`` после дубля доставки не вправе вернуть попытку (UC-04)."""


def _expansion_batches(finish_ops: Sequence[_Finish], batch_ids: list[UUID]) -> bool:
    """Добавить в ``batch_ids`` батчи, которые меняют spawn, expect и под-батчи.

    Returns:
        ``True``, если пачка создаёт под-батчи: строки батчей нужны ``FOR UPDATE``.
    """
    writes_structure = False
    for op in finish_ops:
        value = op.value
        for spawn_request in value.spawns:
            route = spawn_request.route
            batch_ids.extend([route.source_id, route.target_id, route.root_id])
        for expect_request in value.expects:
            route = expect_request.route
            batch_ids.extend([route.source_id, route.target_id, route.root_id])
        for sub_batch in value.sub_batches:
            writes_structure = True
            batch_ids.extend(sub_batch.spec.fed_by)
    return writes_structure


_Owned = TypeVar("_Owned", _Release, _Finish)


def _owners(tx: _Tx, ops: Sequence[_Owned]) -> dict[UUID, _Owned]:
    """Первая операция каждого Item, которой разрешено писать (UC-03).

    Операция с ``attempt`` проходит, только если lease взят этим процессом
    для этой попытки; без ``attempt`` — всегда (CAS по ``state`` сам
    идемпотентен). Устаревшая и текущая попытки того же Item могут прийти в
    одну пачку: применяется операция владельца.

    Returns:
        Item → операция, которая к нему применяется.
    """
    chosen: dict[UUID, _Owned] = {}
    for op in ops:
        if op.item.id in chosen:
            continue
        if op.attempt is None or tx.owns(op.item.id, op.attempt):
            chosen[op.item.id] = op
    return chosen


_USER_SLOTS: Final = 32_767
"""Сколько отрицательных слотов ``th_metric`` у транзакций пути B."""


def _user_slot(delta_ids: Iterable[int]) -> int:
    """Слот ``th_metric`` транзакции пути B.

    Id дельт уникальны и растут, поэтому одновременные транзакции получают
    разные слоты и не блокируют строки друг друга и групповой транзакции
    Completer (слоты процессов неотрицательны).

    Returns:
        ``-1 - min(id дельт) mod 32767``: от ``-32767`` до ``-1``.
    """
    return -1 - min(delta_ids) % _USER_SLOTS


@dataclass(frozen=True, slots=True, kw_only=True)
class _UserCommit:
    """Что сделать после commit транзакции пользователя с ``complete_in``."""

    item_id: UUID
    attempt: int | None
    delta_ids: frozenset[int]
    metric_slot: int
    """Слот транзакции в ``th_metric``."""
    metric_batches: tuple[UUID, ...]


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
        """Completer поверх ``engine``; схема установки записана в ``tables``.

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
        self._held: dict[UUID, tuple[ItemRef, int]] = {}
        """Items с lease этого процесса → попытка, для которой lease взят."""
        self._background: set[asyncio.Task[None]] = set()
        self._attached: set[asyncio.Task[None]] = set()
        self._flushing = False
        self._idle = asyncio.Event()
        self._idle.set()

    # --- публичные операции ------------------------------------------------------------

    async def settled(self) -> None:
        """Дождаться простоя: буфер пуст, flush и после-коммитная работа завершены.

        Результат операции возвращается задаче сразу после commit, а оценка
        политики, финализация и её каскад идут следом — в цикле Completer или
        в фоновой задаче пути ``complete_in``. Метод нужен тому, кто должен
        увидеть их итог детерминированно: тестовому брокеру и остановке.
        """
        while not self._idle.is_set():
            _ = await self._idle.wait()

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        """Event loop, к которому привязан Completer; ``None`` до первой операции."""
        return self._loop

    def attach(self, task: asyncio.Task[None]) -> None:
        """Связать служебную задачу (heartbeat Item) с жизнью Completer.

        :meth:`close` и :meth:`abort` отменяют такие задачи и дожидаются их:
        после закрытия продлевать lease некому.

        Args:
            task: Задача в event loop Completer.
        """
        self._attached.add(task)
        task.add_done_callback(self._attached.discard)

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
        attempt: int | None = None,
        progress_done: int | None = None,
        progress_total: int | None = None,
    ) -> bool:
        """Продлить lease на ``lease_ttl`` и записать прогресс задачи (§9.4).

        Прогресс ``th.item.progress(done, total)`` пишется в ``th_lease`` вместе
        с продлением — отдельных транзакций нет. ``None`` оставляет прежнее
        значение.

        Args:
            item: Item, захваченный этим процессом.
            attempt: Номер попытки из claim. С ним lease продлевается, только
                если взят этим процессом для этой попытки (UC-03): попытка, чей
                lease перехвачен, даже этим же процессом, его не продлевает.
                ``None`` — проверка только по ``worker_id``.
            progress_done: Сколько сделано внутри задачи.
            progress_total: Сколько всего внутри задачи.

        Returns:
            ``False``, если lease уже не у этого процесса или этой попытки
            (истёк и перехвачен, отпущен или удалён sweeper'ом).
        """
        future = self._new_future(bool)
        op = _Heartbeat(item, (progress_done, progress_total), future, attempt)
        return await self._submit(op, future)

    async def release(self, item: ItemRef, *, attempt: int | None = None) -> bool:
        """Отпустить lease перед ретраем брокера: ``attempt += 1`` (UC-04, вердикт RETRY).

        Если за время выполнения брокеру был подтверждён дубль доставки этого
        Item (``th_lease.redelivered``), джоба уже закрыта и ретрая не будет:
        Item в той же транзакции возвращается в outbox, relay отправит его
        заново.

        Args:
            item: Item, захваченный этим процессом.
            attempt: Номер попытки из claim. С ним lease отпускается, только
                если взят этим процессом для этой попытки (UC-03): попытка,
                чей lease перехвачен, даже этим же процессом, ничего не пишет.

        Returns:
            ``True``, если lease был у этого процесса (и этой попытки) и удалён.
        """
        future = self._new_future(bool)
        return await self._submit(_Release(item, future, attempt), future)

    async def finish(
        self,
        item: ItemRef,
        value: FinishResult,
        *,
        attempt: int | None = None,
    ) -> bool:
        """Завершить Item идемпотентным CAS и дождаться commit.

        Args:
            item: Завершаемый Item.
            value: Итог и накопленные динамические операции.
            attempt: Номер попытки из claim. С ним Item завершается, только если
                lease взят этим процессом для этой попытки (UC-03): иначе не
                пишется ничего — ни итог, ни удаление чужого lease. ``None`` —
                только CAS по ``state``.

        Returns:
            ``True``, если этот вызов перевёл Item из active в терминальное
            состояние; ``False`` для повторного или уже завершённого Item и
            для попытки, которой lease уже не принадлежит.

        Raises:
            ConfigurationError: есть динамические операции, но Completer
                создан без Producer.
        """
        if (value.spawns or value.sub_batches) and self.triggers.producer is None:
            raise ConfigurationError(_SPAWN_SERVICES)
        future = self._new_future(bool)
        return await self._submit(_Finish(item, value, future, attempt), future)

    async def complete_in(
        self,
        target: AsyncSession | AsyncConnection,
        item: ItemRef,
        value: FinishResult,
        *,
        attempt: int | None = None,
    ) -> bool:
        """Завершить Item внутри внешней транзакции пользователя (путь B).

        Внешняя транзакция меняет Item и добавляет append-only дельты, но не
        касается горячих строк ``th_counter``. После её commit Completer
        сворачивает дельты в своей короткой транзакции и проверяет
        финализацию. Откат транзакции или savepoint отменяет и записи, и
        зарегистрированное действие после commit.

        С ``attempt`` завершить Item может только попытка, владеющая им: под
        блокировкой строки ``th_item``, затем ``th_lease`` (порядок §9.2)
        проверяется, что lease взят этим процессом для этой попытки. Попытка,
        чей Item завершён без неё (sweeper, отмена) или чей lease перехвачен,
        ничего не пишет — ни в Item, ни в чужой lease.

        Args:
            target: Открытая пользовательская сессия или соединение.
            item: Завершаемый Item.
            value: Итог и накопленные динамические операции.
            attempt: Номер попытки из claim (``ClaimResult.attempt``). ``None``
                — без проверки lease, только CAS по ``state``: для вызова вне
                обёртки задачи, когда lease никто не брал.

        Returns:
            ``True``, если CAS завершил Item; ``False``, если он уже был
            терминальным или попытка им не владеет: записей нет. Вызывающий
            обязан откатить доменные записи транзакции (``th.item.complete_in``
            для этого бросает ``LeaseLostError``).

        Raises:
            ConfigurationError: Нужен spawn, но Completer создан без Producer.
            ClosedError: Completer закрыт; транзакция пользователя не тронута.
        """
        if self._closing:
            raise ClosedError(_CLOSED)
        if (value.spawns or value.sub_batches) and self.triggers.producer is None:
            raise ConfigurationError(_SPAWN_SERVICES)
        conn = await resolve_connection(target)
        tx = _Tx(self, conn, user_tx=True)
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
        if attempt is not None:
            await tx.lock_items([item.id])
            # th_lease — без блокировки и без записи (UC-08): heartbeat этой же
            # попытки не ждёт транзакцию пользователя и не даёт ей 40001.
            await tx.lock_leases([item.id], lock=False)
            if not tx.holds(item, attempt):
                self._forget(item.id, attempt)
                return False
        values = {item.id: (item, value)}
        await tx.finish(values, scalar=True)
        await tx.expand(values)
        if item.id not in tx.applied.finished:
            # CAS не прошёл: Item уже терминальный, ни дельт, ни метрик нет.
            self._forget(item.id, attempt)
            return False
        inserted = await insert_delta(conn, self.tables, tx.deltas, created_at=tx.now)
        delta_ids = frozenset(delta_id for ids in inserted.values() for delta_id in ids)
        # Метрики — в собственный слот транзакции, а не в слот процесса, который
        # в это же время обновляет групповая транзакция Completer (UC-08).
        # Дельта завершённого Item ненулевая, поэтому delta_ids не пуст.
        slot = _user_slot(delta_ids)
        await upsert_metrics(
            conn,
            self.tables,
            {(batch_id, name, slot): n for (batch_id, name, _), n in tx.metrics.items()},
        )
        tx.applied.progress.update(tx.deltas)
        done = _UserCommit(
            item_id=item.id,
            attempt=attempt,
            delta_ids=delta_ids,
            metric_slot=slot,
            metric_batches=tuple(sorted({key[0] for key in tx.metrics})),
        )
        await after_commit(target, lambda: self._schedule_external(tx.applied, done))
        return True

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

        Новые операции после вызова бросают ``ClosedError``; операции,
        принятые раньше, выполняются. Привязанные heartbeat-задачи
        отменяются. Повторный вызов только досылает ``requeue_held``.
        Вызывается в event loop Completer.

        Args:
            requeue_held: Путь ``SIGTERM`` (A-CH-08): lease, которые процесс
                ещё держит (задачи не успели доработать), сразу вернуть в
                outbox, не дожидаясь ``lease_ttl``. Попытка не тратится.

        Raises:
            CompleterError: транзакция возврата lease не прошла; их вернёт
                sweeper по истечении.
        """
        # Транзакция пользователя могла закоммититься прямо перед close, а её
        # after_commit для AsyncConnection доставляет опрос loop, который к этому
        # моменту мог уйти в паузу. Доставка до флага закрытия даёт колбэкам
        # запланировать свёртку и финализацию, и close их дождётся.
        deliver_committed()
        self._closing = True
        self._wakeup.set()
        self._full.set()
        await self._stop(self._attached, cancel=True)
        if self._task is not None:
            # wait, а не await task: отмена вызывающего не должна отменять сброс буфера.
            _ = await asyncio.wait({self._task})
            self._task.result()  # сбой самого цикла сброса не теряется
        await self._stop(self._background, cancel=False)
        if not (requeue_held and self._held):
            return
        refs = [ref for ref, _ in self._held.values()]
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

    async def abort(self) -> None:
        """Оборвать работу, не уложившуюся в срок закрытия (ARCHITECTURE §11.1).

        Цикл сброса, после-коммитные и heartbeat-задачи отменяются, и метод
        дожидается их. Операции, не попавшие в commit, получают
        ``CompleterError``. Lease остаются в БД: их вернёт sweeper по
        истечении ``lease_ttl``. Вызывается в event loop Completer.
        """
        self._closing = True
        tasks = {*self._attached, *self._background}
        if self._task is not None:
            tasks.add(self._task)
        await self._stop(tasks, cancel=True)
        self._task = None
        buffered, self._buffer = self._buffer, []
        self._fail(buffered, _ABORTED)
        self._held.clear()
        self._flushing = False
        self._idle.set()

    @staticmethod
    async def _stop(tasks: Iterable[asyncio.Task[None]], *, cancel: bool) -> None:
        pending = {task for task in tasks if not task.done()}
        if not pending:
            return
        if cancel:
            for task in pending:
                _ = task.cancel()
        _ = await asyncio.wait(pending)

    @staticmethod
    def _fail(ops: Iterable[_Op], message: str, cause: BaseException | None = None) -> None:
        error = CompleterError(message)
        error.__cause__ = cause
        for op in ops:
            if not op.future.done():
                op.future.set_exception(error)

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

    def _forget(self, item_id: UUID, attempt: int | None) -> None:
        """Убрать Item из удерживаемых, если его держит попытка ``attempt``.

        ``None`` — без проверки попытки. Запись более новой попытки того же
        Item (lease перехвачен этим же процессом) остаётся.
        """
        held = self._held.get(item_id)
        if held is not None and (attempt is None or held[1] == attempt):
            del self._held[item_id]

    def _schedule_external(self, applied: _Applied, done: _UserCommit) -> None:
        if self._closing:
            # Completer закрыли, пока транзакция пользователя шла к commit: дельты
            # свернёт и финализацию проверит sweeper. Item остаётся удержанным,
            # чтобы close(requeue_held=True) удалил его lease.
            return
        loop = self._bind()
        task = loop.create_task(
            self._after_external_commit(applied, done),
            name="tallyho-complete-in",
        )
        self._background.add(task)
        self._idle.clear()
        task.add_done_callback(self._background_done)

    def _background_done(self, task: asyncio.Task[None]) -> None:
        self._background.discard(task)
        self._mark_idle()

    def _mark_idle(self) -> None:
        if not self._buffer and not self._flushing and not self._background:
            self._idle.set()

    async def _after_external_commit(self, applied: _Applied, done: _UserCommit) -> None:
        try:
            await run_transaction(
                self.engine,
                lambda conn: self._settle_external(conn, done),
                settings=self.settings.tx,
                policy=self.settings.retry,
            )
            self._notify(len(applied.finished), 0.0, applied)
            await self._after_commit(applied)
        except Exception:  # ruff: ignore[blind-except]  # commit уже состоялся; sweeper повторит fold/finalize и удалит lease
            _log.exception("обработка complete_in после commit упала")
        finally:
            self._forget(done.item_id, done.attempt)

    async def _settle_external(self, conn: AsyncConnection, done: _UserCommit) -> None:
        """Транзакция после commit пути B: lease, дельты, метрики (UC-08).

        Порядок блокировок §9.2: ``th_item`` → ``th_lease`` → ``th_counter``
        → ``th_metric``. Строки слота транзакции в ``th_metric`` другие
        транзакции не трогают, поэтому они забираются до горячих строк: те
        держатся до commit как можно меньше.
        """
        item = self.tables.item
        lease = self.tables.lease
        # Строка Item — под блокировкой; Item, переоткрытый retry_failed после
        # commit, не терминальный, и его новый lease остаётся.
        finished = (
            select(item.c.id)
            .where(item.c.id == done.item_id, item.c.state != _ACTIVE)
            .with_for_update()
            .scalar_subquery()
        )
        _ = await conn.execute(delete(lease).where(lease.c.item_id == finished))
        folded = await fold_delta_ids(conn, self.tables, done.delta_ids)
        taken = await take_metric_slot(
            conn, self.tables, done.metric_batches, slot=done.metric_slot
        )
        slot = self.settings.slot
        metrics = {(batch_id, name, slot): n for (batch_id, name), n in taken.items()}
        await upsert_slots(
            conn,
            self.tables,
            {(batch_id, self.settings.slot): delta for batch_id, delta in folded.items()},
        )
        await upsert_metrics(conn, self.tables, metrics)

    # --- буфер -------------------------------------------------------------------------

    def _new_future(self, _kind: type[_T]) -> asyncio.Future[_T]:
        return self._bind().create_future()

    def _bind(self) -> asyncio.AbstractEventLoop:
        if self._closing:
            raise ClosedError(_CLOSED)
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
            raise ClosedError(_CLOSED)
        self._buffer.append(op)
        self._idle.clear()
        self._notify_buffer()
        self._wakeup.set()
        if len(self._buffer) >= self.settings.max_batch:
            self._full.set()
        return await future

    async def _run(self) -> None:
        tick = self.settings.tick.total_seconds()
        while True:
            if not self._buffer:
                self._flushing = False
                self._mark_idle()
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
            self._flushing = True
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
        except asyncio.CancelledError:
            # abort: транзакция откатилась, ждущие операции не должны зависнуть.
            self._fail(ops, _ABORTED)
            raise
        except Exception as exc:  # ruff: ignore[blind-except]  # ошибка не глотается: уходит в futures операций
            self._fail(ops, _FLUSH_FAILED, exc)
            return
        self._resolve(ops, applied)
        self._notify(len(ops), self.clock.monotonic() - started, applied)
        await self._after_commit(applied)

    async def _apply(self, conn: AsyncConnection, ops: Sequence[_Op]) -> _Applied:
        tx = _Tx(self, conn)
        claims: dict[UUID, ItemRef] = {}
        repeated: set[UUID] = set()
        beats: list[_Heartbeat] = []
        release_ops: list[_Release] = []
        finish_ops: list[_Finish] = []
        for op in ops:
            if isinstance(op, _Claim):
                if op.item.id in claims:
                    repeated.add(op.item.id)
                _ = claims.setdefault(op.item.id, op.item)
            elif isinstance(op, _Heartbeat):
                beats.append(op)
            elif isinstance(op, _Release):
                release_ops.append(op)
            else:
                finish_ops.append(op)
        # Все блокировки строк — в начале и по порядку: batch → item → lease.
        batch_ids = [ref.batch_id for ref in claims.values()]
        # release может вернуть Item в outbox, а там важна пауза батча.
        batch_ids.extend(op.item.batch_id for op in ops if isinstance(op, _Finish | _Release))
        writes_structure = _expansion_batches(finish_ops, batch_ids)
        await tx.lock_batches(batch_ids, write=writes_structure)
        touched = [op.item.id for op in release_ops]
        touched.extend(op.item.id for op in finish_ops)
        await tx.lock_items([*touched, *claims])
        await tx.lock_leases([*touched, *claims, *(op.item.id for op in beats)])
        # release раньше claim: ретрай брокера мог прийти в ту же пачку.
        tx.applied.releasers = _owners(tx, release_ops)
        await tx.release(tx.applied.releasers)
        await tx.claim(claims)
        await tx.mark_redelivered(repeated)
        # Heartbeat — после claim: перехват lease этим же процессом в той же пачке
        # отнимает lease у устаревшей попытки (UC-03).
        await tx.heartbeat(beats)
        # Владение — после claim: перехват lease в этой же пачке отнимает Item у
        # устаревшей попытки (UC-03).
        tx.applied.finishers = _owners(tx, finish_ops)
        finishes = {item_id: (op.item, op.value) for item_id, op in tx.applied.finishers.items()}
        await tx.finish(finishes)
        await tx.expand(finishes)
        await tx.write_counters()
        tx.applied.progress.update(tx.deltas)
        return tx.applied

    def _resolve(self, ops: Sequence[_Op], applied: _Applied) -> None:
        seen: set[UUID] = set()
        for op in ops:
            if isinstance(op, _Claim):
                self._resolve_claim(op, applied.claims[op.item.id], again=op.item.id in seen)
                seen.add(op.item.id)
                continue
            if isinstance(op, _Heartbeat):
                value = op in applied.heartbeats
                if not value:
                    # Lease не у этой попытки: она Item больше не держит.
                    self._forget(op.item.id, op.attempt)
            else:
                chosen = (
                    applied.releasers.get(op.item.id)
                    if isinstance(op, _Release)
                    else applied.finishers.get(op.item.id)
                )
                done = applied.released if isinstance(op, _Release) else applied.finished
                value = chosen is op and op.item.id in done
                if chosen is op:
                    _ = self._held.pop(op.item.id, None)
                elif op.attempt is not None:
                    # Попытка, потерявшая lease, забывает только свою запись: Item,
                    # возможно, снова у этого процесса, но уже с другой попыткой.
                    # Повторная операция того же Item в пачке ничего не меняет.
                    self._forget(op.item.id, op.attempt)
            if not op.future.done():
                op.future.set_result(value)

    def _resolve_claim(self, op: _Claim, result: ClaimResult, *, again: bool) -> None:
        if again and result.run:
            # Второй claim того же Item в одной транзакции — дубль доставки.
            result = ClaimResult(
                outcome=ClaimOutcome.DUPLICATE, attempt=result.attempt, depth=result.depth
            )
        elif result.run:
            self._held[op.item.id] = (op.item, result.attempt)
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
        if applied.requeued:
            _log.info(
                "release: %d Items возвращены в outbox после подтверждённого дубля доставки",
                len(applied.requeued),
            )
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
