"""Продюсер: создание батчей и под-батчей, этапы, добавление Items, seal, expect.

ARCHITECTURE UC-01, UC-02, UC-06, §6.1, §8.1 п.1-2, §11.2.

Все методы :class:`Producer` работают на переданном ``AsyncConnection`` в
открытой транзакции (D-004): это транзакция пользователя (UC-01, откат не
оставляет ни строки) или своя. Commit и ``after_commit`` — забота вызывающего.
«Сейчас» в SQL — только ``sql_now(clock)`` (D-002). Счётчики продюсер пишет
сразу в слот ``th_counter`` (:func:`~tallyho.storage.counters.upsert_slots`),
слот передаёт вызывающий.

Порядок блокировок тот же, что у Completer (§9.2): ``th_batch`` → ``th_item``
→ ``th_counter``.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, TypeVar, cast

from sqlalchemy import (
    BigInteger,
    DateTime,
    Integer,
    Interval,
    LargeBinary,
    SmallInteger,
    Text,
    Uuid,
    exists,
    func,
    literal,
    literal_column,
    select,
    update,
)
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, insert

from tallyho.model.errors import (
    ConfigurationError,
    InvalidStateError,
    NotFoundError,
    SealError,
    SpawnTargetError,
)
from tallyho.model.states import BatchState, ItemState, OnFeederFailed, OutboxKind
from tallyho.storage.counters import CounterDelta, upsert_slots
from tallyho.storage.now import sql_now

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import ColumnElement, ScalarSelect
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.calls import TaskCall
    from tallyho.model.policy import FailurePolicy
    from tallyho.protocols.broker import CallOptionsValidator
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.serialization import PayloadCodec
    from tallyho.storage.tables import Tables

__all__ = [
    "ITEM_CHUNK",
    "MAX_PAYLOAD_BYTES",
    "VIRTUAL_TASK",
    "AddResult",
    "BatchRef",
    "CallbackName",
    "InsertResult",
    "Producer",
    "RootSpec",
    "StoredCallback",
    "SubBatchSpec",
]

_V = TypeVar("_V")

MAX_PAYLOAD_BYTES: Final = 1024 * 1024
"""Предел закодированного payload по умолчанию: ``max_payload_bytes`` flexiq (1 MiB)."""

ITEM_CHUNK: Final = 1000
"""Вызовов в одном запросе add_items: массивы unnest, 6 параметров на чанк."""

VIRTUAL_TASK: Final = "tallyho.sub_batch"
"""``task_name`` виртуального Item под-батча: он не отправляется брокеру."""

_PRODUCER_INTO_STAGE = "в этап с fed_by пишут только его задачи и задачи источников (into=)"
_NOT_OPEN = "батч уже закрыт (seal), финализирован или отменяется: добавлять нельзя"
_SEAL_STAGE = "seal() этапа с fed_by запрещён: его закрывает финализация источников"
_SEAL_CANCELLED = "seal() отменяемого батча запрещён"
_FEED_CYCLE = "fed_by образует цикл"
_FEED_SIBLINGS = "fed_by: источник и этап должны быть под-батчами одного родителя"
_FEED_CLOSED = "fed_by: этап уже закрыт или отменяется"
_FEEDER_FINALIZED = "fed_by: источник уже финализирован"


class CallbackName(StrEnum):
    """Колбэк-задачи батча (ARCHITECTURE §11.2); ключи ``options["callbacks"]``."""

    ON_SUCCEEDED = "on_succeeded"
    ON_COMPLETED_WITH_ERRORS = "on_completed_with_errors"
    ON_FAILED = "on_failed"
    ON_CANCELLED = "on_cancelled"
    ON_FINALIZED_TASK = "on_finalized_task"


_CALLBACK_BROKEN = "options.callbacks: повреждённая запись колбэка"
_OPTIONS_NOT_JSON = "опции колбэка должны сериализоваться в JSON"
_CALL_OPTIONS_NOT_JSON = "опции вызова (queue, опции брокера) должны сериализоваться в JSON"


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredCallback:
    """Колбэк-задача в ``th_batch.options``: аргументы уже закодированы кодеком.

    Finalizer (T4.4) ставит его в ``th_outbox`` как есть.
    """

    task_name: str
    payload: bytes
    queue: str | None = None
    options: Mapping[str, object] = field(default_factory=dict[str, object])

    def to_json(self) -> dict[str, object]:
        """Представление для jsonb.

        Returns:
            Словарь; payload — base64.

        Raises:
            ConfigurationError: опции брокера не сериализуются в JSON.
        """
        options = dict(self.options)
        try:
            _ = json.dumps(options)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(_OPTIONS_NOT_JSON) from exc
        return {
            "task_name": self.task_name,
            "payload": base64.b64encode(self.payload).decode("ascii"),
            "queue": self.queue,
            "options": options,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, object]) -> StoredCallback:
        """Восстановить колбэк из :meth:`to_json`.

        Returns:
            Колбэк, равный исходному.

        Raises:
            ConfigurationError: данные повреждены.
        """
        task_name = data.get("task_name")
        payload = data.get("payload")
        queue = data.get("queue")
        options = data.get("options")
        if not (
            isinstance(task_name, str)
            and isinstance(payload, str)
            and (queue is None or isinstance(queue, str))
            and isinstance(options, dict)
        ):
            raise ConfigurationError(_CALLBACK_BROKEN)
        try:
            raw = base64.b64decode(payload, validate=True)
        except ValueError as exc:  # binascii.Error и не-ASCII строка
            raise ConfigurationError(_CALLBACK_BROKEN) from exc
        return cls(
            task_name=task_name,
            payload=raw,
            queue=queue,
            options=cast("dict[str, object]", options),
        )


def _check_optional(name: str, value: int | None, minimum: int) -> None:
    if value is None:
        return
    if isinstance(value, bool) or value < minimum:
        message = f"{name} должен быть целым >= {minimum}, получено {value!r}"
        raise ConfigurationError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class _BatchSpec:
    """Параметры, общие для корня и под-батча (ARCHITECTURE §11.2)."""

    start_at: datetime | None = None
    deadline: datetime | timedelta | None = None
    callbacks: Mapping[CallbackName, TaskCall] = field(
        default_factory=dict["CallbackName", "TaskCall"]
    )
    failure_policy: FailurePolicy | None = None
    max_in_flight: int | None = None
    expected_total: int | None = None

    def _check(self) -> None:
        _check_optional("max_in_flight", self.max_in_flight, 1)
        _check_optional("expected_total", self.expected_total, 0)


@dataclass(frozen=True, slots=True, kw_only=True)
class RootSpec(_BatchSpec):
    """Параметры корня: ``th.batch(kind, key=, ...)``.

    ``retention``, ``release_required`` и ``max_items`` задаются только на
    корне и наследуются под-батчами.
    """

    kind: str
    key: str | None = None
    max_items: int | None = None
    retention: timedelta | None = None
    release_required: bool = False

    def __post_init__(self) -> None:
        """Проверить параметры.

        Raises:
            ConfigurationError: пустой ``kind`` или недопустимый лимит.
        """
        if not self.kind:
            message = "kind не может быть пустым"
            raise ConfigurationError(message)
        self._check()
        _check_optional("max_items", self.max_items, 1)


@dataclass(frozen=True, slots=True, kw_only=True)
class SubBatchSpec(_BatchSpec):
    """Параметры под-батча или этапа: ``builder.sub_batch(key, fed_by=[...], ...)``.

    ``retention``, ``release_required`` и ``max_items`` копируются от
    родителя (то есть от корня). ``start_at`` по умолчанию — родителя,
    пауза родителя тоже наследуется.

    Attributes:
        key: Ключ под-батча, уникальный в дереве.
        kind: Тип под-батча для tx-хуков; по умолчанию ``"<kind родителя>.<key>"``,
            чтобы хуки корня не срабатывали на этапах.
        fed_by: Источники этапа — под-батчи того же родителя (§8.1).
        on_feeder_failed: Реакция этапа на упавший источник.
        max_depth: Глубина самоподпитки (spawn в свой батч).
    """

    key: str
    kind: str | None = None
    fed_by: Sequence[UUID] = ()
    on_feeder_failed: OnFeederFailed = OnFeederFailed.SEAL
    max_depth: int | None = None

    def __post_init__(self) -> None:
        """Проверить параметры.

        Raises:
            ConfigurationError: пустой ``key`` или ``kind``, недопустимый лимит.
        """
        if not self.key:
            message = "key под-батча не может быть пустым"
            raise ConfigurationError(message)
        if self.kind is not None and not self.kind:
            message = "kind не может быть пустым"
            raise ConfigurationError(message)
        self._check()
        _check_optional("max_depth", self.max_depth, 0)
        object.__setattr__(self, "fed_by", tuple(self.fed_by))


@dataclass(frozen=True, slots=True, kw_only=True)
class _BatchRow:
    """Поля ``th_batch``, нужные продюсеру для проверок и наследования."""

    id: UUID
    root_id: UUID
    parent_id: UUID | None
    kind: str
    state: BatchState
    paused_at: datetime | None
    cancel_requested_at: datetime | None
    start_at: datetime | None
    is_stage: bool
    """Батч — этап с источниками ``th_feed``: продюсер в него не пишет."""


@dataclass(frozen=True, slots=True, kw_only=True)
class AddResult:
    """Итог :meth:`Producer.add_items`.

    Attributes:
        found: Вставлено новых Items.
        duplicates: Отсечено по ``key`` как дубли.
    """

    found: int
    duplicates: int


@dataclass(frozen=True, slots=True, kw_only=True)
class InsertResult:
    """Фактически вставленные строки до записи агрегатных счётчиков."""

    found: int
    weight: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchRef:
    """Созданный или найденный батч.

    Attributes:
        id: Идентификатор батча.
        root_id: Корень дерева (у корня — он сам).
        created: ``False`` — батч с таким ключом уже был (идемпотентный повтор).
    """

    id: UUID
    root_id: UUID
    created: bool
    kind: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class Producer:
    """Операции продюсера над батчами; без состояния, кроме зависимостей.

    Attributes:
        tables: Таблицы установки.
        clock: Часы: «сейчас» в SQL.
        ids: Фабрика идентификаторов батчей и Items.
        codec: Кодек payload адаптера.
        hooks: Реестр tx-хуков: ``th_batch.hooks`` заполняется при создании.
        slot: Слот ``th_counter`` этого процесса.
        max_payload_bytes: Предел закодированного payload.
    """

    tables: Tables
    clock: Clock
    ids: IdFactory
    codec: PayloadCodec
    hooks: HookRegistry
    slot: int = 0
    max_payload_bytes: int = MAX_PAYLOAD_BYTES
    option_validator: CallOptionsValidator | None = None

    # --- корень ------------------------------------------------------------

    async def create_root(self, conn: AsyncConnection, spec: RootSpec) -> BatchRef:
        """Создать корень или вернуть существующий с тем же ``(kind, key)`` (UC-01).

        ``INSERT … ON CONFLICT (kind, key) DO NOTHING RETURNING``: при
        конфликте параллельная транзакция ждёт первую и после её commit
        находит тот же батч.

        Args:
            conn: Соединение в открытой транзакции.
            spec: Параметры корня.

        Returns:
            Ссылка на батч; ``created=False`` для существующего.
        """
        batch = self.tables.batch
        batch_id = self.ids.new_id()
        now = sql_now(self.clock)
        values: dict[str, object] = {
            **self._common_values(spec),
            "id": batch_id,
            "root_id": batch_id,
            "kind": spec.kind,
            "key": spec.key,
            "hooks": list(self.hooks.required_hooks(spec.kind)),
            "max_items": spec.max_items,
            "retention": spec.retention,
            "release_required": spec.release_required,
            "created_at": now,
            "updated_at": now,
            "deadline_at": self._deadline(spec.deadline),
        }
        stmt = (
            insert(batch)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[batch.c.kind, batch.c.key],
                index_where=batch.c.parent_id.is_(None) & batch.c.key.is_not(None),
            )
            .returning(batch.c.id)
        )
        inserted = await conn.scalar(stmt)
        if inserted is not None:
            return BatchRef(id=inserted, root_id=inserted, created=True, kind=spec.kind)
        # Конфликт возможен только при заданном key: партиальный индекс его требует.
        found = await conn.execute(
            select(batch.c.id).where(
                batch.c.kind == spec.kind, batch.c.key == spec.key, batch.c.parent_id.is_(None)
            )
        )
        existing = found.scalar_one()
        return BatchRef(id=existing, root_id=existing, created=False, kind=spec.kind)

    # --- под-батч и этапы --------------------------------------------------

    async def create_sub_batch(
        self, conn: AsyncConnection, parent_id: UUID, spec: SubBatchSpec
    ) -> BatchRef:
        """Создать под-батч или вернуть существующий с тем же ключом (UC-06, §8.1).

        Идемпотентно по ``(root_id, key)``. Родитель получает виртуальный
        Item с ``child_batch_id`` и ``weight=0`` (D-024): для родителя это
        ``pending`` 1 до финализации ребёнка, но на ``ratio`` он не влияет.
        Виртуальный Item не попадает в outbox и не считается в ``tree_total``.
        Источники ``spec.fed_by`` связываются через :meth:`add_feed` только
        при создании.

        Args:
            conn: Соединение в открытой транзакции.
            parent_id: Родитель (корень или другой под-батч).
            spec: Параметры под-батча.

        Returns:
            Ссылка на под-батч; ``created=False`` для существующего.

        """
        ref = await self.create_sub_batch_unaccounted(conn, parent_id, spec)
        if ref.created:
            await upsert_slots(conn, self.tables, {(parent_id, self.slot): CounterDelta(total=1)})
        return ref

    async def create_sub_batch_unaccounted(
        self,
        conn: AsyncConnection,
        parent_id: UUID,
        spec: SubBatchSpec,
        *,
        from_task: bool = False,
    ) -> BatchRef:
        """Создать под-батч без записи счётчика виртуального Item.

        Примитив Completer: вызывающий при ``created=True`` добавляет
        ``CounterDelta(total=1)`` родителю в свой единый агрегированный write.

        Returns:
            Созданный или ранее существовавший под-батч.

        Raises:
            ConfigurationError: ключ в дереве уже занят под-батчем другого
                родителя.
            SealError: задача пытается создать новый под-батч у терминального
                или отменяемого родителя.
        """
        parent = (await self._lock_batches(conn, [parent_id]))[parent_id]
        existing = await self._find_child(conn, parent, spec.key)
        if existing is not None:
            return existing
        if from_task:
            if parent.state.is_terminal or parent.cancel_requested_at is not None:
                raise SealError(_NOT_OPEN)
        else:
            self._check_producer_target(parent)
        child_id = self.ids.new_id()
        virtual_id = self.ids.new_id()
        now = sql_now(self.clock)
        kind = spec.kind or f"{parent.kind}.{spec.key}"
        batch = self.tables.batch

        def inherited(column: ColumnElement[_V]) -> ScalarSelect[_V]:
            # Параметры корня копируются из строки родителя в том же INSERT.
            return select(column).where(batch.c.id == parent.id).scalar_subquery()

        values: dict[str, object] = {
            **self._common_values(spec),
            "id": child_id,
            "root_id": parent.root_id,
            "parent_id": parent.id,
            "parent_item_id": virtual_id,
            "kind": kind,
            "key": spec.key,
            "hooks": list(self.hooks.required_hooks(kind)),
            "start_at": spec.start_at or parent.start_at,
            "paused_at": parent.paused_at,
            "max_items": inherited(batch.c.max_items),
            "max_depth": spec.max_depth,
            "on_feeder_failed": int(spec.on_feeder_failed),
            "retention": inherited(batch.c.retention),
            "release_required": inherited(batch.c.release_required),
            "created_at": now,
            "updated_at": now,
            "deadline_at": self._deadline(spec.deadline),
        }
        stmt = (
            insert(batch)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[batch.c.root_id, batch.c.key],
                index_where=batch.c.parent_id.is_not(None),
            )
            .returning(batch.c.id)
        )
        if await conn.scalar(stmt) is None:
            # Ключ занят под-батчем другого родителя, созданным параллельно.
            raise ConfigurationError(_key_taken(spec.key))
        item = self.tables.item
        _ = await conn.execute(
            insert(item).values(
                id=virtual_id,
                batch_id=parent.id,
                state=int(ItemState.ACTIVE),
                task_name=VIRTUAL_TASK,
                payload=b"",
                child_batch_id=child_id,
                weight=0,
                created_at=now,
            )
        )
        if spec.fed_by:
            await self.add_feed(conn, child_id, spec.fed_by)
        return BatchRef(id=child_id, root_id=parent.root_id, created=True, kind=kind)

    async def _find_child(
        self, conn: AsyncConnection, parent: _BatchRow, key: str
    ) -> BatchRef | None:
        batch = self.tables.batch
        found = await conn.execute(
            select(batch.c.id, batch.c.parent_id).where(
                batch.c.root_id == parent.root_id,
                batch.c.key == key,
                batch.c.parent_id.is_not(None),
            )
        )
        row = found.one_or_none()
        if row is None:
            return None
        child_id, child_parent = row
        if child_parent != parent.id:
            raise ConfigurationError(_key_taken(key))
        return BatchRef(id=child_id, root_id=parent.root_id, created=False)

    async def add_feed(
        self, conn: AsyncConnection, fed_id: UUID, feeder_ids: Iterable[UUID]
    ) -> None:
        """Связать этап ``fed_id`` с источниками ``feeder_ids`` в ``th_feed`` (§8.1 п.1).

        Источники — под-батчи того же родителя, граф ``fed_by`` ацикличен.
        Строки батчей блокируются ``FOR UPDATE`` в порядке id: финализация
        источника (CAS его строки) ждёт commit, а потом видит новую связь и
        закрывает этап. Уже существующие связи пропускаются.

        Args:
            conn: Соединение в открытой транзакции.
            fed_id: Этап.
            feeder_ids: Источники.

        Raises:
            ConfigurationError: источник — сам этап, другой родитель, цикл.
            SealError: этап уже закрыт или отменяется.
            InvalidStateError: источник уже финализирован.
        """
        feeders = sorted(set(feeder_ids))
        if fed_id in feeders:
            raise ConfigurationError(_FEED_CYCLE)
        rows = await self._lock_batches(conn, [fed_id, *feeders])
        fed = rows[fed_id]
        for feeder_id in feeders:
            if fed.parent_id is None or rows[feeder_id].parent_id != fed.parent_id:
                raise ConfigurationError(_FEED_SIBLINGS)
        feed = self.tables.feed
        linked = set(
            await conn.scalars(
                select(feed.c.feeder_id).where(
                    feed.c.fed_id == fed_id, feed.c.feeder_id.in_(feeders)
                )
            )
        )
        new = [feeder_id for feeder_id in feeders if feeder_id not in linked]
        if not new:
            return
        if fed.state is not BatchState.OPEN or fed.cancel_requested_at is not None:
            raise SealError(_FEED_CLOSED)
        if any(rows[feeder_id].state.is_terminal for feeder_id in new):
            raise InvalidStateError(_FEEDER_FINALIZED)
        if await self._reaches(conn, fed_id, new):
            raise ConfigurationError(_FEED_CYCLE)
        _ = await conn.execute(
            insert(feed)
            .values([{"feeder_id": feeder_id, "fed_id": fed_id} for feeder_id in new])
            .on_conflict_do_nothing()
        )

    async def _reaches(self, conn: AsyncConnection, start: UUID, targets: list[UUID]) -> bool:
        # Этапы ниже start по th_feed (feeder → fed); источник среди них — цикл.
        feed = self.tables.feed
        down = (
            select(feed.c.fed_id.label("id")).where(feed.c.feeder_id == start).cte(recursive=True)
        )
        down = down.union(select(feed.c.fed_id).where(feed.c.feeder_id == down.c.id))
        found = await conn.scalar(select(down.c.id).where(down.c.id.in_(targets)).limit(1))
        return found is not None

    # --- Items ---------------------------------------------------------------

    async def add_items(
        self, conn: AsyncConnection, batch_id: UUID, calls: Iterable[TaskCall]
    ) -> AddResult:
        """Добавить Items в батч: ``th_item`` + ``th_outbox`` чанками через ``unnest`` (UC-02).

        Чанк из :data:`ITEM_CHUNK` вызовов — один запрос: ``INSERT th_item …
        ON CONFLICT (batch_id, key) DO NOTHING RETURNING`` и ``INSERT
        th_outbox`` из вернувшихся строк. Дубли по ``key`` (с уже
        существующими Items и внутри одного вызова) не вставляются и
        считаются в ``duplicates``. В конце один ``upsert_slots``: ``total``,
        ``w_total``, ``duplicates`` батча и ``tree_total`` корня.

        ``available_at`` записей outbox — ``start_at`` батча или «сейчас»;
        у батча на паузе — ``infinity`` (запись припаркована до resume).
        Строка батча держится ``FOR SHARE``: параллельные продюсеры не мешают
        друг другу, а seal, отмена и финализация ждут commit.

        Опции постановки из ``TaskCall`` (``queue`` и ``options``) пишутся в
        ``th_item.options`` (D-033): relay передаёт их в ``Message.options``
        при каждой отправке, в том числе повторной.

        Ошибки: ``SpawnTargetError`` — батч является этапом с ``fed_by``;
        ``SealError`` — батч закрыт, финализирован или отменяется;
        ``ConfigurationError`` — payload больше ``max_payload_bytes`` или
        опции вызова не сериализуются в JSON.

        Args:
            conn: Соединение в открытой транзакции.
            batch_id: Батч.
            calls: Вызовы задач; итерируется один раз, потоково.

        Returns:
            Сколько Items вставлено и сколько отсечено как дубли.
        """
        target = (await self._lock_batches(conn, [batch_id], share=True))[batch_id]
        self._check_producer_target(target)
        available_at = self._available_at(target)
        found = duplicates = w_total = 0
        for chunk in _chunked(calls, ITEM_CHUNK):
            inserted = await self.insert_items(
                conn,
                target.id,
                calls=chunk,
                depths=[0] * len(chunk),
                available_at=available_at,
            )
            found += inserted.found
            duplicates += len(chunk) - inserted.found
            w_total += inserted.weight
        batch_key = (target.id, self.slot)
        root_key = (target.root_id, self.slot)
        deltas = {batch_key: CounterDelta(total=found, w_total=w_total, duplicates=duplicates)}
        deltas[root_key] = deltas.get(root_key, CounterDelta()) + CounterDelta(tree_total=found)
        await upsert_slots(conn, self.tables, deltas)
        return AddResult(found=found, duplicates=duplicates)

    async def insert_items(
        self,
        conn: AsyncConnection,
        batch_id: UUID,
        *,
        calls: list[TaskCall],
        depths: Sequence[int],
        available_at: ColumnElement[datetime],
    ) -> InsertResult:
        """Вставить Items и outbox без счётчиков (примитив Producer/Completer).

        Вызывающий агрегирует ``total/w_total/duplicates/tree_total`` и пишет
        счётчики один раз в своей транзакции. Пустой список допустим.

        Returns:
            Число и суммарный вес строк после дедупликации.

        Raises:
            ConfigurationError: число depth не совпадает с числом вызовов,
                payload превышает лимит или опции не сериализуются в JSON.
        """
        if len(calls) != len(depths):
            message = "для каждого spawn требуется depth"
            raise ConfigurationError(message)
        if not calls:
            return InsertResult(found=0, weight=0)
        if self.option_validator is not None:
            for call in calls:
                self.option_validator.validate_options(_call_option_values(call))
        ids = [self.ids.new_id() for _ in calls]
        payloads = [self._encode(call) for call in calls]
        options = [_call_options(call) for call in calls]
        item = self.tables.item
        outbox = self.tables.outbox
        rows = (
            func.unnest(
                literal(ids, ARRAY(Uuid())),
                literal([call.task_name for call in calls], ARRAY(Text())),
                literal(payloads, ARRAY(LargeBinary())),
                literal([call.key for call in calls], ARRAY(Text())),
                literal([call.weight for call in calls], ARRAY(Integer())),
                literal(list(depths), ARRAY(SmallInteger())),
                literal(options, ARRAY(Text())),
            )
            .table_valued("id", "task_name", "payload", "key", "weight", "depth", "options")
            .render_derived("u")
        )
        source = select(
            rows.c.id,
            literal(batch_id, Uuid()),
            _small_literal(ItemState.ACTIVE),
            rows.c.task_name,
            rows.c.payload,
            rows.c.key,
            rows.c.weight,
            rows.c.depth,
            sql_cast(rows.c.options, JSONB),
            sql_now(self.clock),
        )
        inserted = (
            insert(item)
            .from_select(
                [
                    "id",
                    "batch_id",
                    "state",
                    "task_name",
                    "payload",
                    "key",
                    "weight",
                    "depth",
                    "options",
                    "created_at",
                ],
                source,
            )
            .on_conflict_do_nothing(
                index_elements=[item.c.batch_id, item.c.key],
                index_where=item.c.key.is_not(None),
            )
            .returning(item.c.id, item.c.task_name, item.c.weight)
            .cte("ins")
        )
        queued = (
            insert(outbox)
            .from_select(
                ["id", "kind", "batch_id", "item_id", "task_name", "available_at"],
                select(
                    inserted.c.id,
                    _small_literal(OutboxKind.ITEM),
                    literal(batch_id, Uuid()),
                    inserted.c.id,
                    inserted.c.task_name,
                    available_at,
                ),
            )
            .cte("ob")
        )
        summary = (
            select(
                sql_cast(func.count(), BigInteger),
                sql_cast(func.coalesce(func.sum(inserted.c.weight), 0), BigInteger),
            )
            .select_from(inserted)
            .add_cte(queued)
        )
        count, weight = (await conn.execute(summary)).one()
        return InsertResult(found=count, weight=weight)

    def available_at(self, *, paused: bool, start_at: datetime | None) -> ColumnElement[datetime]:
        """Время доступности outbox для известного состояния целевого батча.

        Returns:
            ``infinity`` на паузе, ``start_at`` или текущее время SQL.
        """
        if paused:
            return literal_column("'infinity'::timestamptz", DateTime(timezone=True))
        if start_at is not None:
            return literal(start_at, DateTime(timezone=True))
        return sql_now(self.clock)

    def _available_at(self, target: _BatchRow) -> ColumnElement[datetime]:
        return self.available_at(paused=target.paused_at is not None, start_at=target.start_at)

    # --- seal ----------------------------------------------------------------

    async def seal(self, conn: AsyncConnection, batch_id: UUID) -> bool:
        """Закрыть батч: ``open → sealed`` (UC-02, §6.1).

        Повторный seal ничего не меняет. Проверку ``pending == 0`` и
        финализацию после commit запускает вызывающий (Finalizer, T4.4).

        Args:
            conn: Соединение в открытой транзакции.
            batch_id: Батч.

        Returns:
            ``True``, если батч был открыт и закрыт этим вызовом.

        Raises:
            SealError: батч — этап с ``fed_by`` (его закрывает финализация
                источников) или у батча запрошена отмена.
        """
        target = (await self._lock_batches(conn, [batch_id]))[batch_id]
        if target.is_stage:
            raise SealError(_SEAL_STAGE)
        if target.cancel_requested_at is not None:
            raise SealError(_SEAL_CANCELLED)
        if target.state is not BatchState.OPEN:
            return False
        batch = self.tables.batch
        _ = await conn.execute(
            update(batch)
            .where(batch.c.id == batch_id, batch.c.state == _small_literal(BatchState.OPEN))
            .values(state=_small_literal(BatchState.SEALED), updated_at=sql_now(self.clock))
        )
        return True

    # --- expect ------------------------------------------------------------

    async def expect(self, conn: AsyncConnection, batch_id: UUID, n: int) -> None:
        """Сообщить ожидаемое число Items: ``expected_total = GREATEST(expected_total, n)``.

        Значение только растёт; ``NULL`` в ``GREATEST`` PostgreSQL игнорирует.
        ``n < 0`` — ``ConfigurationError``.

        Args:
            conn: Соединение в открытой транзакции.
            batch_id: Батч.
            n: Ожидаемое число Items, ``>= 0``.

        Raises:
            NotFoundError: батча нет.
        """
        _check_optional("n", n, 0)
        batch = self.tables.batch
        stmt = (
            update(batch)
            .where(batch.c.id == batch_id)
            .values(expected_total=_greatest(batch.c.expected_total, n))
            .returning(batch.c.id)
        )
        if await conn.scalar(stmt) is None:
            raise NotFoundError(str(batch_id))

    # --- общее -------------------------------------------------------------

    async def _lock_batches(
        self, conn: AsyncConnection, batch_ids: Iterable[UUID], *, share: bool = False
    ) -> dict[UUID, _BatchRow]:
        # FOR UPDATE (или FOR SHARE) в порядке id: глобальный порядок блокировок th_batch.
        ids = sorted(set(batch_ids))
        batch = self.tables.batch
        feed = self.tables.feed
        is_stage = exists().where(feed.c.fed_id == batch.c.id).label("is_stage")
        result = await conn.execute(
            select(
                batch.c.id,
                batch.c.root_id,
                batch.c.parent_id,
                batch.c.kind,
                batch.c.state,
                batch.c.paused_at,
                batch.c.cancel_requested_at,
                batch.c.start_at,
                is_stage,
            )
            .where(batch.c.id.in_(ids))
            .order_by(batch.c.id)
            .with_for_update(of=batch, read=share)
        )
        rows: dict[UUID, _BatchRow] = {}
        for (
            batch_id,
            root_id,
            parent_id,
            kind,
            state,
            paused_at,
            cancel_requested_at,
            start_at,
            stage,
        ) in result:
            rows[batch_id] = _BatchRow(
                id=batch_id,
                root_id=root_id,
                parent_id=parent_id,
                kind=kind,
                state=BatchState(state),
                paused_at=paused_at,
                cancel_requested_at=cancel_requested_at,
                start_at=start_at,
                is_stage=stage,
            )
        missing = [batch_id for batch_id in ids if batch_id not in rows]
        if missing:
            message = f"батч не найден: {missing[0]}"
            raise NotFoundError(message)
        return rows

    @staticmethod
    def _check_producer_target(target: _BatchRow) -> None:
        # Продюсер пишет только в открытый батч без запроса отмены и не в этап (§6.1, §8.1 п.2).
        if target.is_stage:
            raise SpawnTargetError(_PRODUCER_INTO_STAGE)
        if target.state is not BatchState.OPEN or target.cancel_requested_at is not None:
            raise SealError(_NOT_OPEN)

    def _common_values(self, spec: _BatchSpec) -> dict[str, object]:
        return {
            "state": int(BatchState.OPEN),
            "start_at": spec.start_at,
            "options": self._options(spec),
            "expected_total": spec.expected_total,
            "max_in_flight": spec.max_in_flight,
        }

    def _options(self, spec: _BatchSpec) -> dict[str, object]:
        options: dict[str, object] = {}
        if spec.failure_policy is not None:
            options["failure_policy"] = spec.failure_policy.to_json()
        if spec.callbacks:
            options["callbacks"] = {
                CallbackName(name).value: self._callback(call).to_json()
                for name, call in spec.callbacks.items()
            }
        return options

    def _callback(self, call: TaskCall) -> StoredCallback:
        if self.option_validator is not None:
            self.option_validator.validate_options(_call_option_values(call))
        return StoredCallback(
            task_name=call.task_name,
            payload=self._encode(call),
            queue=call.queue,
            options=call.options,
        )

    def _encode(self, call: TaskCall) -> bytes:
        payload = self.codec.encode(call.task_name, call.args, call.kwargs)
        if len(payload) > self.max_payload_bytes:
            message = (
                f"payload задачи {call.task_name!r} занимает {len(payload)} байт,"
                f" предел {self.max_payload_bytes}"
            )
            raise ConfigurationError(message)
        return payload

    def _deadline(self, deadline: datetime | timedelta | None) -> ColumnElement[datetime] | None:
        if deadline is None:
            return None
        if isinstance(deadline, timedelta):
            return sql_now(self.clock) + literal(deadline, Interval())
        return literal(deadline, DateTime(timezone=True))


def _call_options(call: TaskCall) -> str | None:
    # Опции постановки Item (D-033): queue вызова и опции брокера; пусто → NULL.
    options = dict(call.options)
    if call.queue is not None:
        options["queue"] = call.queue
    if not options:
        return None
    try:
        return json.dumps(options)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(_CALL_OPTIONS_NOT_JSON) from exc


def _call_option_values(call: TaskCall) -> dict[str, object]:
    options = dict(call.options)
    if call.queue is not None:
        options["queue"] = call.queue
    return options


def _chunked(calls: Iterable[TaskCall], size: int) -> Iterator[list[TaskCall]]:
    chunk: list[TaskCall] = []
    for call in calls:
        chunk.append(call)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _small_literal(value: int) -> ColumnElement[int]:
    # Коды состояний — литералами, а не bind-параметрами (D-020).
    return literal_column(str(int(value)), SmallInteger())


def _key_taken(key: str) -> str:
    return f"ключ под-батча {key!r} в дереве уже занят под-батчем другого родителя"


def _greatest(column: ColumnElement[int], value: int) -> ColumnElement[int]:
    return func.greatest(column, literal(value, BigInteger()))
