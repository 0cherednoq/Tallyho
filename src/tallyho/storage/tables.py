"""Описание таблиц и индексов tallyho (ARCHITECTURE §5.1, §5.2).

Схема установки записана в самих таблицах (``build_metadata(schema=...)``),
поэтому запросы находят их на любом соединении — и на нашем, и на соединении
пользователя с его собственными ``search_path`` и ``schema_translate_map``
(Fix-12). При ``schema=None`` таблицы описаны без схемы и ищутся так же, как
таблицы пользователя без схемы. Имена таблиц и индексов начинаются с
``prefix`` (по умолчанию ``th_``).

Колонки описаны классами :class:`~sqlalchemy.TypedColumns`, поэтому
``tables.item.c.state`` типизирован для mypy и basedpyright.

Правила, которые держит этот модуль:

* на ``th_item`` нет индексов по изменяемым колонкам (``state``, ``label``,
  ``result``, ``error``, ``finished_at``): finish — HOT update без записи в
  индексы;
* FK не объявляются: целостность держит библиотека, retention удаляет деревом;
* ``th_batch_attr`` — отдельная таблица: jsonb с GIN-индексом не лежит в часто
  обновляемой строке ``th_batch``;
* partial-индексы записаны через коды :mod:`tallyho.model.states` (D-005).
  Чтобы планировщик взял такой индекс, запрос должен содержать то же условие
  литералом, а не bind-параметром.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast, final

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Identity,
    Index,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    SmallInteger,
    Table,
    Text,
    Uuid,
    and_,
    any_,
    literal,
    or_,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateTable

from tallyho.model.states import TERMINAL_THRESHOLD, BatchState, OnFeederFailed

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime, timedelta
    from uuid import UUID

    from sqlalchemy import TypedColumns

    _native_typed_columns = True
else:
    try:
        from sqlalchemy import TypedColumns
    except ImportError:
        # SQLAlchemy 2.0 не экспортирует TypedColumns. Runtime-fallback ниже
        # сохраняет ту же декларативную форму, а статическую типизацию проверяем
        # по SQLAlchemy 2.1, где этот API определён.
        class TypedColumns:
            """Совместимая runtime-база контейнеров колонок для SQLAlchemy 2.0."""

        _native_typed_columns = False
    else:
        _native_typed_columns = True

_ColumnsT = TypeVar("_ColumnsT", bound=TypedColumns)
_POSTGRESQL_WITH = "with"


class _CreateTableCompiler(Protocol):
    """Минимальная часть DDLCompiler, используемая compatibility hook."""

    def visit_create_table(self, element: CreateTable, **kwargs: object) -> str:
        """Скомпилировать штатный CREATE TABLE."""
        ...


class _CopyableColumn(Protocol):
    """Внутренний API копирования Column, общий для ветки SQLAlchemy 2.0."""

    def _copy(self) -> Column[object]:
        """Вернуть непривязанную копию колонки."""
        ...


class _TableFactory(Protocol):
    """Общая runtime-сигнатура Table для обеих веток SQLAlchemy."""

    def __call__(
        self,
        name: str,
        metadata: MetaData,
        *items: object,
        postgresql_with: dict[str, int] | None,
    ) -> Table[TypedColumns]:
        """Создать таблицу."""
        ...


if not _native_typed_columns:
    # В 2.1 этот dialect kwarg зарегистрирован самим PostgreSQL dialect.
    Table.argument_for("postgresql", _POSTGRESQL_WITH, None)


@compiles(CreateTable, "postgresql")
def _compile_create_table(element: CreateTable, compiler: object, **kwargs: object) -> str:
    """Добавить PostgreSQL storage parameters на SQLAlchemy 2.0.

    Returns:
        Скомпилированный PostgreSQL ``CREATE TABLE``.
    """
    ddl_compiler = cast("_CreateTableCompiler", compiler)
    statement = ddl_compiler.visit_create_table(element, **kwargs)
    if _native_typed_columns:
        return statement
    parameters = cast(
        "dict[str, int | bool | str | None] | None",
        element.element.dialect_options["postgresql"][_POSTGRESQL_WITH],
    )
    if not parameters:
        return statement
    rendered = ", ".join(
        key
        if value is None
        else f"{key} = {str(value).lower() if isinstance(value, bool) else value}"
        for key, value in parameters.items()
    )
    return f"{statement.rstrip()}\n WITH ({rendered})\n\n"


def _table(
    name: str,
    metadata: MetaData,
    columns: type[_ColumnsT],
    *,
    postgresql_with: dict[str, int] | None = None,
) -> Table[_ColumnsT]:
    """Создать типизированную таблицу на SQLAlchemy 2.0 и 2.1.

    Returns:
        Таблица с типизированной коллекцией колонок.
    """
    factory = cast("_TableFactory", cast("object", Table))
    if _native_typed_columns:
        return cast(
            "Table[_ColumnsT]",
            factory(name, metadata, columns, postgresql_with=postgresql_with),
        )
    copied: list[Column[object]] = []
    namespace = cast("Mapping[str, object]", cast("object", vars(columns)))
    for column_name, value in namespace.items():
        if isinstance(value, Column):
            column = cast("_CopyableColumn", value)._copy()  # ruff: ignore[private-member-access]  # pyright: ignore[reportPrivateUsage]  # Column.copy() deprecated without a public SQLAlchemy 2.0 replacement
            column.name = column_name
            column.key = column_name
            copied.append(column)
    return cast(
        "Table[_ColumnsT]",
        factory(name, metadata, *copied, postgresql_with=postgresql_with),
    )


__all__ = [
    "DEFAULT_PREFIX",
    "PROGRESS_HOOK",
    "BatchAttrColumns",
    "BatchColumns",
    "CounterColumns",
    "CounterDeltaColumns",
    "ExpiryColumns",
    "FeedColumns",
    "ItemColumns",
    "ItemMarkColumns",
    "LeaseColumns",
    "MetaColumns",
    "MetricColumns",
    "OutboxColumns",
    "Tables",
    "WindowColumns",
    "build_metadata",
]

DEFAULT_PREFIX: Final = "th_"
"""Префикс имён таблиц и индексов по умолчанию (ARCHITECTURE §15)."""

PROGRESS_HOOK: Final = "progress"
"""Имя хука в ``th_batch.hooks``, по которому Snapshotter выбирает батчи."""

_ITEM_FILLFACTOR: Final = 85
_HOT_FILLFACTOR: Final = 50

# Агрессивный autovacuum для churn-таблиц (COUNTERS §3.6): порог по числу
# мёртвых строк, а не по доле от размера таблицы.
_AGGRESSIVE_AUTOVACUUM: Final[dict[str, int]] = {
    "autovacuum_vacuum_scale_factor": 0,
    "autovacuum_vacuum_threshold": 1000,
}

_OPEN_OR_SEALED: Final = (int(BatchState.OPEN), int(BatchState.SEALED))


def _uuid(*, nullable: bool = False) -> Column[UUID]:
    return Column(Uuid(), nullable=nullable)


def _text(*, nullable: bool = False) -> Column[str]:
    return Column(Text(), nullable=nullable)


def _utc(*, nullable: bool = False) -> Column[datetime]:
    return Column(DateTime(timezone=True), nullable=nullable)


def _small(*, default: int | None = None, nullable: bool = False) -> Column[int]:
    server_default = None if default is None else text(str(default))
    return Column(SmallInteger(), nullable=nullable, server_default=server_default)


def _big(*, default: int | None = None, nullable: bool = False) -> Column[int]:
    server_default = None if default is None else text(str(default))
    return Column(BigInteger(), nullable=nullable, server_default=server_default)


def _int(*, default: int | None = None, nullable: bool = False) -> Column[int]:
    server_default = None if default is None else text(str(default))
    return Column(Integer(), nullable=nullable, server_default=server_default)


def _bool(*, default: bool) -> Column[bool]:
    return Column(Boolean(), nullable=False, server_default=text(str(default).lower()))


def _interval() -> Column[timedelta]:
    return Column(Interval(), nullable=True)


def _jsonb(*, nullable: bool = False, server_default: str | None = None) -> Column[object]:
    default = None if server_default is None else text(server_default)
    return Column(JSONB(), nullable=nullable, server_default=default)


def _text_array() -> Column[list[str]]:
    return Column(ARRAY(Text()), nullable=False, server_default=text("'{}'::text[]"))


@final
class BatchColumns(TypedColumns):
    """Колонки ``th_batch``."""

    id = Column(Uuid(), primary_key=True)
    root_id = _uuid()
    parent_id = _uuid(nullable=True)
    parent_item_id = _uuid(nullable=True)
    kind = _text()
    key = _text(nullable=True)
    state = _small()
    paused_at = _utc(nullable=True)
    cancel_requested_at = _utc(nullable=True)
    cancel_reason = _text(nullable=True)
    start_at = _utc(nullable=True)
    options = _jsonb(server_default="'{}'::jsonb")
    hooks = _text_array()
    expected_total = _big(nullable=True)
    max_in_flight = _int(nullable=True)
    max_items = _big(nullable=True)
    max_depth = _small(nullable=True)
    on_feeder_failed = _small(default=OnFeederFailed.SEAL)
    deadline_at = _utc(nullable=True)
    snap_seq = _int(default=0)
    hook_attempts = _small(default=0)
    hook_error = _text(nullable=True)
    retention = _interval()
    release_required = _bool(default=False)
    released_at = _utc(nullable=True)
    created_at = _utc()
    updated_at = _utc()
    finished_at = _utc(nullable=True)


@final
class BatchAttrColumns(TypedColumns):
    """Колонки ``th_batch_attr``: неизменяемые ``attributes`` и ``memo`` корня (D-038).

    Строка есть только у корня с атрибутами или ``memo``; пишется один раз.
    """

    batch_id = Column(Uuid(), primary_key=True)
    attributes = _jsonb(server_default="'{}'::jsonb")
    memo = _jsonb(nullable=True)


@final
class ItemColumns(TypedColumns):
    """Колонки ``th_item``."""

    id = Column(Uuid(), primary_key=True)
    batch_id = _uuid()
    state = _small()
    label = _text(nullable=True)
    attempt = _small(default=0)
    depth = _small(default=0)
    task_name = _text()
    payload = Column(LargeBinary(), nullable=False)
    options = _jsonb(nullable=True)
    key = _text(nullable=True)
    child_batch_id = _uuid(nullable=True)
    weight = _int(default=1)
    result = _jsonb(nullable=True)
    error = _jsonb(nullable=True)
    created_at = _utc()
    finished_at = _utc(nullable=True)
    generation = _int(default=0)
    """Поколение отправки: растёт при каждом возврате Item в outbox (ARCHITECTURE §5.1)."""


@final
class OutboxColumns(TypedColumns):
    """Колонки ``th_outbox``."""

    id = Column(Uuid(), primary_key=True)
    kind = _small()
    batch_id = _uuid()
    item_id = _uuid(nullable=True)
    task_name = _text(nullable=True)
    payload = Column(LargeBinary())
    options = _jsonb(nullable=True)
    """Опции постановки колбэка; опции Item хранятся в ``th_item.options`` (D-033)."""
    available_at = _utc()
    attempts = _small(default=0)


@final
class LeaseColumns(TypedColumns):
    """Колонки ``th_lease``."""

    item_id = Column(Uuid(), primary_key=True)
    batch_id = _uuid()
    lease_until = _utc()
    worker_id = _text()
    attempt = _small()
    progress_done = _big(nullable=True)
    progress_total = _big(nullable=True)
    redelivered = _bool(default=False)
    """Брокеру подтверждён дубль доставки при живом lease: ретрая от него не будет (UC-04)."""


@final
class FeedColumns(TypedColumns):
    """Колонки ``th_feed``: какой батч (``feeder_id``) наполняет какой этап (``fed_id``)."""

    feeder_id = Column(Uuid(), primary_key=True)
    fed_id = Column(Uuid(), primary_key=True)


@final
class CounterColumns(TypedColumns):
    """Колонки ``th_counter``: слот счётчиков батча (слот = процесс, COUNTERS §3.3)."""

    batch_id = Column(Uuid(), primary_key=True)
    slot = Column(SmallInteger(), primary_key=True, autoincrement=False)
    total = _big(default=0)
    ok = _big(default=0)
    skip = _big(default=0)
    error = _big(default=0)
    cancelled = _big(default=0)
    dispatched = _big(default=0)
    w_total = _big(default=0)
    w_done = _big(default=0)
    duplicates = _big(default=0)
    skipped_by_limit = _big(default=0)
    tree_total = _big(default=0)


@final
class CounterDeltaColumns(TypedColumns):
    """Колонки ``th_counter_delta``: дельты из транзакций пользователя (путь B).

    На каждый счётчик ``th_counter`` — колонка ``d_<имя>`` в том же порядке.
    """

    id = Column(BigInteger(), Identity(always=True), primary_key=True)
    batch_id = _uuid()
    d_total = _big(default=0)
    d_ok = _big(default=0)
    d_skip = _big(default=0)
    d_error = _big(default=0)
    d_cancelled = _big(default=0)
    d_dispatched = _big(default=0)
    d_w_total = _big(default=0)
    d_w_done = _big(default=0)
    d_duplicates = _big(default=0)
    d_skipped_by_limit = _big(default=0)
    d_tree_total = _big(default=0)
    created_at = _utc()


@final
class MetricColumns(TypedColumns):
    """Колонки ``th_metric``: labels и пользовательские метрики по слотам."""

    batch_id = Column(Uuid(), primary_key=True)
    name = Column(Text(), primary_key=True)
    slot = Column(SmallInteger(), primary_key=True, autoincrement=False)
    value = _big(default=0)


@final
class ItemMarkColumns(TypedColumns):
    """Колонки ``th_item_mark``: только помеченные Items."""

    batch_id = Column(Uuid(), primary_key=True)
    label = Column(Text(), primary_key=True)
    item_id = Column(Uuid(), primary_key=True)


@final
class ExpiryColumns(TypedColumns):
    """Колонки ``th_expiry``: срок Items с flexiq-опцией ``expires`` (ARCHITECTURE §11.4)."""

    item_id = Column(Uuid(), primary_key=True)
    expires_at = _utc()


@final
class WindowColumns(TypedColumns):
    """Колонки ``th_window``: отправленные и не завершённые Items батчей с ``max_in_flight``.

    Строку вставляет relay при захвате записи outbox, удаляет завершение Item.
    Размер таблицы не больше суммы окон активных батчей.
    """

    item_id = Column(Uuid(), primary_key=True)
    batch_id = _uuid()


@final
class MetaColumns(TypedColumns):
    """Колонки ``th_meta``: служебные значения установки, в том числе версия схемы."""

    key = Column(Text(), primary_key=True)
    value = _text()


@dataclass(frozen=True, slots=True, kw_only=True)
class Tables:
    """Все таблицы одной установки tallyho и их общий ``MetaData``."""

    metadata: MetaData
    batch: Table[BatchColumns]
    batch_attr: Table[BatchAttrColumns]
    item: Table[ItemColumns]
    outbox: Table[OutboxColumns]
    lease: Table[LeaseColumns]
    feed: Table[FeedColumns]
    counter: Table[CounterColumns]
    counter_delta: Table[CounterDeltaColumns]
    metric: Table[MetricColumns]
    item_mark: Table[ItemMarkColumns]
    expiry: Table[ExpiryColumns]
    window: Table[WindowColumns]
    meta: Table[MetaColumns]


def build_metadata(
    prefix: str = DEFAULT_PREFIX,
    *,
    schema: str | None = None,
) -> Tables:
    """Описать таблицы tallyho с префиксом ``prefix`` в новом ``MetaData``.

    Args:
        prefix: префикс имён таблиц и индексов. Проверка допустимости —
            задача установки (``migrate``).
        schema: схема установки. Имя попадает в каждый запрос, поэтому
            таблицы находятся на любом соединении. ``None`` — таблицы без
            схемы: их адрес определяет соединение (``search_path`` или его
            ``schema_translate_map``).

    Returns:
        Таблицы установки.
    """
    metadata = MetaData(schema=schema)
    return Tables(
        metadata=metadata,
        batch=_batch(metadata, prefix),
        batch_attr=_batch_attr(metadata, prefix),
        item=_item(metadata, prefix),
        outbox=_outbox(metadata, prefix),
        lease=_lease(metadata, prefix),
        feed=_feed(metadata, prefix),
        counter=_table(
            f"{prefix}counter",
            metadata,
            CounterColumns,
            postgresql_with={"fillfactor": _HOT_FILLFACTOR, **_AGGRESSIVE_AUTOVACUUM},
        ),
        counter_delta=_counter_delta(metadata, prefix),
        metric=_table(
            f"{prefix}metric",
            metadata,
            MetricColumns,
            postgresql_with={"fillfactor": _HOT_FILLFACTOR, **_AGGRESSIVE_AUTOVACUUM},
        ),
        item_mark=_table(f"{prefix}item_mark", metadata, ItemMarkColumns),
        expiry=_expiry(metadata, prefix),
        window=_window(metadata, prefix),
        meta=_table(f"{prefix}meta", metadata, MetaColumns),
    )


def _batch(metadata: MetaData, prefix: str) -> Table[BatchColumns]:
    name = f"{prefix}batch"
    batch = _table(name, metadata, BatchColumns)
    c = batch.c
    Index(
        f"{name}_kind_key_uq",
        c.kind,
        c.key,
        unique=True,
        postgresql_where=and_(c.parent_id.is_(None), c.key.is_not(None)),
    )
    Index(
        f"{name}_root_key_uq",
        c.root_id,
        c.key,
        unique=True,
        postgresql_where=c.parent_id.is_not(None),
    )
    Index(f"{name}_parent_idx", c.parent_id, postgresql_where=c.parent_id.is_not(None))
    # «Активен» = state < 10: то же множество, что open/sealed/finalizing (D-005).
    Index(f"{name}_active_updated_idx", c.updated_at, postgresql_where=c.state < TERMINAL_THRESHOLD)
    Index(
        f"{name}_deadline_idx",
        c.deadline_at,
        postgresql_where=and_(c.deadline_at.is_not(None), c.state.in_(_OPEN_OR_SEALED)),
    )
    Index(
        f"{name}_progress_idx",
        c.id,
        postgresql_where=and_(
            c.state.in_(_OPEN_OR_SEALED), literal(PROGRESS_HOOK) == any_(c.hooks)
        ),
    )
    Index(
        f"{name}_retention_idx",
        c.finished_at,
        postgresql_where=and_(
            c.id == c.root_id,
            c.finished_at.is_not(None),
            c.retention.is_not(None),
            or_(~c.release_required, c.released_at.is_not(None)),
        ),
    )
    # Листинг корней одного kind, keyset по id DESC.
    Index(f"{name}_kind_idx", c.kind, c.id, postgresql_where=c.parent_id.is_(None))
    return batch


def _batch_attr(metadata: MetaData, prefix: str) -> Table[BatchAttrColumns]:
    # Отдельная таблица, а не колонки th_batch: строка батча часто обновляется,
    # и каждое не-HOT обновление заново писало бы jsonb в GIN.
    name = f"{prefix}batch_attr"
    attr = _table(name, metadata, BatchAttrColumns)
    Index(
        f"{name}_attributes_idx",
        attr.c.attributes,
        postgresql_using="gin",
        postgresql_ops={"attributes": "jsonb_path_ops"},
    )
    return attr


def _item(metadata: MetaData, prefix: str) -> Table[ItemColumns]:
    name = f"{prefix}item"
    item = _table(name, metadata, ItemColumns, postgresql_with={"fillfactor": _ITEM_FILLFACTOR})
    c = item.c
    # Только неизменяемые колонки: finish остаётся HOT update.
    Index(f"{name}_batch_idx", c.batch_id, c.id)
    Index(
        f"{name}_batch_key_uq",
        c.batch_id,
        c.key,
        unique=True,
        postgresql_where=c.key.is_not(None),
    )
    return item


def _outbox(metadata: MetaData, prefix: str) -> Table[OutboxColumns]:
    name = f"{prefix}outbox"
    outbox = _table(name, metadata, OutboxColumns, postgresql_with=_AGGRESSIVE_AUTOVACUUM)
    Index(f"{name}_available_idx", outbox.c.available_at)
    # (batch_id, available_at): pause/cancel по батчу и окно max_in_flight —
    # запаркованные (infinity) и готовые к отправке записи одного батча.
    Index(f"{name}_batch_idx", outbox.c.batch_id, outbox.c.available_at)
    return outbox


def _lease(metadata: MetaData, prefix: str) -> Table[LeaseColumns]:
    name = f"{prefix}lease"
    lease = _table(name, metadata, LeaseColumns, postgresql_with=_AGGRESSIVE_AUTOVACUUM)
    Index(f"{name}_until_idx", lease.c.lease_until)
    Index(f"{name}_batch_idx", lease.c.batch_id)
    return lease


def _feed(metadata: MetaData, prefix: str) -> Table[FeedColumns]:
    name = f"{prefix}feed"
    feed = _table(name, metadata, FeedColumns)
    Index(f"{name}_fed_idx", feed.c.fed_id)
    return feed


def _counter_delta(metadata: MetaData, prefix: str) -> Table[CounterDeltaColumns]:
    name = f"{prefix}counter_delta"
    delta = _table(name, metadata, CounterDeltaColumns, postgresql_with=_AGGRESSIVE_AUTOVACUUM)
    Index(f"{name}_batch_idx", delta.c.batch_id)
    Index(f"{name}_created_idx", delta.c.created_at, delta.c.id)
    return delta


def _expiry(metadata: MetaData, prefix: str) -> Table[ExpiryColumns]:
    # Sweeper ищет не захваченные вовремя Items по сроку.
    name = f"{prefix}expiry"
    expiry = _table(name, metadata, ExpiryColumns)
    Index(f"{name}_expires_idx", expiry.c.expires_at)
    return expiry


def _window(metadata: MetaData, prefix: str) -> Table[WindowColumns]:
    # Relay считает занятое окно батча по batch_id.
    name = f"{prefix}window"
    window = _table(name, metadata, WindowColumns, postgresql_with=_AGGRESSIVE_AUTOVACUUM)
    Index(f"{name}_batch_idx", window.c.batch_id)
    return window
