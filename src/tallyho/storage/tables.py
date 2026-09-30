"""Описание таблиц и индексов tallyho (ARCHITECTURE §5.1, §5.2).

Таблицы описаны без схемы (``schema=None``): схему пользователя подставляет
``schema_translate_map={None: schema}`` в опциях выполнения соединения. Имена
таблиц и индексов начинаются с ``prefix`` (по умолчанию ``th_``).

Колонки описаны классами :class:`~sqlalchemy.TypedColumns`, поэтому
``tables.item.c.state`` типизирован для mypy и basedpyright.

Правила, которые держит этот модуль:

* на ``th_item`` нет индексов по изменяемым колонкам (``state``, ``label``,
  ``result``, ``error``, ``finished_at``): finish — HOT update без записи в
  индексы;
* FK не объявляются: целостность держит библиотека, retention удаляет деревом;
* partial-индексы записаны через коды :mod:`tallyho.model.states` (D-005).
  Чтобы планировщик взял такой индекс, запрос должен содержать то же условие
  литералом, а не bind-параметром.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, final

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    SmallInteger,
    Table,
    Text,
    TypedColumns,
    Uuid,
    and_,
    any_,
    literal,
    or_,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from tallyho.model.states import TERMINAL_THRESHOLD, BatchState, OnFeederFailed

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from uuid import UUID

__all__ = [
    "DEFAULT_PREFIX",
    "PROGRESS_HOOK",
    "BatchColumns",
    "FeedColumns",
    "ItemColumns",
    "LeaseColumns",
    "OutboxColumns",
    "Tables",
    "build_metadata",
]

DEFAULT_PREFIX: Final = "th_"
"""Префикс имён таблиц и индексов по умолчанию (ARCHITECTURE §15)."""

PROGRESS_HOOK: Final = "progress"
"""Имя хука в ``th_batch.hooks``, по которому Snapshotter выбирает батчи."""

_ITEM_FILLFACTOR: Final = 85

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
    key = _text(nullable=True)
    child_batch_id = _uuid(nullable=True)
    weight = _int(default=1)
    result = _jsonb(nullable=True)
    error = _jsonb(nullable=True)
    created_at = _utc()
    finished_at = _utc(nullable=True)


@final
class OutboxColumns(TypedColumns):
    """Колонки ``th_outbox``."""

    id = Column(Uuid(), primary_key=True)
    kind = _small()
    batch_id = _uuid()
    item_id = _uuid(nullable=True)
    task_name = _text(nullable=True)
    payload = Column(LargeBinary())
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


@final
class FeedColumns(TypedColumns):
    """Колонки ``th_feed``: какой батч (``feeder_id``) наполняет какой этап (``fed_id``)."""

    feeder_id = Column(Uuid(), primary_key=True)
    fed_id = Column(Uuid(), primary_key=True)


@dataclass(frozen=True, slots=True, kw_only=True)
class Tables:
    """Все таблицы одной установки tallyho и их общий ``MetaData``."""

    metadata: MetaData
    batch: Table[BatchColumns]
    item: Table[ItemColumns]
    outbox: Table[OutboxColumns]
    lease: Table[LeaseColumns]
    feed: Table[FeedColumns]


def build_metadata(prefix: str = DEFAULT_PREFIX) -> Tables:
    """Описать таблицы tallyho с префиксом ``prefix`` в новом ``MetaData``.

    Args:
        prefix: префикс имён таблиц и индексов. Проверка допустимости —
            задача установки (``migrate``).

    Returns:
        Таблицы установки; схема подставляется через ``schema_translate_map``.
    """
    metadata = MetaData()
    return Tables(
        metadata=metadata,
        batch=_batch(metadata, prefix),
        item=_item(metadata, prefix),
        outbox=_outbox(metadata, prefix),
        lease=_lease(metadata, prefix),
        feed=_feed(metadata, prefix),
    )


def _batch(metadata: MetaData, prefix: str) -> Table[BatchColumns]:
    name = f"{prefix}batch"
    batch = Table(name, metadata, BatchColumns)
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
    return batch


def _item(metadata: MetaData, prefix: str) -> Table[ItemColumns]:
    name = f"{prefix}item"
    item = Table(name, metadata, ItemColumns, postgresql_with={"fillfactor": _ITEM_FILLFACTOR})
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
    outbox = Table(name, metadata, OutboxColumns, postgresql_with=_AGGRESSIVE_AUTOVACUUM)
    Index(f"{name}_available_idx", outbox.c.available_at)
    Index(f"{name}_batch_idx", outbox.c.batch_id)
    return outbox


def _lease(metadata: MetaData, prefix: str) -> Table[LeaseColumns]:
    name = f"{prefix}lease"
    lease = Table(name, metadata, LeaseColumns, postgresql_with=_AGGRESSIVE_AUTOVACUUM)
    Index(f"{name}_until_idx", lease.c.lease_until)
    Index(f"{name}_batch_idx", lease.c.batch_id)
    return lease


def _feed(metadata: MetaData, prefix: str) -> Table[FeedColumns]:
    name = f"{prefix}feed"
    feed = Table(name, metadata, FeedColumns)
    Index(f"{name}_fed_idx", feed.c.fed_id)
    return feed
