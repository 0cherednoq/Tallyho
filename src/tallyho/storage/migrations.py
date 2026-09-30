"""Миграции схемы tallyho и установка в схему пользователя (ARCHITECTURE §11.1).

Миграция — список SQL-операций для одной версии схемы. Её выполняют два
пути, и оба получают одни и те же операции:

* :func:`migrate` — сам tallyho: своя транзакция, ``pg_advisory_xact_lock``,
  ``SET LOCAL lock_timeout``, текущая версия читается из ``th_meta``,
  применяются только недостающие версии (повторный вызов ничего не делает);
* :func:`tallyho.storage.alembic.upgrade` — ревизия Alembic пользователя.

Имена схемы и таблиц не подставляются в SQL строками: операции строятся из
копии :func:`~tallyho.storage.tables.build_metadata` со схемой, а кавычки
расставляет диалект. Префикс проверяется регулярным выражением, схема — по
ограничениям PostgreSQL на идентификаторы.

Схема версии 1 заморожена без ``th_counter_delta.created_at``. Версия 2
добавляет timestamp и индекс для ограниченной по возрасту свёртки Sweeper;
``build_metadata`` всегда описывает итоговую актуальную схему.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy import MetaData, Text, func, literal_column, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable, ExecutableDDLElement

from tallyho.model.errors import ConfigurationError
from tallyho.storage.tables import DEFAULT_PREFIX, build_metadata

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Table, TypedColumns
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql.base import Executable
    from sqlalchemy.sql.compiler import DDLCompiler

    from tallyho.storage.tables import MetaColumns

__all__ = [
    "DEFAULT_LOCK_TIMEOUT",
    "SCHEMA_VERSION",
    "VERSION_KEY",
    "migrate",
    "migration_statements",
    "validate_prefix",
    "validate_schema",
]

SCHEMA_VERSION: Final = 2
"""Версия схемы, которую знает эта версия библиотеки."""

VERSION_KEY: Final = "schema_version"
"""Ключ версии схемы в ``th_meta``."""

DEFAULT_LOCK_TIMEOUT: Final = timedelta(seconds=5)
"""``lock_timeout`` миграции (ARCHITECTURE §15): DDL не ждёт чужие блокировки дольше."""

_PREFIX_RE: Final = re.compile(r"[a-z_][a-z0-9_]{0,15}")
# PostgreSQL молча обрезает идентификаторы длиннее NAMEDATALEN - 1 байт.
_MAX_IDENTIFIER_BYTES: Final = 63
_LOCK_NAMESPACE: Final = "tallyho.migrate"
_DELTA_TIMESTAMP_VERSION: Final = 2

_PREFIX_ERROR: Final = "Префикс должен соответствовать ^[a-z_][a-z0-9_]{0,15}$"
_SCHEMA_ERROR: Final = "Имя схемы должно быть непустым, без NUL и не длиннее 63 байт в UTF-8"
_LOCK_TIMEOUT_ERROR: Final = "lock_timeout не может быть отрицательным"
_UNKNOWN_VERSION_ERROR: Final = "Неизвестная версия схемы tallyho"
_NEWER_SCHEMA_ERROR: Final = "Схема tallyho в БД новее, чем знает эта версия библиотеки"


def validate_prefix(prefix: str) -> str:
    """Проверить префикс имён таблиц.

    Args:
        prefix: префикс, например ``"th_"``.

    Returns:
        Тот же префикс.

    Raises:
        ConfigurationError: префикс не соответствует ``^[a-z_][a-z0-9_]{0,15}$``.
    """
    if _PREFIX_RE.fullmatch(prefix) is None:
        raise ConfigurationError(_PREFIX_ERROR)
    return prefix


def validate_schema(schema: str | None) -> str | None:
    """Проверить имя схемы.

    Спецсимволы допустимы (A-NF-03): имя всегда экранируется диалектом.

    Args:
        schema: имя схемы или ``None`` (схема из ``search_path``).

    Returns:
        То же имя.

    Raises:
        ConfigurationError: имя пустое, содержит NUL или длиннее 63 байт.
    """
    if schema is None:
        return None
    if not schema or "\x00" in schema or len(schema.encode()) > _MAX_IDENTIFIER_BYTES:
        raise ConfigurationError(_SCHEMA_ERROR)
    return schema


@dataclass(frozen=True, slots=True)
class _Installation:
    """Таблицы одной установки в её схеме (в порядке имён, как ``sorted_tables``)."""

    schema: str | None
    tables: list[Table[TypedColumns]]
    meta: Table[MetaColumns]
    counter_delta: Table[TypedColumns]


def _installation(
    schema: str | None, prefix: str, *, delta_timestamps: bool = True
) -> _Installation:
    """Таблицы ``build_metadata(prefix)`` в схеме ``schema``.

    Returns:
        Таблицы установки; при ``schema=None`` — без квалификатора схемы.
    """
    source = build_metadata(prefix, _delta_timestamps=delta_timestamps)
    meta = source.meta
    counter_delta: Table[TypedColumns] = source.counter_delta
    tables: list[Table[TypedColumns]] = [
        source.batch,
        source.item,
        source.outbox,
        source.lease,
        source.feed,
        source.counter,
        source.counter_delta,
        source.metric,
        source.item_mark,
        source.expiry,
        source.window,
    ]
    if schema is not None:
        target = MetaData()
        meta = meta.to_metadata(target, schema=schema)
        tables = [table.to_metadata(target, schema=schema) for table in tables]
        counter_delta = next(table for table in tables if table.name == source.counter_delta.name)
    return _Installation(
        schema=schema,
        tables=sorted([*tables, meta], key=lambda t: t.name),
        meta=meta,
        counter_delta=counter_delta,
    )


def _v1(installation: _Installation) -> list[Executable]:
    statements: list[Executable] = []
    if installation.schema is not None:
        statements.append(CreateSchema(installation.schema, if_not_exists=True))
    for table in installation.tables:
        statements.append(CreateTable(table))
        statements.extend(
            CreateIndex(index) for index in sorted(table.indexes, key=lambda i: str(i.name))
        )
    return statements


class _AddCounterDeltaTimestamp(ExecutableDDLElement):
    """Добавить timestamp с безопасно скомпилированным именем таблицы."""

    table: Table[TypedColumns]

    def __init__(self, table: Table[TypedColumns]) -> None:
        self.table = table


class _DropCounterDeltaTimestampDefault(ExecutableDDLElement):
    """Убрать временный DEFAULT после заполнения исторических строк."""

    table: Table[TypedColumns]

    def __init__(self, table: Table[TypedColumns]) -> None:
        self.table = table


@compiles(_AddCounterDeltaTimestamp, "postgresql")
def _compile_add_counter_delta_timestamp(
    element: _AddCounterDeltaTimestamp, compiler: object, **_: object
) -> str:
    preparer = cast("DDLCompiler", compiler).preparer
    table = preparer.format_table(element.table)
    return (
        f"ALTER TABLE {table} ADD COLUMN created_at TIMESTAMP WITH TIME ZONE "
        "NOT NULL DEFAULT CURRENT_TIMESTAMP"
    )


@compiles(_DropCounterDeltaTimestampDefault, "postgresql")
def _compile_drop_counter_delta_timestamp_default(
    element: _DropCounterDeltaTimestampDefault, compiler: object, **_: object
) -> str:
    preparer = cast("DDLCompiler", compiler).preparer
    table = preparer.format_table(element.table)
    return f"ALTER TABLE {table} ALTER COLUMN created_at DROP DEFAULT"


def _v2(installation: _Installation) -> list[Executable]:
    created_index = next(
        index
        for index in installation.counter_delta.indexes
        if str(index.name).endswith("_created_idx")
    )
    return [
        _AddCounterDeltaTimestamp(installation.counter_delta),
        _DropCounterDeltaTimestampDefault(installation.counter_delta),
        CreateIndex(created_index),
    ]


_MIGRATIONS: Final[Mapping[int, Callable[[_Installation], list[Executable]]]] = {
    1: _v1,
    2: _v2,
}


def _lock_timeout_statement(lock_timeout: timedelta) -> Executable:
    if lock_timeout < timedelta(0):
        raise ConfigurationError(_LOCK_TIMEOUT_ERROR)
    milliseconds = lock_timeout // timedelta(milliseconds=1)
    # SET не принимает bind-параметры; значение — целое число, которое мы посчитали сами.
    return text(f"SET LOCAL lock_timeout = '{milliseconds}ms'")


def _set_version_statement(meta: Table[MetaColumns], version: int) -> Executable:
    # Значения — литералами, без bind-параметров: offline-режим Alembic
    # (``upgrade --sql``) печатает параметры как есть, а не подставляет их.
    stmt = insert(meta).values(
        key=literal_column(f"'{VERSION_KEY}'", Text()),
        value=literal_column(f"'{version:d}'", Text()),
    )
    return stmt.on_conflict_do_update(
        index_elements=[meta.c.key], set_={"value": stmt.excluded.value}
    )


def migration_statements(
    version: int,
    *,
    schema: str | None,
    prefix: str = DEFAULT_PREFIX,
    lock_timeout: timedelta = DEFAULT_LOCK_TIMEOUT,
) -> list[Executable]:
    """Операции одной миграции: ``lock_timeout``, DDL версии и запись версии в ``th_meta``.

    Операции выполняются в одной транзакции; управлять ею — дело вызывающего.

    Args:
        version: номер версии схемы (1 … :data:`SCHEMA_VERSION`).
        schema: схема установки или ``None`` (из ``search_path``).
        prefix: префикс имён таблиц.
        lock_timeout: сколько DDL ждёт чужие блокировки.

    Returns:
        Список выполняемых конструкций SQLAlchemy.

    Raises:
        ConfigurationError: неизвестная версия, неверные схема, префикс или
            ``lock_timeout``.
    """
    validate_schema(schema)
    validate_prefix(prefix)
    migration = _MIGRATIONS.get(version)
    if migration is None:
        raise ConfigurationError(_UNKNOWN_VERSION_ERROR)
    installation = _installation(
        schema,
        prefix,
        delta_timestamps=version >= _DELTA_TIMESTAMP_VERSION,
    )
    return [
        _lock_timeout_statement(lock_timeout),
        *migration(installation),
        _set_version_statement(installation.meta, version),
    ]


def _advisory_key(schema: str | None) -> int:
    # Ключ — по схеме, а не по префиксу: CREATE SCHEMA IF NOT EXISTS из двух
    # установок с разными префиксами тоже должен идти по очереди.
    digest = hashlib.blake2b(f"{_LOCK_NAMESPACE}:{schema or ''}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


async def _current_version(conn: AsyncConnection, meta: Table[MetaColumns]) -> int:
    qualified = conn.dialect.identifier_preparer.format_table(meta)
    # to_regclass разбирает квалифицированное имя и не падает, если схемы нет.
    exists = await conn.scalar(select(func.to_regclass(qualified).is_not(None)))
    if not exists:
        return 0
    value = await conn.scalar(select(meta.c.value).where(meta.c.key == VERSION_KEY))
    return 0 if value is None else int(value)


async def migrate(
    engine: AsyncEngine,
    schema: str | None,
    prefix: str = DEFAULT_PREFIX,
    *,
    lock_timeout: timedelta = DEFAULT_LOCK_TIMEOUT,
) -> int:
    """Установить или обновить схему tallyho до :data:`SCHEMA_VERSION`.

    Всё выполняется в одной своей транзакции под ``pg_advisory_xact_lock``:
    параллельные вызовы идут по очереди, повторный вызов ничего не меняет.

    Args:
        engine: движок SQLAlchemy приложения.
        schema: схема установки (создаётся, если её нет) или ``None``.
        prefix: префикс имён таблиц.
        lock_timeout: сколько DDL ждёт чужие блокировки.

    Returns:
        Версия схемы после миграции.

    Raises:
        ConfigurationError: неверные схема, префикс или ``lock_timeout``;
            схема в БД новее библиотеки.
    """
    validate_schema(schema)
    validate_prefix(prefix)
    lock = _lock_timeout_statement(lock_timeout)
    meta = _installation(schema, prefix).meta
    async with engine.begin() as conn:
        # Сначала ждём свою очередь, потом ограничиваем ожидание DDL.
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": _advisory_key(schema)}
        )
        await conn.execute(lock)
        current = await _current_version(conn, meta)
        if current > SCHEMA_VERSION:
            raise ConfigurationError(_NEWER_SCHEMA_ERROR)
        for version in range(current + 1, SCHEMA_VERSION + 1):
            for statement in migration_statements(
                version, schema=schema, prefix=prefix, lock_timeout=lock_timeout
            ):
                await conn.execute(statement)
    return SCHEMA_VERSION
