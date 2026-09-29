"""Хелперы интеграционных тестов для работы с PostgreSQL.

Каждый тест работает в своей схеме (``temporary_schema``), поэтому тесты не мешают
друг другу ни при случайном порядке (``pytest-randomly``), ни при параллельном запуске
(``pytest -n``), ни на общей БД из ``TALLYHO_TEST_DSN``.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = [
    "LockRow",
    "deadlock_count",
    "held_locks",
    "schema_exists",
    "temporary_schema",
    "unique_schema_name",
]

SCHEMA_PREFIX = "t_"


def unique_schema_name() -> str:
    """Имя схемы, уникальное для теста: префикс, xdist-воркер и случайный суффикс."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    return f"{SCHEMA_PREFIX}{worker}_{uuid.uuid4().hex[:12]}"


def _quoted(schema: str) -> str:
    # Имя генерируем сами, но всё равно не подставляем его в SQL без кавычек.
    return '"' + schema.replace('"', '""') + '"'


@contextlib.asynccontextmanager
async def temporary_schema(engine: AsyncEngine) -> AsyncGenerator[str]:
    """Создаёт пустую схему и удаляет её со всем содержимым на выходе."""
    schema = unique_schema_name()
    async with engine.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA {_quoted(schema)}"))
    try:
        yield schema
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {_quoted(schema)} CASCADE"))


async def schema_exists(conn: AsyncConnection, schema: str) -> bool:
    """Есть ли схема с таким именем в текущей БД."""
    found = await conn.scalar(
        text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :schema)"),
        {"schema": schema},
    )
    return bool(found)


async def deadlock_count(conn: AsyncConnection) -> int:
    """Счётчик дедлоков текущей БД из ``pg_stat_database.deadlocks``.

    Счётчик общий на БД, поэтому сравнивайте разницу «до/после», а не абсолютное
    значение. Backend, поймавший дедлок, публикует статистику с задержкой (до ~1 с;
    сразу — после ``pg_stat_force_next_flush()`` или при отключении), поэтому
    после провокации дедлока значение стоит опрашивать. Снимок статистики текущей
    транзакции сбрасывается здесь же.
    """
    await conn.execute(text("SELECT pg_stat_clear_snapshot()"))
    value = await conn.scalar(
        text("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
    )
    return int(value or 0)


@dataclass(frozen=True, slots=True)
class LockRow:
    """Строка ``pg_locks`` в удобном для assert'ов виде."""

    pid: int
    locktype: str
    relation: str | None
    mode: str
    granted: bool


_LOCKS_SQL = text("""
    SELECT l.pid, l.locktype, n.nspname || '.' || c.relname AS relation, l.mode, l.granted
    FROM pg_locks l
    LEFT JOIN pg_class c ON c.oid = l.relation
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE (l.database IS NULL
           OR l.database = (SELECT oid FROM pg_database WHERE datname = current_database()))
      AND l.pid IS NOT NULL
      AND (:include_own OR l.pid <> pg_backend_pid())
    ORDER BY l.pid, l.locktype, relation, l.mode
""")


async def held_locks(conn: AsyncConnection, *, include_own: bool = False) -> list[LockRow]:
    """Блокировки из ``pg_locks`` в текущей БД; по умолчанию — кроме своего backend'а.

    ``relation`` — имя отношения со схемой (``schema.table``), для блокировок
    не на отношениях (``transactionid``, ``virtualxid``, ``advisory``) — ``None``.
    Ожидающие блокировки тоже возвращаются, у них ``granted=False``.
    """
    rows = await conn.execute(_LOCKS_SQL, {"include_own": include_own})
    return [
        LockRow(
            pid=int(row.pid),
            locktype=str(row.locktype),
            relation=None if row.relation is None else str(row.relation),
            mode=str(row.mode),
            granted=bool(row.granted),
        )
        for row in rows
    ]
