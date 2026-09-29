"""Хелперы интеграционных тестов для работы с PostgreSQL.

Каждый тест работает в своей схеме (``temporary_schema``), поэтому тесты не мешают
друг другу ни при случайном порядке (``pytest-randomly``), ни при параллельном запуске
(``pytest -n``), ни на общей БД из ``TALLYHO_TEST_DSN``.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = ["schema_exists", "temporary_schema", "unique_schema_name"]

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
