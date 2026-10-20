"""Миграции через Alembic ``op`` на реальном PostgreSQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import func, select

from tallyho.storage.alembic import upgrade
from tallyho.storage.migrations import SCHEMA_VERSION, VERSION_KEY, migrate
from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from sqlalchemy import Connection
    from sqlalchemy.ext.asyncio import AsyncEngine


async def _version(engine: AsyncEngine, schema: str) -> str | None:
    meta = build_metadata().meta
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        value = await conn.scalar(select(meta.c.value).where(meta.c.key == VERSION_KEY))
    return None if value is None else str(value)


async def _upgrade_to(engine: AsyncEngine, schema: str, version: int) -> None:
    def run(conn: Connection) -> None:
        upgrade(Operations(MigrationContext.configure(conn)), version=version, schema=schema)

    async with engine.begin() as conn:
        await conn.run_sync(run)


async def test_alembic_upgrade_then_migrate_is_noop(engine: AsyncEngine, schema: str) -> None:
    # Ревизия на версию: по одной на каждую версию схемы, как в документации.
    for version in range(1, SCHEMA_VERSION + 1):
        await _upgrade_to(engine, schema, version)
        assert await _version(engine, schema) == str(version)
    assert await migrate(engine, schema) == SCHEMA_VERSION


async def test_alembic_revision_of_version_one_is_upgraded_by_migrate(
    engine: AsyncEngine, schema: str
) -> None:
    # База из опубликованной 0.0.2a1: версия 1, закладки ещё нет (D-088).
    await _upgrade_to(engine, schema, 1)
    assert await _version(engine, schema) == "1"
    async with engine.connect() as conn:
        missing = await conn.scalar(select(func.to_regclass(f"{schema}.th_checkpoint")))
    assert missing is None

    assert await migrate(engine, schema) == SCHEMA_VERSION
    async with engine.connect() as conn:
        created = await conn.scalar(select(func.to_regclass(f"{schema}.th_checkpoint")))
    assert created is not None
    assert await _version(engine, schema) == str(SCHEMA_VERSION)
