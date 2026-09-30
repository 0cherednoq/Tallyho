"""Миграции через Alembic ``op`` на реальном PostgreSQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import select

from tallyho.storage.alembic import upgrade
from tallyho.storage.migrations import SCHEMA_VERSION, VERSION_KEY, migrate
from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from sqlalchemy import Connection
    from sqlalchemy.ext.asyncio import AsyncEngine


async def test_alembic_upgrade_then_migrate_is_noop(engine: AsyncEngine, schema: str) -> None:
    def run(conn: Connection) -> None:
        upgrade(Operations(MigrationContext.configure(conn)), version=1, schema=schema)

    async with engine.begin() as conn:
        await conn.run_sync(run)
    meta = build_metadata().meta
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        version = await conn.scalar(select(meta.c.value).where(meta.c.key == VERSION_KEY))
    assert version == "1"
    assert await migrate(engine, schema) == SCHEMA_VERSION
