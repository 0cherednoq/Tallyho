"""Схема ``th_checkpoint`` на реальном PostgreSQL: колонки, хранение, индексы (RM8.4)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from tallyho.storage.migrations import migrate

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__: list[str] = []

_COLUMNS = text("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    ORDER BY ordinal_position
""")
_INDEXES = text("""
    SELECT indexname, indexdef FROM pg_indexes
    WHERE schemaname = :schema AND tablename = :table
    ORDER BY indexname
""")
_RELOPTIONS = text("""
    SELECT c.reloptions
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :schema AND c.relname = :table
""")


async def _columns(conn: AsyncConnection, schema: str) -> list[tuple[object, ...]]:
    result = await conn.execute(_COLUMNS, {"schema": schema, "table": "th_checkpoint"})
    return [tuple(row) for row in result]


async def test_checkpoint_columns_installed(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        assert await _columns(conn, schema) == [
            ("item_id", "uuid", "NO"),
            ("batch_id", "uuid", "NO"),
            ("position", "bigint", "NO"),
            ("updated_at", "timestamp with time zone", "NO"),
        ]


async def test_checkpoint_has_only_primary_key(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        indexes = [
            tuple(row)
            for row in await conn.execute(_INDEXES, {"schema": schema, "table": "th_checkpoint"})
        ]
    # Закладку читают и пишут только по PK; вторичных индексов у неё нет (§5.2).
    assert len(indexes) == 1
    name, definition = indexes[0]
    assert name == "th_checkpoint_pkey"
    assert "(item_id)" in str(definition)


async def test_checkpoint_is_stored_for_repeated_updates(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        options = await conn.scalar(_RELOPTIONS, {"schema": schema, "table": "th_checkpoint"})
    # Закладка обновляется десятки раз за Item: запас страницы и частый autovacuum.
    assert set(options or []) == {
        "fillfactor=50",
        "autovacuum_vacuum_scale_factor=0",
        "autovacuum_vacuum_threshold=1000",
    }


async def test_item_keeps_its_hot_fillfactor(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        options = await conn.scalar(_RELOPTIONS, {"schema": schema, "table": "th_item"})
    # Закладка не стала колонкой th_item: горячая строка живёт под одно HOT-обновление.
    assert set(options or []) == {"fillfactor=85"}
    async with engine.connect() as conn:
        columns = {
            row[0] for row in await conn.execute(_COLUMNS, {"schema": schema, "table": "th_item"})
        }
    assert {"position", "checkpoint"} & columns == set()
