"""Описание таблиц на реальном PostgreSQL: create_all в схеме пользователя."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import insert, select, text

from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.storage.tables import Tables


async def create_all(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    async with engine.begin() as conn:
        translated = await conn.execution_options(schema_translate_map={None: schema})
        await translated.run_sync(tables.metadata.create_all, checkfirst=False)


async def table_names(conn: AsyncConnection, schema: str) -> set[str]:
    rows = await conn.scalars(
        text("SELECT tablename FROM pg_tables WHERE schemaname = :schema"), {"schema": schema}
    )
    return set(rows)


async def test_create_all_in_schema(engine: AsyncEngine, schema: str) -> None:
    tables = build_metadata()
    await create_all(engine, schema, tables)
    async with engine.connect() as conn:
        assert await table_names(conn, schema) == set(tables.metadata.tables)
        indexes = set(
            await conn.scalars(
                text("SELECT indexname FROM pg_indexes WHERE schemaname = :schema"),
                {"schema": schema},
            )
        )
    expected = {
        str(index.name) for table in tables.metadata.sorted_tables for index in table.indexes
    }
    assert expected <= indexes


async def test_storage_parameters_applied(engine: AsyncEngine, schema: str) -> None:
    await create_all(engine, schema, build_metadata())
    # to_regclass разбирает квалифицированное имя, поэтому схема — в двойных кавычках.
    query = text("SELECT reloptions FROM pg_class WHERE oid = to_regclass(:name)")
    async with engine.connect() as conn:
        item = await conn.scalar(query, {"name": f'"{schema}".th_item'})
        counter = await conn.scalar(query, {"name": f'"{schema}".th_counter'})
    assert item == ["fillfactor=85"]
    assert counter == [
        "fillfactor=50",
        "autovacuum_vacuum_scale_factor=0",
        "autovacuum_vacuum_threshold=1000",
    ]


async def test_prefixes_coexist_in_one_schema(engine: AsyncEngine, schema: str) -> None:
    first, second = build_metadata(), build_metadata("acme_")
    await create_all(engine, schema, first)
    await create_all(engine, schema, second)
    async with engine.connect() as conn:
        names = await table_names(conn, schema)
    assert names == set(first.metadata.tables) | set(second.metadata.tables)


async def test_server_defaults(engine: AsyncEngine, schema: str) -> None:
    tables = build_metadata()
    await create_all(engine, schema, tables)
    batch_id = uuid4()
    async with engine.begin() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        await conn.execute(
            insert(tables.batch).values(
                id=batch_id,
                root_id=batch_id,
                kind="mailing",
                state=0,
                created_at=text("now()"),
                updated_at=text("now()"),
            )
        )
        await conn.execute(insert(tables.counter_delta).values(batch_id=batch_id, d_ok=2))
        row = (
            await conn.execute(
                select(
                    tables.batch.c.options,
                    tables.batch.c.hooks,
                    tables.batch.c.on_feeder_failed,
                    tables.batch.c.release_required,
                )
            )
        ).one()
        delta = (
            await conn.execute(select(tables.counter_delta.c.id, tables.counter_delta.c.d_total))
        ).one()
    assert tuple(row) == ({}, [], 0, False)
    assert tuple(delta) == (1, 0)
