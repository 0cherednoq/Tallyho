"""Миграции на реальном PostgreSQL: установка в схему, идемпотентность, изоляция."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho.model.errors import ConfigurationError
from tallyho.storage.migrations import (
    SCHEMA_VERSION,
    VERSION_KEY,
    migrate,
    migration_statements,
)
from tallyho.storage.tables import build_metadata
from tests.helpers.db import schema_exists, temporary_schema, unique_schema_name

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.storage.tables import Tables

# Каталог схемы без имени схемы: так сравниваются установки в разных схемах.
_COLUMNS = text("""
    SELECT table_name, column_name, ordinal_position, data_type, is_nullable,
           column_default, is_identity, identity_generation
    FROM information_schema.columns WHERE table_schema = :schema
    ORDER BY table_name, ordinal_position
""")
_INDEXES = text("""
    SELECT tablename, indexname,
           replace(indexdef, ' ON ' || quote_ident(schemaname) || '.', ' ON ')
    FROM pg_indexes WHERE schemaname = :schema
    ORDER BY tablename, indexname
""")
_CONSTRAINTS = text("""
    SELECT c.relname, con.conname, pg_get_constraintdef(con.oid)
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :schema
    ORDER BY c.relname, con.conname
""")
_RELOPTIONS = text("""
    SELECT c.relname, c.relkind, c.reloptions
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :schema
    ORDER BY c.relname
""")


def quoted(schema: str) -> str:
    return '"' + schema.replace('"', '""') + '"'


async def catalog(conn: AsyncConnection, schema: str) -> list[list[tuple[object, ...]]]:
    return [
        [tuple(row) for row in await conn.execute(query, {"schema": schema})]
        for query in (_COLUMNS, _INDEXES, _CONSTRAINTS, _RELOPTIONS)
    ]


async def create_all(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    async with engine.begin() as conn:
        translated = await conn.execution_options(schema_translate_map={None: schema})
        await translated.run_sync(tables.metadata.create_all, checkfirst=False)


async def stored_version(engine: AsyncEngine, schema: str, prefix: str = "th_") -> str | None:
    meta = build_metadata(prefix).meta
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        return await conn.scalar(select(meta.c.value).where(meta.c.key == VERSION_KEY))


async def add_batch(engine: AsyncEngine, schema: str, kind: str) -> None:
    tables = build_metadata()
    batch_id = uuid4()
    async with engine.begin() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        await conn.execute(
            insert(tables.batch).values(
                id=batch_id,
                root_id=batch_id,
                kind=kind,
                state=0,
                created_at=func.now(),
                updated_at=func.now(),
            )
        )


async def batch_kinds(engine: AsyncEngine, schema: str) -> list[str]:
    batch = build_metadata().batch
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        return list(await conn.scalars(select(batch.c.kind).order_by(batch.c.kind)))


@contextlib.asynccontextmanager
async def dropped_after(engine: AsyncEngine, schema: str) -> AsyncGenerator[str]:
    """Схема, которую создаст сам тест (``migrate``); удаляется на выходе."""
    try:
        yield schema
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {quoted(schema)} CASCADE"))


async def test_migrate_creates_same_schema_as_tables(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema) == SCHEMA_VERSION
    async with temporary_schema(engine) as reference:
        await create_all(engine, reference, build_metadata())
        async with engine.connect() as conn:
            installed = await catalog(conn, schema)
            expected = await catalog(conn, reference)
    # Колонки, индексы, ограничения и параметры хранения — как у create_all.
    assert all(installed)
    assert installed == expected
    assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_repeated_migrate_is_noop(engine: AsyncEngine, schema: str) -> None:
    await migrate(engine, schema)
    await add_batch(engine, schema, "mailing")
    meta = build_metadata().meta
    oid_query = text("SELECT to_regclass(:name)::oid")
    async with engine.connect() as conn:
        before = await conn.scalar(oid_query, {"name": f"{quoted(schema)}.th_batch"})
    assert await migrate(engine, schema) == SCHEMA_VERSION
    async with engine.connect() as conn:
        after = await conn.scalar(oid_query, {"name": f"{quoted(schema)}.th_batch"})
        translated = await conn.execution_options(schema_translate_map={None: schema})
        meta_rows = await translated.scalar(select(func.count()).select_from(meta))
    assert before == after
    assert meta_rows == 1
    assert await batch_kinds(engine, schema) == ["mailing"]


async def test_upgrade_from_v1_backfills_counter_delta_timestamp(
    engine: AsyncEngine, schema: str
) -> None:
    tables = build_metadata()
    async with engine.begin() as raw:
        for statement in migration_statements(1, schema=schema):
            await raw.execute(statement)
        conn = await raw.execution_options(schema_translate_map={None: schema})
        await conn.execute(insert(tables.counter_delta).values(batch_id=uuid4(), d_ok=1))
    assert await migrate(engine, schema) == SCHEMA_VERSION
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        created_at = await conn.scalar(select(tables.counter_delta.c.created_at))
    assert created_at is not None
    assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_upgrade_from_v2_keeps_data_and_adds_attributes(
    engine: AsyncEngine, schema: str
) -> None:
    tables = build_metadata()
    async with engine.begin() as raw:
        for version in (1, 2):
            for statement in migration_statements(version, schema=schema):
                await raw.execute(statement)
    assert await stored_version(engine, schema) == "2"
    await add_batch(engine, schema, "mailing")
    await add_batch(engine, schema, "catalog")

    assert await migrate(engine, schema) == SCHEMA_VERSION

    assert await batch_kinds(engine, schema) == ["catalog", "mailing"]
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        attr_rows = await conn.scalar(select(func.count()).select_from(tables.batch_attr))
    assert attr_rows == 0
    async with temporary_schema(engine) as reference:
        await create_all(engine, reference, tables)
        async with engine.connect() as conn:
            assert await catalog(conn, schema) == await catalog(conn, reference)
    assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_upgrade_from_v3_keeps_leases_and_marks_them_not_redelivered(
    engine: AsyncEngine, schema: str
) -> None:
    tables = build_metadata()
    lease = tables.lease
    item_id = uuid4()
    async with engine.begin() as raw:
        for version in (1, 2, 3):
            for statement in migration_statements(version, schema=schema):
                await raw.execute(statement)
        conn = await raw.execution_options(schema_translate_map={None: schema})
        # Lease, взятый до обновления библиотеки: колонки redelivered ещё нет.
        await conn.execute(
            insert(lease).values(
                item_id=item_id,
                batch_id=uuid4(),
                lease_until=func.now(),
                worker_id="worker",
                attempt=0,
            )
        )
    assert await stored_version(engine, schema) == "3"

    assert await migrate(engine, schema) == SCHEMA_VERSION

    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        rows = (await conn.execute(select(lease.c.item_id, lease.c.redelivered))).all()
    assert [tuple(row) for row in rows] == [(item_id, False)]
    async with temporary_schema(engine) as reference:
        await create_all(engine, reference, tables)
        async with engine.connect() as conn:
            assert await catalog(conn, schema) == await catalog(conn, reference)
    assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_upgrade_from_v4_keeps_items_in_generation_zero(
    engine: AsyncEngine, schema: str
) -> None:
    tables = build_metadata()
    item = tables.item
    item_id = uuid4()
    async with engine.begin() as raw:
        for version in (1, 2, 3, 4):
            for statement in migration_statements(version, schema=schema):
                await raw.execute(statement)
        conn = await raw.execution_options(schema_translate_map={None: schema})
        # Item, созданный до обновления библиотеки: колонки generation ещё нет.
        await conn.execute(
            insert(item).values(
                id=item_id,
                batch_id=uuid4(),
                state=0,
                task_name="task",
                payload=b"",
                created_at=func.now(),
            )
        )
    assert await stored_version(engine, schema) == "4"

    assert await migrate(engine, schema) == SCHEMA_VERSION

    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        rows = (await conn.execute(select(item.c.id, item.c.generation))).all()
    assert [tuple(row) for row in rows] == [(item_id, 0)]
    async with temporary_schema(engine) as reference:
        await create_all(engine, reference, tables)
        async with engine.connect() as conn:
            assert await catalog(conn, schema) == await catalog(conn, reference)
    assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_migrate_creates_missing_schema(engine: AsyncEngine) -> None:
    async with dropped_after(engine, unique_schema_name()) as schema:
        await migrate(engine, schema)
        async with engine.connect() as conn:
            assert await schema_exists(conn, schema)


async def test_installations_in_two_schemas_are_isolated(engine: AsyncEngine, schema: str) -> None:
    async with temporary_schema(engine) as other:
        await migrate(engine, schema)
        await migrate(engine, other)
        await add_batch(engine, schema, "first")
        await add_batch(engine, other, "second")
        assert await batch_kinds(engine, schema) == ["first"]
        assert await batch_kinds(engine, other) == ["second"]


async def test_prefixes_in_one_schema_have_own_versions(engine: AsyncEngine, schema: str) -> None:
    await migrate(engine, schema)
    await migrate(engine, schema, "acme_")
    assert await stored_version(engine, schema) == str(SCHEMA_VERSION)
    assert await stored_version(engine, schema, "acme_") == str(SCHEMA_VERSION)


async def test_schema_with_special_characters(engine: AsyncEngine) -> None:
    name = f"{unique_schema_name()} \"q'; DROP TABLE x;--"
    async with dropped_after(engine, name) as schema:
        assert await migrate(engine, schema) == SCHEMA_VERSION
        assert await migrate(engine, schema) == SCHEMA_VERSION
        await add_batch(engine, schema, "k'; --")
        assert await batch_kinds(engine, schema) == ["k'; --"]
        assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_schema_none_uses_search_path(postgres_dsn: str, schema: str) -> None:
    # search_path задаётся через asyncpg: так тест не трогает схему public.
    scoped = create_async_engine(
        postgres_dsn, connect_args={"server_settings": {"search_path": schema}}
    )
    try:
        assert await migrate(scoped, None) == SCHEMA_VERSION
        assert await migrate(scoped, None) == SCHEMA_VERSION
        assert await stored_version(scoped, schema) == str(SCHEMA_VERSION)
    finally:
        await scoped.dispose()


async def test_concurrent_migrate(engine: AsyncEngine) -> None:
    async with dropped_after(engine, unique_schema_name()) as schema:
        results = await asyncio.gather(*(migrate(engine, schema) for _ in range(4)))
        assert results == [SCHEMA_VERSION] * 4
        assert await stored_version(engine, schema) == str(SCHEMA_VERSION)


async def test_newer_schema_rejected(engine: AsyncEngine, schema: str) -> None:
    await migrate(engine, schema)
    meta = build_metadata().meta
    async with engine.begin() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        await conn.execute(update(meta).values(value=str(SCHEMA_VERSION + 1)))
    with pytest.raises(ConfigurationError):
        await migrate(engine, schema)


async def test_invalid_prefix_rejected_before_db(engine: AsyncEngine) -> None:
    schema = unique_schema_name()
    with pytest.raises(ConfigurationError):
        await migrate(engine, schema, "Bad-Prefix")
    async with engine.connect() as conn:
        assert not await schema_exists(conn, schema)


async def test_statements_set_lock_timeout(engine: AsyncEngine, schema: str) -> None:
    async with engine.connect() as conn:
        transaction = await conn.begin()
        for statement in migration_statements(1, schema=schema):
            await conn.execute(statement)
        assert await conn.scalar(text("SELECT current_setting('lock_timeout')")) == "5s"
        await transaction.rollback()
        assert await conn.scalar(text("SELECT current_setting('lock_timeout')")) == "0"
