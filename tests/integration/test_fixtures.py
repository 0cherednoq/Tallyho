"""Проверки самих фикстур интеграционных тестов."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import column, func, insert, select, table, text

from tests.helpers.db import schema_exists, temporary_schema

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession


async def test_schema_fixture_creates_empty_schema(engine: AsyncEngine, schema: str) -> None:
    async with engine.connect() as conn:
        assert await schema_exists(conn, schema)
        tables = await conn.scalar(
            text("SELECT count(*) FROM pg_tables WHERE schemaname = :schema"),
            {"schema": schema},
        )
    assert tables == 0


async def test_schema_fixture_isolates_tests(engine: AsyncEngine, schema: str) -> None:
    # Таблица с фиксированным именем: при общей схеме параллельные тесты бы столкнулись.
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE TABLE "{schema}".probe (id int PRIMARY KEY)'))
        probe = table("probe", column("id"), schema=schema)
        await conn.execute(insert(probe).values(id=1))
        count = await conn.scalar(select(func.count()).select_from(probe))
    assert count == 1


async def test_temporary_schema_is_dropped_with_contents(engine: AsyncEngine) -> None:
    async with temporary_schema(engine) as name:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE TABLE "{name}".probe (id int)'))
        async with engine.connect() as conn:
            assert await schema_exists(conn, name)
    async with engine.connect() as conn:
        assert not await schema_exists(conn, name)


async def test_schema_names_are_unique(engine: AsyncEngine) -> None:
    async with temporary_schema(engine) as first, temporary_schema(engine) as second:
        assert first != second


async def test_connection_fixture_rolls_back_uncommitted(
    connection: AsyncConnection, engine: AsyncEngine, schema: str
) -> None:
    await connection.execute(text(f'CREATE TABLE "{schema}".probe (id int)'))
    assert connection.in_transaction()
    await connection.rollback()
    async with engine.connect() as other:
        tables = await other.scalar(
            text("SELECT count(*) FROM pg_tables WHERE schemaname = :schema"),
            {"schema": schema},
        )
    assert tables == 0


async def test_session_fixture_commits_into_schema(
    session: AsyncSession, engine: AsyncEngine, schema: str
) -> None:
    probe = table("probe", column("id"), schema=schema)
    conn = await session.connection()
    await conn.execute(text(f'CREATE TABLE "{schema}".probe (id int)'))
    await session.execute(insert(probe).values(id=7))
    await session.commit()
    async with engine.connect() as other:
        assert await other.scalar(select(probe.c.id)) == 7
