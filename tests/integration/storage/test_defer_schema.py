"""Схема под defer на реальном PostgreSQL: колонки, частичный индекс, счётчики (RM2.1)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from tallyho.storage.migrations import migrate, migration_statements

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__: list[str] = []

_COLUMN = text("""
    SELECT data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table AND column_name = :column
""")
_INDEX = text("""
    SELECT indexdef FROM pg_indexes
    WHERE schemaname = :schema AND indexname = :index
""")
_COUNTER_COLUMNS = text("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    ORDER BY ordinal_position
""")


async def column(
    conn: AsyncConnection, schema: str, table: str, *, name: str
) -> tuple[object, ...]:
    result = await conn.execute(_COLUMN, {"schema": schema, "table": table, "column": name})
    return tuple(result.one())


async def test_defer_columns_installed(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        assert await column(conn, schema, "th_outbox", name="defer_reason") == ("text", "YES", None)
        assert await column(conn, schema, "th_item", name="defers") == ("smallint", "NO", "0")
        assert await column(conn, schema, "th_item", name="first_deferred_at") == (
            "timestamp with time zone",
            "YES",
            None,
        )


async def test_deferred_index_is_partial(engine: AsyncEngine, schema: str) -> None:
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        definition = await conn.scalar(
            _INDEX, {"schema": schema, "index": "th_outbox_deferred_idx"}
        )
    assert definition is not None
    assert "(batch_id)" in definition
    assert "WHERE (defer_reason IS NOT NULL)" in definition


async def test_defer_adds_no_index_to_item(engine: AsyncEngine, schema: str) -> None:
    # Индекс по defers или first_deferred_at сломал бы HOT update на finish.
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        definitions = list(
            await conn.scalars(
                text("SELECT indexdef FROM pg_indexes WHERE schemaname = :s AND tablename = :t"),
                {"s": schema, "t": "th_item"},
            )
        )
    assert definitions
    assert not [
        definition
        for definition in definitions
        if "defers" in definition or "first_deferred_at" in definition
    ]


async def test_counter_tables_unchanged(engine: AsyncEngine, schema: str) -> None:
    # defer — гейдж по th_outbox: новых колонок счётчиков нет (COUNTERS, RM0.2).
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        counter = list(
            await conn.scalars(_COUNTER_COLUMNS, {"schema": schema, "table": "th_counter"})
        )
        delta = list(
            await conn.scalars(_COUNTER_COLUMNS, {"schema": schema, "table": "th_counter_delta"})
        )
    assert counter == [
        "batch_id",
        "slot",
        "total",
        "ok",
        "skip",
        "error",
        "cancelled",
        "dispatched",
        "w_total",
        "w_done",
        "duplicates",
        "skipped_by_limit",
        "tree_total",
    ]
    assert delta == [
        "id",
        "batch_id",
        "d_total",
        "d_ok",
        "d_skip",
        "d_error",
        "d_cancelled",
        "d_dispatched",
        "d_w_total",
        "d_w_done",
        "d_duplicates",
        "d_skipped_by_limit",
        "d_tree_total",
        "created_at",
    ]


async def test_migration_rolls_back_with_its_transaction(engine: AsyncEngine, schema: str) -> None:
    # DDL миграции транзакционно: откат не оставляет половины схемы.
    async with engine.begin() as conn:
        for statement in migration_statements(1, schema=schema):
            _ = await conn.execute(statement)
        await conn.rollback()
    async with engine.connect() as conn:
        tables = list(
            await conn.scalars(
                text("SELECT tablename FROM pg_tables WHERE schemaname = :schema"),
                {"schema": schema},
            )
        )
    assert tables == []
    # Та же схема ставится заново целиком.
    assert await migrate(engine, schema)
    async with engine.connect() as conn:
        assert await column(conn, schema, "th_outbox", name="defer_reason") == ("text", "YES", None)
