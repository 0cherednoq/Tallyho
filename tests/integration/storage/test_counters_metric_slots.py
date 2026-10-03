"""Отрицательные слоты ``th_metric`` пути B: перенос Completer и sweeper (UC-08, UC-15)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import delete, select, text, update

from tallyho.storage.counters import (
    CounterDelta,
    insert_delta,
    take_metric_slot,
    take_stale_metric_slots,
    upsert_metrics,
    user_metric_slot,
)
from tests.helpers.db import schema_connection, schema_transaction

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.storage.tables import Tables

__all__: list[str] = []

A = UUID(int=1)
B = UUID(int=2)
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


async def metric_rows(
    engine: AsyncEngine, schema: str, tables: Tables
) -> list[tuple[UUID, str, int, int]]:
    metric = tables.metric
    async with schema_connection(engine, schema) as conn:
        rows = await conn.execute(
            select(metric.c.batch_id, metric.c.name, metric.c.slot, metric.c.value).order_by(
                metric.c.batch_id, metric.c.name, metric.c.slot
            )
        )
        return [(b, name, slot, value) for b, name, slot, value in rows]


async def add_delta(conn: AsyncConnection, tables: Tables, batch_id: UUID) -> int:
    inserted = await insert_delta(conn, tables, {batch_id: CounterDelta(ok=1)}, created_at=NOW)
    return inserted[batch_id][0]


def test_user_metric_slot_is_negative_and_wraps() -> None:
    assert user_metric_slot(0) == -1
    assert user_metric_slot(32_766) == -32_767
    assert user_metric_slot(32_767) == -1


async def test_take_metric_slot_takes_only_given_batch_slots(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_metrics(
            conn,
            tables,
            {
                (A, "ok", -3): 1,
                (A, "x", -3): 2,
                (A, "ok", -4): 5,
                (B, "ok", -3): 7,
                (A, "ok", 1): 9,
            },
        )
        taken = await take_metric_slot(conn, tables, [(A, -3), (B, -8)])
    assert taken == {(A, "ok"): 1, (A, "x"): 2}
    assert await metric_rows(engine, schema, tables) == [
        (A, "ok", -4, 5),
        (A, "ok", 1, 9),
        (B, "ok", -3, 7),
    ]


async def test_stale_slots_skip_positive_slots_and_empty_keys(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_metrics(conn, tables, {(A, "ok", 2): 4, (A, "ok", -2): 3})
        assert await take_stale_metric_slots(conn, tables, []) == {}
        assert await take_stale_metric_slots(conn, tables, [(A, 2)]) == {}
        assert await take_stale_metric_slots(conn, tables, [(A, -2), (A, -2)]) == {(A, "ok"): 3}
    assert await metric_rows(engine, schema, tables) == [(A, "ok", 2, 4)]


async def test_stale_slot_waits_for_pending_delta_of_same_slot(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    """Несвёрнутая дельта батча с тем же слотом заберёт строку сама."""
    delta = tables.counter_delta
    async with schema_transaction(engine, schema) as conn:
        first = await add_delta(conn, tables, A)
        # Другая транзакция пути B с совпавшим слотом: id на 32767 больше.
        _ = await conn.execute(
            text("SELECT setval(pg_get_serial_sequence(:table, 'id'), :value)"),
            {"table": f'"{schema}".{delta.name}', "value": first + 32_766},
        )
        second = await add_delta(conn, tables, A)
        assert user_metric_slot(second) == user_metric_slot(first)
        slot = user_metric_slot(first)
        await upsert_metrics(conn, tables, {(A, "ok", slot): 2, (B, "ok", slot): 5})
        # Первую дельту свернули, вторая ещё ждёт свёртки своей транзакции.
        _ = await conn.execute(delete(delta).where(delta.c.id == first))
    async with schema_transaction(engine, schema) as conn:
        # Чужой батч не мешает: дельта с этим слотом есть только у A.
        assert await take_stale_metric_slots(conn, tables, [(A, slot), (B, slot)]) == {(B, "ok"): 5}
    async with schema_transaction(engine, schema) as conn:
        _ = await conn.execute(delete(delta).where(delta.c.batch_id == A))
        assert await take_stale_metric_slots(conn, tables, [(A, slot)]) == {(A, "ok"): 2}
    assert await metric_rows(engine, schema, tables) == []


async def test_stale_slot_skips_rows_locked_by_another_transaction(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_metrics(conn, tables, {(A, "a", -5): 1, (A, "b", -5): 2})
    metric = tables.metric
    async with schema_connection(engine, schema) as holder:
        # Живая транзакция пути B с совпавшим слотом держит строку "b".
        _ = await holder.execute(
            update(metric)
            .where(metric.c.batch_id == A, metric.c.name == "b")
            .values(value=metric.c.value + 1)
        )
        async with schema_transaction(engine, schema) as conn:
            _ = await conn.execute(text("SET LOCAL lock_timeout = '1s'"))
            assert await take_stale_metric_slots(conn, tables, [(A, -5)]) == {(A, "a"): 1}
        await holder.commit()
    assert await metric_rows(engine, schema, tables) == [(A, "b", -5, 3)]
