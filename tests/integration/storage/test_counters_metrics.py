"""``upsert_metrics``: labels и метрики по слотам ``th_metric``."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import select, text

from tallyho.storage.counters import upsert_metrics
from tests.helpers.db import schema_connection, schema_transaction

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.storage.tables import Tables

A = UUID(int=1)
B = UUID(int=2)


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


async def test_upsert_creates_and_increments(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_metrics(conn, tables, {(A, "hard_bounce", 0): 2, (A, "sent", 1): 5})
        await upsert_metrics(
            conn,
            tables,
            {(A, "hard_bounce", 0): 1, (A, "sent", 2): 1, (B, "sent", 0): 3, (B, "x", 0): 0},
        )
    assert await metric_rows(engine, schema, tables) == [
        (A, "hard_bounce", 0, 3),
        (A, "sent", 1, 5),
        (A, "sent", 2, 1),
        (B, "sent", 0, 3),
    ]


async def test_upsert_many_rows_in_chunks(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    ids = [uuid4() for _ in range(1200)]
    async with schema_transaction(engine, schema) as conn:
        await upsert_metrics(conn, tables, {(b, "ok", 0): 1 for b in ids})
    rows = await metric_rows(engine, schema, tables)
    assert len(rows) == len(ids)
    assert {value for *_, value in rows} == {1}


async def test_upsert_locks_rows_in_key_order(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_metrics(conn, tables, {(A, "a", 0): 1, (A, "b", 0): 1})
    async with (
        schema_connection(engine, schema) as holder,
        schema_connection(engine, schema) as waiter,
    ):
        await upsert_metrics(holder, tables, {(A, "a", 0): 1})
        blocked = asyncio.create_task(
            upsert_metrics(waiter, tables, {(A, "b", 0): 1, (A, "a", 0): 1})
        )
        await asyncio.sleep(0.2)
        assert not blocked.done()
        # waiter ждёт "a" и ещё не взял "b".
        async with schema_transaction(engine, schema) as third:
            _ = await third.execute(text("SET LOCAL lock_timeout = '1s'"))
            await upsert_metrics(third, tables, {(A, "b", 0): 1})
        await holder.commit()
        await blocked
        await waiter.commit()
    assert await metric_rows(engine, schema, tables) == [(A, "a", 0, 3), (A, "b", 0, 3)]
