"""Свёртка дельт пути B: ``fold_deltas`` на PostgreSQL."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import func, select

from tallyho.storage.counters import (
    CounterDelta,
    CounterTotals,
    fold_deltas,
    insert_delta,
    read_counters,
    upsert_slots,
)
from tests.helpers.db import schema_connection, schema_transaction

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.storage.tables import Tables

A = UUID(int=1)
B = UUID(int=2)
SLOT = 3


async def fold_into_slot(
    engine: AsyncEngine, schema: str, tables: Tables, *ids: UUID
) -> dict[UUID, CounterDelta]:
    async with schema_transaction(engine, schema) as conn:
        folded = await fold_deltas(conn, tables, ids)
        await upsert_slots(conn, tables, {(b, SLOT): d for b, d in folded.items()})
    return folded


async def delta_rows(engine: AsyncEngine, schema: str, tables: Tables) -> int:
    async with schema_connection(engine, schema) as conn:
        return int(await conn.scalar(select(func.count()).select_from(tables.counter_delta)) or 0)


async def read_a(engine: AsyncEngine, schema: str, tables: Tables) -> CounterTotals:
    async with schema_connection(engine, schema) as conn:
        return (await read_counters(conn, tables, [A]))[A]


async def test_fold_nothing(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    async with schema_transaction(engine, schema) as conn:
        assert await fold_deltas(conn, tables, []) == {}
        assert await fold_deltas(conn, tables, [A]) == {}


async def test_fold_moves_deltas_into_slot(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await insert_delta(
            conn, tables, {A: CounterDelta(total=2, ok=1, w_done=3), B: CounterDelta(error=1)}
        )
        await insert_delta(conn, tables, {A: CounterDelta(skip=1, cancelled=1, w_done=1)})
    before = await read_a(engine, schema, tables)
    folded = await fold_into_slot(engine, schema, tables, A)
    assert folded == {A: CounterDelta(total=2, ok=1, skip=1, cancelled=1, w_done=4)}
    assert await read_a(engine, schema, tables) == before
    # Дельты B не тронуты.
    assert await delta_rows(engine, schema, tables) == 1
    assert await fold_into_slot(engine, schema, tables, A, B) == {B: CounterDelta(error=1)}
    assert await delta_rows(engine, schema, tables) == 0


async def test_uncommitted_deltas_are_not_folded(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_connection(engine, schema) as user:
        await insert_delta(user, tables, {A: CounterDelta(total=1, ok=1)})
        assert await fold_into_slot(engine, schema, tables, A) == {}
        await user.commit()
    assert await fold_into_slot(engine, schema, tables, A) == {A: CounterDelta(total=1, ok=1)}
    assert await read_a(engine, schema, tables) == CounterTotals(total=1, ok=1)


async def test_concurrent_folds_count_delta_once(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await insert_delta(conn, tables, {A: CounterDelta(ok=1)})
    async with (
        schema_connection(engine, schema) as first,
        schema_connection(engine, schema) as second,
    ):
        assert await fold_deltas(first, tables, [A]) == {A: CounterDelta(ok=1)}
        waiting = asyncio.create_task(fold_deltas(second, tables, [A]))
        await asyncio.sleep(0.2)
        assert not waiting.done()
        await first.commit()
        assert await waiting == {}
        await second.commit()


async def test_read_does_not_flicker_during_fold(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    iterations = 1000
    started = 0
    inserting = True

    async def insert() -> None:
        nonlocal started, inserting
        for _ in range(iterations):
            started += 1
            async with schema_transaction(engine, schema) as conn:
                await insert_delta(conn, tables, {A: CounterDelta(total=1, ok=1, w_done=2)})
            await asyncio.sleep(0)
        inserting = False

    folds = 0

    async def fold() -> None:
        nonlocal folds
        while inserting or await delta_rows(engine, schema, tables):
            if await fold_into_slot(engine, schema, tables, A):
                folds += 1

    async def watch() -> list[int]:
        seen: list[int] = []
        async with schema_connection(engine, schema) as conn:
            while inserting:
                totals = (await read_counters(conn, tables, [A]))[A]
                # Сумма согласована, не убывает и не считает дельту дважды.
                assert totals.total == totals.ok
                assert totals.w_done == 2 * totals.ok
                assert totals.ok <= started
                assert not seen or totals.ok >= seen[-1]
                seen.append(totals.ok)
                await conn.rollback()
        return seen

    watcher = asyncio.create_task(watch())
    await asyncio.gather(insert(), fold())
    seen = await watcher
    assert len(seen) > 10
    assert folds > 10
    assert await read_a(engine, schema, tables) == CounterTotals(
        total=iterations, ok=iterations, w_done=2 * iterations
    )
    assert await delta_rows(engine, schema, tables) == 0
