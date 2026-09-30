"""``reconcile``: дрейф счётчиков исправляется по строкам ``th_item``."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import func, insert, select, update

from tallyho.model.states import BatchState, ItemState
from tallyho.storage.counters import (
    CounterDelta,
    CounterTotals,
    insert_delta,
    read_counters,
    reconcile,
    upsert_slots,
)
from tests.helpers.db import schema_connection, schema_transaction

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.storage.tables import Tables

A = UUID(int=1)

# (state, weight) для Items батча A.
ITEMS = [
    (ItemState.ACTIVE, 1),
    (ItemState.ACTIVE, 2),
    (ItemState.OK, 3),
    (ItemState.OK, 1),
    (ItemState.SKIP, 1),
    (ItemState.ERROR, 5),
    (ItemState.CANCELLED, 1),
    (ItemState.OK, 0),
]
TRUTH = CounterTotals(total=8, ok=3, skip=1, error=1, cancelled=1, w_total=14, w_done=11)


def item_id(n: int) -> UUID:
    return UUID(int=1000 + n)


async def seed(conn: AsyncConnection, tables: Tables) -> None:
    _ = await conn.execute(
        insert(tables.batch).values(
            id=A,
            root_id=A,
            kind="k",
            state=BatchState.SEALED,
            created_at=func.now(),
            updated_at=func.now(),
        )
    )
    _ = await conn.execute(
        insert(tables.item).values(
            [
                {
                    "id": item_id(n),
                    "batch_id": A,
                    "state": state,
                    "weight": weight,
                    "task_name": "t",
                    "payload": b"",
                    "created_at": func.now(),
                }
                for n, (state, weight) in enumerate(ITEMS)
            ]
        )
    )


async def read_a(engine: AsyncEngine, schema: str, tables: Tables) -> CounterTotals:
    async with schema_connection(engine, schema) as conn:
        return (await read_counters(conn, tables, [A]))[A]


async def slots(engine: AsyncEngine, schema: str, tables: Tables) -> dict[int, int]:
    """``slot → total`` строк ``th_counter`` батча A."""
    counter = tables.counter
    async with schema_connection(engine, schema) as conn:
        rows = await conn.execute(
            select(counter.c.slot, counter.c.total).where(counter.c.batch_id == A)
        )
        return dict(rows.all())


async def test_unknown_batch(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    async with schema_transaction(engine, schema) as conn:
        assert await reconcile(conn, tables, A) is None


async def test_batch_without_counters(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    async with schema_transaction(engine, schema) as conn:
        await seed(conn, tables)
    async with schema_transaction(engine, schema) as conn:
        assert await reconcile(conn, tables, A) == TRUTH.as_delta()
    assert await read_a(engine, schema, tables) == TRUTH
    assert await slots(engine, schema, tables) == {0: 8}


async def test_consistent_counters_have_no_drift(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await seed(conn, tables)
        await upsert_slots(conn, tables, {(A, 2): TRUTH.as_delta()})
    async with schema_transaction(engine, schema) as conn:
        assert await reconcile(conn, tables, A) == CounterDelta()
    assert await read_a(engine, schema, tables) == TRUTH
    assert await slots(engine, schema, tables) == {0: 8, 2: 0}


async def test_reconcile_fixes_artificial_drift(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await seed(conn, tables)
        # Искусственный дрейф: лишний ok, потерянные total и w_done; прочие счётчики — как есть.
        await upsert_slots(
            conn,
            tables,
            {
                (A, 1): CounterDelta(total=3, ok=2, w_total=10, dispatched=5),
                (A, 4): CounterDelta(total=2, skip=1, error=1, w_total=4, w_done=3, duplicates=2),
            },
        )
        await insert_delta(conn, tables, {A: CounterDelta(ok=2, cancelled=1, w_done=4)})
    async with schema_transaction(engine, schema) as conn:
        drift = await reconcile(conn, tables, A)
    assert drift == CounterDelta(total=3, ok=-1, w_done=4)
    after = await read_a(engine, schema, tables)
    assert after == CounterTotals(**TRUTH.as_delta().as_dict() | {"dispatched": 5, "duplicates": 2})
    # Всё — в слоте 0, остальные слоты обнулены; несвёрнутая дельта осталась в th_counter_delta.
    assert await slots(engine, schema, tables) == {0: 8, 1: 0, 4: 0}


async def test_reconcile_keeps_concurrent_completion(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await seed(conn, tables)
        await upsert_slots(conn, tables, {(A, 3): TRUTH.as_delta() + CounterDelta(error=7)})
    item = tables.item
    async with (
        schema_connection(engine, schema) as completer,
        schema_connection(engine, schema) as fixer,
    ):
        # Completer завершил Item и держит строку своего слота, commit ещё не сделан.
        _ = await completer.execute(
            update(item).where(item.c.id == item_id(0)).values(state=ItemState.OK)
        )
        await upsert_slots(completer, tables, {(A, 3): CounterDelta(ok=1, w_done=1)})
        running = asyncio.create_task(reconcile(fixer, tables, A))
        await asyncio.sleep(0.2)
        assert not running.done()
        await completer.commit()
        drift = await running
        await fixer.commit()
    assert drift == CounterDelta(error=-7)
    assert await read_a(engine, schema, tables) == TRUTH + CounterDelta(ok=1, w_done=1)
