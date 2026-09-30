"""Счётчики на PostgreSQL: чтение, upsert слотов, дельты пути B."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text

from tallyho.storage.counters import (
    COUNTER_FIELDS,
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

# UUID сортируются в PostgreSQL побайтно, в Python — как 128-битное число: порядок один.
A = UUID(int=1)
B = UUID(int=2)


async def read(
    engine: AsyncEngine, schema: str, tables: Tables, *ids: UUID
) -> dict[UUID, CounterTotals]:
    async with schema_connection(engine, schema) as conn:
        return await read_counters(conn, tables, ids)


async def slot_rows(engine: AsyncEngine, schema: str, tables: Tables) -> int:
    async with schema_connection(engine, schema) as conn:
        return int(await conn.scalar(select(func.count()).select_from(tables.counter)) or 0)


async def test_read_nothing(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    assert await read(engine, schema, tables) == {}


async def test_read_unknown_batch_is_zero(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    assert await read(engine, schema, tables, A, A) == {A: CounterTotals()}


async def test_upsert_creates_and_increments_slots(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_slots(conn, tables, {(A, 0): CounterDelta(total=5, w_total=10, tree_total=5)})
        await upsert_slots(
            conn,
            tables,
            {
                (A, 0): CounterDelta(ok=1, w_done=2, dispatched=3),
                (A, 3): CounterDelta(error=1, duplicates=2, skipped_by_limit=1),
                (B, 1): CounterDelta(total=1, skip=1),
                (B, 2): CounterDelta(),
            },
        )
    assert await read(engine, schema, tables, A, B) == {
        A: CounterTotals(
            total=5,
            ok=1,
            error=1,
            dispatched=3,
            w_total=10,
            w_done=2,
            duplicates=2,
            skipped_by_limit=1,
            tree_total=5,
        ),
        B: CounterTotals(total=1, skip=1),
    }
    # Нулевая дельта строку слота не создаёт.
    assert await slot_rows(engine, schema, tables) == 3


async def test_upsert_many_rows_in_chunks(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    ids = [uuid4() for _ in range(1500)]
    async with schema_transaction(engine, schema) as conn:
        await upsert_slots(conn, tables, {(b, 0): CounterDelta(total=1) for b in ids})
        await insert_delta(
            conn, tables, {b: CounterDelta(ok=1) for b in ids}, created_at=func.now()
        )
    totals = await read(engine, schema, tables, *ids)
    assert set(totals.values()) == {CounterTotals(total=1, ok=1)}
    assert len(totals) == len(ids)


async def test_delta_adds_to_read(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_slots(conn, tables, {(A, 0): CounterDelta(total=10, w_total=10)})
        await insert_delta(
            conn,
            tables,
            {A: CounterDelta(ok=2, skip=1, error=1, cancelled=1, w_done=4), B: CounterDelta()},
            created_at=func.now(),
        )
        await insert_delta(
            conn,
            tables,
            {A: CounterDelta(total=1, ok=1, w_done=1)},
            created_at=func.now(),
        )
    totals = await read(engine, schema, tables, A, B)
    assert totals[A] == CounterTotals(
        total=11, ok=3, skip=1, error=1, cancelled=1, w_total=10, w_done=5
    )
    assert totals[A].pending == 5
    assert totals[B] == CounterTotals()


@pytest.mark.parametrize("name", COUNTER_FIELDS)
async def test_each_delta_field_round_trip(
    engine: AsyncEngine, schema: str, tables: Tables, *, name: str
) -> None:
    # insert_delta -> read_counters -> fold_deltas -> слот: поле не теряется ни на одном шаге.
    delta = CounterDelta(**{name: 7})
    async with schema_transaction(engine, schema) as conn:
        await insert_delta(conn, tables, {A: delta}, created_at=func.now())
    expected = CounterTotals(**delta.as_dict())
    assert await read(engine, schema, tables, A) == {A: expected}
    async with schema_transaction(engine, schema) as conn:
        folded = await fold_deltas(conn, tables, [A])
        assert folded == {A: delta}
        await upsert_slots(conn, tables, {(A, 1): folded[A]})
    assert await read(engine, schema, tables, A) == {A: expected}
    async with schema_connection(engine, schema) as conn:
        rest = await conn.scalar(select(func.count()).select_from(tables.counter_delta))
    assert rest == 0


async def test_all_delta_fields_at_once(engine: AsyncEngine, schema: str, tables: Tables) -> None:
    delta = CounterDelta(**{name: n for n, name in enumerate(COUNTER_FIELDS, start=1)})
    async with schema_transaction(engine, schema) as conn:
        await upsert_slots(conn, tables, {(A, 0): delta})
        await insert_delta(conn, tables, {A: delta, B: -delta}, created_at=func.now())
    assert await read(engine, schema, tables, A, B) == {
        A: CounterTotals(**(delta + delta).as_dict()),
        B: CounterTotals(**(-delta).as_dict()),
    }
    async with schema_transaction(engine, schema) as conn:
        assert await fold_deltas(conn, tables, [A, B]) == {A: delta, B: -delta}


async def test_upsert_locks_slots_in_key_order(
    engine: AsyncEngine, schema: str, tables: Tables
) -> None:
    async with schema_transaction(engine, schema) as conn:
        await upsert_slots(
            conn, tables, {(A, 0): CounterDelta(total=1), (B, 0): CounterDelta(total=1)}
        )
    async with (
        schema_connection(engine, schema) as holder,
        schema_connection(engine, schema) as waiter,
    ):
        # holder держит строку A; waiter передаёт ключи в обратном порядке.
        await upsert_slots(holder, tables, {(A, 0): CounterDelta(ok=1)})
        blocked = asyncio.create_task(
            upsert_slots(waiter, tables, {(B, 0): CounterDelta(ok=1), (A, 0): CounterDelta(ok=1)})
        )
        await asyncio.sleep(0.2)
        assert not blocked.done()
        # Если бы waiter взял B раньше A, третья транзакция ждала бы его.
        async with schema_transaction(engine, schema) as third:
            _ = await third.execute(text("SET LOCAL lock_timeout = '1s'"))
            await upsert_slots(third, tables, {(B, 0): CounterDelta(skip=1)})
        await holder.commit()
        await blocked
        await waiter.commit()
    totals = await read(engine, schema, tables, A, B)
    assert totals[A] == CounterTotals(total=1, ok=2)
    assert totals[B] == CounterTotals(total=1, ok=1, skip=1)
