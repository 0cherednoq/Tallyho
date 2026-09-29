"""Проверки хелперов tests/helpers/db.py на живом PostgreSQL."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy import column, insert, table, text, update
from sqlalchemy.exc import DBAPIError

from tests.helpers.db import deadlock_count, held_locks

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tests.helpers.db import LockRow

DEADLOCK_SQLSTATE = "40P01"


async def _create_probe(engine: AsyncEngine, schema: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE TABLE "{schema}".probe (id int PRIMARY KEY, v int)'))
        probe = table("probe", column("id"), column("v"), schema=schema)
        await conn.execute(insert(probe), [{"id": 1, "v": 0}, {"id": 2, "v": 0}])


async def _pid(conn: AsyncConnection) -> int:
    return int(await conn.scalar(text("SELECT pg_backend_pid()")) or 0)


def _on_probe(locks: list[LockRow], pid: int, schema: str) -> list[LockRow]:
    return [lock for lock in locks if lock.pid == pid and lock.relation == f"{schema}.probe"]


async def test_held_locks_shows_granted_and_waiting(
    engine: AsyncEngine, connection: AsyncConnection, schema: str
) -> None:
    await _create_probe(engine, schema)
    own_pid = await _pid(connection)
    locks: list[LockRow] = []
    async with engine.connect() as holder, engine.connect() as waiter:
        holder_pid, waiter_pid = await _pid(holder), await _pid(waiter)
        await holder.execute(text(f'LOCK TABLE "{schema}".probe IN EXCLUSIVE MODE'))
        waiting = asyncio.create_task(
            waiter.execute(text(f'LOCK TABLE "{schema}".probe IN SHARE MODE'))
        )
        for _ in range(100):
            locks = await held_locks(connection)
            if _on_probe(locks, waiter_pid, schema):
                break
            await asyncio.sleep(0.05)
        await holder.rollback()
        await waiting
        await waiter.rollback()

    held = _on_probe(locks, holder_pid, schema)
    assert [(lock.mode, lock.granted) for lock in held] == [("ExclusiveLock", True)]
    waits = _on_probe(locks, waiter_pid, schema)
    assert [(lock.mode, lock.granted) for lock in waits] == [("ShareLock", False)]
    assert all(lock.pid != own_pid for lock in locks)


async def test_held_locks_can_include_own_backend(connection: AsyncConnection) -> None:
    own_pid = await _pid(connection)
    # Сам запрос к pg_locks берёт AccessShareLock на системных отношениях.
    assert any(lock.pid == own_pid for lock in await held_locks(connection, include_own=True))
    assert all(lock.pid != own_pid for lock in await held_locks(connection))


async def _update_crosswise(
    conn: AsyncConnection, schema: str, *, order: tuple[int, int], barrier: asyncio.Barrier
) -> None:
    probe = table("probe", column("id"), column("v"), schema=schema)
    try:
        await conn.execute(update(probe).where(probe.c.id == order[0]).values(v=1))
        await barrier.wait()
        await conn.execute(update(probe).where(probe.c.id == order[1]).values(v=1))
    finally:
        await conn.rollback()
        # Опубликовать статистику backend'а сразу, а не через интервал pgstat.
        await conn.execute(text("SELECT pg_stat_force_next_flush()"))
        await conn.commit()


async def test_deadlock_count_grows_after_deadlock(
    engine: AsyncEngine, connection: AsyncConnection, schema: str
) -> None:
    await _create_probe(engine, schema)
    before = await deadlock_count(connection)
    barrier = asyncio.Barrier(2)
    async with engine.connect() as a, engine.connect() as b:
        results = await asyncio.gather(
            _update_crosswise(a, schema, order=(1, 2), barrier=barrier),
            _update_crosswise(b, schema, order=(2, 1), barrier=barrier),
            return_exceptions=True,
        )

    errors = [result for result in results if isinstance(result, BaseException)]
    assert len(errors) == 1
    assert isinstance(errors[0], DBAPIError)
    assert getattr(errors[0].orig, "sqlstate", None) == DEADLOCK_SQLSTATE
    after = before
    for _ in range(100):
        after = await deadlock_count(connection)
        if after > before:
            break
        await asyncio.sleep(0.05)
    # На общей БД (TALLYHO_TEST_DSN, pytest -n) дедлоки могут ловить и другие тесты.
    assert after > before
