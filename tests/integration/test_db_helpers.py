"""Проверки хелперов tests/helpers/db.py на живом PostgreSQL."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import column, insert, table, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.helpers.db import (
    DEADLOCK_SQLSTATE,
    backend_pid,
    deadlock_count,
    deadlocks,
    held_locks,
    record_db_errors,
    wait_blocked_by,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tests.helpers.db import LockRow


async def _create_probe(engine: AsyncEngine, schema: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE TABLE "{schema}".probe (id int PRIMARY KEY, v int)'))
        probe = table("probe", column("id"), column("v"), schema=schema)
        await conn.execute(insert(probe), [{"id": 1, "v": 0}, {"id": 2, "v": 0}])


def _on_probe(locks: list[LockRow], pid: int, schema: str) -> list[LockRow]:
    return [lock for lock in locks if lock.pid == pid and lock.relation == f"{schema}.probe"]


async def test_held_locks_shows_granted_and_waiting(
    engine: AsyncEngine, connection: AsyncConnection, schema: str
) -> None:
    await _create_probe(engine, schema)
    own_pid = await backend_pid(connection)
    locks: list[LockRow] = []
    async with engine.connect() as holder, engine.connect() as waiter:
        holder_pid, waiter_pid = await backend_pid(holder), await backend_pid(waiter)
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
    own_pid = await backend_pid(connection)
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


async def test_record_db_errors_sees_only_deadlocks_of_its_engine(
    engine: AsyncEngine, postgres_dsn: str, schema: str
) -> None:
    await _create_probe(engine, schema)
    other = create_async_engine(postgres_dsn)
    scoped = engine.execution_options(schema_translate_map={None: schema})
    barrier = asyncio.Barrier(2)
    try:
        with record_db_errors(engine) as own, record_db_errors(other) as foreign:
            async with scoped.connect() as a, scoped.connect() as b:
                results = await asyncio.gather(
                    _update_crosswise(a, schema, order=(1, 2), barrier=barrier),
                    _update_crosswise(b, schema, order=(2, 1), barrier=barrier),
                    return_exceptions=True,
                )
        assert len([result for result in results if isinstance(result, DBAPIError)]) == 1
        # Слушатель родительского движка видит соединения движка с execution_options.
        (deadlock,) = deadlocks(own)
        assert deadlock.statement is not None
        assert "probe" in deadlock.statement
        assert foreign == []

        async with engine.connect() as conn:
            with pytest.raises(DBAPIError):
                _ = await conn.execute(text("SELECT 1 / 0"))
        # Слушатель снят: ошибки после выхода из контекста не записываются.
        assert deadlocks(own) == own
        assert [error.sqlstate for error in own] == [DEADLOCK_SQLSTATE]
    finally:
        await other.dispose()


async def test_wait_blocked_by_returns_when_holder_blocks_another_backend(
    engine: AsyncEngine, connection: AsyncConnection, schema: str
) -> None:
    await _create_probe(engine, schema)
    probe = table("probe", column("id"), column("v"), schema=schema)
    async with engine.connect() as holder, engine.connect() as waiter:
        holder_pid = await backend_pid(holder)
        with pytest.raises(AssertionError, match="никто не ждёт"):
            await wait_blocked_by(connection, holder_pid, attempts=3)
        _ = await holder.execute(update(probe).where(probe.c.id == 1).values(v=1))
        waiting = asyncio.create_task(
            waiter.execute(update(probe).where(probe.c.id == 1).values(v=2))
        )
        await wait_blocked_by(connection, holder_pid)
        assert not waiting.done()
        await holder.rollback()
        _ = await waiting
        await waiter.rollback()
