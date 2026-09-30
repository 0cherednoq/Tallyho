"""Транзакции storage на PostgreSQL: чужие сессии, свои транзакции, повтор."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Table, func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho.model.errors import ConcurrentModification
from tallyho.storage.tx import (
    RetryPolicy,
    TxSettings,
    own_transaction,
    resolve_connection,
    run_transaction,
    sqlstate_of,
)
from tests.helpers.probe import ProbeColumns, committed_ids, create_probe, insert_id, seeded

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

FAST = RetryPolicy(attempts=4, base_delay=0.001, max_delay=0.01)


def _raise_sql(code: str) -> str:
    return f"DO $$ BEGIN RAISE EXCEPTION 'test' USING ERRCODE = '{code}'; END $$"


Probe = Table[ProbeColumns]


@pytest.fixture
async def probe(engine: AsyncEngine, schema: str) -> Probe:
    return await create_probe(engine, schema)


class Sleeps:
    """Запоминает паузы между попытками вместо реального ожидания."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


# --- resolve_connection ---------------------------------------------------------------


async def test_session_connection_joins_user_transaction(engine: AsyncEngine, probe: Probe) -> None:
    async with AsyncSession(engine) as session:
        conn = await resolve_connection(session)
        await insert_id(conn, probe, 1)
        await session.rollback()

    assert await committed_ids(engine, probe) == []


async def test_session_commit_keeps_our_writes(engine: AsyncEngine, probe: Probe) -> None:
    async with AsyncSession(engine) as session:
        conn = await resolve_connection(session)
        await insert_id(conn, probe, 1)
        await session.commit()

    assert await committed_ids(engine, probe) == [1]


async def test_savepoint_rollback_discards_our_writes(engine: AsyncEngine, probe: Probe) -> None:
    async with AsyncSession(engine) as session:
        conn = await resolve_connection(session)
        await insert_id(conn, probe, 1)
        nested = await session.begin_nested()
        inner = await resolve_connection(session)
        await insert_id(inner, probe, 2)
        await nested.rollback()
        await session.commit()

    assert await committed_ids(engine, probe) == [1]


@pytest.mark.parametrize(
    ("autoflush", "expire_on_commit"), [(True, True), (False, False), (False, True)]
)
async def test_session_options_do_not_matter(
    engine: AsyncEngine, probe: Probe, *, autoflush: bool, expire_on_commit: bool
) -> None:
    async with AsyncSession(
        engine, autoflush=autoflush, expire_on_commit=expire_on_commit
    ) as session:
        conn = await resolve_connection(session)
        await insert_id(conn, probe, 1)
        await session.commit()

    assert await committed_ids(engine, probe) == [1]


async def test_connection_is_returned_as_is(connection: AsyncConnection) -> None:
    assert await resolve_connection(connection) is connection


# --- own_transaction ------------------------------------------------------------------


async def _setting(conn: AsyncConnection, name: str) -> str:
    return str(await conn.scalar(select(func.current_setting(name))))


async def test_own_transaction_sets_local_timeouts(engine: AsyncEngine) -> None:
    async with own_transaction(engine) as conn:
        backend = await conn.scalar(select(func.pg_backend_pid()))
        assert await _setting(conn, "lock_timeout") == "5s"
        assert await _setting(conn, "statement_timeout") == "30s"

    # SET LOCAL: после транзакции у того же соединения из пула настройки прежние.
    async with engine.connect() as conn:
        assert await conn.scalar(select(func.pg_backend_pid())) == backend
        assert await _setting(conn, "lock_timeout") == "0"


async def test_statement_timeout_can_be_left_to_server(engine: AsyncEngine) -> None:
    settings = TxSettings(lock_timeout=timedelta(milliseconds=250), statement_timeout=None)

    async with own_transaction(engine, settings) as conn:
        assert await _setting(conn, "lock_timeout") == "250ms"
        assert await _setting(conn, "statement_timeout") == "0"


async def test_own_transaction_commits_on_success(engine: AsyncEngine, probe: Probe) -> None:
    async with own_transaction(engine) as conn:
        await insert_id(conn, probe, 1)

    assert await committed_ids(engine, probe) == [1]


async def test_own_transaction_rolls_back_on_error(engine: AsyncEngine, probe: Probe) -> None:
    async def insert_then_fail() -> None:
        async with own_transaction(engine) as conn:
            await insert_id(conn, probe, 1)
            _ = 1 / 0

    with pytest.raises(ZeroDivisionError):
        await insert_then_fail()

    assert await committed_ids(engine, probe) == []


# --- run_transaction ------------------------------------------------------------------


async def test_retries_on_artificial_deadlock(engine: AsyncEngine, probe: Probe) -> None:
    attempts: list[int] = []
    sleeps = Sleeps()

    async def work(conn: AsyncConnection) -> str:
        attempts.append(len(attempts))
        await insert_id(conn, probe, len(attempts))
        if len(attempts) < 3:
            _ = await conn.execute(text(_raise_sql("40P01")))
        return "done"

    result = await run_transaction(engine, work, policy=FAST, rng=seeded(3), sleep=sleeps)

    assert result == "done"
    assert attempts == [0, 1, 2]
    assert len(sleeps.delays) == 2
    # Упавшие попытки откатились целиком.
    assert await committed_ids(engine, probe) == [3]


async def test_retry_delays_are_deterministic_for_seed(engine: AsyncEngine) -> None:
    runs: list[list[float]] = []
    for _ in range(2):
        sleeps = Sleeps()
        calls = 0

        async def work(conn: AsyncConnection) -> None:
            nonlocal calls
            calls += 1
            if calls < 4:
                _ = await conn.execute(text(_raise_sql("40001")))

        await run_transaction(engine, work, policy=FAST, rng=seeded(9), sleep=sleeps)
        runs.append(sleeps.delays)

    assert runs[0] == runs[1]
    assert len(runs[0]) == 3


async def test_gives_up_after_policy_attempts(engine: AsyncEngine) -> None:
    sleeps = Sleeps()
    calls = 0

    async def work(conn: AsyncConnection) -> None:
        nonlocal calls
        calls += 1
        _ = await conn.execute(text(_raise_sql("40P01")))

    with pytest.raises(ConcurrentModification) as info:
        await run_transaction(engine, work, policy=FAST, sleep=sleeps)

    assert calls == FAST.attempts
    assert len(sleeps.delays) == FAST.attempts - 1
    assert sqlstate_of(info.value.__cause__ or info.value) == "40P01"


async def test_other_errors_are_not_retried(engine: AsyncEngine) -> None:
    sleeps = Sleeps()
    calls = 0

    async def work(conn: AsyncConnection) -> None:
        nonlocal calls
        calls += 1
        _ = await conn.execute(text("SELECT 1 / 0"))

    with pytest.raises(DBAPIError) as info:
        await run_transaction(engine, work, policy=FAST, sleep=sleeps)

    assert sqlstate_of(info.value) == "22012"
    assert calls == 1
    assert sleeps.delays == []


async def test_foreign_lock_hits_lock_timeout_and_is_retried(
    engine: AsyncEngine, probe: Probe
) -> None:
    async with engine.begin() as conn:
        await insert_id(conn, probe, 1)

    settings = TxSettings(lock_timeout=timedelta(milliseconds=50))
    holder = await engine.connect()
    try:
        _ = await holder.execute(select(probe.c.id).with_for_update())
        errors: list[str | None] = []

        async def release_on_first_retry(delay: float) -> None:
            await asyncio.sleep(delay)
            if holder.in_transaction():
                await holder.rollback()

        async def work(conn: AsyncConnection) -> int:
            try:
                locked = await conn.scalar(select(probe.c.id).with_for_update())
            except DBAPIError as exc:
                errors.append(sqlstate_of(exc))
                raise
            return int(locked or 0)

        locked = await run_transaction(
            engine, work, settings=settings, policy=FAST, sleep=release_on_first_retry
        )
    finally:
        await holder.close()

    assert locked == 1
    assert errors == ["55P03"]
