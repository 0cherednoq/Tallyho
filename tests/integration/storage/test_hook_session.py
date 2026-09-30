"""``HookSession``: сессия tx-хука в нашей транзакции (ARCHITECTURE §7.3, A-DB-05)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho.model.errors import HookTransactionError
from tallyho.storage.tx import after_commit, hook_session, own_transaction
from tests.helpers.probe import (
    ProbeColumns,
    ProbeRow,
    committed_ids,
    create_probe,
    insert_id,
    mapped_probe,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.storage.tx import HookSession

Probe = Table[ProbeColumns]


@pytest.fixture
async def probe(engine: AsyncEngine, schema: str) -> Probe:
    return await create_probe(engine, schema)


async def test_hook_writes_commit_with_our_transaction(engine: AsyncEngine, probe: Probe) -> None:
    async with own_transaction(engine) as conn:
        async with hook_session(conn) as session:
            assert isinstance(session, AsyncSession)
            await insert_id(await session.connection(), probe, 1)
        # Сессия хука закрыта, наша транзакция продолжается.
        assert conn.in_transaction()
        await insert_id(conn, probe, 2)

    assert await committed_ids(engine, probe) == [1, 2]


async def test_pending_orm_changes_are_flushed_on_exit(engine: AsyncEngine, probe: Probe) -> None:
    with mapped_probe(probe):
        async with own_transaction(engine) as conn:
            async with hook_session(conn) as session:
                session.add(ProbeRow(7))
            # До нашего следующего шага (CAS) изменения хука уже в БД.
            rows = await conn.scalars(select(probe.c.id))
            assert list(rows) == [7]

    assert await committed_ids(engine, probe) == [7]


def _control(session: HookSession, method: str) -> Callable[[], Awaitable[None]]:
    return {"commit": session.commit, "rollback": session.rollback, "close": session.close}[method]


@pytest.mark.parametrize("method", ["commit", "rollback", "close"])
async def test_transaction_control_in_hook_fails_finalization(
    engine: AsyncEngine, probe: Probe, method: str
) -> None:
    async def finalize() -> None:
        async with own_transaction(engine) as conn, hook_session(conn) as session:
            await insert_id(await session.connection(), probe, 1)
            await _control(session, method)()

    with pytest.raises(HookTransactionError, match=method):
        await finalize()

    assert await committed_ids(engine, probe) == []


async def test_hook_error_rolls_back_everything(engine: AsyncEngine, probe: Probe) -> None:
    class HookFailedError(Exception):
        pass

    async def finalize() -> None:
        async with own_transaction(engine) as conn:
            await insert_id(conn, probe, 1)
            async with hook_session(conn) as session:
                await insert_id(await session.connection(), probe, 2)
                raise HookFailedError

    with pytest.raises(HookFailedError):
        await finalize()

    assert await committed_ids(engine, probe) == []


async def test_savepoint_inside_hook_is_allowed(engine: AsyncEngine, probe: Probe) -> None:
    async with own_transaction(engine) as conn, hook_session(conn) as session:
        await insert_id(await session.connection(), probe, 1)
        nested = await session.begin_nested()
        await insert_id(await session.connection(), probe, 2)
        await nested.rollback()

    assert await committed_ids(engine, probe) == [1]


async def test_after_commit_from_hook_waits_for_our_commit(
    engine: AsyncEngine, probe: Probe
) -> None:
    calls: list[str] = []
    async with own_transaction(engine) as conn:
        async with hook_session(conn) as session:
            await insert_id(await session.connection(), probe, 1)
            await after_commit(session, lambda: calls.append("hook"))
        assert calls == []

    assert calls == ["hook"]
