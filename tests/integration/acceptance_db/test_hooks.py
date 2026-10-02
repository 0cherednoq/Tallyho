"""A-DB-04, A-DB-05 and A-DB-08 transactional hook guarantees."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import insert, select

from tallyho.engine.finalizer import Finalizer
from tallyho.engine.operations import Operations
from tallyho.engine.producer import RootSpec
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.errors import TallyhoError
from tallyho.model.states import BatchState
from tallyho.protocols.clock import SystemClock
from tallyho.testing import FakeClock
from tests.helpers.db import deadlocks, record_db_errors
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class _InjectedHookError(TallyhoError):
    pass


def _control(session: AsyncSession, method: str) -> Callable[[], Awaitable[None]]:
    return {"commit": session.commit, "rollback": session.rollback}[method]


async def test_a_db_04_hook_failure_retries_and_commits_each_domain_result_once(
    env: Env,
    registry: HookRegistry,
) -> None:
    clock = FakeClock(datetime(2035, 1, 1, tzinfo=UTC))
    results = await create_probe(env.engine, env.schema)
    attempts: dict[int, int] = {}

    @registry.on_finalized("a-db-04")
    async def persist_result(session: AsyncSession, summary: BatchSummary) -> None:
        campaign_id = int(summary.key or "0")
        attempts[campaign_id] = attempts.get(campaign_id, 0) + 1
        if campaign_id == 5 and attempts[campaign_id] == 1:
            message = "controlled 20 percent hook failure"
            raise _InjectedHookError(message)
        _ = await session.execute(insert(results).values(id=campaign_id))

    batch_ids: list[UUID] = []
    for campaign_id in range(1, 6):
        async with env.transaction() as conn:
            batch = await env.producer.create_root(
                conn,
                RootSpec(kind="a-db-04", key=str(campaign_id)),
            )
            _ = await env.producer.seal(conn, batch.id)
        batch_ids.append(batch.id)

    finalizer = Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock,
        ids=env.producer.ids,
        hooks=registry,
    )
    for batch_id in batch_ids[:4]:
        assert await finalizer.try_finalize(batch_id)
    assert not await finalizer.try_finalize(batch_ids[-1])
    failed_row = await env.batch(batch_ids[-1])
    assert BatchState(failed_row["state"]) is BatchState.SEALED
    assert failed_row["hook_attempts"] == 1
    assert await committed_ids(env.engine, results) == [1, 2, 3, 4]

    sweeper = Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock,
        finalizer=finalizer,
        settings=SweeperSettings(finalize_grace=clock.now() - clock.now()),
    )
    assert await sweeper.finalize_stuck() == 0
    _ = clock.advance(seconds=1)
    assert await sweeper.finalize_stuck() == 1

    assert BatchState((await env.batch(batch_ids[-1]))["state"]) is BatchState.SUCCEEDED
    assert attempts == {1: 1, 2: 1, 3: 1, 4: 1, 5: 2}
    assert await committed_ids(env.engine, results) == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("method", ["commit", "rollback"])
async def test_a_db_05_transaction_control_inside_hook_blocks_finalization(
    env: Env,
    registry: HookRegistry,
    method: str,
) -> None:
    kind = f"a-db-05-{method}"

    @registry.on_finalized(kind)
    async def invalid_control(session: AsyncSession, summary: BatchSummary) -> None:
        del summary
        await _control(session, method)()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=kind))
        _ = await env.producer.seal(conn, root.id)
    finalizer = Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )
    assert not await finalizer.try_finalize(root.id)
    async with env.connection() as conn:
        row = (
            (await conn.execute(select(env.tables.batch).where(env.tables.batch.c.id == root.id)))
            .mappings()
            .one()
        )
    assert BatchState(row["state"]) is BatchState.SEALED
    assert row["hook_attempts"] == 1
    assert method in row["hook_error"]


async def test_a_db_08_domain_lock_then_pause_does_not_deadlock_finalizer(
    env: Env,
    registry: HookRegistry,
) -> None:
    probe = await create_probe(env.engine, env.schema)
    async with env.transaction() as conn:
        await insert_id(conn, probe, 1)
    entered = asyncio.Event()

    @registry.on_finalized("a-db-08")
    async def lock_domain(session: AsyncSession, summary: BatchSummary) -> None:
        del summary
        entered.set()
        _ = await session.execute(select(probe).where(probe.c.id == 1).with_for_update())

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="a-db-08"))
        _ = await env.producer.seal(conn, root.id)
    finalizer = Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )
    operations = Operations(tables=env.tables, clock=SystemClock())
    # Движок финализатора получен из env.engine и наследует слушателя: в записи
    # попадают и транзакция пользователя, и повторённые внутри библиотеки 40P01.
    with record_db_errors(env.engine) as errors:
        async with env.transaction() as conn:
            _ = await conn.execute(select(probe).where(probe.c.id == 1).with_for_update())
            finishing = asyncio.create_task(finalizer.try_finalize(root.id))
            _ = await asyncio.wait_for(entered.wait(), timeout=5)
            await operations.pause(conn, root.id)
        assert await asyncio.wait_for(finishing, timeout=5)

    assert deadlocks(errors) == []
