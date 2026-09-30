"""A-DB-01, A-DB-06, A-DB-07, A-DB-09 and A-DB-10."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal

import pytest
from sqlalchemy import column, func, select, table
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho
from tallyho.engine.completer import FinishResult
from tallyho.engine.completion import complete_in
from tallyho.model.states import ItemState, ResultClass
from tallyho.storage.tx import resolve_connection
from tallyho.testing import InlineBroker
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import open_completer, schema_engine, seed

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

Resource = Literal["session", "connection", "configured-session"]


async def _pending_work(value: int) -> None:
    await asyncio.sleep(0)
    assert value >= 0


async def _item_state(env: Env, item_id: UUID) -> ItemState:
    async with env.connection() as conn:
        value = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == item_id)
        )
    assert value is not None
    return ItemState(value)


@asynccontextmanager
async def _resource(
    env: Env,
    kind: Resource,
) -> AsyncGenerator[AsyncSession | AsyncConnection]:
    engine = schema_engine(env)
    if kind == "connection":
        async with engine.connect() as connection:
            yield connection
        return
    if kind == "configured-session":
        async with AsyncSession(engine, autoflush=False, expire_on_commit=False) as session:
            yield session
        return
    async with AsyncSession(engine) as session:
        yield session


async def test_a_db_01_rollback_then_reexecution_commits_domain_effect_once(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer:
        async with AsyncSession(schema_engine(env)) as first:
            await insert_id(await resolve_connection(first), probe, 1)
            assert await complete_in(
                first,
                ref,
                FinishResult(result_class=ResultClass.OK),
                completer=completer,
            )
            await first.rollback()

        assert await committed_ids(env.engine, probe) == []
        assert await _item_state(env, ref.id) is ItemState.ACTIVE

        async with AsyncSession(schema_engine(env)) as retry:
            await insert_id(await resolve_connection(retry), probe, 1)
            assert await complete_in(
                retry,
                ref,
                FinishResult(result_class=ResultClass.OK),
                completer=completer,
            )
            await retry.commit()

    assert await committed_ids(env.engine, probe) == [1]
    assert await _item_state(env, ref.id) is ItemState.OK
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (1, 0)


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_a_db_06_strict_isolation_has_no_tallyho_serialization_failures(
    env: Env,
    isolation: str,
) -> None:
    seeded = await seed(env, 12)
    engine = schema_engine(env).execution_options(isolation_level=isolation)
    async with open_completer(env) as completer:

        async def finish(index: int) -> bool:
            async with AsyncSession(engine) as session:
                changed = await complete_in(
                    session,
                    seeded.refs[index],
                    FinishResult(result_class=ResultClass.OK, label=f"ok-{index}"),
                    completer=completer,
                )
                await session.commit()
                return changed

        assert all(await asyncio.gather(*(finish(index) for index in range(12))))

    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (12, 0)

    broker = InlineBroker()
    th = Tallyho(engine, schema=env.schema)
    th.install(broker.adapter)
    try:

        async def create(index: int) -> UUID:
            async with AsyncSession(engine) as session:
                async with th.batch(
                    "a-db-06",
                    key=f"{isolation}:{index}",
                    start_at=datetime(2035, 1, 1, tzinfo=UTC),
                    session=session,
                ) as batch:
                    await batch.add(_pending_work, index)
                await session.commit()
                return batch.handle.id

        batch_ids = [await create(index) for index in range(4)]

        async def pause(batch_id: UUID) -> None:
            async with AsyncSession(engine) as session:
                await th.handle(batch_id).pause(session=session)
                await session.commit()

        for batch_id in batch_ids:
            await pause(batch_id)
        views = await asyncio.gather(*(th.handle(batch_id).view() for batch_id in batch_ids))
        assert all(view.paused for view in views)
    finally:
        await broker.close()


async def test_a_db_07_long_user_transactions_do_not_wait_on_counter(env: Env) -> None:
    seeded = await seed(env, 10)
    release = asyncio.Event()
    started = asyncio.Event()
    slow_started = 0
    async with open_completer(env) as completer:

        async def finish(index: int, *, slow: bool) -> bool:
            nonlocal slow_started
            async with AsyncSession(schema_engine(env)) as session:
                changed = await complete_in(
                    session,
                    seeded.refs[index],
                    FinishResult(result_class=ResultClass.OK, label=f"task-{index}"),
                    completer=completer,
                )
                if slow:
                    slow_started += 1
                    if slow_started == 2:
                        started.set()
                    await release.wait()
                await session.commit()
                return changed

        slow_tasks = [asyncio.create_task(finish(index, slow=True)) for index in range(2)]
        await started.wait()
        assert all(await asyncio.gather(*(finish(index, slow=False) for index in range(2, 10))))
        await asyncio.sleep(0.2)
        async with env.connection() as conn:
            locks = table(
                "pg_locks",
                column("relation"),
                column("granted"),
                column("waitstart"),
            )
            waiting = await conn.scalar(
                select(func.count())
                .select_from(locks)
                .where(
                    locks.c.relation == func.to_regclass(f'"{env.schema}"."th_counter"'),
                    locks.c.granted.is_(False),
                    locks.c.waitstart < func.now() - timedelta(milliseconds=100),
                )
            )
        assert waiting == 0
        release.set()
        assert all(await asyncio.gather(*slow_tasks))


async def test_a_db_09_savepoint_rollback_discards_domain_and_completion(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer, AsyncSession(schema_engine(env)) as session:
        nested = await session.begin_nested()
        await insert_id(await resolve_connection(session), probe, 1)
        assert await complete_in(
            session,
            ref,
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        await nested.rollback()
        await session.commit()

    assert await committed_ids(env.engine, probe) == []
    assert await _item_state(env, ref.id) is ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 1


@pytest.mark.parametrize("kind", ["session", "connection", "configured-session"])
async def test_a_db_10_supported_connection_shapes_behave_identically(
    env: Env,
    kind: Resource,
) -> None:
    seeded = await seed(env, 1)
    async with open_completer(env) as completer, _resource(env, kind) as resource:
        assert await complete_in(
            resource,
            seeded.refs[0],
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        await resource.commit()

    assert await _item_state(env, seeded.refs[0].id) is ItemState.OK
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (1, 0)
