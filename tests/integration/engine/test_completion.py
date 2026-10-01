"""Путь B: завершение Item внутри транзакции пользователя."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import column, func, select, table, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho.engine.completer import FinishResult, ItemRef, SpawnRequest
from tallyho.engine.completion import complete_in
from tallyho.engine.spawn import SpawnRoute
from tallyho.model.calls import TaskCall
from tallyho.model.states import ItemState, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.storage.tx import resolve_connection
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    Finalized,
    RecordingProgress,
    open_completer,
    schema_engine,
    seed,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class RecordingFinishObserver(NullObserver):
    """Record the scalar completion event emitted after commit."""

    def __init__(self) -> None:
        self.finished: list[tuple[UUID, ResultClass, str | None, int]] = []

    @override
    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        del batch_id
        self.finished.append((item_id, result, label, attempt))


async def _state(env: Env, item_id: UUID) -> ItemState:
    async with env.connection() as conn:
        value = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == item_id)
        )
    assert value is not None
    return ItemState(value)


async def test_complete_in_commits_domain_item_delta_and_then_folds(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    finalizer = Finalized()
    progress = RecordingProgress()
    async with (
        open_completer(env, finalizer=finalizer, progress=progress) as completer,
        AsyncSession(schema_engine(env)) as session,
    ):
        await insert_id(await resolve_connection(session), probe, 1)
        changed = await complete_in(
            session,
            ref,
            FinishResult(
                result_class=ResultClass.OK,
                result={"message_id": "m-1"},
                metrics={"bytes": 42},
            ),
            completer=completer,
        )
        assert changed
        await session.commit()

    assert await committed_ids(env.engine, probe) == [1]
    assert await _state(env, ref.id) is ItemState.OK
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.w_done, counters.pending) == (1, 1, 0)
    async with env.connection() as conn:
        delta_count = await conn.scalar(select(func.count()).select_from(env.tables.counter_delta))
        metrics = (
            await conn.execute(
                select(
                    env.tables.metric.c.name,
                    env.tables.metric.c.slot,
                    env.tables.metric.c.value,
                ).order_by(env.tables.metric.c.name)
            )
        ).all()
    assert delta_count == 0
    assert metrics == [("bytes", COMPLETER_SLOT, 42), ("ok", COMPLETER_SLOT, 1)]
    assert finalizer.calls == [seeded.batch_id]
    assert progress.calls == [([seeded.batch_id], False)]


async def test_complete_in_outer_rollback_removes_domain_and_completion(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer, AsyncSession(schema_engine(env)) as session:
        await insert_id(await resolve_connection(session), probe, 1)
        assert await complete_in(
            session,
            ref,
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        await session.rollback()

    assert await committed_ids(env.engine, probe) == []
    assert await _state(env, ref.id) is ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 1


async def test_complete_in_savepoint_rollback_discards_callback_and_writes(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    finalizer = Finalized()
    async with (
        open_completer(env, finalizer=finalizer) as completer,
        AsyncSession(schema_engine(env)) as session,
    ):
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
    assert await _state(env, ref.id) is ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 1
    assert finalizer.calls == []


async def test_complete_in_duplicate_returns_false_and_counts_once(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        async with AsyncSession(schema_engine(env)) as first:
            assert await complete_in(
                first,
                ref,
                FinishResult(result_class=ResultClass.SKIP),
                completer=completer,
            )
            await first.commit()
        async with AsyncSession(schema_engine(env)) as second:
            assert not await complete_in(
                second,
                ref,
                FinishResult(result_class=ResultClass.ERROR),
                completer=completer,
            )
            await second.commit()

    counters = await env.counters(seeded.batch_id)
    assert (counters.skip, counters.error, counters.pending) == (1, 0, 0)


async def test_complete_in_scalar_persists_fields_observer_and_batch_guard(env: Env) -> None:
    seeded = await seed(env, 2)
    error_ref, untouched_ref = seeded.refs
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.item).where(env.tables.item.c.id == error_ref.id).values(attempt=4)
        )

    observer = RecordingFinishObserver()
    value = FinishResult(
        result_class=ResultClass.ERROR,
        label="rejected",
        result={"provider": "mx-1"},
        error={"code": 550},
    )
    async with open_completer(env, observer=observer) as completer:
        async with AsyncSession(schema_engine(env)) as session:
            assert await complete_in(session, error_ref, value, completer=completer)
            await session.commit()
        async with AsyncSession(schema_engine(env)) as session:
            wrong = ItemRef(untouched_ref.id, uuid4())
            assert not await complete_in(session, wrong, value, completer=completer)
            await session.commit()

    async with env.connection() as conn:
        rows = (
            await conn.execute(
                select(env.tables.item).where(
                    env.tables.item.c.id.in_([error_ref.id, untouched_ref.id])
                )
            )
        ).mappings()
        by_id = {row["id"]: row for row in rows}
    changed = by_id[error_ref.id]
    assert (
        changed["state"],
        changed["label"],
        changed["result"],
        changed["error"],
        changed["finished_at"],
    ) == (
        ItemState.ERROR,
        "rejected",
        {"provider": "mx-1"},
        {"code": 550},
        NOW,
    )
    assert by_id[untouched_ref.id]["state"] == ItemState.ACTIVE
    assert observer.finished == [(error_ref.id, ResultClass.ERROR, "rejected", 4)]


async def test_complete_in_connection_spawns_atomically_and_folds_all_deltas(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    route = SpawnRoute(
        source_id=seeded.batch_id,
        target_id=seeded.batch_id,
        root_id=seeded.batch_id,
    )
    value = FinishResult(
        result_class=ResultClass.ERROR,
        label="retryable",
        spawns=(
            SpawnRequest(
                route=route,
                call=TaskCall(task_name="retry", key="retry:1"),
            ),
        ),
    )
    async with open_completer(env) as completer, env.transaction() as conn:
        assert await complete_in(conn, ref, value, completer=completer)

    counters = await env.counters(seeded.batch_id)
    assert (counters.total, counters.error, counters.pending, counters.tree_total) == (2, 1, 1, 2)
    assert await env.count(env.tables.counter_delta) == 0
    assert await env.count(env.tables.item_mark) == 1


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_complete_in_under_strict_isolation_has_no_serialization_failures(
    env: Env, isolation: str
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


async def test_open_complete_in_transaction_does_not_lock_counter(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer, AsyncSession(schema_engine(env)) as session:
        assert await complete_in(
            session,
            ref,
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        async with env.connection() as conn:
            locks = table("pg_locks", column("relation"))
            locked = await conn.scalar(
                select(func.count())
                .select_from(locks)
                .where(locks.c.relation == func.to_regclass(f'"{env.schema}"."th_counter"'))
            )
        assert locked == 0
        await session.rollback()


async def test_twenty_percent_long_transactions_do_not_block_counter(env: Env) -> None:
    seeded = await seed(env, 10)
    release = asyncio.Event()
    started = asyncio.Event()
    count = 0

    async with open_completer(env) as completer:

        async def finish(index: int, *, slow: bool) -> bool:
            nonlocal count
            async with AsyncSession(schema_engine(env)) as session:
                changed = await complete_in(
                    session,
                    seeded.refs[index],
                    FinishResult(result_class=ResultClass.OK, label=f"item-{index}"),
                    completer=completer,
                )
                if slow:
                    count += 1
                    if count == 2:
                        started.set()
                    await release.wait()
                await session.commit()
                return changed

        slow = [asyncio.create_task(finish(index, slow=True)) for index in range(2)]
        await started.wait()
        assert all(await asyncio.gather(*(finish(index, slow=False) for index in range(2, 10))))
        await asyncio.sleep(2)
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
        assert all(await asyncio.gather(*slow))
