"""Операции над деревом в пользовательской транзакции."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, insert, select, update

from tallyho.engine.completer import ClaimOutcome, Completer, CompleterSettings, ItemRef
from tallyho.engine.finalizer import Finalizer
from tallyho.engine.operations import Operations, OperationTriggers
from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.model.calls import TaskCall
from tallyho.model.errors import DownstreamFinalized, InvalidStateError, NotFoundError
from tallyho.model.states import BatchState, ItemState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, upsert_metrics, upsert_slots
from tests.helpers.probe import create_probe, insert_id
from tests.integration.engine.completer_env import WORKER, schema_engine

if TYPE_CHECKING:
    from collections.abc import Iterable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class _RelaySpy:
    kicked: list[UUID]

    def __init__(self) -> None:
        self.kicked = []

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        self.kicked.extend(batch_ids)


class _FinalizerSpy:
    tried: list[UUID]

    def __init__(self) -> None:
        self.tried = []

    async def try_finalize(self, batch_id: UUID) -> bool:
        self.tried.append(batch_id)
        return True


def operations(env: Env) -> Operations:
    """Операции над схемой теста."""
    return Operations(tables=env.tables, clock=SystemClock())


async def tree(env: Env, *, start_at: datetime | None = None) -> tuple[UUID, UUID]:
    """Корень, потомок и по одному Item в каждом."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root", start_at=start_at))
        child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="child"))
        _ = await env.producer.add_items(
            conn, root.id, [TaskCall(task_name="work", args=(1,), kwargs={})]
        )
        _ = await env.producer.add_items(
            conn, child.id, [TaskCall(task_name="work", args=(2,), kwargs={})]
        )
    return root.id, child.id


async def test_pause_resume_and_rollback_are_transactional(env: Env) -> None:
    root_id, child_id = await tree(env)
    subject = operations(env)
    async with env.connection() as conn:
        await subject.pause(conn, root_id)
        assert (
            await conn.scalar(
                select(func.count())
                .select_from(env.tables.batch)
                .where(env.tables.batch.c.paused_at.is_not(None))
            )
        ) == 2
    assert (await env.batch(root_id))["paused_at"] is None

    async with env.transaction() as conn:
        await subject.pause(conn, root_id)
    assert (await env.batch(child_id))["paused_at"] is not None
    async with env.transaction() as conn:
        await subject.resume(conn, root_id)
    assert (await env.batch(root_id))["paused_at"] is None
    async with env.connection() as conn:
        assert (
            await conn.scalar(
                select(func.count())
                .select_from(env.tables.outbox)
                .where(env.tables.outbox.c.available_at == datetime.max.replace(tzinfo=UTC))
            )
            == 0
        )


async def test_reschedule_then_cancel_before_start(env: Env) -> None:
    future = datetime.now(UTC) + timedelta(days=2)
    root_id, child_id = await tree(env, start_at=future)
    subject = operations(env)
    moved = future + timedelta(days=1)
    async with env.transaction() as conn:
        assert await subject.reschedule(conn, root_id, moved) == 0
    async with env.transaction() as conn:
        assert await subject.cancel(conn, root_id) == 2
    assert (await env.batch(root_id))["cancel_requested_at"] is not None
    assert (await env.batch(child_id))["cancel_requested_at"] is not None
    assert await env.count(env.tables.outbox) == 0
    async with env.connection() as conn:
        states = set(
            await conn.scalars(
                select(env.tables.item.c.state).where(env.tables.item.c.child_batch_id.is_(None))
            )
        )
    assert states == {int(ItemState.CANCELLED)}


async def test_claim_during_pause_parks_dispatched_item(env: Env) -> None:
    root_id, _ = await tree(env)
    async with env.transaction() as conn:
        item_id = await conn.scalar(
            select(env.tables.item.c.id).where(
                env.tables.item.c.batch_id == root_id,
                env.tables.item.c.child_batch_id.is_(None),
            )
        )
        assert item_id is not None
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.item_id == item_id)
        )
        await operations(env).pause(conn, root_id)
    completer = Completer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        settings=CompleterSettings(worker_id=WORKER),
    )
    try:
        result = await completer.claim(ItemRef(item_id, root_id))
    finally:
        await completer.close()
    assert result.outcome is ClaimOutcome.PARKED
    async with env.connection() as conn:
        parked = await conn.scalar(
            select(func.count())
            .select_from(env.tables.outbox)
            .where(env.tables.outbox.c.item_id == item_id)
        )
    assert parked == 1


async def test_retry_stage_rejects_terminal_downstream(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        sink = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="sink", fed_by=(source.id,))
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id.in_([source.id, sink.id]))
            .values(state=int(BatchState.FAILED))
        )
    with pytest.raises(DownstreamFinalized):
        async with env.transaction() as conn:
            _ = await operations(env).retry_failed(conn, source.id)


async def test_retry_leaf_reopens_ancestors_and_keeps_pause(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root"))
        leaf = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="leaf"))
        _ = await env.producer.add_items(
            conn, leaf.id, [TaskCall(task_name="work", args=(1,), kwargs={})]
        )
        failed_id = await conn.scalar(
            select(env.tables.item.c.id).where(
                env.tables.item.c.batch_id == leaf.id,
                env.tables.item.c.child_batch_id.is_(None),
            )
        )
        virtual_id = await conn.scalar(
            select(env.tables.item.c.id).where(env.tables.item.c.child_batch_id == leaf.id)
        )
        assert failed_id is not None
        assert virtual_id is not None
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id.in_([failed_id, virtual_id]))
            .values(state=int(ItemState.ERROR), label="hard", finished_at=func.now())
        )
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.item_id == failed_id)
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id.in_([root.id, leaf.id]))
            .values(state=int(BatchState.FAILED), finished_at=func.now())
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == leaf.id)
            .values(paused_at=func.now())
        )
    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, leaf.id) == 1
    assert (await env.batch(root.id))["state"] == int(BatchState.SEALED)
    assert (await env.batch(leaf.id))["state"] == int(BatchState.SEALED)
    async with env.connection() as conn:
        available_at = await conn.scalar(
            select(env.tables.outbox.c.available_at).where(env.tables.outbox.c.item_id == failed_id)
        )
    assert available_at is not None
    assert available_at.year == 9999


async def test_retry_failed_requeues_selected_label(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root"))
        _ = await env.producer.add_items(
            conn, root.id, [TaskCall(task_name="work", args=(1,), kwargs={})]
        )
        item_id = await conn.scalar(
            select(env.tables.item.c.id).where(env.tables.item.c.batch_id == root.id)
        )
        assert item_id is not None
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == item_id)
            .values(state=int(ItemState.ERROR), label="hard", finished_at=func.now())
        )
        _ = await conn.execute(
            insert(env.tables.item_mark).values(batch_id=root.id, label="hard", item_id=item_id)
        )
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.item_id == item_id)
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(state=int(BatchState.FAILED), finished_at=func.now())
        )
        await upsert_slots(conn, env.tables, {(root.id, 7): CounterDelta(error=1, w_done=1)})
        await upsert_metrics(conn, env.tables, {(root.id, "hard", 7): 1})
    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, root.id, labels=["hard"]) == 1
    assert (await env.batch(root.id))["state"] == int(BatchState.SEALED)
    async with env.connection() as conn:
        assert await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == item_id)
        ) == int(ItemState.ACTIVE)
        assert (
            await conn.scalar(
                select(func.count())
                .select_from(env.tables.outbox)
                .where(env.tables.outbox.c.item_id == item_id)
            )
            == 1
        )


async def test_retry_root_reopens_pipeline_without_dispatching_virtual_items(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        sink = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="sink", fed_by=(source.id,))
        )
        _ = await env.producer.add_items(
            conn, source.id, [TaskCall(task_name="work", args=(1,), kwargs={})]
        )
        failed_id = await conn.scalar(
            select(env.tables.item.c.id).where(
                env.tables.item.c.batch_id == source.id,
                env.tables.item.c.child_batch_id.is_(None),
            )
        )
        assert failed_id is not None
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == failed_id)
            .values(state=int(ItemState.ERROR), label="hard", finished_at=func.now())
        )
        _ = await conn.execute(
            insert(env.tables.item_mark).values(batch_id=source.id, label="hard", item_id=failed_id)
        )
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.item_id == failed_id)
        )
        virtual_ids = list(
            await conn.scalars(
                select(env.tables.item.c.id).where(
                    env.tables.item.c.child_batch_id.in_([source.id, sink.id])
                )
            )
        )
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id.in_(virtual_ids))
            .values(state=int(ItemState.ERROR), label="sub_batch_failed", finished_at=func.now())
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id.in_([root.id, source.id, sink.id]))
            .values(state=int(BatchState.FAILED), finished_at=func.now())
        )
    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, root.id, labels=["hard"]) == 1
    assert (await env.batch(root.id))["state"] == int(BatchState.SEALED)
    assert (await env.batch(source.id))["state"] == int(BatchState.SEALED)
    assert (await env.batch(sink.id))["state"] == int(BatchState.OPEN)
    async with env.connection() as conn:
        virtual_states = set(
            await conn.scalars(
                select(env.tables.item.c.state).where(env.tables.item.c.id.in_(virtual_ids))
            )
        )
        virtual_outbox = await conn.scalar(
            select(func.count())
            .select_from(env.tables.outbox)
            .where(env.tables.outbox.c.item_id.in_(virtual_ids))
        )
    assert virtual_states == {int(ItemState.ACTIVE)}
    assert virtual_outbox == 0


async def test_pause_after_domain_lock_does_not_deadlock_finalizer(
    env: Env, registry: HookRegistry
) -> None:
    probe = await create_probe(env.engine, env.schema)
    async with env.transaction() as conn:
        await insert_id(conn, probe, 1)
    entered = asyncio.Event()

    @registry.on_finalized("root")
    async def lock_domain(session: AsyncSession, summary: BatchSummary) -> None:
        del summary
        entered.set()
        _ = await session.execute(select(probe).where(probe.c.id == 1).with_for_update())

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root"))
        _ = await env.producer.seal(conn, root.id)
    finalizer = Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )
    async with env.transaction() as conn:
        _ = await conn.execute(select(probe).where(probe.c.id == 1).with_for_update())
        finishing = asyncio.create_task(finalizer.try_finalize(root.id))
        _ = await asyncio.wait_for(entered.wait(), timeout=5)
        await operations(env).pause(conn, root.id)
    assert await asyncio.wait_for(finishing, timeout=5)


async def test_retry_finalize_release_triggers_and_invalid_states(env: Env) -> None:
    root_id, child_id = await tree(env)
    relay = _RelaySpy()
    finalizer = _FinalizerSpy()
    subject = Operations(
        tables=env.tables,
        clock=SystemClock(),
        triggers=OperationTriggers(relay=relay, finalizer=finalizer),
    )
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id.in_([root_id, child_id]))
            .values(hook_attempts=3, hook_error="boom")
        )
        await subject.retry_finalize(conn, root_id)
        await subject.resume(conn, root_id)
    await subject.close()
    assert set(relay.kicked) == {root_id, child_id}
    assert set(finalizer.tried) == {root_id, child_id}
    assert (await env.batch(root_id))["hook_attempts"] == 0
    async with env.transaction() as conn:
        with pytest.raises(InvalidStateError):
            await subject.release(conn, root_id)
        with pytest.raises(InvalidStateError):
            _ = await subject.retry_failed(conn, root_id)
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root_id)
            .values(state=int(BatchState.SUCCEEDED), finished_at=func.now())
        )
        await subject.release(conn, root_id)
    assert (await env.batch(root_id))["released_at"] is not None
    missing = env.producer.ids.new_id()
    async with env.transaction() as conn:
        with pytest.raises(NotFoundError):
            await subject.pause(conn, missing)
        with pytest.raises(NotFoundError):
            _ = await subject.retry_failed(conn, missing)
        with pytest.raises(NotFoundError):
            await subject.release(conn, missing)
