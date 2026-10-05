"""Провал дерева политикой финализирует его снизу вверх (ARCHITECTURE §8.1 п.7, I-09, Fix-28).

Политика с ``action="fail"`` (``fail_fast`` — её частный случай) ставит запрос отмены всему
дереву и сразу отменяет неотправленные Items, как ``cancel()``. Виртуальные Items под-батчей
она не трогает: их завершает финализация под-батча. Этап с ``fed_by`` финализируется не
раньше своего источника, родитель — после всех детей.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from sqlalchemy import select

from tallyho.engine.completer import (
    Completer,
    CompleterSettings,
    CompleterTriggers,
    FinishResult,
    ItemRef,
)
from tallyho.engine.finalizer import Finalizer
from tallyho.engine.operations import Operations
from tallyho.engine.policy import PolicyEnforcer
from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.model.calls import TaskCall
from tallyho.model.policy import FailurePolicy
from tallyho.model.states import BatchState, CancelReason, ItemState, ResultClass
from tallyho.protocols.clock import SystemClock
from tests.integration.engine.completer_env import WORKER, schema_engine

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


def finalizer(env: Env, registry: HookRegistry) -> Finalizer:
    """Finalizer над схемой теста."""
    return Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )


@contextlib.asynccontextmanager
async def completer(env: Env, registry: HookRegistry) -> AsyncGenerator[Completer]:
    """Completer с политиками и финализацией после commit."""
    subject = Completer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        settings=CompleterSettings(worker_id=WORKER),
        triggers=CompleterTriggers(
            policy=PolicyEnforcer(
                tables=env.tables, engine=schema_engine(env), clock=SystemClock(), hooks=registry
            ),
            finalizer=finalizer(env, registry),
        ),
    )
    try:
        yield subject
    finally:
        await subject.close()


def record_finalized(registry: HookRegistry, order: list[str]) -> None:
    """Записывать ``key`` каждого финализированного узла дерева ``pipe``."""

    def register(kind: str) -> None:
        @registry.on_finalized(kind)
        async def save(_session: AsyncSession, summary: BatchSummary) -> None:
            await asyncio.sleep(0)
            order.append(summary.key or "root")

    for kind in ("pipe", "pipe.src", "pipe.dst"):
        register(kind)


async def pipeline(env: Env, *, policy: FailurePolicy) -> tuple[UUID, UUID, UUID, list[ItemRef]]:
    """Корень → ``src`` (с политикой) → ``dst`` (``fed_by=[src]``); три Items в ``src``."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="pipe"))
        src = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="src", failure_policy=policy)
        )
        dst = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="dst", fed_by=(src.id,))
        )
        _ = await env.producer.add_items(
            conn,
            src.id,
            [TaskCall(task_name="work", args=(index,), kwargs={}) for index in range(3)],
        )
        _ = await env.producer.seal(conn, src.id)
        _ = await env.producer.seal(conn, root.id)
        item = env.tables.item
        ids = list(
            await conn.scalars(
                select(item.c.id).where(item.c.batch_id == src.id).order_by(item.c.id)
            )
        )
    return root.id, src.id, dst.id, [ItemRef(item_id, src.id) for item_id in ids]


async def dispatch(env: Env, *refs: ItemRef) -> None:
    """Имитировать отправку: записи outbox Items забрал relay."""
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        _ = await conn.execute(
            outbox.delete().where(outbox.c.item_id.in_([ref.id for ref in refs]))
        )


async def virtual_items(env: Env, root_id: UUID) -> list[tuple[ItemState, str | None]]:
    """``(state, label)`` виртуальных Items корня в порядке id."""
    item = env.tables.item
    async with env.connection() as conn:
        rows = await conn.execute(
            select(item.c.state, item.c.label)
            .where(item.c.batch_id == root_id, item.c.child_batch_id.is_not(None))
            .order_by(item.c.id)
        )
        return [(ItemState(state), label) for state, label in rows]


async def test_policy_fail_finalizes_tree_bottom_up(env: Env, registry: HookRegistry) -> None:
    order: list[str] = []
    record_finalized(registry, order)
    root_id, src_id, dst_id, refs = await pipeline(env, policy=FailurePolicy.fail_fast())
    running, failing, queued = refs
    await dispatch(env, running, failing)

    async with completer(env, registry) as worker:
        assert (await worker.claim(running)).run
        assert (await worker.claim(failing)).run
        assert await worker.finish(
            failing, FinishResult(result_class=ResultClass.ERROR, label="broken")
        )
        await worker.settled()
        # Политика провалила всё дерево; неотправленный Item отменён сразу.
        for batch_id in (root_id, src_id, dst_id):
            assert (await env.batch(batch_id))["cancel_reason"] == CancelReason.FAIL_FAST.value
        async with env.connection() as conn:
            state = await conn.scalar(
                select(env.tables.item.c.state).where(env.tables.item.c.id == queued.id)
            )
        assert state == ItemState.CANCELLED
        # Пока выполняется Item источника, никто в дереве не финализируется: виртуальные
        # Items корня активны, этап ждёт источник, корень — детей.
        assert await virtual_items(env, root_id) == [(ItemState.ACTIVE, None)] * 2
        subject = finalizer(env, registry)
        for batch_id in (dst_id, root_id, src_id):
            assert not await subject.try_finalize(batch_id)
        assert order == []

        assert await worker.finish(running, FinishResult(result_class=ResultClass.OK))
        await worker.settled()

    assert order == ["src", "dst", "root"]
    rows = {batch_id: await env.batch(batch_id) for batch_id in (root_id, src_id, dst_id)}
    for row in rows.values():
        assert (row["state"], row["cancel_reason"]) == (
            BatchState.FAILED,
            CancelReason.FAIL_FAST.value,
        )
    assert rows[src_id]["finished_at"] <= rows[dst_id]["finished_at"]
    assert rows[dst_id]["finished_at"] <= rows[root_id]["finished_at"]
    # Итог виртуальным Items дала финализация под-батчей, а не прямая отмена.
    assert await virtual_items(env, root_id) == [(ItemState.OK, "ok")] * 2
    totals = await env.counters(root_id)
    assert (totals.ok, totals.cancelled, totals.pending) == (2, 0, 0)


async def test_cancel_root_waits_for_running_source(env: Env, registry: HookRegistry) -> None:
    # Тот же порядок у ручной отмены корня: отменённый этап не опережает источник.
    order: list[str] = []
    record_finalized(registry, order)
    root_id, src_id, dst_id, refs = await pipeline(env, policy=FailurePolicy.continue_())

    await dispatch(env, refs[0])
    async with completer(env, registry) as worker:
        assert (await worker.claim(refs[0])).run
        async with env.transaction() as conn:
            assert (
                await Operations(tables=env.tables, clock=SystemClock()).cancel(conn, root_id) == 2
            )
        subject = finalizer(env, registry)
        for batch_id in (dst_id, root_id, src_id):
            assert not await subject.try_finalize(batch_id)
        assert order == []
        assert await worker.finish(refs[0], FinishResult(result_class=ResultClass.OK))
        await worker.settled()

    assert order == ["src", "dst", "root"]
    for batch_id in (root_id, src_id, dst_id):
        assert (await env.batch(batch_id))["state"] == BatchState.CANCELLED
    assert await virtual_items(env, root_id) == [(ItemState.OK, "ok")] * 2
