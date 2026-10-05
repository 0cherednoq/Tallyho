"""Политики после flush: пороги, однократность и атомарность tx-хука."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import event

from tallyho.engine.completer import (
    Completer,
    CompleterSettings,
    CompleterTriggers,
    FinishResult,
    ItemRef,
)
from tallyho.engine.finalizer import Finalizer
from tallyho.engine.policy import PolicyEnforcer
from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.model.calls import TaskCall
from tallyho.model.policy import FailurePolicy, PolicyAction
from tallyho.model.states import BatchState, CancelReason, ItemState, ResultClass
from tallyho.protocols.clock import SystemClock
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import WORKER, schema_engine

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.engine.completer import FinalizeTrigger
    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.policy import PolicyBreach
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


def enforcer(env: Env, registry: HookRegistry) -> PolicyEnforcer:
    """PolicyEnforcer над схемой теста."""
    return PolicyEnforcer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        hooks=registry,
    )


def finalizer(env: Env, registry: HookRegistry) -> Finalizer:
    """Finalizer над схемой теста."""
    return Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )


async def seeded(
    env: Env, *, policy: FailurePolicy, count: int, seal: bool = False
) -> tuple[UUID, list[ItemRef]]:
    """Создать батч и вернуть его id и Items."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail", failure_policy=policy))
        _ = await env.producer.add_items(
            conn,
            root.id,
            [TaskCall(task_name="send", args=(index,), kwargs={}) for index in range(count)],
        )
        if seal:
            _ = await env.producer.seal(conn, root.id)
        item_ids = list(
            await conn.scalars(
                env.tables.item.select()
                .with_only_columns(env.tables.item.c.id)
                .where(env.tables.item.c.batch_id == root.id)
                .order_by(env.tables.item.c.id)
            )
        )
    return root.id, [ItemRef(item_id, root.id) for item_id in item_ids]


async def seeded_child(
    env: Env, *, policy: FailurePolicy, count: int
) -> tuple[UUID, UUID, list[ItemRef]]:
    """Создать дерево с политикой на дочернем батче."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail"))
        child = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(key="send", failure_policy=policy),
        )
        _ = await env.producer.add_items(
            conn,
            child.id,
            [TaskCall(task_name="send", args=(index,), kwargs={}) for index in range(count)],
        )
        item_ids = list(
            await conn.scalars(
                env.tables.item.select()
                .with_only_columns(env.tables.item.c.id)
                .where(env.tables.item.c.batch_id == child.id)
                .order_by(env.tables.item.c.id)
            )
        )
    return root.id, child.id, [ItemRef(item_id, child.id) for item_id in item_ids]


@contextlib.asynccontextmanager
async def policy_completer(
    env: Env, policy: PolicyEnforcer, finalizer_trigger: FinalizeTrigger | None = None
) -> AsyncGenerator[Completer]:
    """Completer, связанный с обработчиком политики теста."""
    subject = Completer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        settings=CompleterSettings(worker_id=WORKER),
        triggers=CompleterTriggers(policy=policy, finalizer=finalizer_trigger),
    )
    try:
        yield subject
    finally:
        await subject.close()


async def test_threshold_pauses_tree_and_hook_commits_once(
    env: Env, registry: HookRegistry
) -> None:
    calls: list[PolicyBreach] = []

    @registry.on_policy_breach("mail")
    async def record(session: AsyncSession, summary: BatchSummary, breach: PolicyBreach) -> None:
        del session
        await asyncio.sleep(0)
        assert summary.kind == "mail"
        calls.append(breach)

    root_id, child_id, refs = await seeded_child(
        env,
        policy=FailurePolicy.threshold(
            ratio=0.05, min_processed=25, labels=["hard_bounce"], action="pause"
        ),
        count=25,
    )
    subject = enforcer(env, registry)
    async with policy_completer(env, subject) as completer:
        for index, ref in enumerate(refs):
            claimed = await completer.claim(ref)
            assert claimed.run
            failed = index >= 23
            assert await completer.finish(
                ref,
                FinishResult(
                    result_class=ResultClass.ERROR if failed else ResultClass.OK,
                    label="hard_bounce" if failed else "sent",
                ),
            )

    row = await env.batch(root_id)
    assert row["paused_at"] is not None
    assert (await env.batch(child_id))["paused_at"] is not None
    assert row["cancel_requested_at"] is None
    assert len(calls) == 1
    assert calls[0].action is PolicyAction.PAUSE
    assert calls[0].batch_key == "send"
    assert calls[0].ratio == pytest.approx(2 / 25)
    await asyncio.gather(subject.evaluate([child_id]), subject.evaluate([child_id]))
    assert len(calls) == 1


async def test_fail_fast_cancels_remainder_and_finalizes_failed(
    env: Env, registry: HookRegistry
) -> None:
    root_id, refs = await seeded(env, policy=FailurePolicy.fail_fast(), count=5, seal=True)
    subject = enforcer(env, registry)
    async with policy_completer(env, subject, finalizer(env, registry)) as completer:
        assert (await completer.claim(refs[0])).run
        assert await completer.finish(
            refs[0], FinishResult(result_class=ResultClass.ERROR, label="hard_bounce")
        )

    row = await env.batch(root_id)
    assert row["state"] == BatchState.FAILED
    assert row["cancel_reason"] == CancelReason.FAIL_FAST.value
    item = env.tables.item
    async with env.connection() as conn:
        states = list(await conn.scalars(item.select().with_only_columns(item.c.state)))
    assert states.count(ItemState.ERROR) == 1
    assert states.count(ItemState.CANCELLED) == 4
    totals = await env.counters(root_id)
    assert totals.error == 1
    assert totals.cancelled == 4
    assert totals.pending == 0


async def test_hook_failure_rolls_back_domain_and_pause(env: Env, registry: HookRegistry) -> None:
    probe = await create_probe(env.engine, env.schema)

    @registry.on_policy_breach("mail")
    async def fail(session: AsyncSession, summary: BatchSummary, breach: PolicyBreach) -> None:
        del summary, breach
        conn = await session.connection()
        await insert_id(conn, probe, 1)
        message = "hook failed"
        raise RuntimeError(message)

    root_id, refs = await seeded(
        env,
        policy=FailurePolicy.threshold(ratio=0.0, min_processed=1, action="pause"),
        count=1,
    )
    subject = enforcer(env, registry)
    async with policy_completer(env, subject) as completer:
        assert (await completer.claim(refs[0])).run
        assert await completer.finish(
            refs[0], FinishResult(result_class=ResultClass.ERROR, label="broken")
        )

    assert await committed_ids(env.engine, probe) == []
    row = await env.batch(root_id)
    assert row["paused_at"] is None
    assert row["hook_attempts"] == 1
    assert row["hook_error"] == "hook failed"


async def test_batch_without_policy_is_read_once(env: Env, registry: HookRegistry) -> None:
    # Оценка политики зовётся после каждого flush Completer; опции батча
    # неизменны, поэтому батч без failure_policy читается один раз (T11.6).
    async with env.transaction() as conn:
        plain = await env.producer.create_root(conn, RootSpec(kind="mail"))
    policy_id, _ = await seeded(env, policy=FailurePolicy.fail_fast(), count=1)
    subject = enforcer(env, registry)
    begins: list[object] = []

    def on_begin(conn: object) -> None:
        begins.append(conn)

    event.listen(subject.engine.sync_engine, "begin", on_begin)
    try:
        assert await subject.evaluate([plain.id, policy_id]) == ()
        assert len(begins) == 2
        assert await subject.evaluate([plain.id, policy_id]) == ()
        assert await subject.evaluate([plain.id]) == ()
    finally:
        event.remove(subject.engine.sync_engine, "begin", on_begin)
    # Батч с политикой читается каждый раз, без политики — только первый.
    assert len(begins) == 3
