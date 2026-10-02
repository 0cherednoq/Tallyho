"""Первая причина отмены выигрывает (ARCHITECTURE §6.1, Fix-15).

Каждый путь, ставящий флаг отмены (``cancel()``, дедлайн, ``fail_fast`` и
политика с ``action="fail"``, ``on_feeder_failed="cancel"``), пишет
``cancel_requested_at`` и ``cancel_reason`` только узлу без флага. Повторный
запрос не меняет ни причину, ни время запроса, ни итог; каскад и немедленная
отмена неотправленных Items при этом срабатывают.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update

from tallyho.engine.completer import (
    ClaimOutcome,
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
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.calls import TaskCall
from tallyho.model.policy import FailurePolicy
from tallyho.model.states import BatchState, CancelReason, ItemState, OnFeederFailed, ResultClass
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, upsert_slots
from tests.integration.engine.completer_env import WORKER, schema_engine

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.engine.completer import FinalizeTrigger
    from tallyho.hooks.registry import HookRegistry
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

PAST = timedelta(seconds=1)


class _NoFinalize:
    """Finalizer Sweeper'а, который ничего не финализирует: итог выбирает сам тест."""

    async def try_finalize(self, batch_id: UUID) -> bool:
        del batch_id
        return False


def operations(env: Env) -> Operations:
    """Операции над схемой теста."""
    return Operations(tables=env.tables, clock=SystemClock())


def finalizer(env: Env, registry: HookRegistry) -> Finalizer:
    """Finalizer над схемой теста."""
    return Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )


def sweeper(env: Env) -> Sweeper:
    """Sweeper, который выставляет дедлайны, но не финализирует."""
    return Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        finalizer=_NoFinalize(),
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )


@contextlib.asynccontextmanager
async def completer(
    env: Env, registry: HookRegistry, finalize: FinalizeTrigger | None = None
) -> AsyncGenerator[Completer]:
    """Completer с обработчиком политик; финализация — только если передана."""
    policy = PolicyEnforcer(
        tables=env.tables, engine=schema_engine(env), clock=SystemClock(), hooks=registry
    )
    subject = Completer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        settings=CompleterSettings(worker_id=WORKER),
        triggers=CompleterTriggers(policy=policy, finalizer=finalize),
    )
    try:
        yield subject
    finally:
        await subject.close()


async def add(conn: AsyncConnection, env: Env, batch_id: UUID, *, count: int) -> None:
    """Добавить ``count`` Items в батч."""
    _ = await env.producer.add_items(
        conn,
        batch_id,
        [TaskCall(task_name="work", args=(index,), kwargs={}) for index in range(count)],
    )


async def items(env: Env, batch_id: UUID) -> list[ItemRef]:
    """Невиртуальные Items батча в порядке id."""
    item = env.tables.item
    async with env.connection() as conn:
        ids = list(
            await conn.scalars(
                select(item.c.id)
                .where(item.c.batch_id == batch_id, item.c.child_batch_id.is_(None))
                .order_by(item.c.id)
            )
        )
    return [ItemRef(item_id, batch_id) for item_id in ids]


async def dispatch(env: Env, ref: ItemRef) -> None:
    """Имитировать отправку: запись outbox Item'а забрал relay."""
    async with env.transaction() as conn:
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.item_id == ref.id)
        )


async def flag(env: Env, batch_id: UUID) -> tuple[datetime | None, str | None]:
    """``(cancel_requested_at, cancel_reason)`` батча."""
    row = await env.batch(batch_id)
    return row["cancel_requested_at"], row["cancel_reason"]


async def outcome(env: Env, batch_id: UUID) -> tuple[BatchState, str | None]:
    """Терминальное состояние и причина батча."""
    row = await env.batch(batch_id)
    return BatchState(row["state"]), row["cancel_reason"]


async def active_items(env: Env) -> int:
    """Число ещё активных невиртуальных Items во всей схеме."""
    item = env.tables.item
    async with env.connection() as conn:
        return int(
            await conn.scalar(
                select(func.count())
                .select_from(item)
                .where(item.c.state == int(ItemState.ACTIVE), item.c.child_batch_id.is_(None))
            )
            or 0
        )


async def test_deadline_after_cancel_keeps_cancelled(env: Env, registry: HookRegistry) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind="work", deadline=datetime.now(UTC) + timedelta(hours=1))
        )
        await add(conn, env, root.id, count=2)
    async with env.transaction() as conn:
        assert await operations(env).cancel(conn, root.id) == 2
    first = await flag(env, root.id)
    assert first[1] == CancelReason.CANCEL.value

    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(deadline_at=func.now() - PAST)
        )
    # Sweeper уже не выбирает батч с флагом...
    assert await sweeper(env).enforce_deadlines() == 0
    # ...а если выбрал раньше нашего commit, его запрос с причиной deadline — no-op для причины.
    async with env.transaction() as conn:
        assert await operations(env).cancel(conn, root.id, reason=CancelReason.DEADLINE) == 0
    assert await flag(env, root.id) == first

    assert await finalizer(env, registry).try_finalize(root.id)
    assert await outcome(env, root.id) == (BatchState.CANCELLED, CancelReason.CANCEL.value)


async def test_cancel_after_deadline_keeps_failed(env: Env, registry: HookRegistry) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind="work", deadline=datetime.now(UTC) - PAST)
        )
        await add(conn, env, root.id, count=2)
    assert await sweeper(env).enforce_deadlines() == 2
    first = await flag(env, root.id)
    assert first[1] == CancelReason.DEADLINE.value

    async with env.transaction() as conn:
        assert await operations(env).cancel(conn, root.id) == 0
    assert await flag(env, root.id) == first

    assert await finalizer(env, registry).try_finalize(root.id)
    assert await outcome(env, root.id) == (BatchState.FAILED, CancelReason.DEADLINE.value)


async def test_cancel_after_fail_fast_keeps_failed(env: Env, registry: HookRegistry) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind="mail", failure_policy=FailurePolicy.fail_fast())
        )
        await add(conn, env, root.id, count=3)
        _ = await env.producer.seal(conn, root.id)
    refs = await items(env, root.id)
    await dispatch(env, refs[0])
    async with completer(env, registry) as subject:
        assert (await subject.claim(refs[0])).run
        assert await subject.finish(refs[0], FinishResult(result_class=ResultClass.ERROR))
    first = await flag(env, root.id)
    assert first[1] == CancelReason.FAIL_FAST.value
    assert await active_items(env) == 0

    async with env.transaction() as conn:
        assert await operations(env).cancel(conn, root.id) == 0
    assert await flag(env, root.id) == first

    assert await finalizer(env, registry).try_finalize(root.id)
    assert await outcome(env, root.id) == (BatchState.FAILED, CancelReason.FAIL_FAST.value)


async def test_fail_fast_after_cancel_keeps_cancelled(env: Env, registry: HookRegistry) -> None:
    """Политика проваливает всё дерево, но узлы с флагом сохраняют свою причину."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind="mail", failure_policy=FailurePolicy.fail_fast())
        )
        await add(conn, env, root.id, count=2)
        child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="send"))
        await add(conn, env, child.id, count=2)
        other = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="other"))
        await add(conn, env, other.id, count=1)
        _ = await env.producer.seal(conn, root.id)
    async with env.transaction() as conn:
        assert await operations(env).cancel(conn, child.id) == 2
    child_flag = await flag(env, child.id)
    assert child_flag[1] == CancelReason.CANCEL.value

    refs = await items(env, root.id)
    await dispatch(env, refs[0])
    async with completer(env, registry) as worker:
        assert (await worker.claim(refs[0])).run
        assert await worker.finish(refs[0], FinishResult(result_class=ResultClass.ERROR))
    # Каскад политики прошёл по дереву: узлы без флага получили fail_fast, Items отменены.
    assert (await flag(env, root.id))[1] == CancelReason.FAIL_FAST.value
    assert (await flag(env, other.id))[1] == CancelReason.FAIL_FAST.value
    assert await flag(env, child.id) == child_flag
    assert await active_items(env) == 0

    subject = finalizer(env, registry)
    for batch_id in (child.id, other.id, root.id):
        _ = await subject.try_finalize(batch_id)
    assert await outcome(env, child.id) == (BatchState.CANCELLED, CancelReason.CANCEL.value)
    assert await outcome(env, other.id) == (BatchState.FAILED, CancelReason.FAIL_FAST.value)
    assert await outcome(env, root.id) == (BatchState.FAILED, CancelReason.FAIL_FAST.value)


async def test_fail_fast_on_cancelled_batch_is_noop(env: Env, registry: HookRegistry) -> None:
    """Ошибка выполнявшегося Item после ``cancel()`` не меняет причину того же батча."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind="mail", failure_policy=FailurePolicy.fail_fast())
        )
        await add(conn, env, root.id, count=2)
    refs = await items(env, root.id)
    await dispatch(env, refs[0])
    async with completer(env, registry) as subject:
        assert (await subject.claim(refs[0])).run
        async with env.transaction() as conn:
            assert await operations(env).cancel(conn, root.id) == 1
        first = await flag(env, root.id)
        assert await subject.finish(refs[0], FinishResult(result_class=ResultClass.ERROR))
    assert await flag(env, root.id) == first

    assert await finalizer(env, registry).try_finalize(root.id)
    assert await outcome(env, root.id) == (BatchState.CANCELLED, CancelReason.CANCEL.value)


async def test_cancel_root_keeps_sub_batch_deadline(env: Env, registry: HookRegistry) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="work"))
        await add(conn, env, root.id, count=1)
        child = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="late", deadline=datetime.now(UTC) - PAST)
        )
        await add(conn, env, child.id, count=1)
        grandchild = await env.producer.create_sub_batch(conn, child.id, SubBatchSpec(key="leaf"))
        await add(conn, env, grandchild.id, count=1)
    # Дедлайн под-батча срабатывает на его поддерево, корень не трогает.
    assert await sweeper(env).enforce_deadlines() == 2
    child_flag = await flag(env, child.id)
    grandchild_flag = await flag(env, grandchild.id)
    assert child_flag[1] == CancelReason.DEADLINE.value
    assert grandchild_flag[1] == CancelReason.DEADLINE.value
    assert await flag(env, root.id) == (None, None)

    async with env.transaction() as conn:
        assert await operations(env).cancel(conn, root.id) == 1
    assert (await flag(env, root.id))[1] == CancelReason.CANCEL.value
    assert await flag(env, child.id) == child_flag
    assert await flag(env, grandchild.id) == grandchild_flag
    assert await active_items(env) == 0

    subject = finalizer(env, registry)
    for batch_id in (grandchild.id, child.id, root.id):
        _ = await subject.try_finalize(batch_id)
    assert await outcome(env, grandchild.id) == (BatchState.FAILED, CancelReason.DEADLINE.value)
    assert await outcome(env, child.id) == (BatchState.FAILED, CancelReason.DEADLINE.value)
    assert await outcome(env, root.id) == (BatchState.CANCELLED, CancelReason.CANCEL.value)


async def test_feeder_failure_keeps_stage_deadline(env: Env, registry: HookRegistry) -> None:
    """``on_feeder_failed="cancel"`` не перезаписывает причину уже отменённого этапа."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="pipeline"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        await add(conn, env, source.id, count=1)
        stage = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(
                key="stage",
                fed_by=(source.id,),
                on_feeder_failed=OnFeederFailed.CANCEL,
                deadline=datetime.now(UTC) - PAST,
            ),
        )
        _ = await env.producer.seal(conn, source.id)
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.batch_id == source.id)
            .values(state=int(ItemState.ERROR), label="rejected", finished_at=func.now())
        )
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.batch_id == source.id)
        )
        await upsert_slots(conn, env.tables, {(source.id, 9): CounterDelta(error=1, w_done=1)})
    assert await sweeper(env).enforce_deadlines() == 0
    stage_flag = await flag(env, stage.id)
    assert stage_flag[1] == CancelReason.DEADLINE.value

    subject = finalizer(env, registry)
    assert await subject.try_finalize(source.id)
    assert (await env.batch(source.id))["state"] == BatchState.COMPLETED_WITH_ERRORS
    assert await flag(env, stage.id) == stage_flag
    _ = await subject.try_finalize(stage.id)
    assert await outcome(env, stage.id) == (BatchState.FAILED, CancelReason.DEADLINE.value)


async def test_orphan_stage_cancel_keeps_first_reason(env: Env) -> None:
    """Страховочный проход sweeper'а по этапам тоже не меняет причину."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="pipeline"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        stage = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(
                key="stage",
                fed_by=(source.id,),
                on_feeder_failed=OnFeederFailed.CANCEL,
                deadline=datetime.now(UTC) - PAST,
            ),
        )
    subject = sweeper(env)
    assert await subject.enforce_deadlines() == 0
    stage_flag = await flag(env, stage.id)
    assert stage_flag[1] == CancelReason.DEADLINE.value
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == source.id)
            .values(state=int(BatchState.FAILED), finished_at=func.now())
        )
    assert await subject.seal_orphan_stages() == 1
    assert await flag(env, stage.id) == stage_flag


async def test_retry_failed_keeps_cancel_flag(env: Env, registry: HookRegistry) -> None:
    """``retry_failed()`` отмену не снимает: повтор снова финализируется с той же причиной."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind="mail", failure_policy=FailurePolicy.fail_fast())
        )
        await add(conn, env, root.id, count=2)
        _ = await env.producer.seal(conn, root.id)
    refs = await items(env, root.id)
    await dispatch(env, refs[0])
    subject = finalizer(env, registry)
    async with completer(env, registry, subject) as worker:
        assert (await worker.claim(refs[0])).run
        assert await worker.finish(refs[0], FinishResult(result_class=ResultClass.ERROR))
    assert await outcome(env, root.id) == (BatchState.FAILED, CancelReason.FAIL_FAST.value)
    first = await flag(env, root.id)

    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, root.id) == 1
    assert (await env.batch(root.id))["state"] == BatchState.SEALED
    assert await flag(env, root.id) == first

    await dispatch(env, refs[0])
    async with completer(env, registry, subject) as worker:
        assert (await worker.claim(refs[0])).outcome is ClaimOutcome.CANCELLED
    _ = await subject.try_finalize(root.id)
    assert await outcome(env, root.id) == (BatchState.FAILED, CancelReason.FAIL_FAST.value)
    assert await flag(env, root.id) == first
