"""``retry_failed`` отменяет прежний ``release()`` корня (ARCHITECTURE §7.6, I-14, A-UC-22)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho.engine.completer import FinishResult, ItemRef
from tallyho.engine.finalizer import Finalizer
from tallyho.engine.operations import Operations
from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.calls import TaskCall
from tallyho.model.errors import InvalidStateError
from tallyho.model.states import BatchState, ItemState, ResultClass
from tallyho.protocols.clock import SystemClock
from tallyho.testing import FakeClock
from tests.integration.engine.completer_env import open_completer, schema_engine

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.hooks.registry import HookRegistry
    from tallyho.protocols.clock import Clock
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

_WITH_ERRORS = {int(BatchState.COMPLETED_WITH_ERRORS), int(BatchState.FAILED)}
_EXPIRED = timedelta(microseconds=1)
_RETENTION = timedelta(hours=1)
"""retention, который не истекает за время теста: ``retry_failed`` после ``release()``
допустим только до его истечения (Fix-30)."""


def after_retention() -> FakeClock:
    """Часы sweeper-а, для которых ``_RETENTION`` деревьев теста уже истёк."""
    return FakeClock(datetime.now(UTC) + 2 * _RETENTION)


def operations(env: Env) -> Operations:
    """Операции над схемой теста."""
    return Operations(tables=env.tables, clock=SystemClock())


def finalizer(env: Env, registry: HookRegistry) -> Finalizer:
    """Настоящий Finalizer над схемой теста."""
    return Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
    )


def sweeper(env: Env, registry: HookRegistry, clock: Clock | None = None) -> Sweeper:
    """Sweeper над схемой теста: retention удаляет по одному дереву за проход."""
    return Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock or SystemClock(),
        finalizer=finalizer(env, registry),
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )


async def releasable_root(
    env: Env, *, with_child: bool, retention: timedelta = _EXPIRED
) -> tuple[UUID, UUID]:
    """Запечатанное дерево с ``release_required`` и одним Item.

    ``retention`` по умолчанию 1 мкс: после ``release()`` дерево сразу подлежит удалению.

    Returns:
        Корень и батч, в котором лежит Item (корень или его под-батч).
    """
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn,
            RootSpec(kind="export", retention=retention, release_required=True),
        )
        owner_id = root.id
        if with_child:
            child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="child"))
            owner_id = child.id
        _ = await env.producer.add_items(
            conn, owner_id, [TaskCall(task_name="send", args=(1,), kwargs={})]
        )
        if with_child:
            _ = await env.producer.seal(conn, owner_id)
        _ = await env.producer.seal(conn, root.id)
    return root.id, owner_id


async def run_item(env: Env, batch_id: UUID, result_class: ResultClass) -> None:
    """Забрать единственный активный Item батча «у брокера» и завершить его."""
    item = env.tables.item
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        item_id = await conn.scalar(
            select(item.c.id).where(
                item.c.batch_id == batch_id,
                item.c.state == int(ItemState.ACTIVE),
                item.c.child_batch_id.is_(None),
            )
        )
        assert item_id is not None
        _ = await conn.execute(delete(outbox).where(outbox.c.item_id == item_id))
    ref = ItemRef(item_id, batch_id)
    async with open_completer(env, clock=SystemClock()) as completer:
        assert (await completer.claim(ref)).run
        assert await completer.finish(ref, FinishResult(result_class=result_class))


async def finalize(env: Env, registry: HookRegistry, *batch_ids: UUID) -> None:
    """Финализировать батчи от листа к корню настоящим Finalizer."""
    subject = finalizer(env, registry)
    for batch_id in batch_ids:
        _ = await subject.try_finalize(batch_id)


async def release(env: Env, root_id: UUID) -> None:
    """``release()`` корня в закоммиченной транзакции."""
    async with env.transaction() as conn:
        await operations(env).release(conn, root_id)


async def test_retry_failed_revokes_release_until_released_again(
    env: Env, registry: HookRegistry
) -> None:
    root_id, _ = await releasable_root(env, with_child=False, retention=_RETENTION)
    retention = sweeper(env, registry, after_retention())

    await run_item(env, root_id, ResultClass.ERROR)
    await finalize(env, registry, root_id)
    assert (await env.batch(root_id))["state"] in _WITH_ERRORS
    await release(env, root_id)
    assert (await env.batch(root_id))["released_at"] is not None

    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, root_id) == 1
    reopened = await env.batch(root_id)
    assert reopened["state"] == int(BatchState.SEALED)
    assert reopened["finished_at"] is None
    assert reopened["released_at"] is None
    with pytest.raises(InvalidStateError):
        await release(env, root_id)

    await run_item(env, root_id, ResultClass.OK)
    await finalize(env, registry, root_id)
    finished = await env.batch(root_id)
    assert finished["state"] == int(BatchState.SUCCEEDED)
    assert finished["released_at"] is None
    assert await retention.retention() == 0
    assert await env.count(env.tables.batch) == 1
    assert await env.count(env.tables.item) == 1

    await release(env, root_id)
    assert await retention.retention() == 1
    assert await env.count(env.tables.batch) == 0
    assert await env.count(env.tables.item) == 0


async def test_retry_failed_on_sub_batch_resets_root_release(
    env: Env, registry: HookRegistry
) -> None:
    root_id, child_id = await releasable_root(env, with_child=True, retention=_RETENTION)
    retention = sweeper(env, registry, after_retention())

    await run_item(env, child_id, ResultClass.ERROR)
    await finalize(env, registry, child_id, root_id)
    assert (await env.batch(child_id))["state"] in _WITH_ERRORS
    assert (await env.batch(root_id))["state"] in _WITH_ERRORS
    await release(env, root_id)
    assert (await env.batch(root_id))["released_at"] is not None

    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, child_id) == 1
    reopened = await env.batch(root_id)
    assert reopened["state"] == int(BatchState.SEALED)
    assert reopened["released_at"] is None

    await run_item(env, child_id, ResultClass.OK)
    await finalize(env, registry, child_id, root_id)
    assert (await env.batch(root_id))["state"] == int(BatchState.SUCCEEDED)
    assert await retention.retention() == 0
    assert await env.count(env.tables.batch) == 2

    await release(env, root_id)
    assert await retention.retention() == 1
    assert await env.count(env.tables.batch) == 0


async def test_rolled_back_retry_failed_keeps_release(env: Env, registry: HookRegistry) -> None:
    root_id, _ = await releasable_root(env, with_child=False, retention=_RETENTION)
    await run_item(env, root_id, ResultClass.ERROR)
    await finalize(env, registry, root_id)
    await release(env, root_id)
    before = await env.batch(root_id)
    assert before["released_at"] is not None

    batch = env.tables.batch
    async with AsyncSession(schema_engine(env)) as session:
        assert await operations(env).retry_failed(session, root_id) == 1
        inside = await session.scalar(select(batch.c.released_at).where(batch.c.id == root_id))
        assert inside is None
        await session.rollback()

    after = await env.batch(root_id)
    assert after["released_at"] == before["released_at"]
    assert after["state"] == before["state"]
    assert after["finished_at"] == before["finished_at"]
    assert await sweeper(env, registry, after_retention()).retention() == 1
