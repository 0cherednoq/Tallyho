"""Операции над деревом, удалённым или удаляемым retention, бросают ``BatchPurged`` (Fix-30).

ARCHITECTURE §7.6: дерево, у которого истёк ``retention`` (и вызван ``release()``,
если он обязателен), считается удалённым ещё до прохода sweeper-а. Иначе
``retry_failed`` мог переоткрыть дерево, которое sweeper уже начал удалять
чанками, вернуть успех, а дерево всё равно исчезало (наблюдение T11.4).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.sweeper import Sweeper
from tallyho.model.errors import BatchPurged, NotFoundError
from tallyho.model.states import BatchState, ResultClass
from tallyho.protocols.clock import SystemClock
from tests.integration.engine.completer_env import schema_engine
from tests.integration.engine.test_retry_release import (
    finalize,
    finalizer,
    operations,
    releasable_root,
    release,
    run_item,
    sweeper,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.engine.operations import Operations
    from tallyho.hooks.registry import HookRegistry
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

_LATER = datetime(2030, 1, 1, tzinfo=UTC)

OPERATIONS: dict[str, Callable[[Operations, AsyncConnection, UUID], Awaitable[object]]] = {
    "retry_failed": lambda ops, conn, batch_id: ops.retry_failed(conn, batch_id),
    "cancel": lambda ops, conn, batch_id: ops.cancel(conn, batch_id),
    "pause": lambda ops, conn, batch_id: ops.pause(conn, batch_id),
    "resume": lambda ops, conn, batch_id: ops.resume(conn, batch_id),
    "reschedule": lambda ops, conn, batch_id: ops.reschedule(conn, batch_id, _LATER),
    "retry_finalize": lambda ops, conn, batch_id: ops.retry_finalize(conn, batch_id),
    "release": lambda ops, conn, batch_id: ops.release(conn, batch_id),
}


@dataclass(eq=False, kw_only=True)
class _PausedSweeper(Sweeper):
    """Sweeper, который останавливается перед первым чанком удаления Items."""

    paused: asyncio.Event = field(default_factory=asyncio.Event)
    proceed: asyncio.Event = field(default_factory=asyncio.Event)

    @override
    async def _purge_items_in(self, conn: AsyncConnection, *, root_id: UUID) -> int:
        if not self.paused.is_set():
            self.paused.set()
            _ = await self.proceed.wait()
        return await super()._purge_items_in(conn, root_id=root_id)


async def expired_tree(env: Env, registry: HookRegistry) -> tuple[UUID, UUID]:
    """Дерево «корень → child» с ошибкой в child, отпущенное ``release()``.

    ``retention`` — 1 мкс, поэтому сразу после ``release()`` дерево подлежит удалению.

    Returns:
        Корень и под-батч.
    """
    root_id, child_id = await releasable_root(env, with_child=True)
    await run_item(env, child_id, ResultClass.ERROR)
    await finalize(env, registry, child_id, root_id)
    assert (await env.batch(root_id))["state"] == int(BatchState.COMPLETED_WITH_ERRORS)
    await release(env, root_id)
    return root_id, child_id


async def assert_purged(env: Env, name: str, batch_id: UUID) -> None:
    """Операция ``name`` над ``batch_id`` бросает ``BatchPurged`` и ничего не пишет."""
    async with env.transaction() as conn:
        with pytest.raises(BatchPurged) as caught:
            _ = await OPERATIONS[name](operations(env), conn, batch_id)
    assert caught.value.batch_id == batch_id


@pytest.mark.parametrize("name", sorted(OPERATIONS))
async def test_operation_on_purged_tree_raises_batch_purged(
    env: Env, registry: HookRegistry, name: str
) -> None:
    root_id, child_id = await expired_tree(env, registry)
    assert await sweeper(env, registry).retention() == 1
    assert await env.count(env.tables.batch) == 0

    await assert_purged(env, name, root_id)
    await assert_purged(env, name, child_id)


@pytest.mark.parametrize("name", sorted(OPERATIONS))
async def test_operation_on_expired_tree_raises_before_sweeper(
    env: Env, registry: HookRegistry, name: str
) -> None:
    """Истёкшее дерево уже «удалено»: операции не дают его переоткрыть или изменить."""
    root_id, child_id = await expired_tree(env, registry)
    before = await env.batch(root_id)

    await assert_purged(env, name, root_id)
    await assert_purged(env, name, child_id)

    after = await env.batch(root_id)
    assert after["state"] == before["state"]
    assert after["finished_at"] == before["finished_at"]
    assert after["released_at"] == before["released_at"]
    assert await sweeper(env, registry).retention() == 1
    assert await env.count(env.tables.batch) == 0
    assert await env.count(env.tables.item) == 0


async def test_retry_failed_during_purge_raises_batch_purged(
    env: Env, registry: HookRegistry
) -> None:
    """Сценарий T11.4: ``retry_failed`` между выбором корня sweeper-ом и удалением."""
    root_id, child_id = await expired_tree(env, registry)
    subject = _PausedSweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        finalizer=finalizer(env, registry),
    )
    purge = asyncio.create_task(subject.retention())
    try:
        _ = await subject.paused.wait()
        await assert_purged(env, "retry_failed", root_id)
        await assert_purged(env, "retry_failed", child_id)
    finally:
        subject.proceed.set()
        assert await purge == 1

    assert await env.count(env.tables.batch) == 0
    assert await env.count(env.tables.outbox) == 0


async def test_unknown_batch_is_reported_as_purged(env: Env) -> None:
    """Tombstone нет: никогда не существовавший батч неотличим от удалённого."""
    missing = env.producer.ids.new_id()
    for name in OPERATIONS:
        await assert_purged(env, name, missing)
    assert issubclass(BatchPurged, NotFoundError)


async def test_operations_before_retention_expires_still_work(
    env: Env, registry: HookRegistry
) -> None:
    """Пока retention не истёк, ``release()`` не мешает ``retry_failed`` (A-UC-22)."""
    root_id, child_id = await releasable_root(env, with_child=True, retention=timedelta(hours=1))
    await run_item(env, child_id, ResultClass.ERROR)
    await finalize(env, registry, child_id, root_id)
    await release(env, root_id)

    async with env.transaction() as conn:
        assert await operations(env).retry_failed(conn, root_id) == 1
    batch = env.tables.batch
    async with env.connection() as conn:
        released = await conn.scalar(select(batch.c.released_at).where(batch.c.id == root_id))
    assert released is None
