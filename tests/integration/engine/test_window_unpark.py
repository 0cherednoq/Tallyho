"""Окно-aware распарковка: pause/resume и проходы relay не прорывают окно (RM1.2)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select, text, update

from tallyho.engine.operations import Operations
from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.relay import release_window
from tallyho.model.calls import TaskCall
from tallyho.model.states import ItemState
from tests.helpers.relay import RecordingDispatcher, relay_env

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from tests.helpers.relay import RelayEnv
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

NOW = datetime(2030, 4, 1, 8, 0, tzinfo=UTC)
GRACE = timedelta(seconds=5)


@pytest.fixture
def rel(env: Env) -> RelayEnv:
    return relay_env(env, NOW)


def operations(rel: RelayEnv) -> Operations:
    return Operations(tables=rel.tables, clock=rel.clock)


async def windowed(rel: RelayEnv, n: int, limit: int | None) -> UUID:
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(conn, RootSpec(kind="k", max_in_flight=limit))
        _ = await rel.producer.add_items(
            conn, root.id, [TaskCall(task_name="render", args=(i,)) for i in range(n)]
        )
        _ = await rel.producer.seal(conn, root.id)
    rel.clock.advance(GRACE)
    return root.id


async def parked(rel: RelayEnv, batch_id: UUID) -> int:
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        found = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id == batch_id, outbox.c.available_at == text("'infinity'"))
        )
        return int(found or 0)


async def finish(rel: RelayEnv, item_ids: Sequence[UUID]) -> list[UUID]:
    item = rel.tables.item
    async with rel.env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id.in_(item_ids)).values(state=int(ItemState.OK))
        )
        return await release_window(conn, rel.tables, item_ids)


async def assert_invariant(rel: RelayEnv, batch_id: UUID, *, held: int = 0) -> int:
    """Строк ``th_window`` батча не больше его окна.

    Сужение окна не отзывает уже отправленное (ARCHITECTURE §11.2), поэтому
    предел — ``max(окно, сколько было отправлено до сужения)``: число строк
    окна от этого не растёт, только держится, пока задачи не доделаются.
    """
    batch = rel.tables.batch
    async with rel.env.connection() as conn:
        limit = await conn.scalar(select(batch.c.max_in_flight).where(batch.c.id == batch_id))
    size = await rel.window_size(batch_id)
    if limit is not None:
        assert size <= max(limit, held), f"окно {limit}, держалось {held}, в th_window {size}"
    return size


async def test_resume_releases_at_most_window(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 6, 2)
    subject = operations(rel)
    assert await rel.relay.scan_once() == 2
    sent = list(rel.dispatcher.ids)

    async with rel.env.transaction() as conn:
        await subject.pause(conn, batch_id)
    assert await parked(rel, batch_id) == 4
    async with rel.env.transaction() as conn:
        await subject.resume(conn, batch_id)
    rel.clock.advance(GRACE)

    # Окно занято отправленным: resume не выпустил ничего сверх него.
    assert await parked(rel, batch_id) == 4
    assert await rel.relay.scan_once() == 0
    assert rel.dispatcher.ids == sent
    assert await assert_invariant(rel, batch_id) == 2


async def test_resume_fills_only_freed_places(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 6, 2)
    subject = operations(rel)
    assert await rel.relay.scan_once() == 2
    async with rel.env.transaction() as conn:
        await subject.pause(conn, batch_id)
    # Одна задача доделалась на паузе: место освободилось, но осталось за окном.
    assert await finish(rel, rel.dispatcher.ids[:1]) == [batch_id]
    assert await parked(rel, batch_id) == 4

    async with rel.env.transaction() as conn:
        await subject.resume(conn, batch_id)
    rel.clock.advance(GRACE)
    assert await parked(rel, batch_id) == 3
    assert await rel.relay.scan_once() == 1
    assert await assert_invariant(rel, batch_id) == 2


async def test_resume_of_batch_without_window_releases_everything(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 4, None)
    subject = operations(rel)
    async with rel.env.transaction() as conn:
        await subject.pause(conn, batch_id)
    assert await parked(rel, batch_id) == 4
    async with rel.env.transaction() as conn:
        await subject.resume(conn, batch_id)
    rel.clock.advance(GRACE)
    assert await parked(rel, batch_id) == 0
    assert await rel.relay.scan_once() == 4


async def test_resume_keeps_start_at_of_windowed_batch(rel: RelayEnv) -> None:
    later = NOW + timedelta(hours=1)
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(
            conn, RootSpec(kind="k", max_in_flight=2, start_at=later)
        )
        _ = await rel.producer.add_items(
            conn, root.id, [TaskCall(task_name="render", args=(i,)) for i in range(3)]
        )
        _ = await rel.producer.seal(conn, root.id)
    subject = operations(rel)
    async with rel.env.transaction() as conn:
        await subject.pause(conn, root.id)
    async with rel.env.transaction() as conn:
        await subject.resume(conn, root.id)
    # Распаркованные записи ждут start_at, а не уезжают сразу.
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 0
    rel.clock.current = later + GRACE
    assert await rel.relay.scan_once() == 2
    assert await assert_invariant(rel, root.id) == 2


async def test_resume_handles_subtree_of_windowed_and_free_batches(rel: RelayEnv) -> None:
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(conn, RootSpec(kind="k"))
        child = await rel.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="limited", max_in_flight=1)
        )
        _ = await rel.producer.add_items(
            conn, root.id, [TaskCall(task_name="render", args=(i,)) for i in range(2)]
        )
        _ = await rel.producer.add_items(
            conn, child.id, [TaskCall(task_name="render", args=(10 + i,)) for i in range(3)]
        )
    rel.clock.advance(GRACE)
    subject = operations(rel)
    async with rel.env.transaction() as conn:
        await subject.pause(conn, root.id)
    async with rel.env.transaction() as conn:
        await subject.resume(conn, root.id)
    rel.clock.advance(GRACE)
    # Корень без окна выпустил всё, под-батч с окном 1 — одну запись.
    assert await parked(rel, root.id) == 0
    assert await parked(rel, child.id) == 2
    assert await rel.relay.scan_once() == 3
    assert await assert_invariant(rel, child.id) == 1


async def test_invariant_holds_through_pause_resume_and_window_changes(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 12, 3)
    subject = operations(rel)
    done: list[UUID] = []
    held = await rel.window_size(batch_id)
    for step, limit in enumerate([3, 0, 2, 0, 5, 1, None, 2]):
        async with rel.env.transaction() as conn:
            if step % 2:
                await subject.pause(conn, batch_id)
            else:
                await subject.resume(conn, batch_id)
            await subject.set_max_in_flight(conn, batch_id, limit)
        rel.clock.advance(GRACE)
        _ = await rel.relay.scan_once()
        held = await assert_invariant(rel, batch_id, held=held)
        live = [value for value in rel.dispatcher.ids if value not in done]
        if live:
            _ = await finish(rel, live[:1])
            done.append(live[0])
        held = await assert_invariant(rel, batch_id, held=held)
    assert done


async def test_window_change_with_live_items_keeps_invariant(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 9, 3)
    subject = operations(rel)
    assert await rel.relay.scan_once() == 3
    live = list(rel.dispatcher.ids)

    async with rel.env.transaction() as conn:
        await subject.set_max_in_flight(conn, batch_id, 0)
    # Окно 0 при трёх живых задачах: строки окна остались, новых не добавилось.
    assert await assert_invariant(rel, batch_id, held=3) == 3
    assert await rel.relay.scan_once() == 0
    assert await finish(rel, live) == [batch_id]
    assert await assert_invariant(rel, batch_id) == 0

    async with rel.env.transaction() as conn:
        await subject.set_max_in_flight(conn, batch_id, 3)
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 3
    assert await assert_invariant(rel, batch_id) == 3


async def test_concurrent_relays_and_resume_keep_window(rel: RelayEnv) -> None:
    rel.dispatcher.delay = 0.01
    batch_id = await windowed(rel, 30, 4)
    subject = operations(rel)
    other = RecordingDispatcher(delay=0.01)
    for step in range(4):
        async with rel.env.transaction() as conn:
            if step % 2:
                await subject.pause(conn, batch_id)
            else:
                await subject.resume(conn, batch_id)
        rel.clock.advance(GRACE)
        _ = await asyncio.gather(
            rel.relay.scan_once(),
            rel.another_relay(other).scan_once(),
            rel.relay.scan_once(),
        )
        _ = await assert_invariant(rel, batch_id)
    ids = rel.dispatcher.ids + other.ids
    assert len(ids) == len(set(ids)) <= 4
    assert await assert_invariant(rel, batch_id) <= 4
