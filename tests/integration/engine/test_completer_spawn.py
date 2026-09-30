"""Spawn/into/expect в той же транзакции, что CAS finish родителя."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select

from tallyho.engine.completer import (
    Completer,
    CompleterTriggers,
    ExpectRequest,
    FinishResult,
    ItemRef,
    SpawnRequest,
    SubBatchRequest,
)
from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.spawn import SpawnRoute, TreeCache
from tallyho.model.calls import TaskCall
from tallyho.model.errors import CompleterError, SpawnTargetError
from tallyho.model.states import BatchState, ItemState, ResultClass
from tallyho.storage.counters import CounterDelta, upsert_slots
from tests.integration.engine.completer_env import (
    RELAY_SLOT,
    SETTINGS,
    Finalized,
    MovableClock,
    open_completer,
    schema_engine,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.engine.spawn import TreeSnapshot
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


@dataclass(frozen=True, slots=True)
class Pipeline:
    root_id: UUID
    pages_id: UUID
    cards_id: UUID
    parent: ItemRef
    tree: TreeSnapshot


async def _pipeline(
    env: Env, *, max_items: int | None = None, max_depth: int | None = None
) -> Pipeline:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="catalog", max_items=max_items))
        pages = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="pages", max_depth=max_depth)
        )
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        _ = await env.producer.add_items(
            conn, pages.id, [TaskCall(task_name="parse_page", args=(1,))]
        )
        assert await env.producer.seal(conn, pages.id)
        item = env.tables.item
        parent_id = await conn.scalar(
            select(item.c.id).where(item.c.batch_id == pages.id, item.c.task_name == "parse_page")
        )
        assert parent_id is not None
        _ = await conn.execute(delete(env.tables.outbox).where(env.tables.outbox.c.id == parent_id))
        await upsert_slots(conn, env.tables, {(pages.id, RELAY_SLOT): CounterDelta(dispatched=1)})
    cache = TreeCache()
    async with env.connection() as conn:
        tree = await cache.load(conn, env.tables, pages.id)
    return Pipeline(
        root_id=root.id,
        pages_id=pages.id,
        cards_id=cards.id,
        parent=ItemRef(parent_id, pages.id),
        tree=tree,
    )


async def test_spawn_self_and_into_stage_are_atomic_with_finish(env: Env) -> None:
    pipe = await _pipeline(env)
    own = pipe.tree.route(pipe.pages_id)
    into_cards = pipe.tree.route(pipe.pages_id, "cards")
    value = FinishResult(
        result_class=ResultClass.OK,
        spawns=(
            SpawnRequest(
                route=own,
                call=TaskCall(task_name="parse_page", args=(2,), key="page:2"),
            ),
            SpawnRequest(
                route=into_cards,
                call=TaskCall(task_name="parse_card", args=("a",), key="card:a", weight=2),
            ),
        ),
        expects=(ExpectRequest(route=own, total=10),),
    )
    async with open_completer(env) as completer:
        assert await completer.finish(pipe.parent, value)

    pages = await env.counters(pipe.pages_id)
    cards = await env.counters(pipe.cards_id)
    root = await env.counters(pipe.root_id)
    assert (pages.total, pages.ok, pages.pending, pages.w_total, pages.w_done) == (2, 1, 1, 2, 1)
    assert (cards.total, cards.pending, cards.w_total) == (1, 1, 2)
    assert root.tree_total == 3
    assert (await env.batch(pipe.pages_id))["expected_total"] == 10
    item = env.tables.item
    async with env.connection() as conn:
        rows = (
            await conn.execute(
                select(item.c.batch_id, item.c.task_name, item.c.depth, item.c.state)
                .where(item.c.id != pipe.parent.id, item.c.task_name != "tallyho.sub_batch")
                .order_by(item.c.task_name)
            )
        ).all()
    assert rows == [
        (pipe.cards_id, "parse_card", 0, ItemState.ACTIVE),
        (pipe.pages_id, "parse_page", 1, ItemState.ACTIVE),
    ]
    assert await env.count(env.tables.outbox) == 2


async def test_spawn_deduplicates_before_counters_and_duplicate_finish_is_noop(env: Env) -> None:
    pipe = await _pipeline(env)
    route = pipe.tree.route(pipe.pages_id, "cards")
    spawn = SpawnRequest(
        route=route,
        call=TaskCall(task_name="parse_card", args=("same",), key="same"),
    )
    value = FinishResult(result_class=ResultClass.OK, spawns=(spawn, spawn))
    async with open_completer(env) as completer:
        first, second = await asyncio.gather(
            completer.finish(pipe.parent, value), completer.finish(pipe.parent, value)
        )
    assert sorted((first, second)) == [False, True]
    cards = await env.counters(pipe.cards_id)
    assert (cards.total, cards.duplicates, cards.pending) == (1, 1, 1)
    assert cards.total + cards.duplicates + cards.skipped_by_limit == len(value.spawns)
    assert (await env.counters(pipe.root_id)).tree_total == 2


async def test_invalid_route_rolls_back_parent_finish_and_all_children(env: Env) -> None:
    pipe = await _pipeline(env)
    invalid = SpawnRequest(
        route=SpawnRoute(
            source_id=pipe.pages_id,
            target_id=uuid4(),
            root_id=pipe.root_id,
        ),
        call=TaskCall(task_name="must_rollback"),
    )
    async with open_completer(env) as completer:
        with pytest.raises(CompleterError) as raised:
            _ = await completer.finish(
                pipe.parent,
                FinishResult(result_class=ResultClass.OK, spawns=(invalid,)),
            )
    assert isinstance(raised.value.__cause__, SpawnTargetError)

    item = env.tables.item
    async with env.connection() as conn:
        state = await conn.scalar(select(item.c.state).where(item.c.id == pipe.parent.id))
        children = await conn.scalar(
            select(func.count()).select_from(item).where(item.c.task_name == "must_rollback")
        )
    assert state == ItemState.ACTIVE
    assert children == 0
    pages = await env.counters(pipe.pages_id)
    assert (pages.total, pages.ok, pages.pending) == (1, 0, 1)
    assert (await env.counters(pipe.root_id)).tree_total == 1


async def test_spawn_limits_count_skipped_without_inserting(env: Env) -> None:
    depth_pipe = await _pipeline(env, max_depth=0)
    depth_route = depth_pipe.tree.route(depth_pipe.pages_id)
    async with open_completer(env) as completer:
        assert await completer.finish(
            depth_pipe.parent,
            FinishResult(
                result_class=ResultClass.OK,
                spawns=(SpawnRequest(route=depth_route, call=TaskCall(task_name="too_deep")),),
            ),
        )
    depth = await env.counters(depth_pipe.pages_id)
    assert (depth.total, depth.ok, depth.skipped_by_limit, depth.pending) == (1, 1, 1, 0)

    size_pipe = await _pipeline(env, max_items=1)
    size_route = size_pipe.tree.route(size_pipe.pages_id, "cards")
    async with open_completer(env) as completer:
        assert await completer.finish(
            size_pipe.parent,
            FinishResult(
                result_class=ResultClass.OK,
                spawns=(SpawnRequest(route=size_route, call=TaskCall(task_name="over_limit")),),
            ),
        )
    size = await env.counters(size_pipe.cards_id)
    assert (size.total, size.skipped_by_limit, size.pending) == (0, 1, 0)
    assert size.total + size.duplicates + size.skipped_by_limit == 1
    assert (await env.counters(size_pipe.root_id)).tree_total == 1


async def test_tree_cache_reuses_and_refreshes_snapshot(env: Env) -> None:
    pipe = await _pipeline(env)
    with pytest.raises(SpawnTargetError, match="не является источником"):
        _ = pipe.tree.route(pipe.cards_id, "pages")
    cache = TreeCache()
    async with schema_engine(env).connect() as conn:
        first = await cache.load(conn, env.tables, pipe.pages_id)
        second = await cache.load(conn, env.tables, pipe.cards_id)
        refreshed = await cache.load(conn, env.tables, pipe.cards_id, refresh=True)
    assert first is second
    assert refreshed is not first
    cache.invalidate(pipe.root_id)
    assert cache.get(pipe.pages_id) is None


async def test_dynamic_sub_batch_is_created_seeded_and_sealed_atomically(env: Env) -> None:
    pipe = await _pipeline(env)
    cache = TreeCache()
    async with env.connection() as conn:
        _ = await cache.load(conn, env.tables, pipe.pages_id)
    finalizer = Finalized()
    request = SubBatchRequest(
        spec=SubBatchSpec(key="parts", max_in_flight=2),
        calls=(
            TaskCall(task_name="part", args=(1,), key="p:1"),
            TaskCall(task_name="part", args=(2,), key="p:2", weight=3),
        ),
    )
    completer = Completer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=MovableClock(),
        settings=SETTINGS,
        triggers=CompleterTriggers(
            finalizer=finalizer,
            producer=env.producer,
            tree_cache=cache,
        ),
    )
    try:
        assert await completer.finish(
            pipe.parent,
            FinishResult(result_class=ResultClass.OK, sub_batches=(request,)),
        )
    finally:
        await completer.close()

    batch = env.tables.batch
    async with env.connection() as conn:
        child_id = await conn.scalar(
            select(batch.c.id).where(batch.c.root_id == pipe.root_id, batch.c.key == "parts")
        )
    assert child_id is not None
    child = await env.batch(child_id)
    assert child["parent_id"] == pipe.pages_id
    assert child["state"] == BatchState.SEALED
    assert child["max_in_flight"] == 2
    pages = await env.counters(pipe.pages_id)
    parts = await env.counters(child_id)
    assert (pages.total, pages.ok, pages.pending, pages.w_done) == (2, 1, 1, 1)
    assert (parts.total, parts.w_total, parts.pending) == (2, 4, 2)
    assert (await env.counters(pipe.root_id)).tree_total == 3
    assert await env.count(env.tables.outbox) == 2
    assert cache.get(pipe.pages_id) is None
    assert child_id in finalizer.calls


async def test_dynamic_sub_batch_applies_tree_limit_and_can_remain_open(env: Env) -> None:
    pipe = await _pipeline(env, max_items=1)
    request = SubBatchRequest(
        spec=SubBatchSpec(key="limited"),
        calls=(TaskCall(task_name="not_inserted"),),
        seal=False,
    )
    async with open_completer(env) as completer:
        assert await completer.finish(
            pipe.parent,
            FinishResult(result_class=ResultClass.OK, sub_batches=(request,)),
        )
    batch = env.tables.batch
    async with env.connection() as conn:
        child_id = await conn.scalar(
            select(batch.c.id).where(batch.c.root_id == pipe.root_id, batch.c.key == "limited")
        )
    assert child_id is not None
    assert (await env.batch(child_id))["state"] == BatchState.OPEN
    counters = await env.counters(child_id)
    assert (counters.total, counters.skipped_by_limit) == (0, 1)
