"""Продюсер: под-батчи (UC-06) и этапы ``fed_by`` (§8.1 п.1)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import func, select, update

from tallyho.engine.producer import VIRTUAL_TASK, RootSpec, SubBatchSpec
from tallyho.model.errors import (
    ConfigurationError,
    InvalidStateError,
    NotFoundError,
    SealError,
    SpawnTargetError,
)
from tallyho.model.states import BatchState, ItemState, OnFeederFailed
from tallyho.storage.counters import CounterTotals
from tests.integration.engine.conftest import PRODUCER_SLOT

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.engine.producer import BatchRef
    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

KIND = "catalog_parse"
START = datetime(2030, 1, 1, tzinfo=UTC)


async def make_root(env: Env, conn: AsyncConnection, key: str = "catalog:1") -> BatchRef:
    spec = RootSpec(
        kind=KIND,
        key=key,
        start_at=START,
        max_items=200,
        retention=timedelta(days=1),
        release_required=True,
    )
    return await env.producer.create_root(conn, spec)


async def set_batch(env: Env, batch_id: UUID, **values: object) -> None:
    async with env.transaction() as conn:
        batch = env.tables.batch
        _ = await conn.execute(update(batch).where(batch.c.id == batch_id).values(values))


async def feeds(env: Env) -> set[tuple[UUID, UUID]]:
    feed = env.tables.feed
    async with env.connection() as conn:
        rows = await conn.execute(select(feed.c.feeder_id, feed.c.fed_id))
        return {(feeder, fed) for feeder, fed in rows}


async def test_sub_batch_row_and_virtual_item(env: Env, registry: HookRegistry) -> None:
    @registry.on_finalized(f"{KIND}.pages")
    async def save(session: object, summary: BatchSummary) -> None:
        del session, summary
        await asyncio.sleep(0)

    async with env.transaction() as conn:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(
                key="pages",
                max_depth=1,
                max_in_flight=5,
                expected_total=7,
                on_feeder_failed=OnFeederFailed.CANCEL,
            ),
        )
    assert pages.created
    assert pages.root_id == root.id
    row = await env.batch(pages.id)
    assert row["root_id"] == root.id
    assert row["parent_id"] == root.id
    assert row["kind"] == f"{KIND}.pages"
    assert row["key"] == "pages"
    assert row["state"] == BatchState.OPEN
    assert row["hooks"] == ["finalized"]
    assert row["max_depth"] == 1
    assert row["max_in_flight"] == 5
    assert row["expected_total"] == 7
    assert row["on_feeder_failed"] == OnFeederFailed.CANCEL
    # Наследуется от корня.
    assert row["start_at"] == START
    assert row["max_items"] == 200
    assert row["retention"] == timedelta(days=1)
    assert row["release_required"] is True

    item = env.tables.item
    async with env.connection() as conn:
        virtual = (
            (await conn.execute(select(item).where(item.c.id == row["parent_item_id"])))
            .mappings()
            .one()
        )
    assert virtual["batch_id"] == root.id
    assert virtual["child_batch_id"] == pages.id
    assert virtual["weight"] == 0
    assert virtual["state"] == ItemState.ACTIVE
    assert virtual["task_name"] == VIRTUAL_TASK
    assert await env.count(env.tables.outbox) == 0
    assert await env.counters(root.id) == CounterTotals(total=1)
    counter = env.tables.counter
    async with env.connection() as conn:
        slots = list(await conn.scalars(select(counter.c.slot)))
    assert slots == [PRODUCER_SLOT]


async def test_sub_batch_explicit_kind_start_and_pause(env: Env) -> None:
    paused = datetime(2026, 1, 1, tzinfo=UTC)
    own_start = datetime(2031, 1, 1, tzinfo=UTC)
    async with env.transaction() as conn:
        root = await make_root(env, conn)
    await set_batch(env, root.id, paused_at=paused)
    async with env.transaction() as conn:
        child = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="parts", kind="parts", start_at=own_start)
        )
    row = await env.batch(child.id)
    assert row["kind"] == "parts"
    assert row["hooks"] == []
    assert row["start_at"] == own_start
    assert row["paused_at"] == paused


async def test_sub_batch_is_idempotent(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        first = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
    await set_batch(env, root.id, state=BatchState.SEALED)
    async with env.transaction() as conn:
        again = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
    assert again.id == first.id
    assert not again.created
    assert await env.count(env.tables.item) == 1
    assert await env.counters(root.id) == CounterTotals(total=1)


async def test_nested_sub_batch(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        part = await env.producer.create_sub_batch(conn, pages.id, SubBatchSpec(key="part"))
    row = await env.batch(part.id)
    assert part.root_id == root.id
    assert row["root_id"] == root.id
    assert row["parent_id"] == pages.id
    assert row["kind"] == f"{KIND}.pages.part"
    assert row["max_items"] == 200
    assert await env.counters(pages.id) == CounterTotals(total=1)


async def test_sub_batch_key_is_unique_in_tree(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        other_root = await make_root(env, conn, key="catalog:2")
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        _ = await env.producer.create_sub_batch(conn, pages.id, SubBatchSpec(key="x"))
        # Тот же ключ в другом дереве — другой под-батч.
        foreign = await env.producer.create_sub_batch(conn, other_root.id, SubBatchSpec(key="x"))
        assert foreign.created
        with pytest.raises(ConfigurationError, match="занят"):
            _ = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="x"))


async def test_concurrent_same_key_under_different_parents(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        left = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="left"))
        right = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="right"))
    inserted = asyncio.Event()
    release = asyncio.Event()

    async def first() -> None:
        async with env.transaction() as conn:
            _ = await env.producer.create_sub_batch(conn, left.id, SubBatchSpec(key="x"))
            inserted.set()
            _ = await release.wait()

    async def second() -> None:
        _ = await inserted.wait()
        async with env.transaction() as conn:
            _ = await env.producer.create_sub_batch(conn, right.id, SubBatchSpec(key="x"))

    first_task = asyncio.create_task(first())
    second_task = asyncio.create_task(second())
    _ = await inserted.wait()
    await asyncio.sleep(0.2)
    release.set()
    await first_task
    with pytest.raises(ConfigurationError, match="занят"):
        await second_task


@pytest.mark.parametrize(
    ("values", "error"),
    [
        ({"state": BatchState.SEALED}, SealError),
        ({"state": BatchState.SUCCEEDED}, SealError),
        ({"cancel_requested_at": START}, SealError),
    ],
)
async def test_sub_batch_needs_open_parent(
    env: Env, values: dict[str, object], error: type[Exception]
) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
    await set_batch(env, root.id, **values)
    async with env.transaction() as conn:
        with pytest.raises(error):
            _ = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))


async def test_sub_batch_of_stage_and_missing_parent(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        with pytest.raises(SpawnTargetError):
            _ = await env.producer.create_sub_batch(conn, cards.id, SubBatchSpec(key="x"))
        with pytest.raises(NotFoundError):
            _ = await env.producer.create_sub_batch(conn, UUID(int=1), SubBatchSpec(key="y"))


async def test_fed_by_creates_links(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        pdfs = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="pdfs", fed_by=(cards.id, pages.id))
        )
        # Повтор связи ничего не меняет.
        await env.producer.add_feed(conn, cards.id, [pages.id])
    assert await feeds(env) == {(pages.id, cards.id), (cards.id, pdfs.id), (pages.id, pdfs.id)}
    # Повтор под-батча не пересоздаёт связи, даже если этап уже закрыт.
    await set_batch(env, cards.id, state=BatchState.SEALED)
    async with env.transaction() as conn:
        again = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        await env.producer.add_feed(conn, cards.id, [pages.id])
    assert again.id == cards.id


async def test_fed_by_cycle_is_rejected(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        a = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="a"))
        b = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="b", fed_by=(a.id,))
        )
        c = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="c", fed_by=(b.id,))
        )
    async with env.transaction() as conn:
        with pytest.raises(ConfigurationError, match="цикл"):
            await env.producer.add_feed(conn, a.id, [c.id])
    async with env.transaction() as conn:
        with pytest.raises(ConfigurationError, match="цикл"):
            await env.producer.add_feed(conn, a.id, [a.id])
    assert await feeds(env) == {(a.id, b.id), (b.id, c.id)}


async def test_fed_by_requires_siblings(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        nested = await env.producer.create_sub_batch(conn, pages.id, SubBatchSpec(key="nested"))
        cards = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="cards"))
        with pytest.raises(ConfigurationError, match="одного родителя"):
            await env.producer.add_feed(conn, cards.id, [nested.id])
        with pytest.raises(ConfigurationError, match="одного родителя"):
            await env.producer.add_feed(conn, root.id, [pages.id])
        with pytest.raises(NotFoundError):
            await env.producer.add_feed(conn, cards.id, [UUID(int=1)])


async def test_fed_by_state_checks(env: Env) -> None:
    async with env.transaction() as conn:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="cards"))
    await set_batch(env, pages.id, state=BatchState.FAILED)
    async with env.transaction() as conn:
        with pytest.raises(InvalidStateError, match="финализирован"):
            await env.producer.add_feed(conn, cards.id, [pages.id])
    await set_batch(env, pages.id, state=BatchState.OPEN)
    await set_batch(env, cards.id, state=BatchState.SEALED)
    async with env.transaction() as conn:
        with pytest.raises(SealError, match="этап"):
            await env.producer.add_feed(conn, cards.id, [pages.id])
    await set_batch(env, cards.id, state=BatchState.OPEN, cancel_requested_at=START)
    async with env.transaction() as conn:
        with pytest.raises(SealError, match="этап"):
            await env.producer.add_feed(conn, cards.id, [pages.id])
    assert await feeds(env) == set()


async def test_rollback_leaves_nothing(env: Env) -> None:
    async with env.connection() as conn, conn.begin() as tx:
        root = await make_root(env, conn)
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        _ = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        await tx.rollback()
    async with env.connection() as conn:
        for table in (env.tables.batch, env.tables.item, env.tables.feed, env.tables.counter):
            assert await conn.scalar(select(func.count()).select_from(table)) == 0
