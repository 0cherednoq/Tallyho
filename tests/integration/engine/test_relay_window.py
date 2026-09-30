"""Relay: окно max_in_flight — парковка, освобождение, повторный захват, конкуренция (T4.2)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, func, select, text, update

from tallyho.engine.producer import RootSpec
from tallyho.engine.relay import RelaySettings, refill_window, release_window, window_lock_key
from tallyho.model.calls import TaskCall
from tallyho.model.states import ItemState
from tests.helpers.relay import RecordingDispatcher, relay_env

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from tests.helpers.relay import RelayEnv
    from tests.integration.engine.conftest import Env

NOW = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
GRACE = timedelta(seconds=5)
TTL = timedelta(seconds=30)


@pytest.fixture
def rel(env: Env) -> RelayEnv:
    return relay_env(env, NOW)


async def add_windowed(rel: RelayEnv, n: int, max_in_flight: int = 3) -> UUID:
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(conn, RootSpec(kind="k", max_in_flight=max_in_flight))
        _ = await rel.producer.add_items(
            conn, root.id, [TaskCall(task_name="render", args=(i,)) for i in range(n)]
        )
    rel.clock.advance(GRACE)
    return root.id


async def finish(rel: RelayEnv, item_ids: Sequence[UUID]) -> list[UUID]:
    # Эмуляция finish Completer (T4.3b): Item завершён и место окна освобождено.
    item = rel.tables.item
    async with rel.env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id.in_(item_ids)).values(state=int(ItemState.OK))
        )
        return await release_window(conn, rel.tables, item_ids)


async def parked(rel: RelayEnv, batch_id: UUID) -> int:
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        found = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id == batch_id, outbox.c.available_at == text("'infinity'"))
        )
        return int(found or 0)


async def test_window_limits_dispatch_and_parks_rest(rel: RelayEnv) -> None:
    batch_id = await add_windowed(rel, 10)
    assert await rel.relay.scan_once() == 3
    assert await rel.window_size(batch_id) == 3
    assert await parked(rel, batch_id) == 7
    # Места нет: ни scan, ни kick больше не шлют.
    assert await rel.relay.scan_once() == 0
    rel.relay.kick([batch_id])
    assert await rel.relay.flush_kicked() == 0
    assert (await rel.env.counters(batch_id)).dispatched == 3


async def test_at_most_max_in_flight_at_any_time(rel: RelayEnv) -> None:
    batch_id = await add_windowed(rel, 10)
    finished: list[UUID] = []
    while len(finished) < 10:
        rel.relay.kick([batch_id])
        _ = await rel.relay.flush_kicked()
        in_flight = [i for i in rel.dispatcher.ids if i not in finished]
        assert 1 <= len(in_flight) <= 3
        assert await rel.window_size(batch_id) == len(in_flight)
        assert await finish(rel, in_flight[:1]) == [batch_id]
        finished.append(in_flight[0])
    assert sorted(rel.dispatcher.ids) == sorted(finished)
    assert len(set(rel.dispatcher.ids)) == 10
    assert await rel.window_size(batch_id) == 0
    assert await parked(rel, batch_id) == 0


async def test_release_unparks_exactly_freed(rel: RelayEnv) -> None:
    batch_id = await add_windowed(rel, 10)
    _ = await rel.relay.scan_once()
    assert await finish(rel, rel.dispatcher.ids[:2]) == [batch_id]
    assert await parked(rel, batch_id) == 5
    rel.relay.kick([batch_id])
    assert await rel.relay.flush_kicked() == 2
    assert await rel.window_size(batch_id) == 3


async def test_release_without_window_rows_is_noop(rel: RelayEnv) -> None:
    async with rel.env.transaction() as conn:
        assert await release_window(conn, rel.tables, []) == []
        assert await release_window(conn, rel.tables, [rel.producer.ids.new_id()]) == []


async def test_release_keeps_paused_batch_parked(rel: RelayEnv) -> None:
    batch_id = await add_windowed(rel, 5)
    _ = await rel.relay.scan_once()
    batch = rel.tables.batch
    async with rel.env.transaction() as conn:
        _ = await conn.execute(update(batch).where(batch.c.id == batch_id).values(paused_at=NOW))
    assert await finish(rel, rel.dispatcher.ids[:1]) == [batch_id]
    assert await parked(rel, batch_id) == 2
    assert await rel.window_size(batch_id) == 2


async def test_scan_refills_lost_window_places(rel: RelayEnv) -> None:
    # Места освободились мимо release_window (resume, sweeper): scan возвращает их сам.
    batch_id = await add_windowed(rel, 10)
    _ = await rel.relay.scan_once()
    window = rel.tables.window
    item = rel.tables.item
    gone = rel.dispatcher.ids[:2]
    async with rel.env.transaction() as conn:
        _ = await conn.execute(delete(window).where(window.c.item_id.in_(gone)))
        _ = await conn.execute(
            update(item).where(item.c.id.in_(gone)).values(state=int(ItemState.OK))
        )
    assert await rel.relay.scan_once() == 2
    assert await rel.window_size(batch_id) == 3
    assert await parked(rel, batch_id) == 5


async def test_refill_counts_ready_records_and_skips_paused(rel: RelayEnv) -> None:
    batch_id = await add_windowed(rel, 6)
    _ = await rel.relay.scan_once()
    window = rel.tables.window
    outbox = rel.tables.outbox
    batch = rel.tables.batch
    async with rel.env.transaction() as conn:
        # Окно пусто, но одна запись уже готова к отправке: вернуть можно только две.
        _ = await conn.execute(delete(window))
        ready = await conn.scalar(select(outbox.c.id).where(outbox.c.batch_id == batch_id).limit(1))
        _ = await conn.execute(
            update(outbox).where(outbox.c.id == ready).values(available_at=rel.clock.current)
        )
        assert await refill_window(conn, rel.tables) == 2
        assert await refill_window(conn, rel.tables) == 0
    async with rel.env.transaction() as conn:
        _ = await conn.execute(update(batch).values(paused_at=NOW))
        _ = await conn.execute(delete(window))
        _ = await conn.execute(update(outbox).values(available_at=text("'infinity'")))
        assert await refill_window(conn, rel.tables) == 0


async def test_reclaim_after_failure_keeps_window_place(rel: RelayEnv) -> None:
    # Relay захватил записи и упал до DELETE: после claim_ttl те же записи уходят, не паркуются.
    batch_id = await add_windowed(rel, 5)
    rel.dispatcher.fail_times = 1
    assert await rel.relay.scan_once() == 0
    assert await rel.window_size(batch_id) == 3
    assert await parked(rel, batch_id) == 2
    rel.clock.advance(TTL + GRACE)
    assert await rel.relay.scan_once() == 3
    assert await rel.window_size(batch_id) == 3
    assert await parked(rel, batch_id) == 2


async def test_busy_window_lock_skips_batch(env: Env) -> None:
    rel = relay_env(env, NOW, RelaySettings(chunk=2))
    batch_id = await add_windowed(rel, 5)
    key = window_lock_key(batch_id)
    async with env.engine.connect() as holder:
        assert await holder.scalar(select(func.pg_try_advisory_lock(key)))
        try:
            assert await rel.relay.scan_once() == 0
            assert await parked(rel, batch_id) == 0
            assert await rel.window_size(batch_id) == 0
        finally:
            _ = await holder.scalar(select(func.pg_advisory_unlock(key)))
            await holder.commit()
    assert await rel.relay.scan_once() == 3


async def test_parallel_relays_respect_window(env: Env) -> None:
    rel = relay_env(env, NOW, RelaySettings(chunk=4))
    rel.dispatcher.delay = 0.01
    batch_id = await add_windowed(rel, 40, max_in_flight=5)
    other = RecordingDispatcher(delay=0.01)
    for _ in range(3):
        _ = await asyncio.gather(
            rel.relay.scan_once(), rel.another_relay(other).scan_once(), rel.relay.scan_once()
        )
    # Записи, пропущенные из-за занятой блокировки окна, паркует следующий проход.
    assert await rel.relay.scan_once() == 0
    ids = rel.dispatcher.ids + other.ids
    assert len(ids) == len(set(ids)) == 5
    assert await rel.window_size(batch_id) == 5
    assert await parked(rel, batch_id) == 35
