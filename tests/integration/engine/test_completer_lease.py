"""Completer: heartbeat и release (UC-03, UC-04, §9.4)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from tallyho.engine.completer import ClaimOutcome
from tallyho.model.states import ItemState
from tests.integration.engine.completer_env import (
    NOW,
    SETTINGS,
    CommitCounter,
    MovableClock,
    lease_row,
    open_completer,
    seed,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tests.integration.engine.conftest import Env

TTL = SETTINGS.lease_ttl


async def attempt_of(env: Env, item_id: UUID) -> int:
    item = env.tables.item
    async with env.connection() as conn:
        return int(await conn.scalar(select(item.c.attempt).where(item.c.id == item_id)) or 0)


async def test_heartbeat_extends_lease_and_stores_progress(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        assert (await completer.claim(ref)).run
        clock.value = NOW + timedelta(seconds=20)
        assert await completer.heartbeat(ref, progress_done=3, progress_total=10)
        lease = await lease_row(env, ref.id)
        assert lease is not None
        assert lease["lease_until"] == clock.value + TTL
        assert (lease["progress_done"], lease["progress_total"]) == (3, 10)
        # Без прогресса — только продление; прежние значения остаются.
        clock.value = NOW + timedelta(seconds=40)
        assert await completer.heartbeat(ref)
        assert completer.held == {ref.id}
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert lease["lease_until"] == clock.value + TTL
    assert (lease["progress_done"], lease["progress_total"]) == (3, 10)


async def test_heartbeats_in_one_transaction_merge_progress(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    counter = CommitCounter()
    async with open_completer(env, counter=counter) as completer:
        assert (await completer.claim(ref)).run
        before = counter.commits
        results = await asyncio.gather(
            completer.heartbeat(ref, progress_done=1, progress_total=5),
            completer.heartbeat(ref, progress_done=2),
        )
        assert counter.commits == before + 1
    assert all(results)
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert (lease["progress_done"], lease["progress_total"]) == (2, 5)


async def test_heartbeat_of_lost_lease(env: Env) -> None:
    seeded = await seed(env, 2)
    mine, never = seeded.refs
    lease = env.tables.lease
    async with open_completer(env) as completer:
        assert (await completer.claim(mine)).run
        # Lease истёк, и его перехватил другой воркер.
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(lease).where(lease.c.item_id == mine.id).values(worker_id="other")
            )
        assert not await completer.heartbeat(mine, progress_done=1)
        assert not await completer.heartbeat(never)
        assert completer.held == frozenset()
    row = await lease_row(env, mine.id)
    assert row is not None
    assert row["progress_done"] is None


async def test_release_frees_lease_and_counts_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        assert await completer.release(ref)
        assert completer.held == frozenset()
        assert not await completer.release(ref)
        # Ретрай брокера снова захватывает Item — уже как попытку 1.
        again = await completer.claim(ref)
    assert again.outcome is ClaimOutcome.CLAIMED
    assert again.attempt == 1
    assert await attempt_of(env, ref.id) == 1
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert lease["attempt"] == 1


async def test_release_and_retry_in_one_transaction(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        released, released_twice, retried = await asyncio.gather(
            completer.release(ref), completer.release(ref), completer.claim(ref)
        )
        assert completer.held == {ref.id}
    assert (released, released_twice) == (True, False)
    assert retried.outcome is ClaimOutcome.CLAIMED
    assert retried.attempt == 1


async def test_release_of_foreign_lease_changes_nothing(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    lease = env.tables.lease
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(lease).where(lease.c.item_id == ref.id).values(worker_id="other")
            )
        assert not await completer.release(ref)
    assert await lease_row(env, ref.id) is not None
    assert await attempt_of(env, ref.id) == 0


async def test_release_of_terminal_item_keeps_attempt(env: Env) -> None:
    # Lease у терминального Item (дубль успел claim, пока оригинал завершался).
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    item = env.tables.item
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(item).where(item.c.id == ref.id).values(state=int(ItemState.OK))
            )
        assert await completer.release(ref)
    assert await lease_row(env, ref.id) is None
    assert await attempt_of(env, ref.id) == 0
