"""Completer: claim и его исходы (UC-03, UC-11, UC-12, §6.2, §11.3, §11.4)."""

from __future__ import annotations

import asyncio
import math
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import func, insert, select, update

from tallyho.engine.completer import (
    CANCELLED_LABEL,
    ClaimOutcome,
    ClaimResult,
    ItemRef,
)
from tallyho.model.states import ItemState, OutboxKind
from tests.integration.engine.completer_env import (
    NOW,
    SETTINGS,
    WORKER,
    CommitCounter,
    Finalized,
    MovableClock,
    lease_row,
    open_completer,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy import RowMapping

    from tests.integration.engine.conftest import Env

TTL = SETTINGS.lease_ttl


async def item_row(env: Env, item_id: UUID) -> RowMapping:
    item = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(item).where(item.c.id == item_id))).mappings().one()


async def put_lease(env: Env, ref: ItemRef, *, until: timedelta, worker: str = "other") -> None:
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=ref.id,
                batch_id=ref.batch_id,
                lease_until=NOW + until,
                worker_id=worker,
                attempt=0,
                progress_done=3,
            )
        )


async def test_claim_takes_lease(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        result = await completer.claim(ref)
        assert completer.held == {ref.id}
    assert result == ClaimResult(outcome=ClaimOutcome.CLAIMED, attempt=0, depth=0)
    assert result.run
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert lease["batch_id"] == seeded.batch_id
    assert lease["worker_id"] == WORKER
    assert lease["lease_until"] == NOW + TTL
    assert lease["attempt"] == 0
    # Claim не трогает Item: он остаётся active.
    assert (await item_row(env, ref.id))["state"] == ItemState.ACTIVE


async def test_repeated_claim_is_duplicate(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        first = await completer.claim(ref)
        second = await completer.claim(ref)
    assert first.outcome is ClaimOutcome.CLAIMED
    assert second.outcome is ClaimOutcome.DUPLICATE
    assert not second.run
    assert await env.count(env.tables.lease) == 1


async def test_duplicate_in_one_transaction(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    counter = CommitCounter()
    async with open_completer(env, counter=counter) as completer:
        results = await asyncio.gather(*(completer.claim(ref) for _ in range(3)))
    outcomes = sorted(result.outcome for result in results)
    assert outcomes == [ClaimOutcome.CLAIMED, ClaimOutcome.DUPLICATE, ClaimOutcome.DUPLICATE]
    assert counter.commits == 1


async def test_live_foreign_lease_is_duplicate(env: Env) -> None:
    # Ретрай брокера при живом чужом lease (FLEXIQ_SPIKE 9a): успех без выполнения.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    await put_lease(env, ref, until=timedelta(seconds=1))
    async with open_completer(env) as completer:
        result = await completer.claim(ref)
        assert completer.held == frozenset()
    assert result.outcome is ClaimOutcome.DUPLICATE
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert lease["worker_id"] == "other"


async def test_expired_foreign_lease_is_taken_over(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    await put_lease(env, ref, until=timedelta(0))
    async with open_completer(env) as completer:
        result = await completer.claim(ref)
    assert result == ClaimResult(outcome=ClaimOutcome.CLAIMED, attempt=1, depth=0)
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert (lease["worker_id"], lease["attempt"]) == (WORKER, 1)
    assert lease["lease_until"] == NOW + TTL
    assert lease["progress_done"] is None
    assert (await item_row(env, ref.id))["attempt"] == 1


async def test_terminal_and_missing_items(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id == ref.id).values(state=int(ItemState.OK), attempt=2)
        )
    unknown_item = ItemRef(uuid4(), seeded.batch_id)
    unknown_batch = ItemRef(ref.id, uuid4())
    async with open_completer(env) as completer:
        done, missing, wrong_batch = await asyncio.gather(
            completer.claim(ref), completer.claim(unknown_item), completer.claim(unknown_batch)
        )
    assert done == ClaimResult(outcome=ClaimOutcome.TERMINAL, attempt=2, depth=0)
    assert missing == ClaimResult(outcome=ClaimOutcome.TERMINAL)
    assert wrong_batch.outcome is ClaimOutcome.TERMINAL
    assert await env.count(env.tables.lease) == 0


async def test_claim_on_paused_batch_parks_item(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    await set_batch(env, seeded.batch_id, paused_at=NOW)
    await put_lease(env, second, until=timedelta(0))
    before = await env.counters(seeded.batch_id)
    async with open_completer(env) as completer:
        results = await asyncio.gather(completer.claim(first), completer.claim(second))
        assert completer.held == frozenset()
    assert {result.outcome for result in results} == {ClaimOutcome.PARKED}
    outbox = env.tables.outbox
    async with env.connection() as conn:
        rows = (await conn.execute(select(outbox).order_by(outbox.c.id))).mappings().all()
        finite = await conn.scalar(select(func.bool_or(func.isfinite(outbox.c.available_at))))
    assert [row["id"] for row in rows] == [first.id, second.id]
    assert [row["item_id"] for row in rows] == [first.id, second.id]
    assert {row["kind"] for row in rows} == {OutboxKind.ITEM}
    assert {row["task_name"] for row in rows} == {"send"}
    assert finite is False
    # Истёкший lease припаркованного Item удалён, окно max_in_flight освобождено.
    assert await env.count(env.tables.lease) == 0
    after = await env.counters(seeded.batch_id)
    assert after.dispatched == before.dispatched - 2
    assert after.pending == before.pending


async def test_park_twice_counts_once(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    await set_batch(env, seeded.batch_id, paused_at=NOW)
    before = await env.counters(seeded.batch_id)
    async with open_completer(env) as completer:
        _ = await completer.claim(ref)
        again = await completer.claim(ref)
    assert again.outcome is ClaimOutcome.PARKED
    assert (await env.counters(seeded.batch_id)).dispatched == before.dispatched - 1


async def test_claim_on_cancelled_batch_cancels_lazily(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    await set_batch(env, seeded.batch_id, cancel_requested_at=NOW, cancel_reason="cancel")
    await put_lease(env, second, until=timedelta(0))
    # Дубль пришёл раньше, чем relay удалил запись outbox.
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.outbox).values(
                id=first.id,
                kind=int(OutboxKind.ITEM),
                batch_id=seeded.batch_id,
                item_id=first.id,
                task_name="send",
                available_at=NOW,
            )
        )
    finalizer = Finalized()
    async with open_completer(env, finalizer=finalizer) as completer:
        results = await asyncio.gather(completer.claim(first), completer.claim(second))
    assert {result.outcome for result in results} == {ClaimOutcome.CANCELLED}
    for ref in (first, second):
        row = await item_row(env, ref.id)
        assert row["state"] == ItemState.CANCELLED
        assert row["label"] == CANCELLED_LABEL
        assert row["finished_at"] == NOW
    counters = await env.counters(seeded.batch_id)
    assert (counters.cancelled, counters.w_done, counters.pending) == (2, 2, 0)
    assert await env.count(env.tables.outbox) == 0
    assert await env.count(env.tables.lease) == 0
    assert finalizer.calls == [seeded.batch_id]


async def test_expired_item_is_not_run(env: Env) -> None:
    seeded = await seed(env, 2)
    late, fresh = seeded.refs
    expiry = env.tables.expiry
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(expiry).values(
                [
                    {"item_id": late.id, "expires_at": NOW},
                    {"item_id": fresh.id, "expires_at": NOW + timedelta(seconds=1)},
                ]
            )
        )
    async with open_completer(env) as completer:
        late_result, fresh_result = await asyncio.gather(
            completer.claim(late), completer.claim(fresh)
        )
    assert late_result.outcome is ClaimOutcome.EXPIRED
    assert fresh_result.outcome is ClaimOutcome.CLAIMED
    # Строку просроченного Item оставляем sweeper'у (error("expired")), захваченного — удаляем.
    async with env.connection() as conn:
        left = list(await conn.scalars(select(expiry.c.item_id)))
    assert left == [late.id]
    assert await lease_row(env, late.id) is None


async def test_thousand_claims_in_few_transactions(env: Env) -> None:
    n = 1000
    seeded = await seed(env, n)
    counter = CommitCounter()
    async with open_completer(env, counter=counter) as completer:
        results = await asyncio.gather(*(completer.claim(ref) for ref in seeded.refs))
        assert len(completer.held) == n
    assert {result.outcome for result in results} == {ClaimOutcome.CLAIMED}
    assert counter.commits <= math.ceil(n / SETTINGS.max_batch) + 1
    assert await env.count(env.tables.lease) == n


async def test_database_clock_drives_lease(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    await put_lease(env, ref, until=timedelta(seconds=30))
    clock = MovableClock(NOW + timedelta(seconds=30))
    settings = replace(SETTINGS, lease_ttl=timedelta(seconds=5))
    async with open_completer(env, clock=clock, settings=settings) as completer:
        result = await completer.claim(ref)
    assert result.outcome is ClaimOutcome.CLAIMED
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert lease["lease_until"] == clock.value + timedelta(seconds=5)
