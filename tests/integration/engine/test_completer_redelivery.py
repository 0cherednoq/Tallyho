"""Completer: подтверждённый дубль доставки и release по вердикту RETRY (UC-04, Fix-7)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import case, func, select, update

from tallyho.engine.completer import ClaimOutcome, FinishResult
from tallyho.model.states import ItemState, ResultClass
from tests.integration.engine.completer_env import (
    NOW,
    SETTINGS,
    MovableClock,
    RecordingRelay,
    TaskLimits,
    lease_row,
    open_completer,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from tests.integration.engine.conftest import Env

OTHER = replace(SETTINGS, worker_id="worker-2", slot=SETTINGS.slot + 2)
TTL = SETTINGS.lease_ttl
LIMITS = TaskLimits(1)
# Одна попытка в запасе: release после дубля возвращает Item в outbox (UC-04).


async def redelivered(env: Env, item_id: UUID) -> bool:
    lease = await lease_row(env, item_id)
    assert lease is not None
    return bool(lease["redelivered"])


async def outbox_rows(env: Env) -> list[tuple[UUID | None, datetime | None]]:
    """Записи outbox: ``(item_id, available_at)``; ``None`` — запаркована (``infinity``)."""
    outbox = env.tables.outbox
    available_at = case((func.isfinite(outbox.c.available_at), outbox.c.available_at), else_=None)
    async with env.connection() as conn:
        rows = await conn.execute(select(outbox.c.item_id, available_at))
        return [(item_id, value) for item_id, value in rows]


async def attempt_of(env: Env, item_id: UUID) -> int:
    item = env.tables.item
    async with env.connection() as conn:
        return int(await conn.scalar(select(item.c.attempt).where(item.c.id == item_id)) or 0)


async def generation_of(env: Env, item_id: UUID) -> int:
    item = env.tables.item
    async with env.connection() as conn:
        return int(await conn.scalar(select(item.c.generation).where(item.c.id == item_id)) or 0)


async def test_fresh_lease_is_not_redelivered(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        assert not await redelivered(env, ref.id)


async def test_duplicate_of_foreign_live_lease_marks_it(env: Env) -> None:
    seeded = await seed(env, 2)
    ref, untouched = seeded.refs
    async with open_completer(env) as owner, open_completer(env, settings=OTHER) as other:
        assert (await owner.claim(ref)).run
        assert (await owner.claim(untouched)).run
        assert (await other.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        # Повторный дубль уже помеченного lease ничего не меняет.
        assert (await other.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        assert await redelivered(env, ref.id)
        assert not await redelivered(env, untouched.id)
        lease = await lease_row(env, ref.id)
        assert lease is not None
        assert lease["worker_id"] == SETTINGS.worker_id
        assert owner.held == {ref.id, untouched.id}


async def test_duplicate_of_own_lease_marks_it(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        assert (await completer.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        assert await redelivered(env, ref.id)


async def test_duplicate_in_the_same_transaction_marks_lease(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        results = await asyncio.gather(completer.claim(ref), completer.claim(ref))
        assert sorted(result.outcome for result in results) == [
            ClaimOutcome.CLAIMED,
            ClaimOutcome.DUPLICATE,
        ]
        assert await redelivered(env, ref.id)


async def test_release_without_duplicate_leaves_retry_to_broker(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    relay = RecordingRelay()
    async with open_completer(env, relay=relay) as completer:
        assert (await completer.claim(ref)).run
        assert await completer.release(ref)
    assert await outbox_rows(env) == []
    assert relay.calls == []
    assert (await env.counters(ref.batch_id)).dispatched == 1


async def test_release_after_duplicate_returns_item_to_outbox(env: Env) -> None:
    seeded = await seed(env, 2)
    ref, plain = seeded.refs
    relay = RecordingRelay()
    async with (
        open_completer(env, relay=relay, limits=LIMITS) as owner,
        open_completer(env, settings=OTHER, limits=LIMITS) as other,
    ):
        assert (await owner.claim(ref)).run
        assert (await owner.claim(plain)).run
        assert (await other.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        released, released_plain = await asyncio.gather(owner.release(ref), owner.release(plain))
        assert released
        assert released_plain
        assert owner.held == frozenset()
        # Повторный release уже ничего не отпускает и второй записи не создаёт.
        assert not await owner.release(ref)
    assert await outbox_rows(env) == [(ref.id, NOW)]
    assert relay.calls == [[seeded.batch_id]]
    assert await lease_row(env, ref.id) is None
    assert await attempt_of(env, ref.id) == 1
    assert await attempt_of(env, plain.id) == 1
    # Возврат в outbox — новое поколение отправки; обычный release его не меняет.
    assert await generation_of(env, ref.id) == 1
    assert await generation_of(env, plain.id) == 0
    # Из двух отправленных Items у брокера остался один: второй вернулся в outbox.
    assert (await env.counters(seeded.batch_id)).dispatched == 1


async def test_release_after_duplicate_parks_item_of_paused_batch(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    relay = RecordingRelay()
    async with open_completer(env, relay=relay, limits=LIMITS) as completer:
        assert (await completer.claim(ref)).run
        assert (await completer.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        await set_batch(env, seeded.batch_id, paused_at=NOW)
        assert await completer.release(ref)
    assert await outbox_rows(env) == [(ref.id, None)]
    assert relay.calls == []
    assert (await env.counters(seeded.batch_id)).dispatched == 0


async def test_release_after_duplicate_without_attempts_left_finishes_item(env: Env) -> None:
    # Fix-19: у Item не осталось попыток (умолчание 0 без RetryLimits) — release после
    # дубля не возвращает его в outbox, а завершает error("exhausted"); обычный release
    # в той же пачке отпускает lease как раньше.
    seeded = await seed(env, 2)
    ref, plain = seeded.refs
    relay = RecordingRelay()
    async with open_completer(env, relay=relay) as completer:
        assert (await completer.claim(ref)).run
        assert (await completer.claim(plain)).run
        assert (await completer.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        released, released_plain = await asyncio.gather(
            completer.release(ref), completer.release(plain)
        )
        assert released
        assert released_plain
        assert completer.held == frozenset()
    assert await outbox_rows(env) == []
    assert relay.calls == []
    assert await lease_row(env, ref.id) is None
    assert await lease_row(env, plain.id) is None
    item = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(item.c.state, item.c.label, item.c.error, item.c.attempt).where(
                    item.c.id == ref.id
                )
            )
        ).one()
    assert (ItemState(row.state), row.label, row.attempt) == (ItemState.ERROR, "exhausted", 0)
    assert row.error["type"] == "RedeliveryExhausted"
    assert await attempt_of(env, plain.id) == 1
    assert await generation_of(env, ref.id) == 0
    counters = await env.counters(seeded.batch_id)
    assert (counters.error, counters.dispatched) == (1, 2)


async def test_release_and_retry_in_one_transaction_do_not_requeue(env: Env) -> None:
    # Ретрай брокера пришёл в ту же пачку, что и release: lease отпущен раньше claim,
    # поэтому это обычная новая попытка, а не дубль.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        released, retried = await asyncio.gather(completer.release(ref), completer.claim(ref))
        assert released
        assert retried.outcome is ClaimOutcome.CLAIMED
        assert not await redelivered(env, ref.id)
    assert await outbox_rows(env) == []


async def test_takeover_of_expired_lease_forgets_duplicates(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    async with (
        open_completer(env, clock=clock) as dead,
        open_completer(env, clock=clock, settings=OTHER) as other,
    ):
        assert (await dead.claim(ref)).run
        assert (await other.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        assert await redelivered(env, ref.id)
        # Владелец умер, lease истёк: следующая доставка — новая попытка со своим ретраем.
        clock.value = NOW + TTL + timedelta(seconds=1)
        taken = await other.claim(ref)
        assert taken.outcome is ClaimOutcome.CLAIMED
        assert taken.attempt == 1
        assert not await redelivered(env, ref.id)
        assert await other.release(ref)
    assert await outbox_rows(env) == []


async def test_finish_after_duplicate_does_not_requeue(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        assert (await completer.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        assert await completer.finish(ref, FinishResult(result_class=ResultClass.OK))
    assert await outbox_rows(env) == []
    assert await lease_row(env, ref.id) is None
    assert (await env.counters(seeded.batch_id)).ok == 1


async def test_release_of_terminal_item_after_duplicate_does_not_requeue(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    item = env.tables.item
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        assert (await completer.claim(ref)).outcome is ClaimOutcome.DUPLICATE
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(item).where(item.c.id == ref.id).values(state=int(ItemState.OK))
            )
        assert await completer.release(ref)
    assert await outbox_rows(env) == []
    assert await attempt_of(env, ref.id) == 0
