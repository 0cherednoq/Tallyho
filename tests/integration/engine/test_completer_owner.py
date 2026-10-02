"""Completer: finish и release применяет только попытка-владелец lease (UC-03).

Устаревшая попытка этого же процесса (тот же ``worker_id``) и попытка, взявшая
lease заново, могут оказаться в одной групповой транзакции. Применяется
операция владельца, остальные ничего не пишут и получают ``False``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from tallyho.engine.completer import ClaimOutcome, FinishResult
from tallyho.model.states import ItemState, ResultClass
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

    from tallyho.engine.completer import Completer, ItemRef
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

LATER = NOW + SETTINGS.lease_ttl + timedelta(seconds=1)
STALE = FinishResult(result_class=ResultClass.ERROR, label="stale")
OWNER = FinishResult(result_class=ResultClass.OK, label="owner")


async def _item(env: Env, item_id: UUID) -> tuple[ItemState, str | None, int]:
    table = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(table.c.state, table.c.label, table.c.attempt).where(table.c.id == item_id)
            )
        ).one()
    return ItemState(row[0]), row[1], int(row[2])


async def _taken_over(completer: Completer, clock: MovableClock, ref: ItemRef) -> None:
    # Попытка 0 взяла lease, он истёк, и этот же процесс перехватил Item попыткой 1.
    assert (await completer.claim(ref)).attempt == 0
    clock.value = LATER
    again = await completer.claim(ref)
    assert (again.outcome, again.attempt) == (ClaimOutcome.CLAIMED, 1)


@pytest.mark.parametrize("stale_first", [True, False])
async def test_finish_of_owner_wins_over_stale_attempt_in_same_flush(
    env: Env, *, stale_first: bool
) -> None:
    ref = (await seed(env, 1)).refs[0]
    clock = MovableClock()
    counter = CommitCounter()
    async with open_completer(env, clock=clock, counter=counter) as completer:
        await _taken_over(completer, clock, ref)
        before = counter.commits
        stale = completer.finish(ref, STALE, attempt=0)
        owner = completer.finish(ref, OWNER, attempt=1)
        if stale_first:
            results = list(await asyncio.gather(stale, owner))
        else:
            results = list(reversed(await asyncio.gather(owner, stale)))
        assert counter.commits == before + 1
        assert results == [False, True]
        assert completer.held == frozenset()
    assert await _item(env, ref.id) == (ItemState.OK, "owner", 1)
    assert await lease_row(env, ref.id) is None
    counters = await env.counters(ref.batch_id)
    assert (counters.ok, counters.error, counters.pending) == (1, 0, 0)


@pytest.mark.parametrize("stale_first", [True, False])
async def test_release_of_owner_wins_over_stale_attempt_in_same_flush(
    env: Env, *, stale_first: bool
) -> None:
    ref = (await seed(env, 1)).refs[0]
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        await _taken_over(completer, clock, ref)
        stale = completer.release(ref, attempt=0)
        owner = completer.release(ref, attempt=1)
        if stale_first:
            results = list(await asyncio.gather(stale, owner))
        else:
            results = list(reversed(await asyncio.gather(owner, stale)))
        assert results == [False, True]
        assert completer.held == frozenset()
    assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 2)
    assert await lease_row(env, ref.id) is None


async def test_stale_operations_keep_new_attempt_of_same_process(env: Env) -> None:
    ref = (await seed(env, 1)).refs[0]
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        await _taken_over(completer, clock, ref)
        assert not await completer.finish(ref, STALE, attempt=0)
        assert not await completer.release(ref, attempt=0)
        # Item снова у этого процесса: запись о нём остаётся для close(requeue_held).
        assert completer.held == {ref.id}
    assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert (lease["worker_id"], lease["attempt"]) == (SETTINGS.worker_id, 1)


async def test_claim_takeover_in_same_flush_drops_stale_finish(env: Env) -> None:
    ref = (await seed(env, 1)).refs[0]
    clock = MovableClock()
    counter = CommitCounter()
    async with open_completer(env, clock=clock, counter=counter) as completer:
        assert (await completer.claim(ref)).run
        clock.value = LATER
        before = counter.commits
        claimed, finished = await asyncio.gather(
            completer.claim(ref), completer.finish(ref, STALE, attempt=0)
        )
        assert counter.commits == before + 1
        assert (claimed.outcome, claimed.attempt, finished) == (ClaimOutcome.CLAIMED, 1, False)
        assert completer.held == {ref.id}
    assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)


async def test_expired_lease_without_takeover_still_belongs_to_attempt(env: Env) -> None:
    refs = (await seed(env, 2)).refs
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        for ref in refs:
            assert (await completer.claim(ref)).run
        clock.value = LATER
        assert await completer.finish(refs[0], OWNER, attempt=0)
        assert await completer.release(refs[1], attempt=0)
    assert await _item(env, refs[0].id) == (ItemState.OK, "owner", 0)
    assert await _item(env, refs[1].id) == (ItemState.ACTIVE, None, 1)


async def test_operation_without_lease_or_with_wrong_attempt_writes_nothing(env: Env) -> None:
    free, other = (await seed(env, 2)).refs
    async with open_completer(env) as completer:
        # Lease нет совсем (не брали, удалил sweeper, вернули в outbox).
        assert not await completer.finish(free, OWNER, attempt=0)
        assert not await completer.release(free, attempt=0)
        # Lease этого процесса, но другой попытки.
        assert (await completer.claim(other)).run
        assert not await completer.finish(other, OWNER, attempt=1)
        assert not await completer.release(other, attempt=1)
        assert completer.held == {other.id}
        # Без attempt — прежний CAS по state (обработчик DLQ и вызовы вне обёртки).
        assert await completer.finish(free, STALE)
    assert await _item(env, free.id) == (ItemState.ERROR, "stale", 0)
    assert await _item(env, other.id) == (ItemState.ACTIVE, None, 0)
    lease = await lease_row(env, other.id)
    assert lease is not None
    assert (lease["worker_id"], lease["attempt"]) == (SETTINGS.worker_id, 0)
