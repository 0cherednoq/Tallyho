"""Completer: мягкая остановка с возвратом lease в outbox (A-CH-08)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select, text, update

from tallyho.model.errors import CompleterError
from tallyho.model.states import ItemState, OutboxKind
from tests.integration.engine.completer_env import (
    NOW,
    lease_row,
    open_completer,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from tests.integration.engine.conftest import Env


async def outbox_rows(env: Env) -> list[tuple[UUID, int, datetime, bool]]:
    """``(id, kind, available_at, isfinite(available_at))`` записей outbox по id."""
    outbox = env.tables.outbox
    stmt = select(
        outbox.c.id, outbox.c.kind, outbox.c.available_at, func.isfinite(outbox.c.available_at)
    ).order_by(outbox.c.id)
    async with env.connection() as conn:
        return [(row[0], row[1], row[2], bool(row[3])) for row in await conn.execute(stmt)]


async def test_close_requeues_held_leases(env: Env) -> None:
    seeded = await seed(env, 4)
    running, finished, stolen, released = seeded.refs
    paused = await seed(env, 1, kind="paused")
    lease = env.tables.lease
    item = env.tables.item
    before = await env.counters(seeded.batch_id)
    async with open_completer(env) as completer:
        for ref in [*seeded.refs, *paused.refs]:
            assert (await completer.claim(ref)).run
        assert await completer.release(released)
        await set_batch(env, paused.batch_id, paused_at=NOW)
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(item).where(item.c.id == finished.id).values(state=int(ItemState.OK))
            )
            _ = await conn.execute(
                update(lease).where(lease.c.item_id == stolen.id).values(worker_id="other")
            )
        await completer.close(requeue_held=True)
        assert completer.held == frozenset()
    # Свой lease у активного Item — сразу в outbox, у терминального — просто удалён.
    assert await lease_row(env, running.id) is None
    assert await lease_row(env, finished.id) is None
    assert await lease_row(env, stolen.id) is not None
    rows = await outbox_rows(env)
    assert [row[0] for row in rows] == [running.id, paused.refs[0].id]
    assert {row[1] for row in rows} == {OutboxKind.ITEM}
    assert rows[0][2] == NOW
    assert [row[3] for row in rows] == [True, False]
    # Попытка не тратится: задача не упала.
    async with env.connection() as conn:
        attempt = await conn.scalar(select(item.c.attempt).where(item.c.id == running.id))
    assert attempt == 0
    after = await env.counters(seeded.batch_id)
    assert after.dispatched == before.dispatched - 1
    assert (await env.counters(paused.batch_id)).dispatched == 0


async def test_repeated_close_can_requeue(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        await completer.close()
        await completer.close(requeue_held=True)
    assert await lease_row(env, ref.id) is None
    assert await env.count(env.tables.outbox) == 1


async def test_failed_requeue_raises(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        async with env.transaction() as conn:
            _ = await conn.execute(text(f'ALTER TABLE "{env.schema}".th_outbox RENAME TO gone'))
        with pytest.raises(CompleterError):
            await completer.close(requeue_held=True)
    # Транзакция откатилась: lease остался, его вернёт sweeper.
    assert await lease_row(env, ref.id) is not None
