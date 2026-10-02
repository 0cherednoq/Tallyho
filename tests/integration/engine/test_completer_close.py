"""Completer: мягкая остановка с возвратом lease в outbox (A-CH-08)."""

from __future__ import annotations

import asyncio
import itertools
import logging
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho.engine.completer import FinishResult
from tallyho.model.errors import ClosedError, CompleterError, InvalidStateError
from tallyho.model.states import ItemState, OutboxKind, ResultClass
from tests.integration.engine.completer_env import (
    NOW,
    lease_row,
    open_completer,
    schema_engine,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from collections.abc import Callable
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
        generations = dict(
            (
                await conn.execute(
                    select(item.c.id, item.c.generation).where(
                        item.c.id.in_([ref.id for ref in (*seeded.refs, *paused.refs)])
                    )
                )
            ).all()
        )
    assert attempt == 0
    # Зато это новая отправка: поколение растёт только у вернувшихся в outbox.
    assert generations == {
        running.id: 1,
        finished.id: 0,
        stolen.id: 0,
        released.id: 0,
        paused.refs[0].id: 1,
    }
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


async def test_closed_completer_rejects_new_operations(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        unbound = completer.loop
        assert (await completer.claim(ref)).run
        assert (unbound, completer.loop) == (None, asyncio.get_running_loop())
        await completer.close()
        # ClosedError — подкласс InvalidStateError: прежние обработчики продолжают работать.
        with pytest.raises(ClosedError, match="Completer закрыт") as raised:
            _ = await completer.heartbeat(ref)
        assert isinstance(raised.value, InvalidStateError)


async def _forever() -> None:
    _ = await asyncio.Event().wait()


async def test_complete_in_after_close_is_rejected_before_any_write(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with (
        open_completer(env) as completer,
        AsyncSession(schema_engine(env)) as session,
    ):
        assert (await completer.claim(ref)).run
        await completer.close()
        with pytest.raises(ClosedError):
            _ = await completer.complete_in(session, ref, FinishResult(result_class=ResultClass.OK))
        await session.commit()
    assert await lease_row(env, ref.id) is not None
    assert await env.count(env.tables.counter_delta) == 0


async def test_commit_after_close_leaves_folding_to_the_sweeper(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with (
        open_completer(env) as completer,
        AsyncSession(schema_engine(env)) as session,
    ):
        assert (await completer.claim(ref)).run
        assert await completer.complete_in(session, ref, FinishResult(result_class=ResultClass.OK))
        # Установку закрыли, пока транзакция пользователя ещё не закоммичена.
        await completer.close()
        with caplog.at_level(logging.WARNING):
            await session.commit()
        await completer.settled()
    # Commit прошёл без ошибок в логе; дельта ждёт sweeper, фоновой задачи нет.
    assert caplog.records == []
    assert await env.count(env.tables.counter_delta) == 1
    assert [task for task in asyncio.all_tasks() if task.get_name() == "tallyho-complete-in"] == []
    item = env.tables.item
    async with env.connection() as conn:
        state = await conn.scalar(select(item.c.state).where(item.c.id == ref.id))
    assert state == int(ItemState.OK)


async def test_close_cancels_attached_heartbeat_tasks(env: Env) -> None:
    async with open_completer(env) as completer:
        beating = asyncio.create_task(_forever(), name="tallyho-heartbeat-probe")
        finished = asyncio.create_task(asyncio.sleep(0), name="tallyho-heartbeat-done")
        completer.attach(beating)
        completer.attach(finished)
        await finished

        await completer.close()

        assert beating.cancelled()


async def test_abort_of_unused_completer_only_closes_it(env: Env) -> None:
    seeded = await seed(env, 1)
    async with open_completer(env) as completer:
        await completer.abort()
        await completer.settled()
        with pytest.raises(ClosedError):
            _ = await completer.claim(seeded.refs[0])


async def test_abort_fails_operations_that_missed_the_commit(env: Env) -> None:
    seeded = await seed(env, 3)
    blocked, buffered, abandoned = seeded.refs
    item = env.tables.item
    async with open_completer(env) as completer:
        async with env.transaction() as conn:
            # Чужая блокировка строки Item: групповая транзакция claim ждёт её.
            _ = await conn.execute(
                select(item.c.id).where(item.c.id == blocked.id).with_for_update()
            )
            first = asyncio.create_task(completer.claim(blocked))
            await _until(lambda: completer.buffered == 1)
            await _until(lambda: completer.buffered == 0)  # операция ушла в транзакцию
            second = asyncio.create_task(completer.claim(buffered))
            third = asyncio.create_task(completer.claim(abandoned))
            await _until(lambda: completer.buffered == 2)
            # Вызывающий третьей операции уже отменён: её будущее ошибкой не затирается.
            _ = third.cancel()
            _ = await asyncio.wait({third})

            await completer.abort()

            for task in (first, second):
                with pytest.raises(CompleterError, match="не уложилось в срок"):
                    _ = await task
            assert third.cancelled()
        # Completer остановлен: простой наступил, повторное закрытие ничего не ждёт.
        await completer.settled()
        await completer.close(requeue_held=True)
        with pytest.raises(ClosedError):
            _ = await completer.claim(buffered)
    assert await lease_row(env, blocked.id) is None
    assert await lease_row(env, buffered.id) is None
    assert await env.count(env.tables.outbox) == 0


async def _until(condition: Callable[[], bool]) -> None:
    async with asyncio.timeout(10):
        for _ in itertools.count():
            if condition():
                return
            await asyncio.sleep(0.001)
