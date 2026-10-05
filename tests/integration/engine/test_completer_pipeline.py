"""Completer: работа после commit не задерживает следующую транзакцию, тик — от первой операции.

T11.6: в профиле цикл Completer тратил больше половины времени на подсказку ``watch``,
политику и ``try_finalize`` после каждого commit, а операции, пришедшие во время транзакции,
ждали после неё ещё один тик. Оба эффекта складывались в задержку claim и finish.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select

from tallyho.engine.completer import FinishResult
from tallyho.model.states import ResultClass
from tests.integration.engine.completer_env import (
    SETTINGS,
    CommitCounter,
    RecordingProgress,
    open_completer,
    seed,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

OK = FinishResult(result_class=ResultClass.OK)


@dataclass
class BlockedFinalizer:
    """``try_finalize`` ждёт разрешения теста; вызовы запоминаются."""

    calls: list[UUID] = field(default_factory=list["UUID"])
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def try_finalize(self, batch_id: UUID) -> bool:
        self.calls.append(batch_id)
        self.entered.set()
        _ = await self.release.wait()
        return False


async def test_flush_does_not_wait_for_after_commit_work(env: Env) -> None:
    seeded = await seed(env, 3)
    first, second, third = seeded.refs
    finalizer = BlockedFinalizer()
    progress = RecordingProgress()
    async with open_completer(env, finalizer=finalizer, progress=progress) as completer:
        assert await completer.finish(first, OK)
        _ = await asyncio.wait_for(finalizer.entered.wait(), timeout=5)
        # try_finalize первого commit ещё висит, а следующие транзакции уже идут.
        assert await asyncio.wait_for(completer.finish(second, OK), timeout=5)
        assert await asyncio.wait_for(completer.finish(third, OK), timeout=5)
        assert finalizer.calls == [seeded.batch_id]
        settled = asyncio.create_task(completer.settled())
        await asyncio.sleep(0.05)
        assert not settled.done()  # settled ждёт и работу после commit
        finalizer.release.set()
        await asyncio.wait_for(settled, timeout=5)
    # Два commit, пришедшие во время занятой фоновой работы, — один следующий проход.
    assert finalizer.calls == [seeded.batch_id, seeded.batch_id]
    assert len(progress.calls) == 2
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (3, 0)


async def test_close_waits_for_deferred_after_commit_work(env: Env) -> None:
    seeded = await seed(env, 1)
    finalizer = BlockedFinalizer()
    async with open_completer(env, finalizer=finalizer) as completer:
        assert await completer.finish(seeded.refs[0], OK)
        _ = await asyncio.wait_for(finalizer.entered.wait(), timeout=5)
        closing = asyncio.create_task(completer.close())
        await asyncio.sleep(0.05)
        assert not closing.done()
        finalizer.release.set()
        await asyncio.wait_for(closing, timeout=5)
    assert finalizer.calls == [seeded.batch_id]


async def test_abort_drops_deferred_after_commit_work(env: Env) -> None:
    seeded = await seed(env, 1)
    finalizer = BlockedFinalizer()
    async with open_completer(env, finalizer=finalizer) as completer:
        assert await completer.finish(seeded.refs[0], OK)
        _ = await asyncio.wait_for(finalizer.entered.wait(), timeout=5)
        await asyncio.wait_for(completer.abort(), timeout=5)
        await asyncio.wait_for(completer.settled(), timeout=5)
    assert finalizer.calls == [seeded.batch_id]


async def test_operation_that_waited_out_the_tick_goes_right_after_previous_flush(
    env: Env,
) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    counter = CommitCounter()
    tick = timedelta(seconds=1)
    item = env.tables.item
    async with open_completer(
        env, counter=counter, settings=replace(SETTINGS, tick=tick)
    ) as completer:
        async with env.transaction() as conn:
            # Строка первого Item занята: его транзакция висит на блокировке.
            _ = await conn.execute(select(item.c.id).where(item.c.id == first.id).with_for_update())
            pending = asyncio.create_task(completer.finish(first, OK))
            await asyncio.sleep(tick.total_seconds() + 0.2)
            follower = asyncio.create_task(completer.finish(second, OK))
            await asyncio.sleep(tick.total_seconds() + 0.2)
            released = asyncio.get_running_loop().time()
        assert await asyncio.wait_for(pending, timeout=5)
        assert await asyncio.wait_for(follower, timeout=5)
        # Вторая операция прождала тик, пока шла первая транзакция: без ожидания ещё одного.
        assert asyncio.get_running_loop().time() - released < tick.total_seconds() / 2
    assert counter.commits >= 2
