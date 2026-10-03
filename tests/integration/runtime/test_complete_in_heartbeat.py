"""Путь B с настоящим claim и heartbeat; удержанные Items Completer (Fix-20).

A-DB-06/07 вызывают ``complete_in`` без claim, поэтому не видят ни строки
``th_lease``, которую продлевает heartbeat, ни словаря ``Completer.held``.
Здесь задача идёт через обёртку ``tracked``: claim, heartbeat с малым
интервалом, ``item.complete_in`` в транзакции пользователя.
"""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

from tallyho.engine.completer import CompleterSettings, FinishResult
from tallyho.engine.spawn import TreeCache
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.states import ItemState, ResultClass
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tests.helpers.probe import create_probe
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    SETTINGS,
    Finalized,
    MovableClock,
    lease_row,
    open_completer,
    schema_engine,
    seed,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from uuid import UUID

    from sqlalchemy import Table

    from tallyho.engine.completer import Completer, ItemRef
    from tests.helpers.probe import ProbeColumns
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

LATER = NOW + SETTINGS.lease_ttl + timedelta(seconds=1)
"""Момент, когда lease, взятый в ``NOW``, уже истёк."""
OTHER = CompleterSettings(worker_id="worker-2", slot=COMPLETER_SLOT + 1)
BEAT = timedelta(milliseconds=40)
"""Малый ``heartbeat_every``: за секунду задача успевает продлить lease десятки раз."""
HOLD = 3.0
"""Сколько секунд транзакция пользователя держится открытой после ``complete_in``."""


@dataclass
class Broker(Runtime):
    """Брокер теста: ошибка задачи — финальная."""

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        return Verdict.FINAL

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)


def _runtime(completer: Completer, *, beat: timedelta = BEAT) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=beat,
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


@pytest.fixture
async def probe(env: Env) -> Table[ProbeColumns]:
    """Доменная таблица пользователя."""
    return await create_probe(env.engine, env.schema)


@pytest.fixture
async def thief(env: Env) -> AsyncGenerator[Completer]:
    """Completer другого воркера, для которого lease из ``NOW`` уже истёк."""
    async with open_completer(env, clock=MovableClock(LATER), settings=OTHER) as completer:
        yield completer


async def _state(env: Env, item_id: UUID) -> ItemState:
    async with env.connection() as conn:
        value = await conn.scalar(
            env.tables.item.select()
            .with_only_columns(env.tables.item.c.state)
            .where(env.tables.item.c.id == item_id)
        )
    assert value is not None
    return ItemState(value)


# --- (1) Completer.held -----------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.timeout(600)
async def test_held_is_empty_after_many_items_completed_in_user_transactions(env: Env) -> None:
    total = 10_000
    seeded = await seed(env, total)
    parallel = 32
    gate = asyncio.Semaphore(parallel)
    users = create_async_engine(
        env.engine.url, pool_size=parallel, max_overflow=0
    ).execution_options(schema_translate_map={None: env.schema})
    async with open_completer(env) as completer:
        runtime = _runtime(completer, beat=timedelta(seconds=30))

        async def task(**_kwargs: object) -> None:
            async with users.begin() as conn:
                item.ok()
                await item.complete_in(conn)

        async def run(ref: ItemRef) -> None:
            async with gate:
                await runtime.wrap(task)(_th=_marker(ref))

        try:
            _ = await asyncio.gather(*(run(ref) for ref in seeded.refs))
        finally:
            await users.dispose()
        await completer.settled()
        assert completer.held == frozenset()

    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (total, 0)
    assert await env.count(env.tables.lease) == 0


async def test_held_forgets_item_lost_in_user_transaction(env: Env, thief: Completer) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    held: list[bool] = []
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            held.append(ref.id in completer.held)
            held.append((await thief.claim(ref)).run)
            async with env.transaction() as conn:
                await item.complete_in(conn)

        # LeaseLostError — не ошибка задачи: обёртка завершает попытку тихо.
        await _runtime(completer, beat=timedelta(seconds=30)).wrap(task)(_th=_marker(ref))
        assert held == [True, True]
        assert completer.held == frozenset()


async def test_held_forgets_item_whose_path_a_attempt_lost_lease(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> str:
            sweeper = Sweeper(
                tables=env.tables,
                engine=schema_engine(env),
                clock=MovableClock(LATER),
                finalizer=Finalized(),
                settings=SweeperSettings(finalize_grace=timedelta(0)),
            )
            assert await sweeper.expire_leases() == 1
            return "done"

        assert (
            await _runtime(completer, beat=timedelta(seconds=30)).wrap(task)(_th=_marker(ref))
            == "done"
        )
        assert completer.held == frozenset()
    assert await _state(env, ref.id) is ItemState.ERROR


# --- (3) heartbeat устаревшей попытки -------------------------------------------------------


async def test_stale_attempt_heartbeat_does_not_extend_lease_of_new_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    seen: list[tuple[object, ...]] = []
    async with open_completer(env, clock=clock) as completer:

        async def task(**_kwargs: object) -> None:
            # Lease попытки 0 истёк, тот же процесс получает Item заново.
            clock.value = LATER
            again = await completer.claim(ref)
            taken = await lease_row(env, ref.id)
            clock.value = LATER + timedelta(seconds=10)
            # Несколько интервалов heartbeat устаревшей попытки 0.
            await asyncio.sleep(BEAT.total_seconds() * 6)
            lease = await lease_row(env, ref.id)
            seen.append((again.run, again.attempt))
            seen.append((None if taken is None else taken["lease_until"],))
            seen.append((None if lease is None else (lease["attempt"], lease["lease_until"]),))
            seen.append((ref.id in completer.held,))

        # Ассерты — снаружи: исключение задачи обёртка обработала бы как её ошибку.
        await _runtime(completer).wrap(task)(_th=_marker(ref))
        assert seen[0] == (True, 1)
        assert seen[2] == ((1, seen[1][0]),), "heartbeat попытки 0 продлил lease попытки 1"
        assert seen[3] == (True,)

        # Попытка 0 не завершила Item, который теперь у попытки 1.
        assert await _state(env, ref.id) is ItemState.ACTIVE
        assert ref.id in completer.held
        value = FinishResult(result_class=ResultClass.OK)
        assert await completer.finish(ref, value, attempt=1)
        assert completer.held == frozenset()


async def test_heartbeat_with_attempt_checks_owner(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        assert (await completer.claim(ref)).attempt == 0
        assert await completer.heartbeat(ref, attempt=0, progress_done=1)
        clock.value = LATER
        assert (await completer.claim(ref)).attempt == 1
        assert not await completer.heartbeat(ref, attempt=0, progress_done=2)
        assert ref.id in completer.held
        assert await completer.heartbeat(ref, attempt=1, progress_done=3)
        # Без attempt — прежняя проверка только по worker_id.
        assert await completer.heartbeat(ref)
        lease = await lease_row(env, ref.id)
        assert lease is not None
        assert (lease["attempt"], lease["progress_done"]) == (1, 3)
