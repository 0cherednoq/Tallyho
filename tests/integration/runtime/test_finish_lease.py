"""Путь A и попытка, потерявшая lease (ARCHITECTURE UC-03, UC-04, D-053).

Пока задача работала, её lease истёк и был перехвачен: claim другого воркера
или этого же процесса взял Item с ``attempt + 1``. Итог устаревшей попытки
(``ok``, ``error("exhausted")``, ``cancelled``) и её ``release`` не должны
задеть Item и lease нового исполнителя, а обёртка завершает такую попытку
тихо — как при ``LeaseLostError`` пути B.
"""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.completer import CompleterSettings
from tallyho.engine.spawn import TreeCache
from tallyho.model.states import ItemState
from tallyho.protocols.broker import CancellationClassifier, DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    SETTINGS,
    MovableClock,
    lease_row,
    open_completer,
    seed,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from uuid import UUID

    from tallyho.engine.completer import Completer, ItemRef
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

LATER = NOW + SETTINGS.lease_ttl + timedelta(seconds=1)
"""Момент, когда lease, взятый в ``NOW``, уже истёк."""
OTHER = CompleterSettings(worker_id="worker-2", slot=COMPLETER_SLOT + 1)
THIEVES = {"other": OTHER, "same": SETTINGS}
"""Кто перехватывает lease: другой воркер или этот же процесс (тот же ``worker_id``)."""


class TaskFailedError(RuntimeError):
    """Ошибка пользовательской задачи в тесте."""


@dataclass
class Broker(Runtime, CancellationClassifier):
    """Брокер теста: заданный вердикт и признак отмены, считает вопросы."""

    verdict: Verdict = Verdict.FINAL
    cancels: bool = False
    asked: int = 0

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        self.asked += 1
        return self.verdict

    @override
    def is_cancelled(self, exc: BaseException) -> bool:
        return self.cancels

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)


def _runtime(completer: Completer, broker: Broker | None = None) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=broker or Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=timedelta(seconds=20),
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


@pytest.fixture(params=sorted(THIEVES))
async def thief(env: Env, request: pytest.FixtureRequest) -> AsyncGenerator[Completer]:
    """Completer, для которого lease из ``NOW`` уже истёк: другой воркер или тот же."""
    settings = THIEVES[str(request.param)]
    async with open_completer(env, clock=MovableClock(LATER), settings=settings) as completer:
        yield completer


async def _item(env: Env, item_id: UUID) -> tuple[ItemState, str | None, int]:
    """``(state, label, attempt)`` Item."""
    table = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(table.c.state, table.c.label, table.c.attempt).where(table.c.id == item_id)
            )
        ).one()
    return ItemState(row[0]), row[1], int(row[2])


async def _owner(env: Env, item_id: UUID) -> tuple[str, int] | None:
    """``(worker_id, attempt)`` lease Item."""
    lease = await lease_row(env, item_id)
    return None if lease is None else (lease["worker_id"], lease["attempt"])


async def _assert_untouched(env: Env, ref: ItemRef, thief: Completer) -> None:
    # Item и lease остались у нового исполнителя, счётчики не тронуты.
    assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)
    assert await _owner(env, ref.id) == (thief.settings.worker_id, 1)
    counters = await env.counters(ref.batch_id)
    assert (counters.ok, counters.error, counters.cancelled, counters.pending) == (0, 0, 0, 1)
    assert await env.count(env.tables.item_mark) == 0
    assert await env.count(env.tables.outbox) == 0


async def test_stale_attempt_result_does_not_finish_item(env: Env, thief: Completer) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> str:
            stolen = await thief.claim(ref)
            assert (stolen.run, stolen.attempt) == (True, 1)
            item.ok("stale")
            return "done"

        # Брокер получает результат как обычно: ретрай бесполезен, итог не записан.
        assert await _runtime(completer).wrap(task)(_th=_marker(ref)) == "done"

    await _assert_untouched(env, ref, thief)


@pytest.mark.parametrize("mode", ["final", "retry", "cancelled"])
async def test_stale_attempt_failure_ends_quietly(
    env: Env, thief: Completer, *, caplog: pytest.LogCaptureFixture, mode: str
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker(
        Verdict.RETRY if mode == "retry" else Verdict.FINAL, cancels=mode == "cancelled"
    )
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> object:
            assert (await thief.claim(ref)).run
            raise TaskFailedError

        with caplog.at_level("INFO", logger="tallyho.runtime.tracked"):
            # Ни error/cancelled, ни release, ни исключение брокеру: ретрай и DLQ
            # задели бы Item, который выполняет другой исполнитель.
            assert await _runtime(completer, broker).wrap(task)(_th=_marker(ref)) is None

    assert "потеряла lease" in caplog.text
    await _assert_untouched(env, ref, thief)


async def test_cancelled_stale_attempt_keeps_new_lease(env: Env, thief: Completer) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    waiting = asyncio.Event()
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            assert (await thief.claim(ref)).run
            waiting.set()
            await asyncio.Event().wait()

        async def invoke() -> None:
            await _runtime(completer).wrap(task)(_th=_marker(ref))

        running: asyncio.Task[None] = asyncio.create_task(invoke())
        await waiting.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

    await _assert_untouched(env, ref, thief)


@pytest.mark.parametrize("mode", ["ok", "final", "retry"])
async def test_expired_but_not_taken_lease_still_completes(env: Env, *, mode: str) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    broker = Broker(Verdict.RETRY if mode == "retry" else Verdict.FINAL)
    async with open_completer(env, clock=clock) as completer:

        async def task(**_kwargs: object) -> str:
            # Lease истёк, но никто его не перехватил: попытка всё ещё владелец.
            clock.value = LATER
            await asyncio.sleep(0)
            if mode != "ok":
                raise TaskFailedError
            return "done"

        wrapped = _runtime(completer, broker).wrap(task)
        if mode == "ok":
            assert await wrapped(_th=_marker(ref)) == "done"
        else:
            with pytest.raises(TaskFailedError):
                await wrapped(_th=_marker(ref))

    expected = {
        "ok": (ItemState.OK, "ok", 0),
        "final": (ItemState.ERROR, "exhausted", 0),
        "retry": (ItemState.ACTIVE, None, 1),
    }
    assert await _item(env, ref.id) == expected[mode]
    assert await lease_row(env, ref.id) is None
