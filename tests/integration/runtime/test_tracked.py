"""Интеграция tracked-обёртки с PostgreSQL Completer."""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import func, select
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.model.states import ItemState
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import CallbackContext, TaskRuntime, callback, item
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import open_completer, seed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")


class TaskFailedError(RuntimeError):
    """Ошибка пользовательской задачи в тесте."""


@dataclass
class FakeRuntime(Runtime):
    verdict: Verdict = Verdict.FINAL

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        return self.verdict

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)


async def _state(env: Env, item_id: UUID) -> ItemState:
    async with env.connection() as conn:
        value = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == item_id)
        )
    assert value is not None
    return ItemState(value)


async def spawned_task(value: int) -> None:
    """Дочерняя async-задача для типизированного spawn."""
    _ = value
    await asyncio.sleep(0)


async def test_success_hides_marker_sets_context_and_duplicate_is_noop(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    calls: list[tuple[object, object]] = []
    broker = FakeRuntime()
    dispatcher = RecordingDispatcher()
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=broker,
            dispatcher=dispatcher,
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(seconds=20),
        )

        async def task(value: int, **kwargs: object) -> int:
            await asyncio.sleep(0)
            calls.append((item.id(), kwargs.get("_th")))
            item.incr("rows", 2)
            item.spawn(spawned_task, 9)
            item.ok("sent", result={"value": value})
            return value + 1

        wrapped = runtime.wrap(task)
        marker = {"i": str(ref.id), "b": str(ref.batch_id)}
        assert await wrapped(4, _th=marker) == 5
        assert await wrapped(4, _th=marker) is None

    assert calls == [(ref.id, None)]
    assert await _state(env, ref.id) is ItemState.OK
    assert (await env.counters(ref.batch_id)).ok == 1


@pytest.mark.parametrize(
    ("verdict", "expected_state"),
    [(Verdict.RETRY, ItemState.ACTIVE), (Verdict.FINAL, ItemState.ERROR)],
)
async def test_exception_releases_or_finishes_exhausted(
    env: Env, verdict: Verdict, expected_state: ItemState
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=FakeRuntime(verdict),
            dispatcher=RecordingDispatcher(),
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(seconds=20),
        )

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            item.incr("attempted")
            raise TaskFailedError

        with pytest.raises(TaskFailedError):
            await runtime.wrap(task)(_th={"i": ref.id, "b": ref.batch_id})

    assert await _state(env, ref.id) is expected_state


async def test_callback_context_is_scoped_and_marker_is_hidden(env: Env) -> None:
    seeded = await seed(env, 0)
    seen_contexts: list[CallbackContext | None] = []
    seen_markers: list[object] = []
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=FakeRuntime(),
            dispatcher=RecordingDispatcher(),
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(seconds=20),
        )

        async def task(**kwargs: object) -> None:
            await asyncio.sleep(0)
            seen_contexts.append(callback.current())
            seen_markers.append(kwargs.get("_th"))

        callback_id = seeded.batch_id
        # Ключ "s" ставит адаптер flexiq (всегда None); маркер с ним принимается и
        # игнорируется — сводки в контексте колбэка нет (ARCHITECTURE §11.2).
        await runtime.wrap(task)(_th={"c": callback_id, "b": seeded.batch_id, "s": {"ok": 1}})

    assert seen_contexts == [CallbackContext(callback_id=callback_id, batch_id=seeded.batch_id)]
    assert seen_markers == [None]
    assert callback.current() is None


async def test_heartbeat_flushes_progress_and_observes_cancel_request(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    observed: list[bool] = []
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=FakeRuntime(),
            dispatcher=RecordingDispatcher(),
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(milliseconds=5),
        )

        async def task(**_kwargs: object) -> None:
            item.progress(3, 10)
            async with env.transaction() as conn:
                _ = await conn.execute(
                    env.tables.batch.update()
                    .where(env.tables.batch.c.id == ref.batch_id)
                    .values(cancel_requested_at=func.now())
                )
            for _ in range(100):
                if item.cancelled():
                    break
                await asyncio.sleep(0.01)
            observed.append(item.cancelled())

        await runtime.wrap(task)(_th={"i": ref.id, "b": ref.batch_id})

    assert observed == [True]


async def test_cancelling_wrapper_releases_lease(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    entered = asyncio.Event()
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=FakeRuntime(),
            dispatcher=RecordingDispatcher(),
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(seconds=20),
        )

        async def task(**_kwargs: object) -> None:
            entered.set()
            await asyncio.Event().wait()

        async def invoke() -> None:
            await runtime.wrap(task)(_th={"i": ref.id, "b": ref.batch_id})

        running: asyncio.Task[None] = asyncio.create_task(invoke())
        await entered.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

    assert await _state(env, ref.id) is ItemState.ACTIVE
    async with env.connection() as conn:
        attempt = await conn.scalar(
            select(env.tables.item.c.attempt).where(env.tables.item.c.id == ref.id)
        )
        lease = await conn.scalar(
            select(env.tables.lease.c.item_id).where(env.tables.lease.c.item_id == ref.id)
        )
    assert attempt == 1
    assert lease is None


@pytest.mark.parametrize("commit", [False, True])
async def test_complete_in_commit_or_rollback_is_visible_to_middleware(
    env: Env, *, commit: bool
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=FakeRuntime(),
            dispatcher=RecordingDispatcher(),
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(seconds=20),
        )

        async def task(**_kwargs: object) -> None:
            if commit:
                async with env.transaction() as conn:
                    item.ok("atomic")
                    await item.complete_in(conn)
            else:
                async with env.connection() as conn:
                    transaction = await conn.begin()
                    item.ok("rolled-back")
                    await item.complete_in(conn)
                    await transaction.rollback()

        await runtime.wrap(task)(_th={"i": ref.id, "b": ref.batch_id})

    assert await _state(env, ref.id) is ItemState.OK
