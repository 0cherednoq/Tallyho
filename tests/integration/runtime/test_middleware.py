"""Цепочка ``ItemMiddleware`` вокруг вызова задачи (RM8.7, ARCHITECTURE §11.2)."""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.model.errors import ConfigurationError
from tallyho.model.outcomes import Defer, Ok
from tallyho.model.states import ItemState
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import open_completer, seed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from uuid import UUID

    from sqlalchemy import RowMapping

    from tallyho.engine.completer import Completer, ItemRef
    from tallyho.runtime import WorkItem
    from tallyho.runtime.middleware import ItemMiddleware
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")


class TaskFailedError(RuntimeError):
    """Ошибка пользовательской задачи в тесте."""


@dataclass
class Broker(Runtime):
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


@dataclass
class Recorder:
    """Middleware, записывающая порядок входа и выхода."""

    name: str
    log: list[str]
    seen: list[UUID] = field(default_factory=list["UUID"])

    async def __call__(self, work: WorkItem, call_next: Callable[[], Awaitable[object]]) -> object:
        self.log.append(f"{self.name}:до")
        self.seen.append(work.id)
        # Единица работы доступна и как зависимость, и через фасад.
        assert item.require() is work
        try:
            result = await call_next()
        except TaskFailedError:
            self.log.append(f"{self.name}:исключение")
            raise
        self.log.append(f"{self.name}:после")
        return result


def _runtime(completer: Completer, middleware: Sequence[ItemMiddleware] = ()) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=timedelta(seconds=20),
        middleware=middleware,
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


async def _item(env: Env, item_id: UUID) -> RowMapping:
    table = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(table).where(table.c.id == item_id))).mappings().one()


async def test_chain_order_is_outside_in(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    log: list[str] = []
    outer, inner = Recorder("внешняя", log), Recorder("внутренняя", log)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            log.append("задача")
            return Ok("chunk")

        assert await _runtime(completer, [outer, inner]).wrap(task)(_th=_marker(ref)) == Ok("chunk")

    assert log == [
        "внешняя:до",
        "внутренняя:до",
        "задача",
        "внутренняя:после",
        "внешняя:после",
    ]
    # Итог значением прошёл цепочку насквозь и записан обычным путём.
    assert ItemState((await _item(env, ref.id))["state"]) is ItemState.OK
    assert (outer.seen, inner.seen) == ([ref.id], [ref.id])


async def test_returned_defer_passes_through_chain(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    log: list[str] = []
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Defer:
            await asyncio.sleep(0)
            return Defer(reason="no_accounts", delay=timedelta(minutes=1))

        _ = await _runtime(completer, [Recorder("одна", log)]).wrap(task)(_th=_marker(ref))

    assert log == ["одна:до", "одна:после"]
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["defers"]) == (ItemState.ACTIVE, 1)


async def test_exception_passes_through_chain(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    log: list[str] = []
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise TaskFailedError

        with pytest.raises(TaskFailedError):
            await _runtime(completer, [Recorder("одна", log)]).wrap(task)(_th=_marker(ref))

    assert log == ["одна:до", "одна:исключение"]
    # Итог записала обёртка, а не middleware: вердикт брокера FINAL.
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.ERROR, "exhausted")


async def test_one_instance_serves_every_attempt(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    log: list[str] = []
    recorder = Recorder("одна", log)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            return Ok("chunk")

        runtime = _runtime(completer, [recorder])
        _ = await runtime.wrap(task)(_th=_marker(first))
        _ = await runtime.wrap(task)(_th=_marker(second))

    assert recorder.seen == [first.id, second.id]
    assert log.count("одна:до") == 2


async def test_chain_is_not_used_for_skipped_claim(env: Env) -> None:
    # Дубль доставки: задача не вызывается, значит и оборачивать нечего.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    log: list[str] = []
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            return Ok("chunk")

        runtime = _runtime(completer, [Recorder("одна", log)])
        _ = await runtime.wrap(task)(_th=_marker(ref))
        assert await runtime.wrap(task)(_th=_marker(ref)) is None

    assert log.count("одна:до") == 1


async def test_empty_chain_keeps_plain_path(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            return Ok("chunk")

        assert await _runtime(completer).wrap(task)(_th=_marker(ref)) == Ok("chunk")

    assert ItemState((await _item(env, ref.id))["state"]) is ItemState.OK


async def test_non_callable_middleware_is_rejected_at_setup(env: Env) -> None:
    async with open_completer(env) as completer:
        with pytest.raises(ConfigurationError, match="middleware"):
            _ = TaskRuntime(
                completer=completer,
                broker=Broker(),
                dispatcher=RecordingDispatcher(),
                tree_cache=TreeCache(),
                heartbeat_every=timedelta(seconds=20),
                middleware=[object()],  # type: ignore[list-item]  # pyright: ignore[reportArgumentType]  # проверка валидации
            )
