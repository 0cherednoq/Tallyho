"""Интеграция с Dishka: REQUEST-scope на попытку, WorkItem зависимостью (RM8.8)."""

from __future__ import annotations

import asyncio
import functools
import subprocess  # ruff: ignore[suspicious-subprocess-import]  # тест запускает тот же интерпретатор
import sys
from collections.abc import AsyncIterator  # ruff: ignore[typing-only-standard-library-import]  # dishka читает аннотации в рантайме
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar, final

import pytest
from dishka import (  # ruff: ignore[typing-only-third-party-import]  # dishka читает аннотации в рантайме
    BaseScope,
    Provider,
    Scope,
    make_async_container,
    provide,  # pyright: ignore[reportUnknownVariableType]  # decorator без аннотаций в dishka
)
from dishka.exceptions import NoContextValueError
from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.integrations.dishka import (  # ruff: ignore[typing-only-first-party-import]  # dishka читает аннотации в рантайме
    DishkaItemScope,
    FromDishka,
    TallyhoProvider,
    current_container,
    inject,
)
from tallyho.model.errors import ConfigurationError
from tallyho.model.outcomes import Ok
from tallyho.model.states import ItemState
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, WorkItem, item
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import open_completer, seed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from dishka import AsyncContainer

    from tallyho.engine.completer import Completer, ItemRef
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
class ChunkProgress:
    """Инфраструктурный адаптер: единица работы приходит конструктором."""

    work: WorkItem

    def position(self) -> int:
        return self.work.checkpoint() or 0


@dataclass
class Lifecycle:
    """Счётчик открытых и закрытых REQUEST-scope."""

    opened: int = 0
    closed: int = 0


@final
class AppProvider(Provider):
    """Зависимости приложения: прогресс пачки поверх WorkItem."""

    scope: BaseScope | None = Scope.REQUEST

    def __init__(self, lifecycle: Lifecycle) -> None:
        super().__init__()
        self._lifecycle: Lifecycle = lifecycle

    @provide
    async def progress(self, work: WorkItem) -> AsyncIterator[ChunkProgress]:
        self._lifecycle.opened += 1
        try:
            yield ChunkProgress(work)
        finally:
            self._lifecycle.closed += 1


def _runtime(completer: Completer, container: AsyncContainer) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=timedelta(seconds=20),
        middleware=[DishkaItemScope(container)],
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


async def test_dependency_gets_the_current_work_item(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    lifecycle = Lifecycle()
    container = make_async_container(AppProvider(lifecycle), TallyhoProvider())
    seen: list[tuple[UUID, int]] = []

    @inject
    async def task(progress: FromDishka[ChunkProgress], **_kwargs: object) -> Ok:
        await asyncio.sleep(0)
        seen.append((progress.work.id, progress.position()))
        # Та же единица работы, что у фасада: один объект на попытку.
        assert progress.work is item.require()
        return Ok("chunk")

    async with open_completer(env) as completer:
        assert await _runtime(completer, container).wrap(task)(_th=_marker(ref)) == Ok("chunk")
    await container.close()

    assert seen == [(ref.id, 0)]
    assert (lifecycle.opened, lifecycle.closed) == (1, 1)
    async with env.connection() as conn:
        state = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == ref.id)
        )
    assert ItemState(state or 0) is ItemState.OK


async def test_scope_closes_on_exception(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    lifecycle = Lifecycle()
    container = make_async_container(AppProvider(lifecycle), TallyhoProvider())

    @inject
    async def task(progress: FromDishka[ChunkProgress], **_kwargs: object) -> None:
        await asyncio.sleep(0)
        _ = progress.position()
        raise TaskFailedError

    async with open_completer(env) as completer:
        with pytest.raises(TaskFailedError):
            await _runtime(completer, container).wrap(task)(_th=_marker(ref))
    await container.close()

    assert (lifecycle.opened, lifecycle.closed) == (1, 1)


async def test_scope_closes_on_cancellation(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    lifecycle = Lifecycle()
    container = make_async_container(AppProvider(lifecycle), TallyhoProvider())

    @inject
    async def task(progress: FromDishka[ChunkProgress], **_kwargs: object) -> None:
        await asyncio.sleep(0)
        _ = progress.position()
        raise asyncio.CancelledError

    async with open_completer(env) as completer:
        with pytest.raises(asyncio.CancelledError):
            await _runtime(completer, container).wrap(task)(_th=_marker(ref))
    await container.close()

    assert (lifecycle.opened, lifecycle.closed) == (1, 1)


async def test_each_attempt_gets_its_own_scope(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    lifecycle = Lifecycle()
    container = make_async_container(AppProvider(lifecycle), TallyhoProvider())
    seen: list[UUID] = []

    @inject
    async def task(progress: FromDishka[ChunkProgress], **_kwargs: object) -> Ok:
        await asyncio.sleep(0)
        seen.append(progress.work.id)
        return Ok("chunk")

    async with open_completer(env) as completer:
        runtime = _runtime(completer, container)
        _ = await runtime.wrap(task)(_th=_marker(first))
        _ = await runtime.wrap(task)(_th=_marker(second))
    await container.close()

    assert seen == [first.id, second.id]
    assert (lifecycle.opened, lifecycle.closed) == (2, 2)


async def test_work_item_is_unavailable_outside_a_task() -> None:
    # A-WI-08: понятная ошибка вместо фиктивного объекта и тихого no-op.
    container = make_async_container(TallyhoProvider())
    with pytest.raises(ConfigurationError, match="scope единицы работы"):
        _ = current_container()
    async with container(scope=Scope.REQUEST) as request:
        with pytest.raises(NoContextValueError):
            _ = await request.get(WorkItem)
    await container.close()


def test_importing_tallyho_does_not_import_dishka() -> None:
    # Extra остаётся необязательным: обычный импорт библиотеки его не тянет.
    code = (
        "import sys; import tallyho; "
        "assert 'dishka' not in sys.modules, sorted(n for n in sys.modules if 'dishka' in n)"
    )
    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]  # статическая команда интерпретатора
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
