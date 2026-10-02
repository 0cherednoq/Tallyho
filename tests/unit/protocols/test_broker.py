"""Контракт адаптера: Message, Verdict, DeadLetters и проверка протоколов на фейках."""

from __future__ import annotations

import asyncio
import dataclasses
import functools
from typing import TYPE_CHECKING, ParamSpec, TypeVar
from uuid import UUID

import pytest

from tallyho.model.states import OutboxKind
from tallyho.protocols.broker import (
    DeadLetters,
    Dispatcher,
    Message,
    RelayPolicy,
    Runtime,
    Verdict,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

P = ParamSpec("P")
R = TypeVar("R")

ITEM = UUID("01920000-0000-7000-8000-000000000001")
BATCH = UUID("01920000-0000-7000-8000-000000000002")


class _RecordingDispatcher:
    def __init__(self) -> None:
        self.sent: list[Message] = []

    def task_name(self, fn: Callable[P, object]) -> str:
        return f"{fn.__module__}.{fn.__qualname__}"

    async def dispatch(self, messages: Sequence[Message]) -> None:
        self.sent.extend(messages)


class _CountingRuntime:
    def __init__(self) -> None:
        self.calls: int = 0

    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            self.calls += 1
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    def retry_verdict(self, exc: BaseException) -> Verdict:
        return Verdict.FINAL if isinstance(exc, ValueError) else Verdict.RETRY

    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((ITEM,), cursor=since or "c1")


async def _double(value: int) -> int:
    await asyncio.sleep(0)
    return value * 2


def test_verdict_values_are_stable() -> None:
    assert {verdict.name: verdict.value for verdict in Verdict} == {
        "RETRY": "retry",
        "FINAL": "final",
    }


def test_message_is_immutable_with_empty_options() -> None:
    message = Message(
        id=ITEM, batch_id=BATCH, kind=OutboxKind.ITEM, task_name="app.send", payload=b"{}"
    )
    assert message.options == {}
    attribute = "task_name"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(message, attribute, "other")


async def test_dispatcher_fake_satisfies_protocol() -> None:
    dispatcher: Dispatcher = _RecordingDispatcher()
    assert isinstance(dispatcher, Dispatcher)
    message = Message(
        id=ITEM,
        batch_id=BATCH,
        kind=OutboxKind.CALLBACK,
        task_name=dispatcher.task_name(_double),
        payload=b"",
        options={"priority": 5},
    )
    await dispatcher.dispatch([message])
    assert isinstance(dispatcher, _RecordingDispatcher)
    assert dispatcher.sent == [message]
    assert message.task_name.endswith("._double")


async def test_runtime_fake_satisfies_protocol() -> None:
    runtime: Runtime = _CountingRuntime()
    assert isinstance(runtime, Runtime)
    wrapped = runtime.wrap(_double)
    assert await wrapped(21) == 42
    assert wrapped.__name__ == "_double"
    assert runtime.retry_verdict(ValueError()) is Verdict.FINAL
    assert runtime.retry_verdict(OSError()) is Verdict.RETRY
    assert await runtime.reconcile_dead(None) == DeadLetters((ITEM,), "c1")


def test_protocols_reject_incomplete_fakes() -> None:
    assert not isinstance(_RecordingDispatcher(), Runtime)
    assert not isinstance(_CountingRuntime(), Dispatcher)
    # Обычный адаптер политику relay не объявляет: фоновый цикл стартует сам.
    assert not isinstance(_RecordingDispatcher(), RelayPolicy)
