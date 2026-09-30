"""Хелперы тестов relay: ручные часы, записывающий Dispatcher, сборка Relay над схемой теста."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, ParamSpec

from typing_extensions import override

from tallyho.engine.relay import Relay, RelaySettings
from tallyho.protocols.broker import Dispatcher
from tallyho.protocols.clock import SystemClock

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime, timedelta
    from uuid import UUID

    from tallyho.engine.producer import Producer
    from tallyho.protocols.broker import Message
    from tallyho.storage.tables import Tables
    from tests.integration.engine.conftest import Env

__all__ = ["DispatchFailedError", "ManualClock", "RecordingDispatcher", "RelayEnv", "relay_env"]

P = ParamSpec("P")


@dataclass
class ManualClock(SystemClock):
    """Часы, которые двигает тест: «сейчас» в SQL — ``current``."""

    current: datetime

    @override
    def now(self) -> datetime | None:
        return self.current

    def advance(self, delta: timedelta) -> None:
        """Сдвинуть «сейчас» вперёд."""
        self.current += delta


class DispatchFailedError(RuntimeError):
    """Брокер «недоступен» (исключение адаптера, не tallyho)."""


@dataclass(eq=False)
class RecordingDispatcher(Dispatcher):
    """Фейковый брокер: складывает пачки в список; умеет падать и задерживаться."""

    batches: list[list[Message]] = field(default_factory=list[list["Message"]])
    fail_times: int = 0
    """Сколько следующих вызовов ``dispatch`` упадут."""
    fail_tasks: frozenset[str] = frozenset()
    """Задачи, пачки которых всегда падают."""
    delay: float = 0.0
    dispatched: asyncio.Event = field(default_factory=asyncio.Event)

    @override
    def task_name(self, fn: Callable[P, object]) -> str:
        return "task"

    @override
    async def dispatch(self, messages: Sequence[Message]) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise DispatchFailedError
        if messages and messages[0].task_name in self.fail_tasks:
            raise DispatchFailedError
        self.batches.append(list(messages))
        self.dispatched.set()

    @property
    def messages(self) -> list[Message]:
        """Все принятые сообщения по порядку."""
        return [message for batch in self.batches for message in batch]

    @property
    def ids(self) -> list[UUID]:
        """id принятых сообщений по порядку."""
        return [message.id for message in self.messages]


@dataclass(frozen=True, slots=True)
class RelayEnv:
    """Relay, продюсер и часы над установленной схемой теста."""

    env: Env
    clock: ManualClock
    dispatcher: RecordingDispatcher
    relay: Relay
    producer: Producer

    @property
    def tables(self) -> Tables:
        """Таблицы установки."""
        return self.env.tables

    def another_relay(self, dispatcher: RecordingDispatcher | None = None) -> Relay:
        """Второй relay над той же схемой (другой «процесс»)."""
        return replace(self.relay, dispatcher=dispatcher or self.dispatcher)


def relay_env(env: Env, now: datetime, settings: RelaySettings | None = None) -> RelayEnv:
    """Relay с ручными часами и записывающим брокером; продюсер на тех же часах."""
    clock = ManualClock(now)
    dispatcher = RecordingDispatcher()
    engine = env.engine.execution_options(schema_translate_map={None: env.schema})
    relay = Relay(
        engine=engine,
        tables=env.tables,
        clock=clock,
        dispatcher=dispatcher,
        settings=settings or RelaySettings(),
    )
    return RelayEnv(
        env=env,
        clock=clock,
        dispatcher=dispatcher,
        relay=relay,
        producer=replace(env.producer, clock=clock),
    )
