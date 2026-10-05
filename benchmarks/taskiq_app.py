"""Задача ``print(f"task {i}")`` для taskiq (T11.6): брокер, middleware отметки выполнения.

Один и тот же код используют вариант ``taskiq-memory`` (``InMemoryBroker`` в процессе харнесса)
и процессы-воркеры варианта ``taskiq-redis`` (``ListQueueBroker`` поверх Redis). Тело задачи —
только ``print``, как у flexiq и tallyho; момент выполнения записывает middleware после возврата
из функции: в память процесса (InMemory) или в список Redis ``bench:done`` (Redis). Для Redis это
единственный дополнительный round-trip на задачу: flexiq пишет итог джобы в PostgreSQL сам, а
taskiq без result backend не хранит ничего.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Final, Protocol, cast

from redis.asyncio import Redis
from taskiq import TaskiqMiddleware
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from taskiq import AsyncBroker, TaskiqMessage, TaskiqResult

__all__ = [
    "DONE_KEY",
    "TASK_NAME",
    "DoneSink",
    "MemorySink",
    "RedisClient",
    "RedisSink",
    "TaskiqNoop",
    "connect",
    "register",
]

TASK_NAME: Final = "bench.noop"
DONE_KEY: Final = "bench:done"


class RedisClient(Protocol):
    """Команды Redis, нужные бенчмарку (``redis.asyncio.Redis`` типизирован не полностью)."""

    async def ping(self) -> bool:
        """``PING``."""
        ...

    async def flushdb(self) -> bool:
        """``FLUSHDB``."""
        ...

    async def rpush(self, name: str, value: str, /) -> int:
        """``RPUSH``."""
        ...

    async def llen(self, name: str, /) -> int:
        """``LLEN``."""
        ...

    async def lrange(self, name: str, start: int, end: int, /) -> list[bytes]:
        """``LRANGE``."""
        ...

    async def info(self, section: str, /) -> dict[str, object]:
        """``INFO``."""
        ...

    async def aclose(self) -> None:
        """Закрыть соединения."""
        ...


def connect(url: str) -> RedisClient:
    """Клиент Redis по URL.

    Returns:
        Клиент.
    """
    return _as_client(Redis.from_url(url))  # pyright: ignore[reportUnknownMemberType]  # **kwargs redis-py без аннотации


def _as_client(client: object) -> RedisClient:
    return cast("RedisClient", client)


class DoneSink(Protocol):
    """Куда middleware записывает ``(i, момент выполнения)``."""

    async def done(self, index: int, at: float) -> None:
        """Записать выполнение задачи ``index``."""
        ...


class MemorySink:
    """Выполнения в памяти процесса (вариант ``taskiq-memory``)."""

    def __init__(self) -> None:
        """Пустой журнал выполнений."""
        self.events: list[tuple[int, float]] = []
        self._target: int | None = None
        self._reached: asyncio.Event = asyncio.Event()

    async def done(self, index: int, at: float) -> None:
        """Записать выполнение."""
        self.events.append((index, at))
        if self._target is not None and len(self.events) >= self._target:
            self._reached.set()

    async def reached(self, total: int) -> None:
        """Дождаться, пока выполнений станет не меньше ``total``."""
        self._reached.clear()
        self._target = total
        if len(self.events) >= total:
            return
        _ = await self._reached.wait()


class RedisSink:
    """Выполнения в списке Redis ``bench:done`` (вариант ``taskiq-redis``)."""

    def __init__(self, client: RedisClient) -> None:
        """Писать в список через ``client``."""
        self._client: RedisClient = client

    async def done(self, index: int, at: float) -> None:
        """Дописать ``"i момент"`` в список."""
        _ = await self._client.rpush(DONE_KEY, f"{index} {at!r}")


class _DoneMiddleware(TaskiqMiddleware):
    def __init__(self, sink: DoneSink) -> None:
        super().__init__()
        self._sink: DoneSink = sink

    @override
    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[object],
    ) -> None:
        _ = result
        args = cast("list[object]", message.args)
        await self._sink.done(int(cast("int", args[0])), time.time())


class TaskiqNoop(Protocol):
    """Зарегистрированная задача: ``await noop.kiq(i)``."""

    async def kiq(self, i: int, /) -> object:
        """Поставить задачу."""
        ...


def register(broker: AsyncBroker, sink: DoneSink) -> TaskiqNoop:
    """Зарегистрировать ``bench.noop`` и middleware отметки выполнения.

    Returns:
        Задача для постановки.
    """

    async def noop(i: int) -> None:  # ruff: ignore[unused-async]  # задача taskiq — корутина, как у flexiq
        print(f"task {i}")  # ruff: ignore[print]  # T11.6: вывод задачи идёт в лог воркера

    _ = broker.with_middlewares(_DoneMiddleware(sink))
    decorator = cast(
        "Callable[[Callable[[int], Awaitable[None]]], object]",
        broker.task(task_name=TASK_NAME),
    )
    return cast("TaskiqNoop", decorator(noop))
