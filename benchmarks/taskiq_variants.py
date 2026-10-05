"""Варианты taskiq для бенчмарка оверхеда (T11.6) на протоколе :class:`LoadVariant`.

* ``taskiq-memory`` — ``InMemoryBroker`` в процессе харнесса: постановка сразу запускает задачу в
  том же event loop. Ни сети, ни хранилища, ни отдельных процессов — нижняя граница стоимости
  «вызвать корутину через брокер».
* ``taskiq-redis`` — ``ListQueueBroker`` (LPUSH/BRPOP) поверх Redis 7 в Docker; воркеры —
  отдельные процессы :mod:`benchmarks.taskiq_worker`, столько же и с той же параллельностью,
  что у flexiq. Без result backend и подтверждений: сообщение, взятое упавшим воркером, теряется.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import IO, TYPE_CHECKING, Final, TextIO

from redis.exceptions import ConnectionError as RedisConnectionError
from taskiq import InMemoryBroker
from taskiq_redis import ListQueueBroker

from benchmarks.overhead import Completion, TaskTimings
from benchmarks.stand import BenchError, free_port, run_command
from benchmarks.taskiq_app import DONE_KEY, MemorySink, RedisSink, connect, register
from benchmarks.workers import ROOT

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from benchmarks.overhead import OverheadSpec
    from benchmarks.taskiq_app import RedisClient, TaskiqNoop

__all__ = [
    "REDIS_IMAGE",
    "TaskiqMemoryVariant",
    "TaskiqRedisVariant",
    "parse_done",
    "redis_container",
]

REDIS_IMAGE: Final = "redis:7-alpine"
_KIQ_CHUNK: Final = 1_000
_POLL: Final = 0.05
_READY_TIMEOUT: Final = 60.0
_STOP_TIMEOUT: Final = 30.0
_LF: Final = "\n"


@contextlib.asynccontextmanager
async def redis_container() -> AsyncGenerator[str]:
    """Поднять ``bench-redis-<run>`` (настройки образа по умолчанию) и удалить по выходе.

    Yields:
        URL ``redis://127.0.0.1:<порт>/0``.
    """
    name = f"bench-redis-{uuid.uuid4().hex[:8]}"
    port = free_port()
    _ = await run_command(
        "docker", "run", "-d", "--name", name, "-p", f"127.0.0.1:{port}:6379", REDIS_IMAGE
    )  # fmt: skip
    url = f"redis://127.0.0.1:{port}/0"
    try:
        await _wait_redis(url, name)
        yield url
    finally:
        _ = await run_command("docker", "rm", "-f", "-v", name, check=False)


async def _ping(client: RedisClient) -> bool:
    try:
        _ = await client.ping()
    except (RedisConnectionError, OSError):
        return False
    return True


async def _wait_redis(url: str, name: str) -> None:
    client = connect(url)
    deadline = time.monotonic() + _READY_TIMEOUT
    try:
        while not await _ping(client):
            if time.monotonic() > deadline:
                message = f"Redis {name} не ответил за {_READY_TIMEOUT:.0f} с"
                raise BenchError(message)
            await asyncio.sleep(0.2)
    finally:
        await client.aclose()


def _prepare(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)


def _open_text(path: Path) -> IO[str]:
    return path.open("w", encoding="utf-8", newline=_LF)


def _open_binary(path: Path) -> IO[bytes]:
    return path.open("wb")


def _remove(*paths: Path) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def _exists(path: Path) -> bool:
    return path.exists()


def _write(path: Path, content: str) -> None:
    _ = path.write_text(content, encoding="utf-8", newline=_LF)


def parse_done(raw: list[bytes], first: int, count: int) -> dict[int, float]:
    """Разобрать записи ``"i момент"`` списка ``bench:done`` задач повтора.

    Returns:
        Момент выполнения по номеру задачи (первая запись, если задача выполнилась дважды).
    """
    done: dict[int, float] = {}
    for entry in raw:
        index_raw, at_raw = entry.decode().split(" ", 1)
        index = int(index_raw)
        if first <= index < first + count:
            _ = done.setdefault(index, float(at_raw))
    return done


def _latencies(sent: dict[int, float], done: dict[int, float]) -> TaskTimings:
    return TaskTimings(latency=[at - sent[index] for index, at in done.items() if index in sent])


async def _kiq_all(noop: TaskiqNoop, sent: dict[int, float], *, first: int, count: int) -> None:
    async def one(index: int) -> None:
        sent[index] = time.time()
        _ = await noop.kiq(index)

    for start in range(first, first + count, _KIQ_CHUNK):
        stop = min(start + _KIQ_CHUNK, first + count)
        _ = await asyncio.gather(*(one(index) for index in range(start, stop)))


@dataclass(slots=True)
class TaskiqMemoryVariant:
    """taskiq ``InMemoryBroker``: задачи в процессе харнесса, stdout — в файл на время прогона."""

    broker: InMemoryBroker | None = None
    noop: TaskiqNoop | None = None
    sink: MemorySink = field(default_factory=MemorySink)
    sent: dict[int, float] = field(default_factory=dict[int, float])
    _done_before: dict[int, int] = field(default_factory=dict[int, int])
    _log: IO[str] | None = None
    _stdout: TextIO | None = None

    @property
    def name(self) -> str:
        """Имя варианта."""
        return "taskiq-memory"

    async def start(self, spec: OverheadSpec, root: Path) -> None:
        """Брокер с ``max_async_tasks`` = процессы x параллельность flexiq."""
        _prepare(root)
        self.broker = InMemoryBroker(max_async_tasks=spec.processes * spec.concurrency)
        self.noop = register(self.broker, self.sink)
        await self.broker.startup()
        self._log = _open_text(root / "worker-memory.log")
        self._stdout = sys.stdout
        sys.stdout = self._log

    def _require(self) -> tuple[InMemoryBroker, TaskiqNoop]:
        if self.broker is None or self.noop is None:
            message = "вариант не запущен"
            raise BenchError(message)
        return self.broker, self.noop

    async def submit(self, first: int, count: int) -> None:
        """``kiq`` по одной задаче, пачками по 1 000 через ``gather``."""
        _, noop = self._require()
        self._done_before[first] = len(self.sink.events)
        await _kiq_all(noop, self.sent, first=first, count=count)

    async def wait(self, first: int, count: int, within: float) -> Completion:
        """Ждать, пока middleware отметит все задачи повтора.

        Returns:
            Момент завершения.
        """
        async with asyncio.timeout(within):
            await self.sink.reached(self._done_before[first] + count)
        return Completion(time.monotonic())

    async def timings(self, first: int, count: int) -> TaskTimings:
        """«Поставлена → выполнена»: момент ``kiq`` → middleware после тела.

        Returns:
            Задержки задач повтора.
        """
        done = {index: at for index, at in self.sink.events if first <= index < first + count}
        return _latencies(self.sent, done)

    async def stop(self) -> None:
        """Вернуть stdout и остановить брокер."""
        if self._stdout is not None:
            sys.stdout = self._stdout
            self._stdout = None
        if self._log is not None:
            self._log.close()
            self._log = None
        if self.broker is not None:
            await self.broker.wait_all()
            await self.broker.shutdown()


@dataclass(slots=True)
class _Process:
    process: asyncio.subprocess.Process
    log: IO[bytes]
    stop: Path


@dataclass(slots=True)
class TaskiqRedisVariant:
    """taskiq ``ListQueueBroker`` поверх Redis; воркеры — отдельные процессы."""

    url: str
    broker: ListQueueBroker | None = None
    client: RedisClient | None = None
    noop: TaskiqNoop | None = None
    sent: dict[int, float] = field(default_factory=dict[int, float])
    _workers: list[_Process] = field(default_factory=list[_Process])

    @property
    def name(self) -> str:
        """Имя варианта."""
        return "taskiq-redis"

    async def start(self, spec: OverheadSpec, root: Path) -> None:
        """Очистить Redis и запустить ``spec.processes`` воркеров."""
        _prepare(root)
        self.client = connect(self.url)
        _ = await self.client.flushdb()
        self.broker = ListQueueBroker(self.url)
        # Producer не выполняет задачи: middleware ему не нужен, но регистрация общая.
        self.noop = register(self.broker, RedisSink(self.client))
        await self.broker.startup()
        await asyncio.gather(*(self._spawn(root, index, spec) for index in range(spec.processes)))

    async def _spawn(self, root: Path, index: int, spec: OverheadSpec) -> None:
        ready = root / f"ready-{index}"
        stop = root / f"stop-{index}"
        _remove(ready, stop)
        log = _open_binary(root / f"worker-{index}.log")
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "benchmarks.taskiq_worker", "--url", self.url,
            "--concurrency", str(spec.concurrency), "--ready", str(ready), "--stop", str(stop),
            cwd=ROOT, stdout=log, stderr=asyncio.subprocess.STDOUT,
        )  # fmt: skip
        self._workers.append(_Process(process, log, stop))
        deadline = time.monotonic() + _READY_TIMEOUT
        while not _exists(ready):
            if process.returncode is not None or time.monotonic() > deadline:
                message = f"воркер taskiq {index} не стартовал (код {process.returncode})"
                raise BenchError(message)
            await asyncio.sleep(0.05)

    def _require(self) -> tuple[RedisClient, TaskiqNoop]:
        if self.client is None or self.noop is None:
            message = "вариант не запущен"
            raise BenchError(message)
        return self.client, self.noop

    async def submit(self, first: int, count: int) -> None:
        """``kiq`` (LPUSH) по одной задаче, пачками по 1 000 через ``gather``."""
        _, noop = self._require()
        await _kiq_all(noop, self.sent, first=first, count=count)

    async def wait(self, first: int, count: int, within: float) -> Completion:
        """Ждать, пока в ``bench:done`` наберутся все задачи (повторы идут подряд).

        Returns:
            Момент завершения.

        Raises:
            BenchError: воркер завершился.
        """
        client, _ = self._require()
        target = first + count
        async with asyncio.timeout(within):
            while True:
                done = await client.llen(DONE_KEY)
                if done >= target:
                    return Completion(time.monotonic())
                for worker in self._workers:
                    if worker.process.returncode is not None:
                        message = f"воркер taskiq завершился с кодом {worker.process.returncode}"
                        raise BenchError(message)
                await asyncio.sleep(_POLL)

    async def timings(self, first: int, count: int) -> TaskTimings:
        """«Поставлена → выполнена»: момент ``kiq`` → запись middleware воркера.

        Returns:
            Задержки задач повтора.
        """
        client, _ = self._require()
        raw = await client.lrange(DONE_KEY, 0, -1)
        return _latencies(self.sent, parse_done(raw, first, count))

    async def stop(self) -> None:
        """Остановить воркеры stop-файлом, закрыть клиентов."""
        workers, self._workers = self._workers, []
        for worker in workers:
            _write(worker.stop, "stop")
        for worker in workers:
            try:
                async with asyncio.timeout(_STOP_TIMEOUT):
                    _ = await worker.process.wait()
            except TimeoutError:
                worker.process.kill()
                _ = await worker.process.wait()
            worker.log.close()
        if self.broker is not None:
            await self.broker.shutdown()
        if self.client is not None:
            await self.client.aclose()
