"""Процесс-воркер taskiq + Redis (T11.6): ``python -m benchmarks.taskiq_worker --url ...``.

Один процесс — один приёмник taskiq (``run_receiver_task``) с ``max_async_tasks`` =
``--concurrency``, как ``async_concurrency`` воркера flexiq. Остановка — файлом ``--stop``,
как у :mod:`benchmarks.worker`: его ждёт служебный поток.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from taskiq.api import run_receiver_task
from taskiq_redis import ListQueueBroker

from benchmarks.taskiq_app import RedisSink, connect, register

__all__ = ["main"]

_POLL = 0.1


@dataclass(frozen=True, slots=True)
class _Options:
    url: str
    concurrency: int
    ready: Path
    stop: Path


def _watch(stop: Path, loop: asyncio.AbstractEventLoop, stopped: asyncio.Event) -> None:
    pause = threading.Event()
    while not stop.exists():
        _ = pause.wait(_POLL)
    _ = loop.call_soon_threadsafe(stopped.set)


async def _serve(options: _Options) -> None:
    broker = ListQueueBroker(options.url)
    client = connect(options.url)
    _ = register(broker, RedisSink(client))
    await broker.startup()
    receiver = asyncio.create_task(
        run_receiver_task(broker, max_async_tasks=options.concurrency, run_startup=False),
        name="bench-taskiq-receiver",
    )
    stopped = asyncio.Event()
    watcher = threading.Thread(
        target=_watch,
        args=(options.stop, asyncio.get_running_loop(), stopped),
        name="bench-stop-watch",
        daemon=True,
    )
    watcher.start()
    _ = options.ready.write_text("ready", encoding="utf-8", newline="\n")
    waiter = asyncio.create_task(stopped.wait(), name="bench-stop")
    try:
        _ = await asyncio.wait([receiver, waiter], return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (receiver, waiter):
            _ = task.cancel()
        _ = await asyncio.wait([receiver, waiter])
        await broker.shutdown()
        await client.aclose()
        sys.stdout.flush()


def main() -> None:
    """Обслуживать очередь Redis до появления stop-файла."""
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--url", required=True)
    _ = parser.add_argument("--concurrency", type=int, required=True)
    _ = parser.add_argument("--ready", type=Path, required=True)
    _ = parser.add_argument("--stop", type=Path, required=True)
    values = parser.parse_args()
    options = _Options(
        url=values.url,  # pyright: ignore[reportAny]  # argparse.Namespace
        concurrency=values.concurrency,  # pyright: ignore[reportAny]  # argparse.Namespace
        ready=values.ready,  # pyright: ignore[reportAny]  # argparse.Namespace
        stop=values.stop,  # pyright: ignore[reportAny]  # argparse.Namespace
    )
    asyncio.run(_serve(options))


if __name__ == "__main__":
    main()
