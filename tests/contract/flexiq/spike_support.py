"""Общая обвязка спайка T8.0: PostgreSQL в Docker, воркер flexiq в потоке, вывод.

Не тест: pytest собирает только ``test_*.py``. Запуск спайков — см. docs/plan/FLEXIQ_SPIKE.md.
"""

from __future__ import annotations

import contextlib
import json
import sys
import threading
import time
from typing import TYPE_CHECKING

from testcontainers.community.postgres import PostgresContainer

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from flexiq import Queue

__all__ = ["POSTGRES_IMAGE", "postgres_url", "say", "wait_until", "worker_thread"]

POSTGRES_IMAGE = "postgres:16-alpine"


def say(fact: str, value: object) -> None:
    """Печатает строку результата спайка: ``<факт>: <значение в JSON>``."""
    text = json.dumps(value, ensure_ascii=False, default=repr, sort_keys=True)
    sys.stdout.write(f"{fact}: {text}\n")
    sys.stdout.flush()


@contextlib.contextmanager
def postgres_url() -> Generator[str, None, None]:
    """Поднимает ``postgres:16-alpine`` через testcontainers и отдаёт DSN для flexiq."""
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as pg:
        yield pg.get_connection_url()


def wait_until(predicate: Callable[[], bool], timeout: float, step: float = 0.05) -> bool:
    """Ждёт, пока ``predicate()`` станет истинным; ``False`` — если не дождались."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


@contextlib.contextmanager
def worker_thread(
    queue: Queue, queues: list[str] | None = None
) -> Generator[threading.Thread, None, None]:
    """Запускает ``queue.run_worker(pool="thread")`` в фоновом потоке и гасит его на выходе.

    Не главный поток — flexiq не ставит обработчики сигналов; остановка через
    ``queue.shutdown()`` (программный аналог SIGTERM).
    """
    thread = threading.Thread(
        target=queue.run_worker,
        kwargs={"queues": queues, "pool": "thread"},
        name="spike-worker",
        daemon=True,
    )
    thread.start()
    try:
        yield thread
    finally:
        queue.shutdown()
        thread.join(timeout=30)
