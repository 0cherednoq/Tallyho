"""Спайк T8.0: как быстро flexiq возвращает джобу убитого воркера (нет per-job heartbeat).

Запуск (нужен Docker): ``uv run python tests/contract/flexiq/spike_dead_worker.py``.
Родитель поднимает PostgreSQL, ставит длинную джобу, убивает процесс воркера посреди неё
(TerminateProcess / SIGKILL), запускает второй воркер и меряет, через сколько секунд
джоба снова исполняется. Не тест и не часть CI.
"""

from __future__ import annotations

import asyncio
import subprocess  # ruff: ignore[suspicious-subprocess-import]  # спайк запускает свой же скрипт
import sys
import time
from pathlib import Path

from flexiq import Queue, current_job
from spike_support import postgres_url, say, wait_until  # pyright: ignore[reportImplicitRelativeImport]  # скрипт запускают напрямую, sys.path[0] — его каталог

__all__ = ["main"]

HERE = Path(__file__).resolve()
DONE_FAST = 1


def make_queue(url: str) -> Queue:
    queue = Queue(backend="postgres", db_url=url, workers=2, drain_timeout=1)
    queue.task(max_retries=3, timeout=300)(long_task)
    return queue


async def long_task() -> int:
    if current_job.retry_count >= DONE_FAST:
        return current_job.retry_count
    await asyncio.sleep(120)
    return 0


def spawn_worker(url: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # ruff: ignore[subprocess-without-shell-equals-true]  # аргументы — константы
        [sys.executable, str(HERE), "worker", url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def job_state(queue: Queue, job_id: str) -> tuple[str, int]:
    job = queue.get_job(job_id)
    assert job is not None
    return job.status, job._py_job.retry_count  # ruff: ignore[private-member-access]  # поле PyJob


def run_parent() -> None:
    with postgres_url() as url:
        queue = make_queue(url)
        first = spawn_worker(url)
        second: subprocess.Popen[bytes] | None = None
        try:
            job = queue.enqueue(task_name=f"{HERE.stem}.long_task", max_retries=3, timeout=300)
            wait_until(lambda: job_state(queue, job.id)[0] == "running", 60)
            say("D1.running_before_kill", job_state(queue, job.id))
            first.kill()
            first.wait()
            killed_at = time.monotonic()
            second = spawn_worker(url)
            timeline: list[tuple[float, str, int]] = []

            def settled() -> bool:
                status, retries = job_state(queue, job.id)
                if not timeline or timeline[-1][1:] != (status, retries):
                    timeline.append((round(time.monotonic() - killed_at, 1), status, retries))
                return status == "complete"

            wait_until(settled, 180, step=0.5)
            say("D2.timeline_after_kill_s", timeline)
        finally:
            for proc in (first, second):
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.wait()


def run_worker(url: str) -> None:
    make_queue(url).run_worker(queues=["default"], pool="thread")


def main() -> None:
    if len(sys.argv) == 3 and sys.argv[1] == "worker":
        run_worker(sys.argv[2])
    else:
        run_parent()


if __name__ == "__main__":
    main()
