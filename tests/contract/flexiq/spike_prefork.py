"""Спайк T8.0: как ``pool="prefork"`` исполняет ``async def``-задачи (ARCHITECTURE §16, вопрос 1).

Самодостаточный (только flexiq + SQLite), чтобы запускаться в чистом Linux-контейнере::

    docker run --rm -v "$PWD/tests/contract/flexiq:/spike" -w /spike python:3.11-slim \
        sh -c "pip install -q flexiq==2.0.0 && python spike_prefork.py"

На Windows prefork недоступен — скрипт печатает ошибку flexiq. Не тест и не часть CI.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

from flexiq import Queue, current_job

__all__ = ["main", "queue"]

DB = Path(tempfile.gettempdir()) / "tallyho-spike-prefork.db"
queue = Queue(db_path=str(DB), workers=2, drain_timeout=2)


@queue.task(max_retries=0)
async def probe(i: int) -> dict[str, object]:
    await asyncio.sleep(0.05)
    return {
        "i": i,
        "pid": os.getpid(),
        "loop": id(asyncio.get_running_loop()),
        "thread": threading.current_thread().name,
        "retry_count": current_job.retry_count,
    }


def say(fact: str, value: object) -> None:
    sys.stdout.write(f"{fact}: {json.dumps(value, default=repr, sort_keys=True)}\n")
    sys.stdout.flush()


def main() -> None:
    say("P0.platform", {"platform": sys.platform, "python": sys.version.split()[0]})
    try:
        queue.run_worker(pool="prefork", app="spike_prefork:queue")
    except NotImplementedError as exc:
        say("P1.prefork_error", repr(exc))
        return
    except ValueError as exc:
        say("P1.prefork_error", repr(exc))
        return


def run_linux() -> None:
    os.environ["PYTHONPATH"] = str(Path(__file__).resolve().parent)
    jobs = [probe.delay(i) for i in range(6)]
    worker = threading.Thread(
        target=queue.run_worker,
        kwargs={"pool": "prefork", "app": "spike_prefork:queue"},
        daemon=True,
    )
    worker.start()
    results = [job.result(timeout=60) for job in jobs]
    queue.shutdown()
    worker.join(timeout=30)
    time.sleep(0.1)
    say("P2.results", results)
    say(
        "P2.summary",
        {
            "jobs": len(results),
            "distinct_pids": len({r["pid"] for r in results}),
            "distinct_loops": len({(r["pid"], r["loop"]) for r in results}),
            "threads": sorted({str(r["thread"]) for r in results}),
        },
    )


if __name__ == "__main__":
    if sys.platform == "win32":
        main()
    else:
        run_linux()
