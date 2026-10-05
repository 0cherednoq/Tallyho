"""Процесс воркера flexiq бенчмарка: ``python -m benchmarks.worker --config cfg.json ...``.

Остановка — файлом ``--stop`` (одинаково на Windows и Linux, где ``terminate`` — это
``TerminateProcess`` без обработчиков): воркер дорабатывает начатые задачи, закрывает
установку tallyho (Completer дописывает итоги) и сбрасывает события в ``stats_path``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading
from pathlib import Path

from benchmarks.app import AppConfig, build_app

__all__ = ["main"]

_POLL = 0.1
_DRAIN = 10.0


def main() -> None:
    """Запустить пул потоков flexiq до появления stop-файла."""
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--config", type=Path, required=True)
    _ = parser.add_argument("--ready", type=Path, required=True)
    _ = parser.add_argument("--stop", type=Path, required=True)
    values = parser.parse_args()
    config_path: Path = values.config  # pyright: ignore[reportAny]  # argparse.Namespace
    ready: Path = values.ready  # pyright: ignore[reportAny]  # argparse.Namespace
    stop: Path = values.stop  # pyright: ignore[reportAny]  # argparse.Namespace
    app = build_app(AppConfig.from_json(config_path.read_text(encoding="utf-8")))
    app.stats.start()
    failures: list[BaseException] = []

    def run() -> None:
        try:
            app.queue.run_worker(queues=["default"], pool="thread")
        except BaseException as exc:
            failures.append(exc)
            raise

    worker = threading.Thread(target=run, name="flexiq-worker", daemon=True)
    worker.start()
    _ = ready.write_text("ready", encoding="utf-8", newline="\n")
    stopped = threading.Event()
    while worker.is_alive() and not stop.exists():
        _ = stopped.wait(_POLL)
    app.queue.shutdown()
    worker.join(timeout=_DRAIN)
    drained = not worker.is_alive()
    if app.th is not None:
        asyncio.run(app.th.aclose())
    app.stats.close()
    if failures:
        raise failures[0]
    sys.stdout.flush()
    if drained:
        app.queue.close()
        return
    os._exit(0)  # flexiq ещё дорабатывает: обычный выход ждал бы его


if __name__ == "__main__":
    main()
