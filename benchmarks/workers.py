"""Пул процессов-воркеров flexiq: запуск, остановка, ``kill -9``, сбор их событий."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final, cast

from benchmarks.stand import BenchError

if TYPE_CHECKING:
    from benchmarks.app import AppConfig

__all__ = ["WorkerEvents", "WorkerPool", "read_events"]

ROOT: Final = Path(__file__).resolve().parents[1]
_READY_TIMEOUT: Final = 60.0
_STOP_TIMEOUT: Final = 30.0


@dataclass(slots=True)
class _Worker:
    index: int
    generation: int
    process: asyncio.subprocess.Process
    log: IO[bytes]
    stop: Path


@dataclass(slots=True)
class WorkerPool:
    """Процессы ``python -m benchmarks.worker`` с общей конфигурацией приложения."""

    root: Path
    config: AppConfig
    _workers: dict[int, _Worker] = field(default_factory=dict[int, "_Worker"])
    _generation: int = 0

    @property
    def size(self) -> int:
        """Сколько воркеров запущено."""
        return len(self._workers)

    async def start(self, count: int) -> None:
        """Запустить ``count`` воркеров и дождаться их готовности."""
        self.root.mkdir(parents=True, exist_ok=True)
        await asyncio.gather(*(self._spawn(index) for index in range(count)))

    async def _spawn(self, index: int) -> None:
        self._generation += 1
        tag = f"{index}-{self._generation}"
        config = replace(self.config, stats_path=str(self.root / f"stats-{tag}.jsonl"))
        config_path = self.root / f"worker-{tag}.json"
        _ = config_path.write_text(config.to_json(), encoding="utf-8", newline="\n")
        ready = self.root / f"ready-{tag}"
        stop = self.root / f"stop-{tag}"
        ready.unlink(missing_ok=True)
        stop.unlink(missing_ok=True)
        log = (self.root / f"worker-{tag}.log").open("wb")
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "benchmarks.worker",
            "--config", str(config_path), "--ready", str(ready), "--stop", str(stop),
            cwd=ROOT, stdout=log, stderr=asyncio.subprocess.STDOUT,
        )  # fmt: skip
        worker = _Worker(index, self._generation, process, log, stop)
        self._workers[index] = worker
        deadline = time.monotonic() + _READY_TIMEOUT
        while not ready.exists():
            if process.returncode is not None or time.monotonic() > deadline:
                log.flush()
                text = (self.root / f"worker-{tag}.log").read_text(errors="replace")
                message = f"воркер {tag} не стартовал (код {process.returncode}):\n{text[-4000:]}"
                raise BenchError(message)
            await asyncio.sleep(0.05)

    def assert_alive(self) -> None:
        """Упасть, если какой-то воркер завершился сам.

        Raises:
            BenchError: воркер завершился.
        """
        for worker in self._workers.values():
            if worker.process.returncode is not None:
                message = f"воркер {worker.index} завершился с кодом {worker.process.returncode}"
                raise BenchError(message)

    async def kill(self, index: int) -> None:
        """Убить воркер без корректной остановки (``kill -9`` / ``TerminateProcess``)."""
        worker = self._workers.pop(index)
        worker.process.kill()
        _ = await worker.process.wait()
        worker.log.close()

    async def respawn(self, index: int) -> None:
        """Поднять воркер с номером ``index`` заново (супервизор A-CH-01)."""
        await self._spawn(index)

    async def stop(self) -> None:
        """Корректно остановить все воркеры; не уложившиеся в срок — убить."""
        workers = list(self._workers.values())
        self._workers.clear()
        for worker in workers:
            _ = worker.stop.write_text("stop", encoding="utf-8", newline="\n")
        for worker in workers:
            try:
                async with asyncio.timeout(_STOP_TIMEOUT):
                    _ = await worker.process.wait()
            except TimeoutError:
                worker.process.kill()
                _ = await worker.process.wait()
            worker.log.close()

    def events(self) -> WorkerEvents:
        """События всех поколений воркеров этого пула.

        Returns:
            События воркеров.
        """
        return read_events(self.root)


@dataclass(slots=True)
class WorkerEvents:
    """События воркеров; моменты — ``time.time()`` процессов воркеров."""

    flushes: list[tuple[float, int, float]] = field(default_factory=list[tuple[float, int, float]])
    buffers: list[tuple[float, int]] = field(default_factory=list[tuple[float, int]])
    relays: list[tuple[float, int, float]] = field(default_factory=list[tuple[float, int, float]])
    bodies: list[tuple[float, str, str]] = field(default_factory=list[tuple[float, str, str]])
    ops: list[tuple[float, str, float]] = field(default_factory=list[tuple[float, str, float]])
    before: dict[str, float] = field(default_factory=dict[str, float])
    after: dict[str, float] = field(default_factory=dict[str, float])


def _add(events: WorkerEvents, record: list[object]) -> None:
    kind = record[0]
    at = float(cast("float", record[1]))
    if kind in {"flush", "relay"}:
        entry = (at, int(cast("int", record[2])), float(cast("float", record[3])))
        (events.flushes if kind == "flush" else events.relays).append(entry)
    elif kind == "buffer":
        events.buffers.append((at, int(cast("int", record[2]))))
    elif kind == "body":
        events.bodies.append((at, str(record[2]), str(record[3])))
    elif kind == "op":
        events.ops.append((at, str(record[2]), float(cast("float", record[3]))))
    elif kind in {"before", "after"}:
        (events.before if kind == "before" else events.after)[str(record[2])] = at


def read_events(root: Path) -> WorkerEvents:
    """Прочитать ``stats-*.jsonl`` каталога пула.

    Returns:
        Разобранные события.
    """
    events = WorkerEvents()
    for path in sorted(root.glob("stats-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                _add(events, cast("list[object]", json.loads(line)))
    return events
