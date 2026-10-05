"""Контекст прогона P-NN и реестр сценариев."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.profiles import Profile
    from benchmarks.report import ScenarioResult
    from benchmarks.stand import Stand

__all__ = ["RunContext", "Scenario", "ScenarioRun"]


@dataclass(slots=True)
class RunContext:
    """Что получает сценарий: профиль, стенд, каталог отчёта и seed."""

    profile: Profile
    stand: Stand
    out: Path
    seed: int
    _origin: float = field(default_factory=time.monotonic)

    def log(self, message: str) -> None:
        """Строка прогресса в stderr (харнесс — инструмент, не библиотека)."""
        _ = sys.stderr.write(f"[bench {time.monotonic() - self._origin:7.1f}s] {message}\n")
        _ = sys.stderr.flush()


class ScenarioRun(Protocol):
    """Тело сценария: заполняет ``result`` проверками, метриками, графиками."""

    async def __call__(self, ctx: RunContext, result: ScenarioResult) -> None:
        """Выполнить сценарий."""
        ...


@dataclass(frozen=True, slots=True)
class Scenario:
    """Описание P-NN из ACCEPTANCE §9."""

    id: str
    title: str
    measures: str
    target: str
    run: ScenarioRun
