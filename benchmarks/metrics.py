"""Сбор задержек и пропускной способности: перцентили, фазы прогона, окна времени."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

__all__ = [
    "LatencyLog",
    "Sample",
    "Summary",
    "percentile",
    "phase_summaries",
    "rate_series",
    "steady_rate",
    "summarize",
]

PHASE_SHARE = 0.1
"""Доля прогона, которая считается «началом» и «концом» (COUNTERS §4.1)."""


def percentile(values: Sequence[float], q: float) -> float:
    """Перцентиль ``q`` (0…100) с линейной интерполяцией; для пустой выборки — ``nan``.

    Returns:
        Значение перцентиля.
    """
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * min(max(q, 0.0), 100.0) / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


@dataclass(frozen=True, slots=True)
class Summary:
    """Распределение одной метрики, в единицах выборки (обычно секунды)."""

    count: int
    mean: float
    p50: float
    p95: float
    p99: float
    max: float

    def as_dict(self, *, scale: float = 1.0) -> dict[str, float | int]:
        """Значения для JSON; ``scale`` переводит единицы (``1000`` — секунды в мс).

        Returns:
            Словарь ``count/mean/p50/p95/p99/max``.
        """
        return {
            "count": self.count,
            "mean": _round(self.mean * scale),
            "p50": _round(self.p50 * scale),
            "p95": _round(self.p95 * scale),
            "p99": _round(self.p99 * scale),
            "max": _round(self.max * scale),
        }


def _round(value: float) -> float:
    return value if math.isnan(value) else round(value, 3)


def summarize(values: Sequence[float]) -> Summary:
    """Сводка распределения; пустая выборка даёт ``nan`` и ``count=0``.

    Returns:
        Сводка.
    """
    if not values:
        return Summary(0, math.nan, math.nan, math.nan, math.nan, math.nan)
    return Summary(
        count=len(values),
        mean=statistics.fmean(values),
        p50=percentile(values, 50),
        p95=percentile(values, 95),
        p99=percentile(values, 99),
        max=max(values),
    )


@dataclass(frozen=True, slots=True)
class Sample:
    """Одно измерение: момент (секунды от начала прогона) и значение."""

    at: float
    value: float


@dataclass(slots=True)
class LatencyLog:
    """Задержки по операциям с моментом замера: для фаз «начало / середина / конец»."""

    samples: dict[str, list[Sample]] = field(default_factory=dict[str, list[Sample]])

    def add(self, operation: str, at: float, seconds: float) -> None:
        """Записать одну задержку операции ``operation``."""
        self.samples.setdefault(operation, []).append(Sample(at, seconds))

    def extend(self, operation: str, values: Iterable[Sample]) -> None:
        """Добавить готовые замеры операции."""
        self.samples.setdefault(operation, []).extend(values)

    def summary(self, operation: str) -> Summary:
        """Сводка по всем замерам операции.

        Returns:
            Сводка.
        """
        return summarize([sample.value for sample in self.samples.get(operation, [])])

    def operations(self) -> list[str]:
        """Операции, по которым есть замеры, в порядке добавления.

        Returns:
            Имена операций.
        """
        return [name for name, values in self.samples.items() if values]


def phase_summaries(samples: Sequence[Sample]) -> dict[str, Summary]:
    """Сводки в начале, на 50% и в конце прогона (по 10% длительности каждая).

    Границы фаз — по моментам замеров: «начало» — первые 10% интервала
    ``[min(at), max(at)]``, «середина» — 10% вокруг его середины, «конец» — последние 10%.

    Returns:
        Сводки по ключам ``start``, ``mid``, ``end``.
    """
    if not samples:
        empty = summarize([])
        return {"start": empty, "mid": empty, "end": empty}
    first = min(sample.at for sample in samples)
    last = max(sample.at for sample in samples)
    span = max(last - first, 1e-9)
    width = span * PHASE_SHARE
    middle = first + span / 2
    windows = {
        "start": (first, first + width),
        "mid": (middle - width / 2, middle + width / 2),
        "end": (last - width, last),
    }
    return {
        name: summarize([sample.value for sample in samples if low <= sample.at <= high])
        for name, (low, high) in windows.items()
    }


def rate_series(points: Sequence[tuple[float, int]], *, window: float) -> list[tuple[float, float]]:
    """Скорость роста счётчика: ``(момент, событий в секунду)`` по окнам ``window`` секунд.

    ``points`` — пары ``(момент, накопленное значение)``, отсортированные по времени.

    Returns:
        Пары ``(конец окна, скорость)``.
    """
    series: list[tuple[float, float]] = []
    if not points:
        return series
    start_at, start_value = points[0]
    for at, value in points[1:]:
        if at - start_at >= window:
            series.append((at, (value - start_value) / (at - start_at)))
            start_at, start_value = at, value
    return series


def steady_rate(series: Sequence[tuple[float, float]], *, warmup: float) -> float:
    """Устойчивая скорость: медиана окон после прогрева; без окон — ``nan``.

    Returns:
        Событий в секунду.
    """
    values = [rate for at, rate in series if at >= warmup]
    if not values:
        values = [rate for _, rate in series]
    return statistics.median(values) if values else math.nan
