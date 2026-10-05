"""Общие куски отчётов сценариев на устойчивой нагрузке S1."""

from __future__ import annotations

import math
import statistics
from typing import TYPE_CHECKING, Final

from benchmarks.charts import LineChart, Series
from benchmarks.metrics import summarize
from benchmarks.report import Check, Table

if TYPE_CHECKING:
    from collections.abc import Sequence

    from benchmarks.load import SteadyRun
    from benchmarks.metrics import Summary
    from benchmarks.report import JsonValue

__all__ = [
    "OPERATION_LABELS",
    "buffer_growth",
    "latency_table",
    "oracle_check",
    "rate_chart",
    "run_metrics",
    "windowed_max",
]

OPERATION_LABELS: Final = {
    "create": "create (батч + add_calls, producer)",
    "read_progress": "read progress (handle.view, producer)",
    "failed_items": "failed_items (handle.items, producer)",
    "maintenance": "проход maintenance (sweeper и др., producer)",
    "claim": "claim (вызов задачи → тело, воркер)",
    "finish": "finish (complete_in + commit, воркер)",
    "completer_flush": "транзакция Completer (воркер)",
    "finalization": "финализация (последний Item → батч)",
}
_HALVES: Final = 2
"""Сравнение половин прогона: нужно хотя бы по окну на половину."""


def windowed_max(
    points: Sequence[tuple[float, float]], *, window: float
) -> list[tuple[float, float]]:
    """Максимум значения в каждом окне ``window`` секунд.

    Returns:
        Пары ``(конец окна, максимум)``.
    """
    buckets: dict[int, float] = {}
    for at, value in points:
        index = math.floor(at / window)
        buckets[index] = max(buckets.get(index, value), value)
    return [((index + 1) * window, value) for index, value in sorted(buckets.items())]


def buffer_growth(run: SteadyRun) -> tuple[float, float]:
    """Медиана максимумов буфера Completer по окнам: первая и вторая половина после прогрева.

    Returns:
        Медианы первой и второй половины.
    """
    points = [
        (at, float(items))
        for at, items in run.buffers()
        if run.spec.warmup <= at <= run.spec.duration
    ]
    windows = windowed_max(points, window=run.spec.window)
    if len(windows) < _HALVES:
        return 0.0, 0.0
    middle = len(windows) // 2
    first = statistics.median(value for _, value in windows[:middle])
    second = statistics.median(value for _, value in windows[middle:])
    return first, second


def rate_chart(
    name: str, title: str, runs: Sequence[tuple[str, SteadyRun]], *, limit: float | None
) -> LineChart:
    """Завершения в секунду по времени для одного или нескольких прогонов.

    Returns:
        График.
    """
    return LineChart(
        name,
        title,
        "секунды прогона",
        "завершений/с",
        tuple(Series(label, tuple(run.rates())) for label, run in runs),
        limit=limit,
    )


def _fmt(value: float, scale: float) -> str:
    return "—" if math.isnan(value) else f"{value * scale:.1f}"


def _row(name: str, summary: Summary, *, scale: float = 1000) -> tuple[str, ...]:
    return (
        name,
        str(summary.count),
        _fmt(summary.p50, scale),
        _fmt(summary.p99, scale),
        _fmt(summary.max, scale),
    )


def latency_table(title: str, run: SteadyRun) -> Table:
    """p50/p99 операций producer и воркеров (мс) и размер транзакций Completer.

    Returns:
        Таблица отчёта.
    """
    rows: list[tuple[str, ...]] = []
    for log in (run.latencies, run.worker_ops()):
        for operation in log.operations():
            label = OPERATION_LABELS.get(operation, operation)
            rows.append(_row(label, log.summary(operation)))
    sizes = summarize([float(items) for _, items, _ in run.flushes()])
    rows.append(_row("Items в транзакции Completer (шт.)", sizes, scale=1.0))
    return Table(title, ("операция", "замеров", "p50, мс", "p99, мс", "max, мс"), tuple(rows))


def oracle_check(run: SteadyRun) -> Check:
    """Корректность на нагрузке (ACCEPTANCE §9: «оракул на выборке»).

    Returns:
        Проверка отчёта.
    """
    return Check(
        "корректность (оракул на выборке)",
        "все батчи succeeded, 1 финализация на батч, эффект ровно один раз",
        run.oracle.describe(),
        run.oracle.ok,
    )


def run_metrics(run: SteadyRun) -> dict[str, JsonValue]:
    """Сырые ряды прогона для ``result.json``.

    Returns:
        Словарь для ``metrics``.
    """
    operations: dict[str, JsonValue] = {
        name: dict(log.summary(name).as_dict(scale=1000))
        for log in (run.latencies, run.worker_ops())
        for name in log.operations()
    }
    buffers = [(at, float(items)) for at, items in run.buffers()]
    return {
        "steady_per_s": round(run.steady(), 1),
        "rates": [[round(at, 2), round(rate, 1)] for at, rate in run.rates()],
        "done": [[round(at, 2), done] for at, done in run.done_points],
        "completer_buffer_max": [
            [round(at, 2), value] for at, value in windowed_max(buffers, window=run.spec.window)
        ],
        "operations_ms": operations,
        "oracle": {"ok": run.oracle.ok, "violations": list(run.oracle.violations)},
    }
