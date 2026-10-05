"""P-04 — матрица масштаба (ARCHITECTURE §14, COUNTERS §4.1): батчи x Items и история."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from benchmarks.charts import LineChart, Series
from benchmarks.context import Scenario
from benchmarks.metrics import percentile, phase_summaries
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.scenarios.common import OPERATION_LABELS
from benchmarks.scenarios.p04_cell import Cell, run_cell

if TYPE_CHECKING:
    from collections.abc import Sequence

    from benchmarks.context import RunContext
    from benchmarks.metrics import Sample
    from benchmarks.report import JsonValue, ScenarioResult
    from benchmarks.scenarios.p04_cell import CellRun

__all__ = ["PARAMS", "SCENARIO", "ScaleParams"]


@dataclass(frozen=True, slots=True)
class ScaleParams:
    """Ячейки и воркеры профиля."""

    cells: tuple[Cell, ...]
    processes: int
    concurrency: int
    within: float


PARAMS: Final = {
    Profile.SMOKE: ScaleParams(
        (Cell("S-mini 40x50", 40, 50), Cell("S-mini + история 20k", 40, 50, history=20_000)),
        processes=2,
        concurrency=20,
        within=600,
    ),
    # ACCEPTANCE §11: профиль S и история 5M.
    Profile.NIGHTLY: ScaleParams(
        (Cell("S 1kx1k", 1_000, 1_000), Cell("S + история 5M", 1_000, 1_000, history=5_000_000)),
        processes=4,
        concurrency=50,
        within=3_000,
    ),
    Profile.FULL: ScaleParams(
        (
            Cell("S 1kx1k", 1_000, 1_000),
            Cell("M-wide 50kx1k", 50_000, 1_000),
            Cell("M-deep 1kx50k", 1_000, 50_000),
            Cell("Fan-out 100 x (1→100→500)", 100, 1, fanout=(100, 500)),
            Cell("S + история 50M", 1_000, 1_000, history=50_000_000),
        ),
        processes=8,
        concurrency=100,
        within=6 * 3_600,
    ),
}
GROWTH: Final = 1.5
MIN_PHASE_SAMPLES: Final = 20
EXPLAIN_MIN_ROWS: Final = 1_000_000
"""COUNTERS §4.2: EXPLAIN-гард осмыслен на БД от 1M строк; на меньшей Seq Scan выгоднее."""
_WINDOWS: Final = 20
_PHASE_HEADERS: Final = (
    "операция",
    "замеров н/с/к",
    "начало",
    "середина",
    "конец",
    "конец/начало p99",
    "≤ 1,5x",
)


def _fmt_ms(value: float) -> str:
    return "—" if math.isnan(value) else f"{value * 1000:.1f}"


def _phase_rows(run: CellRun) -> tuple[list[tuple[str, ...]], list[bool], dict[str, JsonValue]]:
    rows: list[tuple[str, ...]] = []
    verdicts: list[bool] = []
    data: dict[str, JsonValue] = {}
    for operation in run.log.operations():
        phases = phase_summaries(run.log.samples[operation])
        start, mid, end = phases["start"], phases["mid"], phases["end"]
        enough = start.count >= MIN_PHASE_SAMPLES and end.count >= MIN_PHASE_SAMPLES
        ratio = end.p99 / start.p99 if enough and start.p99 > 0 else math.nan
        if enough:
            verdicts.append(ratio <= GROWTH)
        rows.append(
            (
                OPERATION_LABELS.get(operation, operation),
                f"{start.count}/{mid.count}/{end.count}",
                f"{_fmt_ms(start.p50)} / {_fmt_ms(start.p99)}",
                f"{_fmt_ms(mid.p50)} / {_fmt_ms(mid.p99)}",
                f"{_fmt_ms(end.p50)} / {_fmt_ms(end.p99)}",
                "—" if math.isnan(ratio) else f"{ratio:.2f}",
                "мало замеров" if not enough else ("да" if ratio <= GROWTH else "нет"),
            )
        )
        data[operation] = {name: dict(item.as_dict(scale=1000)) for name, item in phases.items()}
    return rows, verdicts, data


def _windowed_p99(samples: Sequence[Sample]) -> list[tuple[float, float]]:
    if not samples:
        return []
    first = min(sample.at for sample in samples)
    last = max(sample.at for sample in samples)
    window = max((last - first) / _WINDOWS, 1.0)
    buckets: dict[int, list[float]] = {}
    for sample in samples:
        buckets.setdefault(int((sample.at - first) / window), []).append(sample.value)
    return [
        (first + (index + 1) * window, percentile(values, 99) * 1000)
        for index, values in sorted(buckets.items())
    ]


def _cell_json(run: CellRun, phase_data: dict[str, JsonValue]) -> dict[str, JsonValue]:
    return {
        "items": run.cell.total,
        "history": run.cell.history,
        "duration_s": round(run.duration, 1),
        "history_load_s": round(run.history_s, 1),
        "items_per_s": round(run.cell.total / run.duration, 1) if run.duration else None,
        "phases_ms": phase_data,
        "in_flight_tables": {
            name: [[round(at, 1), count] for at, count in points]
            for name, points in run.sizes.items()
        },
        "index_cache_hit": run.index_hit,
        "relation_mb": {name: round(size, 2) for name, size in run.relation_mb.items()},
        "explain": dict(run.plans),
    }


def _cell_charts(run: CellRun, index: int) -> list[LineChart]:
    charts = [
        LineChart(
            f"in_flight_{index}",
            f"P-04 {run.cell.name}: размер th_lease / th_outbox / th_counter_delta",
            "секунды прогона",
            "строк",
            tuple(
                Series(name, tuple((at, float(count)) for at, count in points))
                for name, points in run.sizes.items()
            ),
        )
    ]
    claims = run.log.samples.get("claim", [])
    if claims:
        charts.append(
            LineChart(
                f"claim_p99_{index}",
                f"P-04 {run.cell.name}: p99 claim по окнам",
                "секунды прогона",
                "мс",
                (Series("claim p99", tuple(_windowed_p99(claims))),),
            )
        )
    return charts


def _checks(runs: list[CellRun], growth: list[bool]) -> list[Check]:
    bad_plans = [
        f"{run.cell.name} — {name}: {problem}"
        for run in runs
        for name, problem in run.plans
        if problem is not None
    ]
    violations = [f"{run.cell.name}: {item}" for run in runs for item in run.violations]
    small_db = all(run.item_rows < EXPLAIN_MIN_ROWS for run in runs)
    explain = "нарушений нет" if not bad_plans else "; ".join(bad_plans[:4])
    if small_db:
        explain += " (в th_item < 1M строк: Seq Scan законен, проверка информативна)"
    return [
        Check(
            "p99 каждой операции в конце ≤ 1,5x p99 в начале",
            f"все операции, где в фазах ≥ {MIN_PHASE_SAMPLES} замеров",
            f"{sum(growth)} из {len(growth)} операций",
            all(growth) if growth else None,
        ),
        Check(
            "EXPLAIN-гард (нет Seq Scan по горячим таблицам)",
            "запросы горячего пути на заполненной БД",
            explain,
            None if small_db else not bad_plans,
        ),
        Check(
            "корректность (оракул на выборке)",
            "все корни succeeded, все Items ok, 1 финализация на батч",
            "нарушений нет" if not violations else "; ".join(violations[:4]),
            not violations,
        ),
    ]


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Каждая ячейка — свежие схемы, (история), все батчи сразу, фазы начало/середина/конец."""
    params = PARAMS[ctx.profile]
    result.parameters = {
        "cells": [
            {
                "name": cell.name,
                "batches": cell.batches,
                "items": cell.items,
                "history": cell.history,
                "fanout": list(cell.fanout) if cell.fanout else None,
            }
            for cell in params.cells
        ],
        "processes": params.processes,
        "concurrency": params.concurrency,
    }
    runs = [
        await run_cell(
            ctx,
            cell,
            index=index,
            processes=params.processes,
            concurrency=params.concurrency,
            within=params.within,
        )
        for index, cell in enumerate(params.cells)
    ]
    cells_json: dict[str, JsonValue] = {}
    growth: list[bool] = []
    for index, cell_run in enumerate(runs):
        rows, verdicts, phase_data = _phase_rows(cell_run)
        growth.extend(verdicts)
        result.tables.append(
            Table(
                f"{cell_run.cell.name}: p50 / p99, мс (начало, 50%, конец — по 10% прогона)",
                _PHASE_HEADERS,
                tuple(rows),
            )
        )
        cells_json[cell_run.cell.name] = _cell_json(cell_run, phase_data)
        result.charts.extend(_cell_charts(cell_run, index))
    result.checks = _checks(runs, growth)
    result.metrics = {"cells": cells_json}
    result.notes.extend(
        (
            (
                prose(
                    """
                    Все батчи ячейки создаются подряд в начале (живая нагрузка), воркеры выполняют
                    их параллельно. Фазы — первые 10%, 10% вокруг середины и последние 10%
                    длительности.
                    """
                )
            ),
            (
                prose(
                    """
                    История — терминальные деревья по 1 000 Items, вставленные SQL-генератором
                    (benchmarks/history.py) до начала нагрузки; retention у них выключен.
                    """
                )
            ),
            (
                prose(
                    """
                    Операции: create — producer, батч и add_calls одной транзакцией; read progress —
                    handle.view() случайного батча раз в 0,5 с; failed_items — первая страница
                    handle.items(states=[ERROR]) раз в 2 с; maintenance — run_maintenance_once раз в
                    1 с (sweeper, relay-scan, сверка DLQ); claim — воркер, вызов задачи → тело;
                    транзакция Completer — finish пути A; финализация — последний Item дерева →
                    finished_at корня. spawn отдельно не меряется: он входит в транзакцию finish
                    родителя (ячейка Fan-out).
                    """
                )
            ),
        )
    )


SCENARIO: Final = Scenario(
    id="P-04",
    title="Масштаб по числу батчей и Items",
    measures=(
        prose(
            """
            матрица ARCHITECTURE §14 (и история): p50/p99 операций в начале, на 50% и в конце;
            EXPLAIN-гард
            """
        )
    ),
    target="p99 каждой операции в конце ≤ 1,5x p99 в начале; EXPLAIN-гард без Seq Scan",
    run=run,
)
