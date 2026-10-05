"""P-08 — снимки прогресса: много активных деревьев с ``on_progress(every=2s)``."""

from __future__ import annotations

import asyncio
import bisect
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, cast

from benchmarks.app import KIND_PROGRESS
from benchmarks.charts import BarChart
from benchmarks.context import Scenario
from benchmarks.harness import harness
from benchmarks.metrics import summarize
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.stand import sql

if TYPE_CHECKING:
    from uuid import UUID

    from benchmarks.app import AppConfig
    from benchmarks.context import RunContext
    from benchmarks.harness import Harness
    from benchmarks.report import ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "SnapshotParams", "snapshot_lags"]


@dataclass(frozen=True, slots=True)
class SnapshotParams:
    """Деревья, задачи в дереве, длительность задачи и воркеры."""

    trees: int
    items: int
    step_seconds: float
    every: float
    processes: int
    concurrency: int
    within: float


PARAMS: Final = {
    Profile.SMOKE: SnapshotParams(200, 20, 1.0, 2.0, processes=2, concurrency=50, within=600),
    Profile.NIGHTLY: SnapshotParams(
        2_000, 10, 2.0, 2.0, processes=4, concurrency=100, within=1_800
    ),
    Profile.FULL: SnapshotParams(
        10_000, 20, 5.0, 2.0, processes=8, concurrency=100, within=4 * 3_600
    ),
}
LEADER_CORES: Final = 1.0
_PROGRESS: Final = (
    "SELECT batch_id, extract(epoch FROM at) FROM {progress_log} ORDER BY batch_id, at"
)
_FINISHES: Final = """
SELECT batch_id, extract(epoch FROM finished_at) FROM {item}
WHERE finished_at IS NOT NULL ORDER BY batch_id, finished_at
"""


def snapshot_lags(snapshots: list[float], finishes: list[float], *, every: float) -> list[float]:
    """Отставание каждого снимка дерева от момента, когда он стал положен.

    Снимок положен не раньше ``предыдущий + every`` и не раньше первого изменения
    (завершения Item) после предыдущего снимка: ``lag = at - max(prev + every, first_change)``.
    ``snapshots`` и ``finishes`` отсортированы по времени.

    Returns:
        Отставание каждого снимка, секунды.
    """
    lags: list[float] = []
    previous: float | None = None
    for at in snapshots:
        start = bisect.bisect_right(finishes, previous) if previous is not None else 0
        first_change = finishes[start] if start < len(finishes) else at
        due = first_change if previous is None else max(previous + every, first_change)
        lags.append(max(0.0, at - due))
        previous = at
    return lags


async def _series(stand: Harness) -> tuple[dict[UUID, list[float]], dict[UUID, list[float]]]:
    progress_sql = sql(_PROGRESS, **stand.idents)
    finish_sql = sql(_FINISHES, **stand.idents)
    snapshots: dict[UUID, list[float]] = {}
    finishes: dict[UUID, list[float]] = {}
    async with stand.observer.connect() as connection:
        for row in await connection.execute(progress_sql):
            snapshots.setdefault(cast("UUID", row[0]), []).append(float(cast("float", row[1])))
        for row in await connection.execute(finish_sql):
            finishes.setdefault(cast("UUID", row[0]), []).append(float(cast("float", row[1])))
    return snapshots, finishes


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Деревья с медленными задачами; Snapshotter лидера (этот процесс) пишет снимки."""
    params = PARAMS[ctx.profile]

    def configure(config: AppConfig) -> AppConfig:
        return replace(config, step_seconds=params.step_seconds, progress_every=params.every)

    async with harness(
        ctx,
        name="p08",
        processes=params.processes,
        concurrency=params.concurrency,
        configure=configure,
        maintenance_interval=None,
    ) as stand:
        ctx.log(f"P-08: {params.trees} деревьев x {params.items} задач по {params.step_seconds} с")
        for tree in range(params.trees):
            async with stand.th.batch(
                KIND_PROGRESS, key=f"tree:{tree}", expected_total=params.items
            ) as batch:
                await batch.add_calls(
                    stand.th.call(stand.tasks.step, i) for i in range(params.items)
                )
        runner = stand.th.maintenance()
        cpu_started = time.process_time()
        wall_started = time.monotonic()
        task = asyncio.create_task(runner.run(), name="bench-p08-maintenance")
        try:
            _ = await stand.wait_roots(KIND_PROGRESS, within=params.within)
        finally:
            runner.stop()
            _ = await asyncio.wait([task])
        cores = (time.process_time() - cpu_started) / max(time.monotonic() - wall_started, 1e-9)
        snapshots, finishes = await _series(stand)
    lags: list[float] = []
    for batch_id, points in snapshots.items():
        lags.extend(snapshot_lags(points, finishes.get(batch_id, []), every=params.every))
    summary = summarize(lags)
    trees_with_snapshots = len(snapshots)
    result.parameters = {
        "trees": params.trees,
        "items_per_tree": params.items,
        "step_s": params.step_seconds,
        "every_s": params.every,
        "processes": params.processes,
        "concurrency": params.concurrency,
    }
    result.checks = [
        Check(
            "отставание снимков",
            f"≤ 1 интервала ({params.every:.0f} с), p99",
            prose(
                f"""
                p99 {summary.p99:.2f} с, p50 {summary.p50:.2f} с, max {summary.max:.2f} с
                ({summary.count} снимков, {trees_with_snapshots} деревьев)
                """
            ),
            summary.count > 0 and summary.p99 <= params.every,
        ),
        Check(
            "CPU лидера maintenance",
            f"< {LEADER_CORES:.0f} ядра",
            f"{cores:.2f} ядра (процесс producer целиком)",
            cores < LEADER_CORES,
        ),
    ]
    result.metrics = {
        "lag_s": dict(summary.as_dict()),
        "leader_cores": round(cores, 3),
        "snapshots": summary.count,
        "trees_with_snapshots": trees_with_snapshots,
    }
    result.tables.append(
        Table(
            "Отставание снимков, секунды",
            ("n", "p50", "p95", "p99", "max"),
            (
                (
                    str(summary.count),
                    f"{summary.p50:.2f}",
                    f"{summary.p95:.2f}",
                    f"{summary.p99:.2f}",
                    f"{summary.max:.2f}",
                ),
            ),
        )
    )
    result.charts.append(
        BarChart(
            "lag",
            "P-08: отставание снимков, с",
            "секунды",
            (("p50", summary.p50), ("p95", summary.p95), ("p99", summary.p99)),
            limit=params.every,
        )
    )
    result.notes.extend(
        (
            (
                prose(
                    """
                    Лидер maintenance (Snapshotter) — этот процесс: maintenance().run() с
                    настройками по умолчанию (snapshot_tick 500 мс). Хук on_progress пишет момент
                    вызова часами процесса.
                    """
                )
            ),
            (
                prose(
                    """
                    Отставание снимка — момент вызова хука минус момент, когда снимок стал положен:
                    не раньше предыдущего снимка + every и не раньше первого завершения Item после
                    него (без изменений снимок не делается).
                    """
                )
            ),
            (
                prose(
                    """
                    CPU — время процессора всего процесса producer за время работы maintenance,
                    делённое на длительность.
                    """
                )
            ),
        )
    )


SCENARIO: Final = Scenario(
    id="P-08",
    title="Снимки прогресса",
    measures="много активных деревьев с on_progress(every=2s): отставание снимков и CPU лидера",
    target="отставание снимков ≤ 1 интервала; CPU лидера < 1 ядра (10 000 деревьев)",
    run=run,
)
