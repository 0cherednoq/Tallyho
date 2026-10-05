"""P-03 — масштабирование по числу процессов-воркеров."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final

from benchmarks.charts import LineChart, Series
from benchmarks.context import Scenario
from benchmarks.load import SteadySpec, steady_load
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.scenarios.common import oracle_check
from benchmarks.stand import CpuSampler

if TYPE_CHECKING:
    from benchmarks.context import RunContext
    from benchmarks.report import JsonValue, ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "ScalingParams"]


@dataclass(frozen=True, slots=True)
class ScalingParams:
    """Шаги по числу процессов и нагрузка одного шага."""

    steps: tuple[int, ...]
    step: SteadySpec


PARAMS: Final = {
    Profile.SMOKE: ScalingParams(
        (1, 2),
        SteadySpec(
            processes=1, concurrency=20, duration=20, warmup=5, batch_size=100, in_flight=200
        ),
    ),
    Profile.NIGHTLY: ScalingParams(
        (1, 2, 4),
        SteadySpec(
            processes=1, concurrency=50, duration=120, warmup=20, batch_size=500, in_flight=1_000
        ),
    ),
    Profile.FULL: ScalingParams(
        (1, 2, 4, 8, 16),
        SteadySpec(
            processes=1, concurrency=100, duration=300, warmup=60, batch_size=1_000, in_flight=2_000
        ),
    ),
}
EFFICIENCY: Final = 0.7
CPU_LIMIT: Final = 0.8


@dataclass(frozen=True, slots=True)
class _Step:
    processes: int
    rate: float
    cpu: float | None
    oracle_ok: bool
    oracle: str


async def _step(ctx: RunContext, base: SteadySpec, processes: int) -> _Step:
    spec = replace(
        base,
        processes=processes,
        batch_size=base.batch_size * processes,
        in_flight=base.in_flight * processes,
    )
    sampler = None if ctx.stand.container is None else CpuSampler(ctx.stand.container)
    async with steady_load(ctx, spec, name=f"p03_{processes}") as (load, _):
        ctx.log(f"P-03: {processes} процесс(ов) x {spec.concurrency}, {spec.duration:.0f} с")
        origin = time.monotonic()
        if sampler is not None:
            await sampler.start(origin)
        try:
            run = await load.run(spec.duration)
        finally:
            if sampler is not None:
                await sampler.stop()
        await load.drain(run)
    cpu = None if sampler is None else sampler.mean_between(spec.warmup, spec.duration)
    check = oracle_check(run)
    return _Step(processes, run.steady(), cpu, bool(check.passed), check.measured)


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Прогнать шаги на свежих схемах; эффективность — к линейному росту от 1 процесса."""
    params = PARAMS[ctx.profile]
    result.parameters = {
        "steps": list(params.steps),
        "concurrency": params.step.concurrency,
        "duration_s": params.step.duration,
    }
    steps = [await _step(ctx, params.step, processes) for processes in params.steps]
    base = steps[0].rate
    rows: list[tuple[str, ...]] = []
    applicable: list[bool] = []
    worst = math.inf
    for step in steps:
        efficiency = step.rate / (base * step.processes) if base > 0 else 0.0
        bound = step.cpu is not None and step.cpu >= CPU_LIMIT
        if step.processes > steps[0].processes and not bound:
            applicable.append(efficiency >= EFFICIENCY)
            worst = min(worst, efficiency)
        rows.append(
            (
                str(step.processes),
                f"{step.rate:.0f}",
                f"{efficiency:.2f}",
                "—" if step.cpu is None else f"{step.cpu:.0%}",
                step.oracle,
            )
        )
    cpu_known = all(step.cpu is not None for step in steps)
    result.checks = [
        Check(
            "рост к линейному",
            f"≥ {EFFICIENCY} от линейного, пока CPU PostgreSQL < {CPU_LIMIT:.0%}",
            f"худший шаг {worst:.2f}" if applicable else "нет шагов ниже порога CPU",
            all(applicable) if applicable else None,
        ),
        Check(
            "CPU PostgreSQL известен",
            "docker stats своего контейнера",
            "да" if cpu_known else "нет (внешний DSN) — порог CPU не учитывается",
            None,
        ),
        Check(
            "корректность (оракул на выборке)",
            "на каждом шаге",
            "нарушений нет" if all(step.oracle_ok for step in steps) else "есть нарушения",
            all(step.oracle_ok for step in steps),
        ),
    ]
    steps_json: list[JsonValue] = [
        {"processes": step.processes, "steady_per_s": round(step.rate, 1), "pg_cpu": step.cpu}
        for step in steps
    ]
    result.metrics = {"steps": steps_json}
    result.tables.append(
        Table(
            "Шаги",
            ("процессов", "завершений/с", "доля линейного", "CPU PostgreSQL", "оракул"),
            tuple(rows),
        )
    )
    result.charts.append(
        LineChart(
            "scaling",
            "P-03: завершений в секунду от числа процессов",
            "процессов",
            "завершений/с",
            (
                Series("замер", tuple((float(s.processes), s.rate) for s in steps)),
                Series("линейный", tuple((float(s.processes), base * s.processes) for s in steps)),
                Series(
                    f"{EFFICIENCY} линейного",
                    tuple((float(s.processes), EFFICIENCY * base * s.processes) for s in steps),
                ),
            ),
        )
    )
    result.notes.append(
        prose(
            """
            Каждый шаг — нагрузка S1 без сна на свежих схемах; batch_size и in_flight растут
            пропорционально числу процессов. CPU PostgreSQL — доля всех ядер Docker по docker stats.
            """
        )
    )


SCENARIO: Final = Scenario(
    id="P-03",
    title="Масштабирование по воркерам",
    measures="S1 без сна на 1 → 2 → 4 → 8 → 16 процессах: доля линейного роста",
    target="рост ≥ 0,7 от линейного, пока CPU PostgreSQL < 80%",
    run=run,
)
