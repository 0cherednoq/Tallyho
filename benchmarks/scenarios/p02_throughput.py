"""P-02 — абсолютная пропускная способность: S1 без сна, устойчиво, без роста очереди Completer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from benchmarks.charts import LineChart, Series
from benchmarks.context import Scenario
from benchmarks.load import SteadySpec, steady_load
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check
from benchmarks.scenarios.common import (
    buffer_growth,
    latency_table,
    oracle_check,
    rate_chart,
    run_metrics,
    windowed_max,
)

if TYPE_CHECKING:
    from benchmarks.context import RunContext
    from benchmarks.report import ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "TARGET_PER_S"]

PARAMS: Final = {
    Profile.SMOKE: SteadySpec(
        processes=2, concurrency=20, duration=30, warmup=8, batch_size=200, in_flight=400
    ),
    Profile.NIGHTLY: SteadySpec(
        processes=4, concurrency=50, duration=300, warmup=30, batch_size=1_000, in_flight=2_000
    ),
    Profile.FULL: SteadySpec(
        processes=8,
        concurrency=100,
        duration=1_800,
        warmup=120,
        batch_size=2_000,
        in_flight=8_000,
        window=10,
    ),
}
TARGET_PER_S: Final = 5_000.0
GROWTH: Final = 1.5
GROWTH_SLACK: Final = 10.0


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Держать S1 без сна ``duration`` секунд, затем дождаться финализации и оракула."""
    spec = PARAMS[ctx.profile]
    result.parameters = {
        "processes": spec.processes,
        "concurrency": spec.concurrency,
        "duration_s": spec.duration,
        "warmup_s": spec.warmup,
        "batch_size": spec.batch_size,
        "in_flight": spec.in_flight,
    }
    async with steady_load(ctx, spec, name="p02") as (load, _):
        ctx.log(f"P-02: {spec.processes} x {spec.concurrency}, {spec.duration:.0f} с")
        steady = await load.run(spec.duration)
        await load.drain(steady)
    rate = steady.steady()
    first, second = buffer_growth(steady)
    result.checks = [
        Check(
            "устойчивая пропускная способность",
            f"≥ {TARGET_PER_S:,.0f} завершений/с".replace(",", " "),
            f"{rate:.0f} завершений/с",
            rate >= TARGET_PER_S,
        ),
        Check(
            "очередь Completer не растёт",
            f"медиана максимумов буфера во 2-й половине ≤ {GROWTH} x 1-й (+{GROWTH_SLACK:.0f})",
            f"{first:.0f} → {second:.0f} Items",
            second <= first * GROWTH + GROWTH_SLACK,
        ),
        oracle_check(steady),
    ]
    result.metrics = run_metrics(steady)
    result.tables.append(latency_table("Операции (p50/p99)", steady))
    result.charts.extend(
        (
            rate_chart(
                "throughput", "P-02: завершений в секунду", [("S1", steady)], limit=TARGET_PER_S
            ),
            LineChart(
                "completer_buffer",
                "P-02: буфер Completer (максимум за окно по воркерам)",
                "секунды прогона",
                "Items",
                (
                    Series(
                        "буфер",
                        tuple(
                            windowed_max(
                                [(at, float(items)) for at, items in steady.buffers()],
                                window=spec.window,
                            )
                        ),
                    ),
                ),
            ),
        )
    )
    result.notes.append(
        prose(
            f"""
            S1 «без сна»: задача пишет доменную строку и завершает Item в одной транзакции
            (complete_in), без задержек «сети» и удержания транзакций. Producer держит в работе не
            больше {spec.in_flight} Items, новые батчи — по {spec.batch_size}.
            """
        )
    )


SCENARIO: Final = Scenario(
    id="P-02",
    title="Абсолютная пропускная способность",
    measures="S1 без сна: устойчивые завершения в секунду и буфер Completer",
    target=(
        "≥ 5 000 завершений/с устойчиво (8 процессов x 100, 30 мин); без роста очереди Completer"
    ),
    run=run,
)
