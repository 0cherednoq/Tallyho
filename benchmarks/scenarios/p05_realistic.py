"""P-05 — реалистичная нагрузка: «сеть» 1-5 с в каждой задаче, toxiproxy перед PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final

from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.context import Scenario
from benchmarks.dbstats import LockSampler
from benchmarks.load import SteadySpec, steady_load
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check
from benchmarks.scenarios.common import latency_table, oracle_check, rate_chart, run_metrics

if TYPE_CHECKING:
    from benchmarks.app import AppConfig
    from benchmarks.context import RunContext
    from benchmarks.load import SteadyRun
    from benchmarks.report import ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "RealisticOutcome", "RealisticParams", "realistic_run"]


@dataclass(frozen=True, slots=True)
class RealisticParams:
    """Нагрузка и «сеть»: задача спит ``uniform(sleep_min, sleep_max)`` секунд."""

    load: SteadySpec
    sleep_min: float
    sleep_max: float
    proxy_latency_ms: int = 2
    proxy_jitter_ms: int = 1

    @property
    def ideal(self) -> float:
        """Потолок: параллельность / средний сон, задач в секунду."""
        return self.load.parallelism / ((self.sleep_min + self.sleep_max) / 2)


PARAMS: Final = {
    # smoke: сон x0,5 (0,5-2,5 с), параллельность 40 → потолок ≈ 27 задач/с.
    Profile.SMOKE: RealisticParams(
        SteadySpec(
            processes=2, concurrency=20, duration=45, warmup=12, batch_size=40, in_flight=120
        ),
        0.5,
        2.5,
    ),
    Profile.NIGHTLY: RealisticParams(
        SteadySpec(
            processes=4, concurrency=50, duration=300, warmup=30, batch_size=200, in_flight=600
        ),
        1.0,
        5.0,
    ),
    Profile.FULL: RealisticParams(
        SteadySpec(
            processes=8,
            concurrency=100,
            duration=900,
            warmup=60,
            batch_size=400,
            in_flight=2_400,
            window=10,
        ),
        1.0,
        5.0,
    ),
}
SHARE: Final = 0.9


@dataclass(frozen=True, slots=True)
class RealisticOutcome:
    """Прогон S1 со «сетью»: сам прогон, был ли toxiproxy, доля ожиданий блокировок tallyho."""

    run: SteadyRun
    proxied: bool
    lock_share: float | None


async def realistic_run(
    ctx: RunContext,
    params: RealisticParams,
    *,
    name: str,
    hold_share: float = 0.0,
    sample_locks: bool = False,
) -> RealisticOutcome:
    """Прогон S1 со «сетью» (и долгими транзакциями при ``hold_share``).

    Returns:
        Прогон, был ли toxiproxy и доля ожиданий блокировок.
    """

    def configure(config: AppConfig) -> AppConfig:
        return replace(
            config,
            sleep_min=params.sleep_min,
            sleep_max=params.sleep_max,
            hold_share=hold_share,
        )

    proxy = await ctx.stand.toxiproxy() if ctx.stand.owned else None
    observer = create_async_engine(ctx.stand.dsn)
    try:
        if proxy is not None:
            await proxy.latency(
                latency_ms=params.proxy_latency_ms, jitter_ms=params.proxy_jitter_ms
            )
        async with steady_load(
            ctx,
            params.load,
            name=name,
            configure=configure,
            dsn=None if proxy is None else proxy.dsn,
        ) as (load, names):
            ctx.log(
                prose(
                    f"""
                    {name}: {params.load.processes} x {params.load.concurrency}, сон
                    {params.sleep_min}-{params.sleep_max} с, долгих транзакций {hold_share:.0%}
                    """
                )
            )
            sampler = LockSampler(observer, names.tallyho) if sample_locks else None
            if sampler is not None:
                sampler.start()
            try:
                run = await load.run(params.load.duration)
            finally:
                if sampler is not None:
                    await sampler.stop()
            await load.drain(run)
    finally:
        await observer.dispose()
        await ctx.stand.remove_proxies()
    return RealisticOutcome(run, proxy is not None, None if sampler is None else sampler.share)


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """S1 со «сетью»; цель — доля от потолка ``параллельность / средний сон``."""
    params = PARAMS[ctx.profile]
    outcome = await realistic_run(ctx, params, name="p05")
    steady, proxied = outcome.run, outcome.proxied
    rate = steady.steady()
    share = rate / params.ideal
    result.parameters = {
        "processes": params.load.processes,
        "concurrency": params.load.concurrency,
        "sleep_s": [params.sleep_min, params.sleep_max],
        "duration_s": params.load.duration,
        "toxiproxy_latency_ms": params.proxy_latency_ms if proxied else None,
    }
    result.checks = [
        Check(
            "доля от потолка «параллельность / средний сон»",
            f"≥ {SHARE:.0%} (потолок {params.ideal:.1f} задач/с)",
            f"{share:.1%} ({rate:.1f} задач/с)",
            share >= SHARE,
        ),
        oracle_check(steady),
    ]
    result.metrics = {"ideal_per_s": round(params.ideal, 2), "share": round(share, 4)}
    result.metrics.update(run_metrics(steady))
    result.tables.append(latency_table("Операции (p50/p99)", steady))
    result.charts.append(
        rate_chart(
            "throughput",
            "P-05: завершений в секунду",
            [("S1 + сеть", steady)],
            limit=params.ideal * SHARE,
        )
    )
    result.notes.append(
        prose(
            f"""
            toxiproxy с задержкой {params.proxy_latency_ms}±{params.proxy_jitter_ms} мс стоит между
            всеми процессами и PostgreSQL.
            """
        )
        if proxied
        else "Внешний DSN: toxiproxy не поднимался, задержки только в задачах."
    )


SCENARIO: Final = Scenario(
    id="P-05",
    title="Реалистичная нагрузка",
    measures="S1 с «сетью» 1-5 с в задаче и toxiproxy перед PostgreSQL",
    target="≥ 90% от «параллельность / средний сон» (≈ 240 из 267 задач/с на 8 x 100)",
    run=run,
)
