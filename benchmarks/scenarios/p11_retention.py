"""P-11 — retention не блокирует рабочую нагрузку: p99 finish во время удаления истории."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.app import KIND_HISTORY
from benchmarks.charts import BarChart, LineChart, Series
from benchmarks.context import Scenario
from benchmarks.history import load_history
from benchmarks.load import SteadySpec, steady_load
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check
from benchmarks.scenarios.common import latency_table, oracle_check, rate_chart
from benchmarks.stand import ident, sql

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from benchmarks.app import AppConfig
    from benchmarks.context import RunContext
    from benchmarks.load import SteadyRun
    from benchmarks.report import ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "RetentionParams"]


@dataclass(frozen=True, slots=True)
class RetentionParams:
    """Нагрузка S1 и истёкшая история: деревьев x Items."""

    load: SteadySpec
    trees: int
    items_per_tree: int


PARAMS: Final = {
    Profile.SMOKE: RetentionParams(
        SteadySpec(
            processes=2, concurrency=20, duration=40, warmup=8, batch_size=200, in_flight=400
        ),
        trees=40,
        items_per_tree=1_000,
    ),
    Profile.NIGHTLY: RetentionParams(
        SteadySpec(
            processes=4, concurrency=50, duration=600, warmup=30, batch_size=1_000, in_flight=2_000
        ),
        trees=500,
        items_per_tree=10_000,
    ),
    # 50M Items истории (ACCEPTANCE §9 P-11).
    Profile.FULL: RetentionParams(
        SteadySpec(
            processes=8,
            concurrency=100,
            duration=1_800,
            warmup=120,
            batch_size=2_000,
            in_flight=8_000,
            window=10,
        ),
        trees=500,
        items_per_tree=100_000,
    ),
}
RATIO: Final = 1.2
SWEEP_INTERVAL: Final = 1.0
_WATCH_EVERY: Final = 2.0
_REMAINING: Final = "SELECT count(*) FROM {batch} WHERE kind = :kind"


async def _watch(engine: AsyncEngine, schema: str, remaining: list[tuple[float, int]]) -> None:
    """Сколько истёкших корней осталось: раз в 2 с, пока задачу не отменят."""
    started = time.monotonic()
    statement = sql(_REMAINING, batch=ident(schema, "th_batch"))
    async with engine.connect() as connection:
        while True:
            count = cast("int | None", await connection.scalar(statement, {"kind": KIND_HISTORY}))
            await connection.commit()
            remaining.append((time.monotonic() - started, count or 0))
            await asyncio.sleep(_WATCH_EVERY)


async def _phases(
    ctx: RunContext, params: RetentionParams, remaining: list[tuple[float, int]]
) -> tuple[SteadyRun, SteadyRun, float]:
    observer = create_async_engine(ctx.stand.dsn)

    def configure(config: AppConfig) -> AppConfig:
        return replace(config, sweep_interval=SWEEP_INTERVAL)

    try:
        async with steady_load(ctx, params.load, name="p11", configure=configure) as (load, names):
            ctx.log(f"P-11: база {params.load.duration:.0f} с")
            base = await load.run(params.load.duration)
            ctx.log(f"P-11: истёкшая история {params.trees * params.items_per_tree} Items")
            loaded_s = await load_history(
                observer,
                names.tallyho,
                trees=params.trees,
                items_per_tree=params.items_per_tree,
                retention=timedelta(seconds=1),
                finished_ago=timedelta(days=1),
            )
            watcher = asyncio.create_task(
                _watch(observer, names.tallyho, remaining), name="bench-p11-watch"
            )
            try:
                ctx.log(f"P-11: нагрузка во время retention {params.load.duration:.0f} с")
                during = await load.run(params.load.duration)
            finally:
                _ = watcher.cancel()
                _ = await asyncio.wait([watcher])
            await load.drain(during)
    finally:
        await observer.dispose()
    return base, during, loaded_s


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Фаза без retention, вставка истёкшей истории, фаза во время её удаления sweeper-ом."""
    params = PARAMS[ctx.profile]
    remaining: list[tuple[float, int]] = []
    base, during, loaded_s = await _phases(ctx, params, remaining)
    base_finish = base.worker_ops().summary("finish")
    during_finish = during.worker_ops().summary("finish")
    ratio = during_finish.p99 / base_finish.p99 if base_finish.p99 > 0 else math.nan
    purged = (remaining[0][1] - remaining[-1][1]) if remaining else 0
    result.parameters = {
        "processes": params.load.processes,
        "concurrency": params.load.concurrency,
        "phase_s": params.load.duration,
        "history_trees": params.trees,
        "history_items_per_tree": params.items_per_tree,
        "sweep_interval_s": SWEEP_INTERVAL,
    }
    result.checks = [
        Check(
            "p99 finish во время retention",
            f"≤ {RATIO}x без неё",
            f"{ratio:.2f}x ({during_finish.p99 * 1000:.1f} против {base_finish.p99 * 1000:.1f} мс)",
            ratio <= RATIO,
        ),
        Check(
            "retention шёл во время замера",
            "удалено хотя бы одно дерево",
            f"удалено {purged} из {params.trees} деревьев за фазу",
            purged > 0,
        ),
        oracle_check(during),
    ]
    result.metrics = {
        "finish_ms": {
            "base": dict(base_finish.as_dict(scale=1000)),
            "during": dict(during_finish.as_dict(scale=1000)),
        },
        "ratio": None if math.isnan(ratio) else round(ratio, 3),
        "history_load_s": round(loaded_s, 1),
        "history_trees_remaining": [[round(at, 1), count] for at, count in remaining],
        "throughput_per_s": {"base": round(base.steady(), 1), "during": round(during.steady(), 1)},
    }
    result.tables.append(latency_table("Операции во время retention (p50/p99)", during))
    result.charts.extend(
        (
            BarChart(
                "finish_p99",
                "P-11: p99 finish (complete_in + commit), мс",
                "мс",
                (
                    ("без retention", base_finish.p99 * 1000),
                    ("во время retention", during_finish.p99 * 1000),
                ),
                limit=base_finish.p99 * 1000 * RATIO,
            ),
            LineChart(
                "history_remaining",
                "P-11: осталось истёкших деревьев",
                "секунды фазы",
                "деревьев",
                (Series("деревьев", tuple((at, float(count)) for at, count in remaining)),),
            ),
            rate_chart(
                "throughput",
                "P-11: завершений в секунду",
                [("без retention", base), ("во время retention", during)],
                limit=None,
            ),
        )
    )
    result.notes.extend(
        (
            (
                prose(
                    f"""
                    Обе фазы — одна и та же нагрузка S1 без сна на одних схемах; между фазами
                    SQL-генератор вставляет истёкшую историю (retention 1 с, finished_at сутки
                    назад), и sweeper лидера (sweep_interval {SWEEP_INTERVAL:.0f} с) удаляет её по
                    дереву за проход.
                    """
                )
            ),
            "finish — длительность complete_in и commit доменной транзакции в задаче S1 (воркер).",
        )
    )


SCENARIO: Final = Scenario(
    id="P-11",
    title="Retention",
    measures="S1 без сна до и во время удаления истёкших деревьев sweeper-ом: p99 finish",
    target=(
        prose(
            """
            удаление деревьев на 50M Items не блокирует нагрузку: p99 finish во время retention ≤
            1,2x без неё
            """
        )
    ),
    run=run,
)
