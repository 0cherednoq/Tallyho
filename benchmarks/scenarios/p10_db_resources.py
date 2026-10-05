"""P-10 — ресурсы БД под P-02: WAL на завершение, рост th_counter/th_lease, autovacuum."""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.charts import LineChart, Series
from benchmarks.context import Scenario
from benchmarks.dbstats import TableSampler
from benchmarks.load import SteadySpec, steady_load
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.scenarios.common import oracle_check, rate_chart

if TYPE_CHECKING:
    from benchmarks.context import RunContext
    from benchmarks.dbstats import TableSample
    from benchmarks.load import SteadyRun
    from benchmarks.report import JsonValue, ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "DbParams", "monotonic_growth"]


@dataclass(frozen=True, slots=True)
class DbParams:
    """Нагрузка P-02 и период срезов БД."""

    load: SteadySpec
    interval: float


PARAMS: Final = {
    Profile.SMOKE: DbParams(
        SteadySpec(
            processes=2, concurrency=20, duration=60, warmup=10, batch_size=200, in_flight=400
        ),
        interval=5,
    ),
    Profile.NIGHTLY: DbParams(
        SteadySpec(
            processes=4, concurrency=50, duration=600, warmup=30, batch_size=1_000, in_flight=2_000
        ),
        interval=30,
    ),
    Profile.FULL: DbParams(
        SteadySpec(
            processes=8,
            concurrency=100,
            duration=3_600,
            warmup=120,
            batch_size=2_000,
            in_flight=8_000,
            window=10,
        ),
        interval=60,
    ),
}
_GROWTH_SLACK: Final = 1_000
_MIN_POINTS: Final = 3
"""По двум срезам тренд не виден."""
_AUTOVACUUM_TABLES: Final = ("th_counter", "th_lease", "th_outbox")
_MB: Final = 1_048_576
_HEADERS: Final = (
    "таблица",
    "живых строк",
    "мёртвых строк",
    "размер, МБ",
    "pgstattuple dead %",
    "autovacuum",
)


def monotonic_growth(series: list[TableSample]) -> bool:
    """Мёртвые строки только росли (ни одного снижения) и выросли больше чем на 1 000.

    Returns:
        Растут ли мёртвые строки монотонно.
    """
    if len(series) < _MIN_POINTS:  # тренд по двум точкам не виден
        return False
    pairs = itertools.pairwise(series)
    return (
        all(b.dead >= a.dead for a, b in pairs) and series[-1].dead > series[0].dead + _GROWTH_SLACK
    )


def _mb(value: int) -> str:
    return f"{value / _MB:.1f}"


async def _observe(ctx: RunContext, params: DbParams) -> tuple[SteadyRun, TableSampler]:
    observer = create_async_engine(ctx.stand.dsn)
    try:
        async with steady_load(ctx, params.load, name="p10") as (load, names):
            sampler = TableSampler(observer, names.tallyho, params.interval)
            origin = time.monotonic()
            await sampler.start(origin)
            ctx.log(f"P-10: {params.load.duration:.0f} с, срез раз в {params.interval:.0f} с")
            try:
                steady = await load.run(params.load.duration)
            finally:
                await sampler.stop(origin)
            await load.drain(steady)
    finally:
        await observer.dispose()
    return steady, sampler


def _wal_per_completion(
    steady: SteadyRun, sampler: TableSampler, warmup: float
) -> tuple[float, int]:
    points = steady.done_points
    start = next(((at, done) for at, done in points if at >= warmup), points[0])
    wal_start = min(sampler.wal, key=lambda point: abs(point[0] - start[0]))
    completions = max(points[-1][1] - start[1], 1)
    return (sampler.wal[-1][1] - wal_start[1]) / completions, completions


def _table_row(table: str, series: list[TableSample]) -> tuple[str, ...]:
    first, last = series[0], series[-1]
    dead_pct = (
        "—"
        if last.dead_tuple_percent is None or first.dead_tuple_percent is None
        else f"{first.dead_tuple_percent:.1f} → {last.dead_tuple_percent:.1f}"
    )
    return (
        table,
        f"{first.live} → {last.live}",
        f"{first.dead} → {last.dead} (max {max(sample.dead for sample in series)})",
        f"{_mb(first.size_bytes)} → {_mb(last.size_bytes)}",
        dead_pct,
        str(last.autovacuums - first.autovacuums),
    )


def _series_json(series: list[TableSample]) -> list[JsonValue]:
    return [
        {
            "at": round(sample.at, 1),
            "live": sample.live,
            "dead": sample.dead,
            "size_bytes": sample.size_bytes,
            "dead_tuple_percent": sample.dead_tuple_percent,
            "free_percent": sample.free_percent,
        }
        for sample in series
    ]


def _charts(steady: SteadyRun, sampler: TableSampler) -> tuple[LineChart, ...]:
    wal_origin = sampler.wal[0][1]
    return (
        LineChart(
            "dead_tuples",
            "P-10: мёртвые строки",
            "секунды прогона",
            "строк",
            tuple(
                Series(table, tuple((s.at, float(s.dead)) for s in sampler.series(table)))
                for table in _AUTOVACUUM_TABLES
            ),
        ),
        LineChart(
            "wal",
            "P-10: WAL с начала прогона",
            "секунды прогона",
            "МБ",
            (Series("WAL", tuple((at, (lsn - wal_origin) / _MB) for at, lsn in sampler.wal)),),
        ),
        rate_chart("throughput", "P-10: завершений в секунду", [("S1", steady)], limit=None),
    )


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """P-02 под наблюдением: WAL, pg_stat_user_tables, pgstattuple."""
    params = PARAMS[ctx.profile]
    steady, sampler = await _observe(ctx, params)
    wal_per, completions = _wal_per_completion(steady, sampler, params.load.warmup)
    growing = [table for table in _AUTOVACUUM_TABLES if monotonic_growth(sampler.series(table))]
    result.checks = [
        Check("WAL на завершение", "в отчёте (без порога)", f"{wal_per / 1024:.1f} КБ", None),
        Check(
            "autovacuum успевает",
            "мёртвые строки th_counter/th_lease/th_outbox не растут монотонно",
            "растут: " + ", ".join(growing) if growing else "монотонного роста нет",
            not growing,
        ),
        Check(
            "pgstattuple",
            "th_counter, th_lease",
            "есть" if sampler.pgstattuple else f"недоступен: {sampler.pgstattuple_error}",
            None,
        ),
        oracle_check(steady),
    ]
    tables = sorted({sample.table for sample in sampler.tables})
    result.parameters = {
        "processes": params.load.processes,
        "concurrency": params.load.concurrency,
        "duration_s": params.load.duration,
        "sample_interval_s": params.interval,
    }
    result.metrics = {
        "wal_bytes_per_completion": round(wal_per, 1),
        "completions": completions,
        "steady_per_s": round(steady.steady(), 1),
        "wal": [[round(at, 1), lsn] for at, lsn in sampler.wal],
        "tables": {table: _series_json(sampler.series(table)) for table in tables},
    }
    result.tables.append(
        Table(
            "Таблицы tallyho (начало → конец)",
            _HEADERS,
            tuple(_table_row(table, sampler.series(table)) for table in tables),
        )
    )
    result.charts.extend(_charts(steady, sampler))
    result.notes.append(
        prose(
            """
            WAL на завершение — прирост pg_current_wal_lsn между концом прогрева и концом прогона,
            делённый на число завершённых Items за это время (WAL всей БД, включая flexiq).
            """
        )
    )


SCENARIO: Final = Scenario(
    id="P-10",
    title="Ресурсы БД",
    measures=(
        "P-02 под наблюдением: WAL на завершение, th_counter/th_lease (pgstattuple), autovacuum"
    ),
    target="WAL на завершение и рост th_counter/th_lease — в отчёте; autovacuum успевает",
    run=run,
)
