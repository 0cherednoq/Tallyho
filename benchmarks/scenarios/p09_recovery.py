"""P-09 — восстановление пропускной способности после kill -9 воркера и падения PostgreSQL."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from benchmarks.context import Scenario
from benchmarks.load import SteadySpec, steady_load
from benchmarks.metrics import steady_rate
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.scenarios.common import oracle_check, rate_chart, run_metrics

if TYPE_CHECKING:
    from collections.abc import Sequence

    from benchmarks.context import RunContext
    from benchmarks.load import Action
    from benchmarks.report import ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "RecoveryParams", "recovery_time"]


@dataclass(frozen=True, slots=True)
class RecoveryParams:
    """Нагрузка P-02 и расписание отказов (секунды от начала прогона)."""

    load: SteadySpec
    kill_worker_at: float
    worker_down: float
    kill_postgres_at: float
    postgres_down: float


PARAMS: Final = {
    Profile.SMOKE: RecoveryParams(
        SteadySpec(
            processes=2, concurrency=20, duration=120, warmup=10, batch_size=200, in_flight=400
        ),
        kill_worker_at=30,
        worker_down=1,
        kill_postgres_at=65,
        postgres_down=5,
    ),
    Profile.NIGHTLY: RecoveryParams(
        SteadySpec(
            processes=4, concurrency=50, duration=480, warmup=30, batch_size=1_000, in_flight=2_000
        ),
        kill_worker_at=120,
        worker_down=5,
        kill_postgres_at=280,
        postgres_down=20,
    ),
    Profile.FULL: RecoveryParams(
        SteadySpec(
            processes=8,
            concurrency=100,
            duration=1_500,
            warmup=120,
            batch_size=2_000,
            in_flight=8_000,
            window=10,
        ),
        kill_worker_at=400,
        worker_down=10,
        kill_postgres_at=900,
        postgres_down=30,
    ),
}
SHARE: Final = 0.9
# ACCEPTANCE §4: T_rec = lease_ttl + 2 x sweep_interval + 30 с при настройках по умолчанию.
T_REC: Final = 60 + 2 * 5 + 30


def recovery_time(
    rates: Sequence[tuple[float, float]], *, since: float, baseline: float, share: float = SHARE
) -> float | None:
    """Секунды от ``since`` до первого окна, с которого скорость держится ≥ ``share`` базы.

    «Держится» — это окно и следующее за ним (последнее окно прогона засчитывается одно).
    ``None`` — до конца прогона не восстановилась.

    Returns:
        Секунды или ``None``.
    """
    after = [(at, rate) for at, rate in rates if at > since]
    threshold = baseline * share
    for index, (at, rate) in enumerate(after):
        following = after[index + 1 : index + 2]
        if rate >= threshold and all(value >= threshold for _, value in following):
            return at - since
    return None


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """База до отказов, затем kill -9 воркера и docker kill PostgreSQL."""
    params = PARAMS[ctx.profile]
    events: dict[str, float] = {}

    async with steady_load(ctx, params.load, name="p09") as (load, _):
        _, pool = load.require()

        async def kill_worker() -> None:
            ctx.log("P-09: kill -9 воркера 0")
            await pool.kill(0)
            await asyncio.sleep(params.worker_down)
            await pool.respawn(0)
            events["worker_back"] = time.time()

        async def kill_postgres() -> None:
            ctx.log("P-09: docker kill PostgreSQL")
            await ctx.stand.restart_postgres(down_seconds=params.postgres_down)
            events["postgres_back"] = time.time()

        actions: list[Action] = [(params.kill_worker_at, kill_worker)]
        if ctx.stand.owned:
            actions.append((params.kill_postgres_at, kill_postgres))
        ctx.log(
            prose(
                f"""
                P-09: {params.load.processes} x {params.load.concurrency},
                {params.load.duration:.0f} с
                """
            )
        )
        steady = await load.run(params.load.duration, actions=tuple(actions), tolerate_outage=True)
        await load.drain(steady)
    rates = steady.rates()
    baseline = steady_rate(
        [(at, rate) for at, rate in rates if at <= params.kill_worker_at], warmup=params.load.warmup
    )
    rows: list[tuple[str, ...]] = []
    checks: list[Check] = []
    for label, key in (
        ("A-CH-01: kill -9 воркера", "worker_back"),
        ("A-CH-02: docker kill PostgreSQL", "postgres_back"),
    ):
        if key not in events:
            checks.append(Check(label, f"≤ T_rec = {T_REC} с", "не выполнялся (внешний DSN)", None))
            continue
        since = events[key] - steady.wall_origin
        took = recovery_time(rates, since=since, baseline=baseline)
        measured = (
            f"{took:.0f} с до ≥ {SHARE:.0%} базы"
            if took is not None
            else f"не восстановилась за {params.load.duration - since:.0f} с"
        )
        checks.append(
            Check(label, f"≤ T_rec = {T_REC} с", measured, took is not None and took <= T_REC)
        )
        rows.append((label, f"{since:.0f}", measured))
    checks.append(oracle_check(steady))
    result.checks = checks
    result.parameters = {
        "processes": params.load.processes,
        "concurrency": params.load.concurrency,
        "duration_s": params.load.duration,
        "kill_worker_at_s": params.kill_worker_at,
        "kill_postgres_at_s": params.kill_postgres_at,
        "postgres_down_s": params.postgres_down,
    }
    result.metrics = {"baseline_per_s": round(baseline, 1), "outage_ticks": steady.outage_ticks}
    result.metrics.update(run_metrics(steady))
    result.tables.append(
        Table("Отказы", ("отказ", "восстановление начато, с", "результат"), tuple(rows))
    )
    result.charts.append(
        rate_chart(
            "throughput",
            "P-09: завершений в секунду",
            [("S1", steady)],
            limit=baseline * SHARE,
        )
    )
    result.notes.append(
        prose(
            f"""
            База — устойчивая скорость до первого отказа ({baseline:.0f} завершений/с). Время
            восстановления считается от момента, когда воркер поднят заново / PostgreSQL снова
            принимает соединения, до первого окна, с которого скорость держится ≥ 90% базы.
            """
        )
    )


SCENARIO: Final = Scenario(
    id="P-09",
    title="Восстановление",
    measures=(
        "S1 без сна: kill -9 воркера (A-CH-01) и docker kill PostgreSQL (A-CH-02) под нагрузкой"
    ),
    target="возврат к ≥ 90% пропускной способности ≤ T_rec после восстановления",
    run=run,
)
