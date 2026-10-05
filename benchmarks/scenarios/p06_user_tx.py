"""P-06 — путь «завершение в транзакции пользователя» с долгими транзакциями."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from benchmarks.context import Scenario
from benchmarks.prose import prose
from benchmarks.report import Check
from benchmarks.scenarios.common import latency_table, oracle_check, rate_chart, run_metrics
from benchmarks.scenarios.p05_realistic import PARAMS as P05_PARAMS
from benchmarks.scenarios.p05_realistic import realistic_run

if TYPE_CHECKING:
    from benchmarks.context import RunContext
    from benchmarks.report import ScenarioResult

__all__ = ["HOLD_SHARE", "SCENARIO"]

HOLD_SHARE: Final = 0.2
LOCK_SHARE: Final = 0.01
THROUGHPUT_SHARE: Final = 0.8


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Сначала P-05 как база, затем то же с 20% транзакций, открытых ещё одну «сеть»."""
    params = P05_PARAMS[ctx.profile]
    base = await realistic_run(ctx, params, name="p06_base")
    held = await realistic_run(
        ctx, params, name="p06_hold", hold_share=HOLD_SHARE, sample_locks=True
    )
    base_rate = base.run.steady()
    held_rate = held.run.steady()
    share = held_rate / base_rate if base_rate else 0.0
    lock_share = held.lock_share if held.lock_share is not None else 0.0
    result.parameters = {
        "processes": params.load.processes,
        "concurrency": params.load.concurrency,
        "sleep_s": [params.sleep_min, params.sleep_max],
        "hold_share": HOLD_SHARE,
        "duration_s": params.load.duration,
    }
    result.checks = [
        Check(
            "ожидания блокировок на таблицах tallyho",
            f"< {LOCK_SHARE:.0%} времени",
            f"{lock_share:.2%} backend-времени",
            lock_share < LOCK_SHARE,
        ),
        Check(
            "пропускная способность к P-05",
            f"≥ {THROUGHPUT_SHARE:.0%}",
            f"{share:.1%} ({held_rate:.1f} из {base_rate:.1f} задач/с)",
            share >= THROUGHPUT_SHARE,
        ),
        oracle_check(held.run),
    ]
    result.metrics = {
        "lock_wait_share": round(lock_share, 5),
        "throughput_share": round(share, 4),
        "base": run_metrics(base.run),
        "hold": run_metrics(held.run),
    }
    result.tables.append(latency_table("Операции с долгими транзакциями (p50/p99)", held.run))
    result.charts.append(
        rate_chart(
            "throughput",
            "P-06: завершений в секунду",
            [("P-05 (база)", base.run), ("20% долгих транзакций", held.run)],
            limit=base_rate * THROUGHPUT_SHARE,
        )
    )
    result.notes.extend(
        (
            (
                prose(
                    """
                    Задача S1 открывает доменную транзакцию; у 20% задач (детерминированно от seed)
                    транзакция остаётся открытой ещё одну «сеть», затем пишет строку и вызывает
                    complete_in — как в эталонном приложении ACCEPTANCE §3.1.
                    """
                )
            ),
            (
                prose(
                    """
                    Ожидания блокировок: раз в 100 мс считаются не-idle backend'ы БД с
                    wait_event_type = 'Lock' в запросе к схеме tallyho, делённые на все не-idle
                    backend'ы.
                    """
                )
            ),
            (
                prose(
                    """
                    Часть падения пропускной способности заложена самой нагрузкой: удержание
                    удлиняет 20% задач на одну «сеть» (≈ x1,2 к среднему времени задачи).
                    """
                )
            ),
        )
    )


SCENARIO: Final = Scenario(
    id="P-06",
    title="Завершение в транзакции пользователя",
    measures=(
        prose(
            """
            S1 с complete_in и 20% транзакций, открытых 1-5 с: ожидания блокировок и пропускная
            способность
            """
        )
    ),
    target=(
        "ожидания блокировок на таблицах tallyho < 1% времени; пропускная способность ≥ 80% от P-05"
    ),
    run=run,
)
