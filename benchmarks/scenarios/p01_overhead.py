"""P-01 — накладные расходы tallyho к «голому» flexiq с той же конфигурацией."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.charts import BarChart
from benchmarks.context import Scenario
from benchmarks.overhead import (
    FlexiqVariant,
    OverheadSpec,
    TallyhoVariant,
    measure,
    median_run,
)
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.stand import schemas

if TYPE_CHECKING:
    from benchmarks.context import RunContext
    from benchmarks.overhead import (
        LoadVariant,
        RunMeasurement,
    )
    from benchmarks.report import JsonValue, ScenarioResult
    from benchmarks.stand import Schemas

__all__ = ["PARAMS", "SCENARIO", "run_measurement_json"]

PARAMS: Final = {
    Profile.SMOKE: OverheadSpec(
        tasks=1_000, processes=2, concurrency=20, warmup=100, repeats=1, timeout=600
    ),
    Profile.NIGHTLY: OverheadSpec(
        tasks=10_000, processes=4, concurrency=50, warmup=1_000, repeats=3, timeout=1_800
    ),
    Profile.FULL: OverheadSpec(
        tasks=100_000, processes=8, concurrency=100, warmup=5_000, repeats=3, timeout=7_200
    ),
}
THROUGHPUT_SHARE: Final = 0.8
ADDED_P99_MS: Final = 50.0


def run_measurement_json(run: RunMeasurement) -> dict[str, JsonValue]:
    """Повтор в JSON отчёта (мс для задержек).

    Returns:
        Словарь повтора.
    """
    return {
        "variant": run.variant,
        "repeat": run.repeat,
        "tasks": run.tasks,
        "enqueue_s": round(run.enqueue_s, 3),
        "total_s": round(run.total_s, 3),
        "finalize_s": None if run.finalize_s is None else round(run.finalize_s, 3),
        "throughput_per_s": round(run.throughput, 1),
        "latency_ms": dict(run.latency.as_dict(scale=1000)),
        "dispatch_ms": dict(run.dispatch.as_dict(scale=1000)),
        "service_ms": dict(run.service.as_dict(scale=1000)),
    }


def _row(run: RunMeasurement) -> tuple[str, ...]:
    finalize = "—" if run.finalize_s is None else f"{run.finalize_s:.2f}"
    return (
        run.variant,
        str(run.tasks),
        f"{run.enqueue_s:.2f}",
        f"{run.total_s:.2f}",
        finalize,
        f"{run.throughput:.0f}",
        f"{run.latency.p50 * 1000:.0f} / {run.latency.p99 * 1000:.0f}",
        f"{run.dispatch.p50 * 1000:.1f} / {run.dispatch.p99 * 1000:.1f}",
        f"{run.service.p50 * 1000:.1f} / {run.service.p99 * 1000:.1f}",
    )


async def _variant_runs(
    ctx: RunContext, variant_type: type[FlexiqVariant | TallyhoVariant], spec: OverheadSpec
) -> list[RunMeasurement]:
    engine = create_async_engine(ctx.stand.dsn)
    try:
        async with schemas(engine, "p01") as names:
            variant: LoadVariant = _make(variant_type, ctx.stand.dsn, names)
            ctx.log(f"P-01: вариант {variant.name}, {spec.repeats} x {spec.tasks} задач")
            return await measure(variant, spec, ctx.out / variant.name)
    finally:
        await engine.dispose()


def _make(
    variant_type: type[FlexiqVariant | TallyhoVariant], dsn: str, names: Schemas
) -> LoadVariant:
    return variant_type(dsn, names)


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Оба варианта по очереди на свежих схемах одной БД."""
    spec = PARAMS[ctx.profile]
    result.parameters = {
        "tasks": spec.tasks,
        "processes": spec.processes,
        "concurrency": spec.concurrency,
        "warmup": spec.warmup,
        "repeats": spec.repeats,
    }
    flexiq_runs = await _variant_runs(ctx, FlexiqVariant, spec)
    tallyho_runs = await _variant_runs(ctx, TallyhoVariant, spec)
    flexiq = median_run(flexiq_runs)
    tallyho = median_run(tallyho_runs)
    share = tallyho.throughput / flexiq.throughput if flexiq.throughput else 0.0
    added_ms = (tallyho.service.p99 - flexiq.service.p99) * 1000
    result.checks = [
        Check(
            "пропускная способность tallyho / flexiq",
            f"≥ {THROUGHPUT_SHARE:.0%}",
            f"{share:.1%} ({tallyho.throughput:.0f} из {flexiq.throughput:.0f} задач/с)",
            share >= THROUGHPUT_SHARE,
        ),
        Check(
            "добавленная p99-задержка на задачу",
            f"≤ {ADDED_P99_MS:.0f} мс",
            prose(
                f"""
                {added_ms:.1f} мс (p99 service {tallyho.service.p99 * 1000:.1f} против
                {flexiq.service.p99 * 1000:.1f} мс)
                """
            ),
            added_ms <= ADDED_P99_MS,
        ),
    ]
    result.metrics = {
        "throughput_share": round(share, 4),
        "added_p99_ms": round(added_ms, 2),
        "median": {
            "flexiq": run_measurement_json(flexiq),
            "tallyho": run_measurement_json(tallyho),
        },
        "runs": [run_measurement_json(item) for item in [*flexiq_runs, *tallyho_runs]],
    }
    result.tables.append(
        Table(
            "Повторы (медианный по пропускной способности — в проверках)",
            (
                "вариант",
                "задач",
                "постановка, с",
                "выполнены, с",
                "финализация, с",
                "задач/с",
                "поставлена→выполнена p50/p99, мс",
                "вызов→тело p50/p99, мс",
                "вызов→итог p50/p99, мс",
            ),
            tuple(_row(item) for item in [*flexiq_runs, *tallyho_runs]),
        )
    )
    result.charts.extend(
        (
            BarChart(
                "throughput",
                "P-01: пропускная способность",
                "задач/с",
                (("flexiq", flexiq.throughput), ("tallyho", tallyho.throughput)),
                limit=flexiq.throughput * THROUGHPUT_SHARE,
            ),
            BarChart(
                "service_p99",
                "P-01: p99 «вызов задачи → итог записан»",
                "мс",
                (("flexiq", flexiq.service.p99 * 1000), ("tallyho", tallyho.service.p99 * 1000)),
                limit=flexiq.service.p99 * 1000 + ADDED_P99_MS,
            ),
        )
    )
    result.notes.extend(
        (
            (
                prose(
                    """
                    «Голый» flexiq и tallyho используют одну и ту же конфигурацию Queue
                    (benchmarks/app.py::_queue), одну БД и одинаковое число процессов и потоков; у
                    каждого варианта свои свежие схемы.
                    """
                )
            ),
            (
                prose(
                    """
                    Пропускная способность — задачи повтора / время от начала постановки до записи
                    итога последней задачи (для tallyho — последнего Item; время до финализации
                    батча — отдельной колонкой).
                    """
                )
            ),
            (
                prose(
                    """
                    Добавленная задержка — разница p99 «вызов функции задачи воркером → итог
                    записан» (моменты — middleware flexiq в обоих вариантах): у flexiq — возврат из
                    функции, у tallyho — max(finished_at Item, возврат из функции). Сюда входят
                    обёртка и claim tallyho, Completer (тик 20 мс) и запись итога; общая для обоих
                    вариантов запись итога джобы flexiq и очередь до взятия джобы не входят (очередь
                    зависит от пропускной способности, а не от накладных расходов на задачу).
                    """
                )
            ),
        )
    )


SCENARIO: Final = Scenario(
    id="P-01",
    title="Накладные расходы tallyho",
    measures="пустые задачи: tallyho поверх flexiq против «голого» flexiq той же конфигурации",
    target="пропускная способность ≥ 80% от «голого» flexiq; добавленная p99-задержка ≤ 50 мс",
    run=run,
)
