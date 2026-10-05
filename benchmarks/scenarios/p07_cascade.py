"""P-07 — каскад конвейера: финализация источника → seal этапа; пустой этап → финализация."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, cast

from benchmarks.app import KIND_PIPELINE
from benchmarks.charts import BarChart
from benchmarks.context import Scenario
from benchmarks.harness import create_pipeline, harness
from benchmarks.metrics import summarize
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.report import Check, Table
from benchmarks.stand import sql
from tallyho.model.states import BatchState

if TYPE_CHECKING:
    from uuid import UUID

    from benchmarks.app import AppConfig
    from benchmarks.context import RunContext
    from benchmarks.harness import Harness
    from benchmarks.report import ScenarioResult

__all__ = ["PARAMS", "SCENARIO", "CascadeParams"]


@dataclass(frozen=True, slots=True)
class CascadeParams:
    """Конвейеры: корней, страниц, карточек на страницу, PDF на карточку; воркеры."""

    pipelines: int
    pages: int
    cards_per_page: int
    pdfs_per_card: int
    empty_pipelines: int
    processes: int
    concurrency: int
    within: float

    @property
    def items(self) -> int:
        """Items одного обычного конвейера."""
        cards = self.pages * self.cards_per_page
        return self.pages + cards + cards * self.pdfs_per_card


PARAMS: Final = {
    Profile.SMOKE: CascadeParams(20, 1, 10, 5, 10, processes=2, concurrency=20, within=600),
    Profile.NIGHTLY: CascadeParams(20, 1, 50, 100, 20, processes=4, concurrency=50, within=3_600),
    # 100 x (1 → 100 → 500) ≈ 5M Items, как ячейка Fan-out матрицы масштаба.
    Profile.FULL: CascadeParams(
        100, 1, 100, 500, 100, processes=8, concurrency=100, within=6 * 3_600
    ),
}
SEAL_P99: Final = 1.0
EMPTY_P99: Final = 2.0
_POLL: Final = 0.05
_FEEDER: Final = {"cards": "pages", "pdfs": "cards"}
# Этапы получают вид «<вид корня>.<ключ>», поэтому отбираются по корню.
_SEALED: Final = """
SELECT id FROM {batch}
WHERE parent_id IS NOT NULL AND state >= :sealed
  AND root_id IN (SELECT id FROM {batch} WHERE kind = :kind AND parent_id IS NULL)
"""
_STAGES: Final = """
SELECT id, root_id, key, extract(epoch FROM finished_at) FROM {batch}
WHERE parent_id IS NOT NULL
  AND root_id IN (SELECT id FROM {batch} WHERE kind = :kind AND parent_id IS NULL)
"""


async def _watch_seals(stand: Harness, seen: dict[UUID, float]) -> None:
    """Опрос этапов раз в 50 мс: первый момент, когда этап виден ``sealed`` или дальше."""
    statement = sql(_SEALED, **stand.idents)
    async with stand.observer.connect() as connection:
        while True:
            rows = await connection.scalars(
                statement, {"kind": KIND_PIPELINE, "sealed": int(BatchState.SEALED)}
            )
            now = time.time()
            for stage_id in cast("list[UUID]", rows.all()):
                _ = seen.setdefault(stage_id, now)
            await connection.commit()
            await asyncio.sleep(_POLL)


async def _stages(stand: Harness) -> list[tuple[UUID, UUID, str, float | None]]:
    async with stand.observer.connect() as connection:
        rows = (
            await connection.execute(sql(_STAGES, **stand.idents), {"kind": KIND_PIPELINE})
        ).all()
    return [
        (
            cast("UUID", row[0]),
            cast("UUID", row[1]),
            cast("str", row[2]),
            None if row[3] is None else float(cast("float", row[3])),
        )
        for row in rows
    ]


async def _run_pipelines(
    ctx: RunContext, params: CascadeParams, *, empty: bool
) -> tuple[list[float], list[float], float]:
    """Вернуть лаги seal (с), лаги финализации пустого этапа (с) и длительность прогона.

    Returns:
        Лаги seal, лаги пустого этапа и длительность прогона, секунды.
    """
    pdfs = 0 if empty else params.pdfs_per_card
    count = params.empty_pipelines if empty else params.pipelines

    def configure(config: AppConfig) -> AppConfig:
        return replace(config, cards_per_page=params.cards_per_page, pdfs_per_card=pdfs)

    name = "p07_empty" if empty else "p07"
    async with harness(
        ctx,
        name=name,
        processes=params.processes,
        concurrency=params.concurrency,
        configure=configure,
    ) as stand:
        seen: dict[UUID, float] = {}
        watcher = asyncio.create_task(_watch_seals(stand, seen), name="bench-p07-seals")
        try:
            ctx.log(f"{name}: {count} конвейеров")
            for run in range(count):
                _ = await create_pipeline(stand.th, stand.tasks, run, pages=params.pages)
            duration = await stand.wait_roots(KIND_PIPELINE, within=params.within)
        finally:
            _ = watcher.cancel()
            _ = await asyncio.wait([watcher])
        stages = await _stages(stand)
    by_root: dict[tuple[UUID, str], tuple[UUID, float | None]] = {
        (root, key): (stage_id, finished) for stage_id, root, key, finished in stages
    }
    seal_lags: list[float] = []
    empty_lags: list[float] = []
    for stage_id, root, key, finished in stages:
        feeder_key = _FEEDER.get(key)
        if feeder_key is None:
            continue
        feeder = by_root.get((root, feeder_key))
        if feeder is None or feeder[1] is None:
            continue
        if stage_id in seen:
            seal_lags.append(max(0.0, seen[stage_id] - feeder[1]))
        if empty and key == "pdfs" and finished is not None:
            empty_lags.append(max(0.0, finished - feeder[1]))
    return seal_lags, empty_lags, duration


async def run(ctx: RunContext, result: ScenarioResult) -> None:
    """Обычные конвейеры (лаг seal), затем конвейеры с пустым этапом ``pdfs``."""
    params = PARAMS[ctx.profile]
    seal, _, duration = await _run_pipelines(ctx, params, empty=False)
    seal_empty, empty, empty_duration = await _run_pipelines(ctx, params, empty=True)
    seal_all = summarize(seal + seal_empty)
    empty_summary = summarize(empty)
    result.parameters = {
        "pipelines": params.pipelines,
        "pages": params.pages,
        "cards_per_page": params.cards_per_page,
        "pdfs_per_card": params.pdfs_per_card,
        "items_per_pipeline": params.items,
        "empty_pipelines": params.empty_pipelines,
        "processes": params.processes,
        "concurrency": params.concurrency,
    }
    result.checks = [
        Check(
            "финализация источника → seal этапа",
            f"p99 ≤ {SEAL_P99:.0f} с",
            f"p99 {seal_all.p99:.3f} с (p50 {seal_all.p50:.3f}, n={seal_all.count})",
            seal_all.count > 0 and seal_all.p99 <= SEAL_P99,
        ),
        Check(
            "финализация источника → финализация пустого этапа",
            f"p99 ≤ {EMPTY_P99:.0f} с",
            f"p99 {empty_summary.p99:.3f} с (p50 {empty_summary.p50:.3f}, n={empty_summary.count})",
            empty_summary.count > 0 and empty_summary.p99 <= EMPTY_P99,
        ),
    ]
    result.metrics = {
        "seal_s": dict(seal_all.as_dict()),
        "empty_stage_s": dict(empty_summary.as_dict()),
        "run_s": round(duration, 1),
        "empty_run_s": round(empty_duration, 1),
        "items": params.pipelines * params.items,
    }
    result.tables.append(
        Table(
            "Каскад, секунды",
            ("переход", "n", "p50", "p99", "max"),
            (
                (
                    "seal этапа",
                    str(seal_all.count),
                    f"{seal_all.p50:.3f}",
                    f"{seal_all.p99:.3f}",
                    f"{seal_all.max:.3f}",
                ),
                (
                    "пустой этап",
                    str(empty_summary.count),
                    f"{empty_summary.p50:.3f}",
                    f"{empty_summary.p99:.3f}",
                    f"{empty_summary.max:.3f}",
                ),
            ),
        )
    )
    result.charts.append(
        BarChart(
            "cascade_p99",
            "P-07: p99 каскада, с",
            "секунды",
            (("seal этапа", seal_all.p99), ("пустой этап", empty_summary.p99)),
            limit=SEAL_P99,
        )
    )
    result.notes.extend(
        (
            (
                prose(
                    """
                    Конвейер S3 без фейкового сайта: страница порождает карточки (spawn в этап
                    cards), карточка — PDF (spawn в этап pdfs); этапы связаны fed_by. Так меряется
                    сам каскад tallyho, без HTTP.
                    """
                )
            ),
            (
                prose(
                    """
                    Момент seal — первый опрос (раз в 50 мс), в котором этап виден sealed или
                    дальше; момент финализации источника — его finished_at. Точность — один период
                    опроса.
                    """
                )
            ),
        )
    )


SCENARIO: Final = Scenario(
    id="P-07",
    title="Каскад конвейера",
    measures=(
        prose(
            """
            конвейер pages → cards → pdfs: от финализации источника до seal этапа и до финализации
            пустого этапа
            """
        )
    ),
    target="до seal этапа p99 ≤ 1 с; до финализации пустого этапа p99 ≤ 2 с",
    run=run,
)
