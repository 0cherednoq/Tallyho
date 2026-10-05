"""Один прогон A-UC: стенд, сценарий, затишье, оракул I-01…I-14, ожидания, артефакты."""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING

from tests.acceptance.app.site import CatalogGenerator
from tests.acceptance.chaos.journal import ChaosJournal
from tests.acceptance.chaos.plan import PROCESSES
from tests.acceptance.chaos.runner import host_app
from tests.acceptance.chaos.stand import Stand, StandSettings
from tests.acceptance.chaos.verdict import Expectation, OracleInput, run_oracle, wait_quiescent
from tests.acceptance.uc.context import UcContext
from tests.acceptance.uc.scenarios import SCENARIOS, STAND_VARIANTS

if TYPE_CHECKING:
    from pathlib import Path

    from tests.acceptance.chaos.verdict import Recovery
    from tests.acceptance.oracle import InvariantReport
    from tests.acceptance.uc.context import UcConfig

__all__ = ["UcReport", "run_usecase"]

_SCENARIO_CAP = 1800.0
"""Потолок одного сценария без учёта стенда, секунды (функциональный объём - минуты)."""
_QUIESCENCE_CAP = 900.0


@dataclass(frozen=True, slots=True)
class UcReport:
    """Итог прогона A-UC."""

    config: UcConfig
    invariants: tuple[InvariantReport, ...]
    recovery: Recovery
    expectations: tuple[Expectation, ...]
    stats: dict[str, int] = field(default_factory=dict[str, int])
    directory: Path | None = None
    seconds: float = 0.0

    @property
    def violated(self) -> list[InvariantReport]:
        """Нарушенные инварианты."""
        return [report for report in self.invariants if not report.ok]

    @property
    def unmet(self) -> list[Expectation]:
        """Невыполненные ожидания сценария."""
        return [expectation for expectation in self.expectations if not expectation.ok]

    @property
    def oracle_ok(self) -> bool:
        """Инварианты зелёные и стенд пришёл к затишью без остановок дольше ``T_rec``."""
        return not self.violated and self.recovery.ok

    def describe(self) -> str:
        """Короткая сводка для сообщения об ошибке теста."""
        config = self.config
        lines = [
            f"{config.uc}, seed={config.seed}, scale={config.scale}, {self.seconds:.0f} с",
            f"артефакты: {self.directory}",
            f"затишье: {self.recovery}",
        ]
        lines.extend(
            f"НАРУШЕН {report.invariant}: {report.violations} из {report.checked}: "
            + "; ".join(report.evidence[:5])
            for report in self.violated
        )
        lines.extend(
            f"НЕ ВЫПОЛНЕНО «{expectation.name}»: {expectation.detail}" for expectation in self.unmet
        )
        return "\n".join(lines)


def _write_json(path: Path, value: object) -> None:
    text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    _ = path.write_text(text + "\n", encoding="utf-8", newline="\n")


def _settings(config: UcConfig) -> StandSettings:
    variant = STAND_VARIANTS.get(config.uc, {})
    return StandSettings(
        seed=config.seed,
        pages=config.pages,
        empty_pdfs=variant.get("empty_pdfs", False),
        threads=config.threads,
        lease_ttl=config.lease_ttl,
        heartbeat_every=config.heartbeat_every,
        sweep_interval=config.sweep_interval,
    )


async def _processes(stand: Stand) -> Expectation:
    restarts = await stand.restart_counts()
    running = {service: await stand.is_running(service) for service in PROCESSES}
    return Expectation(
        "процессы приложения живы и не падали сами",
        all(running.values()) and not any(restarts.values()),
        f"running={running}, docker_restarts={restarts}",
    )


async def _execute(
    config: UcConfig, stand: Stand, journal: ChaosJournal, *, directory: Path
) -> UcReport:
    started = time.monotonic()
    settings = stand.settings
    generated = CatalogGenerator.build(
        config.seed, page_count=settings.pages, empty_pdfs=settings.empty_pdfs
    )
    app = host_app(stand)
    try:
        context = UcContext(config, stand, app, journal, generated)
        journal.record("stand_ready", project=stand.project, scale=config.scale)
        async with asyncio.timeout(_SCENARIO_CAP):
            await SCENARIOS[config.uc](context)
        journal.record("scenario_done")
        recovery = await wait_quiescent(stand, journal, hard_cap=_QUIESCENCE_CAP)
        await app.engine.dispose()
        invariants, stats = await run_oracle(
            OracleInput(
                stand,
                app,
                context.roots,
                generated,
                journal,
                retry_failed=dict(context.retry_failed),
                purged=tuple(context.purged),
                frozen=context.frozen,
                truth=context.truth,
            )
        )
        stats.update(context.stats)
        expectations = [await _processes(stand), *context.expectations]
    finally:
        await app.close()
    seconds = round(time.monotonic() - started, 1)
    _write_json(
        directory / "oracle.json",
        {
            "invariants": [asdict(item) | {"ok": item.ok} for item in invariants],
            "recovery": asdict(recovery) | {"ok": recovery.ok},
            "expectations": [asdict(item) for item in expectations],
            "stats": stats,
            "seconds": seconds,
        },
    )
    return UcReport(
        config, tuple(invariants), recovery, tuple(expectations), stats, directory, seconds
    )


async def run_usecase(config: UcConfig) -> UcReport:
    """Выполнить один сценарий A-UC на собственном стенде и вернуть вердикт.

    Стенд поднимается с нуля и удаляется по окончании, даже если прогон упал; при
    неудаче логи контейнеров остаются в каталоге артефактов.
    """
    directory = config.artifacts / config.name
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    _write_json(directory / "config.json", asdict(config))
    journal = ChaosJournal(directory / "journal.jsonl")
    stand = Stand(config.project, _settings(config))
    report: UcReport | None = None
    try:
        await stand.up()
        journal.start()
        report = await _execute(config, stand, journal, directory=directory)
        return report
    finally:
        if report is None or not report.oracle_ok or report.unmet:
            await stand.save_logs(directory / "containers.log")
        if config.keep_stand:
            await stand.close()
        else:
            await stand.down()
