"""Один хаос-прогон: стенд, нагрузка и хаос, восстановление, оракул, артефакты."""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from tests.acceptance.app.application import StandTuning, build_app
from tests.acceptance.app.site import CatalogGenerator
from tests.acceptance.chaos.controller import ChaosController
from tests.acceptance.chaos.journal import ChaosJournal
from tests.acceptance.chaos.load import MEAN_TASK_SECONDS, LoadDriver, plan_load
from tests.acceptance.chaos.plan import PROCESSES, WORKERS, build_plan
from tests.acceptance.chaos.stand import ROOT, Stand, StandSettings, run_command
from tests.acceptance.chaos.verdict import (
    Facts,
    OracleInput,
    check_expectations,
    run_oracle,
    wait_recovery,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tests.acceptance.app.application import AcceptanceApp
    from tests.acceptance.chaos.plan import ChaosPlan
    from tests.acceptance.chaos.verdict import Expectation, Recovery
    from tests.acceptance.oracle import InvariantReport

__all__ = ["ARTIFACTS", "RunConfig", "RunReport", "run_chaos"]

ARTIFACTS = ROOT / ".work-tmp" / "acceptance"


@dataclass(frozen=True, slots=True, kw_only=True)
class RunConfig:
    """Параметры прогона ``poe acceptance``.

    Attributes:
        seed: Seed расписания хаоса, данных и инъекции ошибок.
        scenario: ``S1``, ``S2`` или ``S3``.
        chaos: ``A-CH-01`` … ``A-CH-12``.
        duration: Длительность окна хаоса, секунды (локально 120, nightly 600,
            pre-release 3600 и 7200 для A-CH-12).
        lease_ttl: ``lease_ttl`` стенда, секунды.
        sweep_interval: ``sweep_interval`` стенда, секунды.
        threads: Параллельность одного воркера.
        artifacts: Каталог артефактов всех прогонов.
        keep_stand: Не удалять стенд после прогона (разбор упавшего прогона).
    """

    seed: int
    scenario: str
    chaos: str
    duration: float = 120.0
    lease_ttl: float = 60.0
    sweep_interval: float = 5.0
    threads: int = 8
    artifacts: Path = ARTIFACTS
    keep_stand: bool = False

    @property
    def name(self) -> str:
        """Имя прогона: каталог артефактов и суффикс compose-проекта."""
        return f"{self.chaos}-{self.scenario}-seed{self.seed}".lower()

    @property
    def project(self) -> str:
        """Уникальное имя compose-проекта."""
        return f"tallyho-chaos-{self.name.replace('a-ch-', 'ch')}"


@dataclass(frozen=True, slots=True)
class RunReport:
    """Итог прогона: то, что прикладывается к релизу (ACCEPTANCE §11)."""

    config: RunConfig
    invariants: tuple[InvariantReport, ...]
    recovery: Recovery
    expectations: tuple[Expectation, ...]
    stats: dict[str, int] = field(default_factory=dict[str, int])
    directory: Path = ARTIFACTS

    @property
    def violated(self) -> list[InvariantReport]:
        """Нарушенные инварианты."""
        return [report for report in self.invariants if not report.ok]

    @property
    def unmet(self) -> list[Expectation]:
        """Невыполненные дополнительные ожидания A-CH."""
        return [expectation for expectation in self.expectations if not expectation.ok]

    @property
    def oracle_ok(self) -> bool:
        """Все инварианты зелёные и восстановление уложилось в ``T_rec``."""
        return not self.violated and self.recovery.ok

    def describe(self) -> str:
        """Короткая сводка для сообщения об ошибке теста."""
        config = self.config
        lines = [
            f"{config.chaos} / {config.scenario}, seed={config.seed}, duration={config.duration}",
            f"артефакты: {self.directory}",
            f"восстановление: {self.recovery}",
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


@dataclass(frozen=True, slots=True)
class _Run:
    config: RunConfig
    plan: ChaosPlan
    stand: Stand
    journal: ChaosJournal
    directory: Path


def _write_json(path: Path, value: object) -> None:
    text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    _ = path.write_text(text + "\n", encoding="utf-8", newline="\n")


def _settings(config: RunConfig) -> StandSettings:
    return StandSettings(
        seed=config.seed,
        threads=config.threads,
        lease_ttl=config.lease_ttl,
        heartbeat_every=config.lease_ttl / 3,
        sweep_interval=config.sweep_interval,
    )


def _host_app(stand: Stand) -> AcceptanceApp:
    """Процесс нагрузки на хосте: тот же граф задач, PostgreSQL через прокси control."""
    settings = stand.settings
    return build_app(
        dsn=stand.control_dsn,
        tallyho_schema="th",
        flexiq_schema="flexiq",
        domain_schema="app",
        site_url=stand.site_url,
        mail_url=stand.mail_url,
        seed=settings.seed,
        tuning=StandTuning(
            application_name="driver",
            lease_ttl=timedelta(seconds=settings.lease_ttl),
            heartbeat_every=timedelta(seconds=settings.heartbeat_every),
            sweep_interval=timedelta(seconds=settings.sweep_interval),
        ),
    )


async def _clock_skew(stand: Stand) -> dict[str, float]:
    """Насколько часы каждого воркера отличаются от часов хоста, секунды."""
    skew: dict[str, float] = {}
    for worker in WORKERS:
        result = await run_command("docker", "exec", stand.container(worker), "date", "+%s")
        if result.ok and result.output.isdigit():
            skew[worker] = round(int(result.output) - time.time(), 3)
    return skew


async def _load_and_chaos(run: _Run, driver: LoadDriver, controller: ChaosController) -> float:
    """Запустить нагрузку и хаос, остановить хаос и вернуть предел ожидания доработки."""
    settings = run.stand.settings
    load_task = asyncio.create_task(driver.run())
    try:
        try:
            await controller.run()
        finally:
            await controller.heal()
    except BaseException:
        # Контроллер упал: нагрузка без хаоса никому не нужна, прогон прерывается.
        _ = load_task.cancel()
        _ = await asyncio.gather(load_task, return_exceptions=True)
        raise
    drain = driver.profile.expected_items * MEAN_TASK_SECONDS / settings.concurrency
    hard_cap = 4 * drain + 2 * run.config.duration + 10 * settings.recovery.total_seconds()
    try:
        async with asyncio.timeout(hard_cap):
            await load_task
    except TimeoutError:
        run.journal.record("load_not_started", launched=len(driver.roots))
    return hard_cap


async def _execute(run: _Run) -> RunReport:
    config, stand, journal = run.config, run.stand, run.journal
    profile = plan_load(config.scenario, config.seed, config.duration, settings=stand.settings)
    generated = CatalogGenerator.build(config.seed, page_count=stand.settings.pages)
    app = _host_app(stand)
    try:
        skew = await _clock_skew(stand)
        journal.record("stand_ready", project=stand.project, load=asdict(profile), clock_skew=skew)
        driver = LoadDriver(app, profile, config.seed, journal)
        hard_cap = await _load_and_chaos(
            run, driver, ChaosController(stand, run.plan, journal, app.queue)
        )
        recovery = await wait_recovery(stand, driver, journal, hard_cap=hard_cap)
        # После отказов PostgreSQL в пуле хоста остаются мёртвые соединения.
        await app.engine.dispose()
        invariants, stats = await run_oracle(OracleInput(stand, app, driver, generated, journal))
        facts = Facts(
            restarts=await stand.restart_counts(),
            running={service: await stand.is_running(service) for service in PROCESSES},
            skew=skew,
            sweep_interval=config.sweep_interval,
            stats=stats,
        )
        expectations = check_expectations(config.chaos, journal, facts)
    finally:
        await app.close()
    _write_json(
        run.directory / "oracle.json",
        {
            "invariants": [asdict(item) | {"ok": item.ok} for item in invariants],
            "recovery": asdict(recovery) | {"ok": recovery.ok},
            "expectations": [asdict(item) for item in expectations],
            "stats": stats,
        },
    )
    return RunReport(config, tuple(invariants), recovery, tuple(expectations), stats, run.directory)


async def run_chaos(config: RunConfig) -> RunReport:
    """Выполнить один прогон «сценарий и отказ» и вернуть вердикт.

    Стенд поднимается с нуля и удаляется по окончании, даже если прогон упал.
    Артефакты (расписание, журнал хаоса, отчёт оракула, логи контейнеров при неудаче)
    остаются в ``config.artifacts / config.name``.
    """
    directory = config.artifacts / config.name
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    plan = build_plan(config.chaos, config.seed, config.duration)
    _write_json(
        directory / "plan.json",
        {
            "config": asdict(config),
            "environment": dict(plan.environment),
            "actions": [asdict(action) for action in plan.actions],
        },
    )
    journal = ChaosJournal(directory / "chaos-journal.jsonl")
    stand = Stand(config.project, _settings(config), dict(plan.environment))
    report: RunReport | None = None
    try:
        await stand.up()
        report = await _execute(_Run(config, plan, stand, journal, directory))
        return report
    finally:
        if report is None or not report.oracle_ok or report.unmet:
            await stand.save_logs(directory / "containers.log")
        if config.keep_stand:
            await stand.close()
        else:
            await stand.down()
