"""Командная строка ``poe bench``: разбор ``--id``/``--profile`` и прогон сценариев."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Final

from benchmarks.context import RunContext
from benchmarks.profiles import Profile
from benchmarks.registry import SCENARIOS
from benchmarks.report import ScenarioResult, write_result, write_summary
from benchmarks.stand import provision

if TYPE_CHECKING:
    from collections.abc import Sequence

    from benchmarks.context import Scenario

__all__ = ["BenchArgs", "main", "parse_args", "parse_ids"]

DSN_ENV: Final = "TALLYHO_BENCH_DSN"
DEFAULT_OUT: Final = Path(".work-tmp") / "bench"


class BenchArgs(argparse.Namespace):
    """Разобранные аргументы."""

    ids: str = "all"
    profile: str = Profile.SMOKE.value
    dsn: str = ""
    out: Path = DEFAULT_OUT
    seed: int = 1


def parse_ids(raw: str) -> list[str]:
    """``P-01,P-4,p-11`` или ``all`` → нормализованные id в порядке реестра.

    Returns:
        Id сценариев в порядке реестра.

    Raises:
        ValueError: неизвестный id.
    """
    if raw.strip().lower() in {"", "all"}:
        return list(SCENARIOS)
    wanted: list[str] = []
    for part in raw.split(","):
        token = part.strip().upper()
        if not token:
            continue
        number = token.removeprefix("P-").removeprefix("P")
        if not number.isdigit():
            message = f"неизвестный сценарий {part.strip()!r}; есть: {', '.join(SCENARIOS)}"
            raise ValueError(message)
        normalized = f"P-{int(number):02}"
        if normalized not in SCENARIOS:
            message = f"неизвестный сценарий {part.strip()!r}; есть: {', '.join(SCENARIOS)}"
            raise ValueError(message)
        if normalized not in wanted:
            wanted.append(normalized)
    return [scenario_id for scenario_id in SCENARIOS if scenario_id in wanted]


def parse_args(argv: Sequence[str] | None = None) -> tuple[list[str], Profile, BenchArgs]:
    """Разобрать аргументы; ошибка аргументов — ``SystemExit(2)`` от argparse.

    Returns:
        Id сценариев, профиль и остальные аргументы.
    """
    parser = argparse.ArgumentParser(
        prog="poe bench", description="Бенчмарки A-PERF (ACCEPTANCE §9)"
    )
    _ = parser.add_argument(
        "--id", dest="ids", default="all", help="P-01[,P-04,...] или all (по умолчанию)"
    )
    _ = parser.add_argument(
        "--profile",
        default=Profile.SMOKE.value,
        choices=[profile.value for profile in Profile],
        help="smoke — минуты локально; nightly — ACCEPTANCE §11; full — §9, эталонный стенд",
    )
    _ = parser.add_argument(
        "--dsn",
        default="",
        help=f"внешний PostgreSQL (postgresql+asyncpg://…); пусто — ${DSN_ENV} или свой контейнер",
    )
    _ = parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="каталог отчётов")
    _ = parser.add_argument("--seed", type=int, default=1, help="seed нагрузки")
    args = parser.parse_args(argv, namespace=BenchArgs())
    try:
        ids = parse_ids(args.ids)
    except ValueError as exc:
        parser.error(str(exc))
    return ids, Profile(args.profile), args


async def _run_one(scenario: Scenario, ctx: RunContext) -> ScenarioResult:
    result = ScenarioResult(
        id=scenario.id,
        title=scenario.title,
        measures=scenario.measures,
        target=scenario.target,
        profile=ctx.profile,
    )
    started = time.monotonic()
    shutil.rmtree(ctx.out, ignore_errors=True)
    ctx.log(f"{scenario.id} ({ctx.profile}) — старт")
    try:
        await scenario.run(ctx, result)
    except Exception:  # ruff: ignore[blind-except]  # падение одного P-NN — в его отчёт, остальные идут дальше
        result.error = traceback.format_exc()
        ctx.log(f"{scenario.id}: ошибка\n{result.error}")
    result.duration_s = time.monotonic() - started
    report = write_result(result, ctx.out)
    ctx.log(f"{scenario.id}: {result.verdict} за {result.duration_s:.0f} с → {report}")
    return result


async def _run(ids: list[str], profile: Profile, args: BenchArgs) -> list[ScenarioResult]:
    dsn = args.dsn or os.environ.get(DSN_ENV) or None
    results: list[ScenarioResult] = []
    async with provision(dsn) as stand:
        for scenario_id in ids:
            out = args.out / f"{scenario_id}-{profile}"
            ctx = RunContext(profile=profile, stand=stand, out=out, seed=args.seed)
            results.append(await _run_one(SCENARIOS[scenario_id], ctx))
    return results


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа; код выхода 1 — ошибка прогона или невыполненная цель вне ``smoke``.

    Returns:
        Код выхода процесса.
    """
    ids, profile, args = parse_args(argv)
    results = asyncio.run(_run(ids, profile, args))
    write_summary(results, args.out / f"summary-{profile}.md")
    failed = [result for result in results if result.verdict.name in {"ERROR", "NOT_MET"}]
    return 1 if failed else 0
