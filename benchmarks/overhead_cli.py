"""``poe bench-overhead``: N задач ``print`` через taskiq, flexiq и tallyho (T11.6).

Варианты — реализации :class:`benchmarks.overhead.LoadVariant`: ``taskiq-memory``,
``taskiq-redis``, ``flexiq``, ``tallyho``. Каждый повтор каждого варианта идёт на чистом
состоянии: свежие схемы PostgreSQL (или ``FLUSHDB`` Redis), новые процессы-воркеры, свой
прогрев. Повторы чередуются по вариантам (r1: все варианты, r2: все варианты, …), чтобы
дрейф машины делился между ними поровну. Итог — ``results.json`` и ``results.md`` в ``--out``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import json
import math
import os
import platform
import statistics
import sys
import time
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.cli import DSN_ENV
from benchmarks.overhead import (
    FlexiqVariant,
    OverheadSpec,
    TallyhoVariant,
    measure,
    median_run,
)
from benchmarks.scenarios.p01_overhead import run_measurement_json
from benchmarks.stand import provision, run_command, schemas
from benchmarks.taskiq_app import connect
from benchmarks.taskiq_variants import TaskiqMemoryVariant, TaskiqRedisVariant, redis_container
from benchmarks.workers import read_events

if TYPE_CHECKING:
    from collections.abc import Sequence

    from benchmarks.overhead import LoadVariant, RunMeasurement
    from benchmarks.report import JsonValue
    from benchmarks.stand import Stand

__all__ = ["VARIANTS", "OverheadArgs", "compare", "main", "parse_args", "parse_variants"]

VARIANTS: Final = ("taskiq-memory", "taskiq-redis", "flexiq", "tallyho")
BASELINE: Final = "flexiq"
_SETTINGS: Final = ("fsync", "synchronous_commit", "shared_buffers", "max_connections")
_SETTINGS_SQL: Final = "SELECT name, setting FROM pg_settings WHERE name = ANY(:names)"
_PACKAGES: Final = ("flexiq", "taskiq", "taskiq-redis", "redis", "sqlalchemy", "asyncpg")


class OverheadArgs(argparse.Namespace):
    """Разобранные аргументы."""

    tasks: int = 100_000
    repeats: int = 3
    warmup: int = 2_000
    processes: int = 2
    concurrency: int = 20
    scheduler_batch: int = 1
    variants: str = "all"
    timeout: float = 7_200.0
    label: str = "run"
    dsn: str = ""
    out: Path = Path(".work-tmp") / "overhead"


def parse_variants(raw: str) -> list[str]:
    """``all`` или список через запятую → варианты в порядке :data:`VARIANTS`.

    Returns:
        Варианты.

    Raises:
        ValueError: неизвестный вариант.
    """
    if raw.strip().lower() in {"", "all"}:
        return list(VARIANTS)
    wanted = {part.strip().lower() for part in raw.split(",") if part.strip()}
    unknown = sorted(wanted - set(VARIANTS))
    if unknown:
        message = f"неизвестные варианты {unknown}; есть: {', '.join(VARIANTS)}"
        raise ValueError(message)
    return [name for name in VARIANTS if name in wanted]


def parse_args(argv: Sequence[str] | None = None) -> tuple[list[str], OverheadArgs]:
    """Разобрать аргументы; ошибка — ``SystemExit(2)`` от argparse.

    Returns:
        Варианты и аргументы.
    """
    parser = argparse.ArgumentParser(
        prog="poe bench-overhead", description="Оверхед: taskiq / flexiq / tallyho (T11.6)"
    )
    _ = parser.add_argument("--tasks", type=int, default=100_000, help="задач в повторе")
    _ = parser.add_argument("--repeats", type=int, default=3, help="повторов каждого варианта")
    _ = parser.add_argument("--warmup", type=int, default=2_000, help="задач прогрева")
    _ = parser.add_argument("--processes", type=int, default=2, help="процессов-воркеров")
    _ = parser.add_argument("--concurrency", type=int, default=20, help="задач в процессе")
    _ = parser.add_argument(
        "--scheduler-batch",
        type=int,
        default=1,
        help="scheduler_batch_size flexiq (1 — умолчание flexiq)",
    )
    _ = parser.add_argument("--variants", default="all", help=f"all или {','.join(VARIANTS)}")
    _ = parser.add_argument("--timeout", type=float, default=7_200.0, help="предел повтора, с")
    _ = parser.add_argument("--label", default="run", help="метка прогона (до/после)")
    _ = parser.add_argument("--dsn", default="", help=f"внешний PostgreSQL; пусто — ${DSN_ENV}")
    _ = parser.add_argument("--out", type=Path, default=OverheadArgs.out, help="каталог")
    args = parser.parse_args(argv, namespace=OverheadArgs())
    try:
        variants = parse_variants(args.variants)
    except ValueError as exc:
        parser.error(str(exc))
    if min(args.tasks, args.repeats, args.processes, args.concurrency) < 1 or args.warmup < 0:
        parser.error("tasks, repeats, processes, concurrency ≥ 1; warmup ≥ 0")
    return variants, args


def _log(message: str) -> None:
    _ = sys.stderr.write(f"[overhead {time.strftime('%H:%M:%S')}] {message}\n")
    _ = sys.stderr.flush()


@dataclass(slots=True)
class _Results:
    runs: dict[str, list[RunMeasurement]] = field(default_factory=dict[str, "list[RunMeasurement]"])
    completer: dict[str, list[dict[str, JsonValue]]] = field(
        default_factory=dict[str, "list[dict[str, JsonValue]]"]
    )


def _completer_stats(root: Path) -> dict[str, JsonValue]:
    """Групповые транзакции Completer воркеров tallyho (из их событий).

    Returns:
        Число, средний размер и длительность транзакций; пусто, если событий нет.
    """
    flushes = read_events(root).flushes
    if not flushes:
        return {}
    durations = sorted(duration for _, _, duration in flushes)
    moments = sorted(at for at, _, _ in flushes)
    return {
        "flushes": len(flushes),
        "items_mean": round(statistics.fmean(items for _, items, _ in flushes), 1),
        "duration_p50_ms": round(durations[len(durations) // 2] * 1000, 1),
        "duration_p99_ms": round(
            durations[min(len(durations) - 1, len(durations) * 99 // 100)] * 1000, 1
        ),
        "span_s": round(moments[-1] - moments[0], 1),
    }


async def _one(
    name: str, spec: OverheadSpec, *, stand: Stand, redis_url: str | None, root: Path
) -> list[RunMeasurement]:
    if name == "taskiq-memory":
        return await measure(TaskiqMemoryVariant(), spec, root)
    if name == "taskiq-redis":
        if redis_url is None:
            message = "нет Redis"
            raise TypeError(message)
        return await measure(TaskiqRedisVariant(redis_url), spec, root)
    engine = create_async_engine(stand.dsn)
    try:
        async with schemas(engine, "ovh") as names:
            variant: LoadVariant = (
                FlexiqVariant(stand.dsn, names)
                if name == "flexiq"
                else TallyhoVariant(stand.dsn, names)
            )
            return await measure(variant, spec, root)
    finally:
        await engine.dispose()


async def _environment(stand: Stand, redis_url: str | None) -> dict[str, JsonValue]:
    engine = create_async_engine(stand.dsn)
    try:
        async with engine.connect() as connection:
            postgres = str(cast("object", await connection.scalar(text("SELECT version()"))))
            rows = await connection.execute(text(_SETTINGS_SQL), {"names": list(_SETTINGS)})
            settings: dict[str, JsonValue] = {
                str(cast("object", row[0])): str(cast("object", row[1])) for row in rows
            }
    finally:
        await engine.dispose()
    redis_version: str | None = None
    if redis_url is not None:
        client = connect(redis_url)
        try:
            info = await client.info("server")
        finally:
            await client.aclose()
        redis_version = str(info.get("redis_version"))
    docker = await run_command("docker", "version", "--format", "{{.Server.Version}}", check=False)
    docker_info = await run_command(
        "docker",
        "info",
        "--format",
        "{{.OperatingSystem}}; {{.KernelVersion}}; {{.NCPU}} CPU; {{.MemTotal}} B",
        check=False,
    )
    packages: dict[str, JsonValue] = {}
    for package in _PACKAGES:
        try:
            packages[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            packages[package] = None
    return {
        "os": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.split()[0],
        "packages": packages,
        "postgres": postgres,
        "postgres_settings": settings,
        "redis": redis_version,
        "docker": docker,
        "docker_info": docker_info,
    }


def compare(runs: dict[str, list[RunMeasurement]]) -> dict[str, JsonValue]:
    """Медианы вариантов и оверхед к ``flexiq`` в процентах.

    Оверхед по пропускной способности — ``(flexiq / вариант - 1)``: на сколько процентов больше
    времени вариант тратит на ту же работу; по полному времени — ``(t_вариант / t_flexiq - 1)``
    медианных повторов (для одинакового числа задач это одно и то же).

    Returns:
        Словарь ``{вариант: {...}}``.
    """
    summary: dict[str, JsonValue] = {}
    base = median_run(runs[BASELINE]) if runs.get(BASELINE) else None
    for name, items in runs.items():
        if not items:
            continue
        median = median_run(items)
        throughputs = [run.throughput for run in items]
        entry: dict[str, JsonValue] = {
            "median": run_measurement_json(median),
            "throughput_min": round(min(throughputs), 1),
            "throughput_max": round(max(throughputs), 1),
            "total_s_median": round(statistics.median(run.total_s for run in items), 3),
        }
        if base is not None and median.throughput > 0 and base.total_s > 0:
            entry["throughput_share_vs_flexiq"] = round(median.throughput / base.throughput, 4)
            entry["overhead_pct_throughput"] = round(
                (base.throughput / median.throughput - 1) * 100, 1
            )
            entry["overhead_pct_total_time"] = round((median.total_s / base.total_s - 1) * 100, 1)
            entry["added_p99_latency_ms"] = (
                None
                if math.isnan(median.latency.p99)
                else round((median.latency.p99 - base.latency.p99) * 1000, 1)
            )
            if not (math.isnan(median.service.p99) or math.isnan(base.service.p99)):
                # P-01: «вызов функции задачи воркером -> итог записан», без очереди.
                entry["added_p99_service_ms"] = round(
                    (median.service.p99 - base.service.p99) * 1000, 1
                )
        summary[name] = entry
    return summary


def _finite(value: JsonValue) -> JsonValue:
    """NaN (пустые выборки taskiq) → ``None``: в JSON NaN недопустим.

    Returns:
        Значение без NaN.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, list):
        return [_finite(item) for item in value]
    if isinstance(value, dict):
        return {key: _finite(item) for key, item in value.items()}
    return value


def _row(*cells: str) -> str:
    return "| " + " | ".join(cells) + " |"


def _ms(value: float, digits: int = 0) -> str:
    return "—" if math.isnan(value) else f"{value * 1000:.{digits}f}"


def markdown(runs: dict[str, list[RunMeasurement]], summary: dict[str, JsonValue]) -> str:
    """Таблица медианных повторов и все повторы.

    Returns:
        Markdown.
    """
    lines = [
        _row(
            "вариант",
            "постановка, с",
            "полное время, с",
            "финализация, с",
            "задач/с (мин..макс)",
            "поставлена→выполнена p50 / p99, мс",
            "вызов→итог p50 / p99, мс",
            "оверхед к flexiq, %",
        ),
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, items in runs.items():
        if not items:
            continue
        run = median_run(items)
        entry = cast("dict[str, JsonValue]", summary[name])
        overhead = entry.get("overhead_pct_throughput")
        finalize = "—" if run.finalize_s is None else f"{run.finalize_s:.1f}"
        lines.append(
            _row(
                name,
                f"{run.enqueue_s:.1f}",
                f"{run.total_s:.1f}",
                finalize,
                f"{run.throughput:.0f} ({entry['throughput_min']}..{entry['throughput_max']})",
                f"{_ms(run.latency.p50)} / {_ms(run.latency.p99)}",
                f"{_ms(run.service.p50, 1)} / {_ms(run.service.p99, 1)}",
                "—" if overhead is None else f"{overhead:+.1f}",
            )
        )
    lines.extend(
        (
            "",
            "Все повторы:",
            "",
            "| вариант | повтор | полное время, с | задач/с | p50 / p99, мс |",
            "|---|---|---|---|---|",
        )
    )
    for name, items in runs.items():
        lines.extend(
            _row(
                name,
                str(run.repeat),
                f"{run.total_s:.1f}",
                f"{run.throughput:.0f}",
                f"{_ms(run.latency.p50)} / {_ms(run.latency.p99)}",
            )
            for run in items
        )
    return "\n".join(lines) + "\n"


async def _run(variants: list[str], args: OverheadArgs) -> dict[str, JsonValue]:
    spec = OverheadSpec(
        tasks=args.tasks,
        processes=args.processes,
        concurrency=args.concurrency,
        warmup=args.warmup,
        repeats=1,
        timeout=args.timeout,
        print_output=True,
        scheduler_batch=args.scheduler_batch,
    )
    results = _Results(runs={name: [] for name in variants})
    out = args.out / args.label
    dsn = args.dsn or os.environ.get(DSN_ENV) or None
    started = time.time()
    async with provision(dsn) as stand, contextlib.AsyncExitStack() as stack:
        redis_url = (
            await stack.enter_async_context(redis_container())
            if "taskiq-redis" in variants
            else None
        )
        environment = await _environment(stand, redis_url)
        for repeat in range(args.repeats):
            for name in variants:
                root = out / name / f"r{repeat}"
                _log(f"{name}: повтор {repeat + 1}/{args.repeats}, {args.tasks} задач")
                (run,) = await _one(name, spec, stand=stand, redis_url=redis_url, root=root)
                run = dataclasses.replace(run, repeat=repeat)
                results.runs[name].append(run)
                if name == "tallyho":
                    results.completer.setdefault(name, []).append(_completer_stats(root))
                p99 = _ms(run.latency.p99)
                _log(f"{name}: {run.total_s:.1f} с, {run.throughput:.0f} задач/с, p99 {p99} мс")
    summary = compare(results.runs)
    document: dict[str, JsonValue] = {
        "label": args.label,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(started)),
        "duration_s": round(time.time() - started, 1),
        "parameters": {
            "tasks": args.tasks,
            "repeats": args.repeats,
            "warmup": args.warmup,
            "processes": args.processes,
            "concurrency": args.concurrency,
            "scheduler_batch": args.scheduler_batch,
            "variants": cast("list[JsonValue]", variants),
        },
        "environment": environment,
        "summary": summary,
        "completer": cast("dict[str, JsonValue]", results.completer),
        "runs": [run_measurement_json(run) for items in results.runs.values() for run in items],
    }
    out.mkdir(parents=True, exist_ok=True)
    _ = (out / "results.json").write_text(
        json.dumps(_finite(document), ensure_ascii=False, indent=1, allow_nan=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _ = (out / "results.md").write_text(
        markdown(results.runs, summary), encoding="utf-8", newline="\n"
    )
    return document


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа.

    Returns:
        Код выхода.
    """
    variants, args = parse_args(argv)
    _ = asyncio.run(_run(variants, args))
    _log(f"готово: {args.out / args.label / 'results.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
