"""Чистые части бенчмарка оверхеда T11.6: аргументы, сравнение с flexiq, таблица, taskiq."""

from __future__ import annotations

import asyncio
import math
from typing import TYPE_CHECKING, cast

import pytest

from benchmarks.metrics import summarize
from benchmarks.overhead import RunMeasurement
from benchmarks.overhead_cli import VARIANTS, compare, markdown, parse_args, parse_variants
from benchmarks.taskiq_app import MemorySink
from benchmarks.taskiq_variants import parse_done

if TYPE_CHECKING:
    from benchmarks.report import JsonValue


def run(variant: str, total: float, *, repeat: int = 0, latency: float = 1.0) -> RunMeasurement:
    service = [] if variant.startswith("taskiq") else [latency / 100]
    return RunMeasurement(
        variant=variant,
        repeat=repeat,
        tasks=1_000,
        enqueue_s=0.1,
        total_s=total,
        finalize_s=None if variant != "tallyho" else total + 0.5,
        latency=summarize([latency, latency * 2]),
        dispatch=summarize([]),
        service=summarize(service),
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("all", list(VARIANTS)),
        ("", list(VARIANTS)),
        ("tallyho, flexiq", ["flexiq", "tallyho"]),
        ("TASKIQ-REDIS", ["taskiq-redis"]),
    ],
)
def test_parse_variants_keeps_canonical_order(raw: str, expected: list[str]) -> None:
    assert parse_variants(raw) == expected


def test_parse_variants_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="неизвестные варианты"):
        _ = parse_variants("flexiq,celery")


def test_parse_args_defaults_and_validation() -> None:
    variants, args = parse_args([])
    assert variants == list(VARIANTS)
    assert (args.tasks, args.repeats, args.processes, args.concurrency) == (100_000, 3, 2, 20)
    assert args.scheduler_batch == 1
    with pytest.raises(SystemExit):
        _ = parse_args(["--tasks", "0"])
    with pytest.raises(SystemExit):
        _ = parse_args(["--variants", "celery"])


def test_compare_reports_overhead_against_flexiq_median() -> None:
    runs = {
        "flexiq": [run("flexiq", 10.0), run("flexiq", 12.0, repeat=1), run("flexiq", 8.0)],
        "tallyho": [run("tallyho", 12.5, latency=1.5)],
        "taskiq-memory": [run("taskiq-memory", 1.0)],
    }
    summary = compare(runs)
    flexiq = cast("dict[str, JsonValue]", summary["flexiq"])
    tallyho = cast("dict[str, JsonValue]", summary["tallyho"])
    assert flexiq["overhead_pct_throughput"] == pytest.approx(0.0)
    assert (flexiq["throughput_min"], flexiq["throughput_max"]) == pytest.approx((83.3, 125.0))
    # Медиана flexiq — 10 с: tallyho на 25% дольше, пропускная способность 80%.
    assert tallyho["overhead_pct_total_time"] == pytest.approx(25.0)
    assert tallyho["overhead_pct_throughput"] == pytest.approx(25.0)
    assert tallyho["throughput_share_vs_flexiq"] == pytest.approx(0.8)
    assert tallyho["added_p99_latency_ms"] == pytest.approx(995.0)
    table = markdown(runs, summary)
    assert tallyho["added_p99_service_ms"] == pytest.approx(5.0)
    assert "| tallyho | 0.1 | 12.5 | 13.0 | 80 (80.0..80.0) | 2250 / 2985 | 15.0 / 15.0 |" in table
    assert "| taskiq-memory | 0 | 1.0 | 1000 |" in table


def test_compare_without_flexiq_has_no_overhead() -> None:
    summary = compare({"taskiq-memory": [run("taskiq-memory", 1.0)], "flexiq": []})
    entry = cast("dict[str, JsonValue]", summary["taskiq-memory"])
    assert "overhead_pct_throughput" not in entry
    assert (
        "| taskiq-memory | 0.1 | 1.0 | — | 1000 (1000.0..1000.0) | 1500 / 1990 | — / — | — |"
        in (markdown({"taskiq-memory": [run("taskiq-memory", 1.0)]}, summary))
    )
    assert "flexiq" not in summary


def test_parse_done_keeps_repeat_range_and_first_completion() -> None:
    raw = [b"1 10.5", b"2 11.0", b"2 12.0", b"7 13.0"]
    assert parse_done(raw, 1, 5) == {1: 10.5, 2: 11.0}


async def test_memory_sink_waits_for_total() -> None:
    sink = MemorySink()
    await sink.done(0, 1.0)
    await sink.reached(1)
    waiter = asyncio.create_task(sink.reached(3))
    await sink.done(1, 2.0)
    await asyncio.sleep(0)
    assert not waiter.done()
    await sink.done(2, 3.0)
    await asyncio.wait_for(waiter, timeout=1)
    assert [index for index, _ in sink.events] == [0, 1, 2]
    assert not math.isnan(sink.events[-1][1])
