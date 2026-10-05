"""Чистые части бенчмарк-харнесса A-PERF (T11.5): метрики, аргументы, отчёт, расчёты P-NN."""

from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, cast

import pytest

from benchmarks.app import AppConfig, Variant
from benchmarks.charts import BarChart, LineChart, Series, render_svg
from benchmarks.cli import parse_args, parse_ids
from benchmarks.dbstats import TableSample
from benchmarks.metrics import (
    LatencyLog,
    Sample,
    percentile,
    phase_summaries,
    rate_series,
    steady_rate,
    summarize,
)
from benchmarks.profiles import Profile
from benchmarks.prose import prose
from benchmarks.registry import SCENARIOS
from benchmarks.report import Check, ScenarioResult, Table, Verdict, write_result, write_summary
from benchmarks.scenarios.common import windowed_max
from benchmarks.scenarios.p08_snapshots import snapshot_lags
from benchmarks.scenarios.p09_recovery import recovery_time
from benchmarks.scenarios.p10_db_resources import monotonic_growth
from benchmarks.stand import Schemas, ident, sql
from benchmarks.workers import read_events

if TYPE_CHECKING:
    from pathlib import Path


def test_registry_covers_every_acceptance_perf_id() -> None:
    assert list(SCENARIOS) == [f"P-{index:02}" for index in range(1, 12)]
    assert all(scenario.target and scenario.measures for scenario in SCENARIOS.values())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("P-01", ["P-01"]),
        ("p-4, P-01", ["P-01", "P-04"]),
        ("P11,P-11", ["P-11"]),
        ("all", list(SCENARIOS)),
        ("", list(SCENARIOS)),
    ],
)
def test_parse_ids_normalizes_and_keeps_registry_order(raw: str, expected: list[str]) -> None:
    assert parse_ids(raw) == expected


@pytest.mark.parametrize("raw", ["P-12", "X-01", "P-0x"])
def test_parse_ids_rejects_unknown(raw: str) -> None:
    with pytest.raises(ValueError, match="неизвестный сценарий"):
        _ = parse_ids(raw)


def test_parse_args_nightly_contract() -> None:
    ids, profile, args = parse_args(["--id", "P-01", "--profile", "nightly"])
    assert ids == ["P-01"]
    assert profile is Profile.NIGHTLY
    assert profile.enforced
    assert args.seed == 1


def test_parse_args_rejects_unknown_profile_and_id() -> None:
    with pytest.raises(SystemExit):
        _ = parse_args(["--profile", "weekly"])
    with pytest.raises(SystemExit):
        _ = parse_args(["--id", "P-99"])


def test_percentile_and_summary() -> None:
    values = [float(value) for value in range(1, 101)]
    assert percentile(values, 50) == pytest.approx(50.5)
    assert percentile(values, 99) == pytest.approx(99.01)
    assert percentile([7.0], 99) == pytest.approx(7.0)
    assert math.isnan(percentile([], 50))
    summary = summarize(values)
    assert summary.count == 100
    assert summary.max == pytest.approx(100.0)
    assert summary.as_dict(scale=1000)["p50"] == pytest.approx(50500.0)
    assert summarize([]).count == 0


def test_phase_summaries_split_start_mid_end() -> None:
    samples = [Sample(float(at), 1.0 if at < 50 else 3.0) for at in range(101)]
    phases = phase_summaries(samples)
    assert phases["start"].p99 == pytest.approx(1.0)
    assert phases["end"].p99 == pytest.approx(3.0)
    assert phases["mid"].count > 0
    assert phase_summaries([])["start"].count == 0


def test_latency_log_keeps_operation_order() -> None:
    log = LatencyLog()
    log.add("claim", 1.0, 0.01)
    log.extend("finish", [Sample(2.0, 0.02)])
    assert log.operations() == ["claim", "finish"]
    assert log.summary("finish").p50 == pytest.approx(0.02)


def test_rate_series_and_steady_rate() -> None:
    points = [(float(at), at * 10) for at in range(11)]
    series = rate_series(points, window=2.0)
    assert series
    assert all(rate == pytest.approx(10.0) for _, rate in series)
    assert steady_rate(series, warmup=4.0) == pytest.approx(10.0)
    assert math.isnan(steady_rate([], warmup=0.0))
    assert rate_series([], window=1.0) == []


def test_windowed_max() -> None:
    assert windowed_max([(0.1, 1.0), (0.5, 5.0), (1.2, 2.0)], window=1.0) == [
        (1.0, 5.0),
        (2.0, 2.0),
    ]


def test_recovery_time_requires_two_windows_above_threshold() -> None:
    rates = [(10.0, 100.0), (12.0, 10.0), (14.0, 95.0), (16.0, 50.0), (18.0, 95.0), (20.0, 96.0)]
    assert recovery_time(rates, since=11.0, baseline=100.0) == pytest.approx(7.0)
    assert recovery_time(rates[:2], since=11.0, baseline=100.0) is None


def test_snapshot_lags_measure_from_due_moment() -> None:
    lags = snapshot_lags([1.0, 3.5, 10.0], [0.5, 2.0, 9.0], every=2.0)
    assert lags == pytest.approx([0.5, 0.5, 1.0])


def _sample(at: float, dead: int) -> TableSample:
    return TableSample(at, "th_lease", 10, dead, 0, 8192, None, None)


def test_monotonic_growth_needs_trend_and_slack() -> None:
    assert monotonic_growth([_sample(0, 0), _sample(1, 900), _sample(2, 2_000)])
    assert not monotonic_growth([_sample(0, 0), _sample(1, 3_000), _sample(2, 100)])
    assert not monotonic_growth([_sample(0, 0), _sample(1, 10)])


def test_prose_collapses_whitespace() -> None:
    assert prose("\n    a  b\n    c\n") == "a b c"


def test_ident_and_sql_quote_identifiers() -> None:
    assert ident('we"ird', "t") == '"we""ird"."t"'
    assert str(sql("SELECT 1 FROM {t}", t=ident("s", "t"))) == 'SELECT 1 FROM "s"."t"'


def test_app_config_json_round_trip() -> None:
    config = AppConfig(
        dsn="postgresql+asyncpg://u:p@h/db",
        schemas=Schemas("th", "fq", "app"),
        variant=Variant.FLEXIQ,
        concurrency=7,
        sleep_min=0.5,
        sleep_max=2,
        print_output=True,
        stats_path=None,
    )
    assert AppConfig.from_json(config.to_json()) == config


def test_read_events_parses_every_kind(tmp_path: Path) -> None:
    lines = [
        ["flush", 1.0, 3, 0.01],
        ["relay", 1.5, 4, 0.02],
        ["buffer", 2.0, 5],
        ["body", 3.0, "job", "item"],
        ["op", 4.0, "claim", 0.03],
        ["before", 5.0, "job"],
        ["after", 6.0, "job"],
    ]
    _ = (tmp_path / "stats-0-1.jsonl").write_text(
        "\n".join(json.dumps(line) for line in lines) + "\n\n", encoding="utf-8", newline="\n"
    )
    events = read_events(tmp_path)
    assert events.flushes == [(1.0, 3, 0.01)]
    assert events.relays == [(1.5, 4, 0.02)]
    assert events.buffers == [(2.0, 5)]
    assert events.bodies == [(3.0, "job", "item")]
    assert events.ops == [(4.0, "claim", 0.03)]
    assert events.before == {"job": 5.0}
    assert events.after == {"job": 6.0}


def _result(profile: Profile, *, passed: bool | None) -> ScenarioResult:
    result = ScenarioResult(id="P-01", title="t", measures="m", target="цель", profile=profile)
    result.checks = [Check("проверка", "≥ 80%", "75%", passed)]
    result.metrics = {"nan": math.nan, "list": [math.inf, 1.0]}
    result.tables.append(Table("Таблица", ("a", "b"), (("1", "x|y"),)))
    result.charts.extend(
        (
            LineChart(
                "line", "Линия", "x", "y", (Series("s", ((0.0, 1.0), (1.0, 2.0))),), limit=1.5
            ),
            BarChart("bar", "Столбцы", "y", (("a", 1.0), ("b", math.nan)), limit=0.5),
        )
    )
    result.notes.append("заметка")
    return result


def test_verdict_depends_on_profile_and_checks() -> None:
    assert _result(Profile.SMOKE, passed=False).verdict is Verdict.INFO
    assert _result(Profile.NIGHTLY, passed=False).verdict is Verdict.NOT_MET
    assert _result(Profile.FULL, passed=True).verdict is Verdict.MET
    assert _result(Profile.FULL, passed=None).met is None
    failed = _result(Profile.SMOKE, passed=True)
    failed.error = "boom"
    assert failed.verdict is Verdict.ERROR


def test_write_result_and_summary(tmp_path: Path) -> None:
    result = _result(Profile.NIGHTLY, passed=False)
    report = write_result(result, tmp_path / "P-01-nightly")
    text = report.read_text(encoding="utf-8")
    assert "цель не выполнена" in text
    assert "x\\|y" in text
    data = cast(
        "dict[str, object]",
        json.loads((tmp_path / "P-01-nightly" / "result.json").read_text(encoding="utf-8")),
    )
    assert data["verdict"] == "цель не выполнена"
    assert cast("dict[str, object]", data["metrics"])["nan"] is None
    assert (tmp_path / "P-01-nightly" / "line.svg").read_text(encoding="utf-8").startswith("<svg")
    write_summary([result], tmp_path / "summary-nightly.md")
    assert "P-01" in (tmp_path / "summary-nightly.md").read_text(encoding="utf-8")


def test_render_svg_handles_empty_series() -> None:
    svg = render_svg(LineChart("e", "Пусто", "x", "y", (Series("s", ()),)))
    assert svg.startswith("<svg")
    assert svg.rstrip().endswith("</svg>")
