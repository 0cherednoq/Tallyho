"""Расписание хаоса, журнал, объём нагрузки и ожидания A-CH - без Docker и БД."""

from __future__ import annotations

import json
from itertools import pairwise
from typing import TYPE_CHECKING

import pytest

from tests.acceptance.chaos.journal import ChaosJournal
from tests.acceptance.chaos.load import audience_for, plan_load
from tests.acceptance.chaos.plan import (
    CHAOS_IDS,
    FAKETIME_LIBRARY,
    PROCESSES,
    WORKERS,
    ActionKind,
    PlanError,
    build_plan,
)
from tests.acceptance.chaos.stand import StandSettings
from tests.acceptance.chaos.verdict import Facts, check_expectations, disruption_budget

if TYPE_CHECKING:
    from pathlib import Path

    from tests.acceptance.chaos.plan import Action

__all__: list[str] = []

DURATIONS = (120.0, 600.0, 3600.0, 7200.0)


def _of(actions: tuple[Action, ...], *kinds: ActionKind) -> list[Action]:
    return [action for action in actions if action.kind in kinds]


def _facts(**stats: int) -> Facts:
    return Facts(
        restarts=dict.fromkeys(PROCESSES, 0),
        running=dict.fromkeys(PROCESSES, True),
        skew={},
        sweep_interval=1.0,
        stats=stats,
    )


@pytest.mark.parametrize("chaos", CHAOS_IDS)
@pytest.mark.parametrize("duration", DURATIONS)
def test_plan_is_reproducible_from_seed(chaos: str, duration: float) -> None:
    first = build_plan(chaos, 17, duration)
    second = build_plan(chaos, 17, duration)
    other = build_plan(chaos, 18, duration)

    assert first == second
    assert first.actions == tuple(sorted(first.actions, key=lambda action: action.at))
    assert all(action.at >= 0 for action in first.actions)
    if chaos != "A-CH-09":
        assert first.actions
        assert first.actions != other.actions


def test_plan_rejects_unknown_chaos_and_too_short_window() -> None:
    with pytest.raises(PlanError, match="A-CH-13"):
        _ = build_plan("A-CH-13", 1, 120)
    with pytest.raises(PlanError, match="меньше 30"):
        _ = build_plan("A-CH-01", 1, 10)


@pytest.mark.parametrize("duration", DURATIONS)
def test_worker_kills_every_5_to_30_seconds_with_restart(duration: float) -> None:
    plan = build_plan("A-CH-01", 3, duration)
    kills = _of(plan.actions, ActionKind.KILL_WORKER)
    starts = _of(plan.actions, ActionKind.START_SERVICE)

    assert len(kills) == len(starts) >= duration / 30 - 1
    assert {kill.target for kill in kills} <= set(WORKERS)
    assert 5 <= kills[0].at <= 30
    gaps = [later.at - earlier.at for earlier, later in pairwise(kills)]
    assert all(5 <= gap <= 30.001 for gap in gaps)
    assert all(kill.at < duration for kill in kills)


@pytest.mark.parametrize("duration", DURATIONS)
def test_postgres_outages_alternate_both_stop_modes(duration: float) -> None:
    plan = build_plan("A-CH-02", 3, duration)
    stops = _of(plan.actions, ActionKind.KILL_PG, ActionKind.STOP_PG_IMMEDIATE)
    starts = _of(plan.actions, ActionKind.START_PG)

    assert len(stops) == len(starts) >= 2
    assert [stop.kind for stop in stops[:2]] == [ActionKind.KILL_PG, ActionKind.STOP_PG_IMMEDIATE]
    for stop, start in zip(stops, starts, strict=True):
        assert 5 <= start.at - stop.at <= 60
    assert all(later.at > earlier.at for earlier, later in zip(starts, stops[1:], strict=False)), (
        "следующая остановка начинается после подъёма"
    )


def test_triggered_stops_carry_watch_window_and_downtime() -> None:
    hook = build_plan("A-CH-03", 5, 120)
    flush = build_plan("A-CH-04", 5, 120)

    assert hook.environment == {"HOOK_DELAY": "3"}
    arms = _of(hook.actions, ActionKind.KILL_PG_ON_HOOK)
    assert arms
    assert all(20 <= arm.params["wait"] <= 40 or arm.at + arm.params["wait"] >= 119 for arm in arms)
    assert all(5 <= arm.params["down"] <= 15 for arm in arms)
    assert all(
        later.at == pytest.approx(earlier.at + earlier.params["wait"], abs=0.01)
        for earlier, later in pairwise(arms)
    ), "окна наблюдения идут вплотную"
    assert 115 <= arms[-1].at + arms[-1].params["wait"] <= 120.01
    assert _of(flush.actions, ActionKind.STOP_PG_ON_FLUSH)
    slowed = _of(flush.actions, ActionKind.ADD_LATENCY)
    assert {action.target for action in slowed} == set(WORKERS)
    assert all(action.at == 0 for action in slowed)


@pytest.mark.parametrize("duration", DURATIONS)
def test_network_cuts_one_worker_for_10_to_60_seconds(duration: float) -> None:
    plan = build_plan("A-CH-05", 9, duration)
    cuts = _of(plan.actions, ActionKind.CUT_NETWORK)
    heals = _of(plan.actions, ActionKind.HEAL_NETWORK)

    assert len(cuts) == len(heals) >= 1
    for cut, heal in zip(cuts, heals, strict=True):
        assert cut.target == heal.target
        assert cut.target in WORKERS
        assert 10 <= heal.at - cut.at <= 60


def test_latency_covers_every_process_for_the_whole_run() -> None:
    plan = build_plan("A-CH-06", 2, 120)
    added = _of(plan.actions, ActionKind.ADD_LATENCY)
    removed = _of(plan.actions, ActionKind.REMOVE_LATENCY)

    assert {action.target for action in added} == set(PROCESSES)
    assert {action.target for action in removed} == set(PROCESSES)
    assert all(action.at == 0 for action in added)
    assert all(action.at == 120 for action in removed)
    assert len({action.params["latency_ms"] for action in added}) == 1
    assert 50 <= added[0].params["latency_ms"] <= 500
    assert added[0].params["jitter_ms"] == 100


def test_clock_skew_moves_two_different_workers_by_five_minutes() -> None:
    plan = build_plan("A-CH-09", 4, 120)
    offsets = {key: value for key, value in plan.environment.items() if "LIB" not in key}
    libraries = {key: value for key, value in plan.environment.items() if "LIB" in key}

    assert not plan.actions
    assert sorted(offsets.values()) == ["+5m", "-5m"]
    assert len(offsets) == 2
    assert set(libraries.values()) == {FAKETIME_LIBRARY}
    assert {key.removeprefix("FAKETIME_LIB_") for key in libraries} == {
        key.removeprefix("FAKETIME_") for key in offsets
    }


def test_remaining_single_faults_have_their_actions() -> None:
    leader = _of(build_plan("A-CH-07", 1, 120).actions, ActionKind.KILL_LEADER)
    soft = _of(build_plan("A-CH-08", 1, 120).actions, ActionKind.TERM_WORKERS)
    redeliver = _of(build_plan("A-CH-10", 1, 120).actions, ActionKind.REDELIVER)
    long_tx = build_plan("A-CH-11", 1, 120).actions

    assert leader
    assert all(3 <= action.params["restart"] <= 8 for action in leader)
    assert soft
    assert len(redeliver) >= 120 / 10
    assert all(action.params["fraction"] == pytest.approx(0.1) for action in redeliver)
    assert [action.kind for action in long_tx] == [
        ActionKind.BEGIN_LONG_TX,
        ActionKind.END_LONG_TX,
    ]
    assert long_tx[1].at - long_tx[0].at == pytest.approx(48)
    ten_minutes = build_plan("A-CH-11", 1, 3600).actions
    assert ten_minutes[1].at - ten_minutes[0].at == pytest.approx(600)


def test_everything_at_once_combines_five_faults() -> None:
    plan = build_plan("A-CH-12", 6, 120)
    kinds = {action.kind for action in plan.actions}

    assert {
        ActionKind.KILL_WORKER,
        ActionKind.KILL_PG,
        ActionKind.START_PG,
        ActionKind.CUT_NETWORK,
        ActionKind.ADD_LATENCY,
        ActionKind.REDELIVER,
    } <= kinds
    assert ActionKind.KILL_LEADER not in kinds
    assert not plan.environment


def test_journal_appends_jsonl_and_filters_events(tmp_path: Path) -> None:
    path = tmp_path / "chaos-journal.jsonl"
    journal = ChaosJournal(path)
    journal.start()

    _ = journal.record("kill_worker", "worker-2", signal="KILL", delivered=True)
    _ = journal.record("start_service", "worker-2")
    _ = journal.record("kill_worker", "worker-3", signal="KILL", delivered=False)

    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [line["seq"] for line in lines] == [1, 2, 3]
    assert lines[0]["event"] == "kill_worker"
    assert lines[0]["target"] == "worker-2"
    assert lines[0]["detail"] == {"signal": "KILL", "delivered": True}
    assert lines[0]["t"] >= 0
    assert lines[0]["wall"].endswith("+00:00")
    assert [entry.target for entry in journal.events("kill_worker")] == ["worker-2", "worker-3"]


def test_load_grows_with_duration_and_keeps_batches_small() -> None:
    settings = StandSettings(seed=1)
    short = plan_load("S1", 1, 120, settings=settings)
    long = plan_load("S1", 1, 600, settings=settings)
    campaign = plan_load("S2", 1, 120, settings=settings)
    catalog = plan_load("S3", 1, 120, settings=settings)

    assert short.batches >= 2
    assert long.batches > short.batches
    assert 100 <= short.size <= 250
    assert short.expected_items == short.batches * short.size
    assert short.stagger * short.batches == pytest.approx(0.6 * 120, abs=0.01)
    assert campaign.expected_items > campaign.batches * campaign.size
    assert catalog.size == settings.pages
    assert catalog.batches >= 2


def test_audience_is_seeded_and_contains_duplicates() -> None:
    first = audience_for(1, 3, 400)

    assert first == audience_for(1, 3, 400)
    assert first != audience_for(2, 3, 400)
    unique = {address.strip().casefold() for address in first}
    assert len(first) - len(unique) == len(first) // 100
    assert any(address.startswith("retry-") for address in first)


def test_disruption_budget_counts_only_interrupting_faults() -> None:
    journal = ChaosJournal()
    for event in ("kill_worker", "cut_network", "kill_pg", "redeliver", "add_latency"):
        _ = journal.record(event)

    assert disruption_budget(journal, threads=8, workers=4) == 8 + 8 + 32


def test_expectations_require_the_fault_to_happen() -> None:
    journal = ChaosJournal()

    missing = check_expectations("A-CH-01", journal, _facts())
    _ = journal.record("kill_worker", "worker-1")
    happened = check_expectations("A-CH-01", journal, _facts())
    premature = check_expectations("A-CH-01", journal, _facts(lease_expired_with_attempts_left=2))

    assert [item.ok for item in missing] == [True, False, True]
    assert all(item.ok for item in happened)
    assert [item.ok for item in premature] == [True, True, False]


def test_expectations_for_leader_soft_stop_and_crashed_process() -> None:
    journal = ChaosJournal()
    _ = journal.record("kill_leader", "api-1", takeover_seconds=0.4, successor="api-2")
    _ = journal.record("term_workers", exit_seconds=dict.fromkeys(WORKERS, 2.5), leases_left=0)
    crashed = Facts(
        restarts={**dict.fromkeys(PROCESSES, 0), "worker-1": 1},
        running=dict.fromkeys(PROCESSES, True),
        skew={},
        sweep_interval=1.0,
        stats={},
    )

    assert all(item.ok for item in check_expectations("A-CH-07", journal, _facts()))
    assert all(item.ok for item in check_expectations("A-CH-08", journal, _facts()))
    assert not check_expectations("A-CH-07", journal, crashed)[0].ok

    _ = journal.record("kill_leader", "api-2", takeover_seconds=9.0, successor="api-1")
    _ = journal.record("term_workers", exit_seconds=dict.fromkeys(WORKERS, 2.5), leases_left=3)

    assert not check_expectations("A-CH-07", journal, _facts())[1].ok
    assert not check_expectations("A-CH-08", journal, _facts())[1].ok


def test_expectations_for_skew_redelivery_and_long_transaction() -> None:
    journal = ChaosJournal()
    skewed = Facts(
        restarts=dict.fromkeys(PROCESSES, 0),
        running=dict.fromkeys(PROCESSES, True),
        skew={"worker-1": 299.6, "worker-2": -0.2, "worker-3": -300.4, "worker-4": 0.1},
        sweep_interval=1.0,
        stats={},
    )
    _ = journal.record("redeliver", counts={"requeue_job": 2, "replay": 1, "retry_dead": 0})
    _ = journal.record("begin_long_tx", backend_xmin="771", rate_before=8.0)
    _ = journal.record("end_long_tx", backend_xmin="771", held_seconds=48.0, rate_during=7.5)

    assert all(item.ok for item in check_expectations("A-CH-09", journal, skewed))
    assert not check_expectations("A-CH-09", journal, _facts())[1].ok
    assert all(item.ok for item in check_expectations("A-CH-10", journal, _facts()))
    assert all(item.ok for item in check_expectations("A-CH-11", journal, _facts()))
