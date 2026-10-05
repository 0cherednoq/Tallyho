"""ETA: EMA скорости и время до опустошения (ARCHITECTURE §9.4)."""

from __future__ import annotations

import math
from datetime import timedelta
from typing import cast
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tallyho.model.errors import ConfigurationError
from tallyho.model.progress import (
    DEFAULT_ETA_WINDOW,
    NodeCounters,
    ProgressSettings,
    RateTracker,
    compute_progress,
    ema_rate,
    estimate_eta,
)
from tallyho.model.states import BatchState

ROOT = UUID(int=1)
PAGES = UUID(int=2)
CARDS = UUID(int=3)


# --- ema_rate ------------------------------------------------------------------------


def test_first_sample_is_instant_rate() -> None:
    assert ema_rate(None, done_delta=100, elapsed=timedelta(seconds=2)) == pytest.approx(50)


def test_no_time_passed_keeps_previous() -> None:
    assert ema_rate(7.0, done_delta=100, elapsed=timedelta(0)) == pytest.approx(7.0)
    assert ema_rate(None, done_delta=100, elapsed=timedelta(0)) is None


def test_smoothing_depends_on_elapsed_time() -> None:
    # Замер длиной в окно получает вес 1 - 1/e.
    rate = ema_rate(10.0, done_delta=200, elapsed=DEFAULT_ETA_WINDOW)
    alpha = 1 - math.exp(-1)
    assert rate == pytest.approx(10.0 + alpha * (200 / 60 - 10.0))


def test_short_sample_moves_rate_a_little() -> None:
    rate = ema_rate(10.0, done_delta=0, elapsed=timedelta(seconds=1), window=timedelta(seconds=60))
    assert rate is not None
    assert 9.8 < rate < 10.0


def test_negative_delta_counts_as_zero() -> None:
    assert ema_rate(None, done_delta=-5, elapsed=timedelta(seconds=1)) == pytest.approx(0.0)


@given(
    st.floats(0, 1e6),
    st.integers(0, 10**6),
    st.floats(0.001, 3600),
)
def test_ema_stays_between_previous_and_instant(
    previous: float, delta: int, seconds: float
) -> None:
    rate = ema_rate(previous, done_delta=delta, elapsed=timedelta(seconds=seconds))
    assert rate is not None
    instant = delta / timedelta(seconds=seconds).total_seconds()
    low, high = min(previous, instant), max(previous, instant)
    assert low - 1e-6 * max(1.0, high) <= rate <= high + 1e-6 * max(1.0, high)


# --- RateTracker ---------------------------------------------------------------------


def test_tracker_first_sample_has_no_rate() -> None:
    assert RateTracker().observe({ROOT: 10}, now=5.0) == {}


def test_tracker_rate_follows_samples_and_waits_for_time() -> None:
    tracker = RateTracker()
    _ = tracker.observe({ROOT: 0, PAGES: 3}, now=0.0)
    # Без прошедшего времени замер копится: прирост учтётся следующим чтением.
    assert tracker.observe({ROOT: 4, PAGES: 3}, now=0.0) == {}
    rates = tracker.observe({ROOT: 10, PAGES: 3}, now=2.0)
    assert rates == {ROOT: pytest.approx(5.0)}


def test_tracker_forgets_batches_missing_from_sample() -> None:
    tracker = RateTracker()
    _ = tracker.observe({ROOT: 0}, now=0.0)
    _ = tracker.observe({PAGES: 0}, now=1.0)
    # ROOT пропал и вернулся: это снова первый замер, скорости нет.
    assert tracker.observe({ROOT: 50, PAGES: 2}, now=2.0) == {PAGES: pytest.approx(2.0)}


def test_tracker_uses_window() -> None:
    tracker = RateTracker()
    _ = tracker.observe({ROOT: 0}, now=0.0)
    _ = tracker.observe({ROOT: 10}, now=1.0)
    rates = tracker.observe({ROOT: 10}, now=2.0, window=timedelta(seconds=1))
    assert rates == {ROOT: pytest.approx(10.0 * math.exp(-1))}


# --- estimate_eta --------------------------------------------------------------------


def test_eta_is_remaining_over_rate() -> None:
    assert estimate_eta(expected=1000, done=400, rate=10.0) == timedelta(seconds=60)


def test_eta_without_expected_or_rate_is_none() -> None:
    assert estimate_eta(expected=None, done=0, rate=10.0) is None
    assert estimate_eta(expected=100, done=0, rate=None) is None
    assert estimate_eta(expected=100, done=0, rate=0.0) is None


def test_eta_when_nothing_left_is_zero() -> None:
    assert estimate_eta(expected=100, done=100, rate=None) == timedelta(0)


# --- ETA в compute_progress ----------------------------------------------------------


def test_leaf_eta_uses_its_rate() -> None:
    node = NodeCounters(id=ROOT, total=100, ok=40, expected_total=100)
    assert compute_progress([node])[ROOT].eta is None
    assert compute_progress([node], rates={ROOT: 2.0})[ROOT].eta == timedelta(seconds=30)


def test_root_eta_is_slowest_stage() -> None:
    root = NodeCounters(id=ROOT, state=BatchState.SEALED, total=2)
    fast = NodeCounters(id=PAGES, parent_id=ROOT, state=BatchState.SEALED, total=100, ok=50)
    slow = NodeCounters(id=CARDS, parent_id=ROOT, total=10, expected_total=100)
    rates = {PAGES: 10.0, CARDS: 1.0}
    result = compute_progress([root, fast, slow], rates=rates)
    assert result[PAGES].eta == timedelta(seconds=5)
    assert result[CARDS].eta == timedelta(seconds=100)
    assert result[ROOT].eta == timedelta(seconds=100)


def test_root_eta_unknown_if_stage_unknown() -> None:
    root = NodeCounters(id=ROOT, state=BatchState.SEALED, total=2)
    known = NodeCounters(id=PAGES, parent_id=ROOT, state=BatchState.SEALED, total=100, ok=50)
    unknown = NodeCounters(id=CARDS, parent_id=ROOT, total=10)
    result = compute_progress([root, known, unknown], rates={PAGES: 10.0, CARDS: 1.0})
    assert result[ROOT].eta is None


def test_parent_own_items_count_without_virtual() -> None:
    # 4 своих Item (2 готово) + виртуальный Item финализированного ребёнка.
    root = NodeCounters(id=ROOT, state=BatchState.SEALED, total=5, ok=3)
    child = NodeCounters(id=PAGES, parent_id=ROOT, state=BatchState.SUCCEEDED, total=1, ok=1)
    result = compute_progress([root, child], rates={ROOT: 1.0})
    assert result[ROOT].eta == timedelta(seconds=2)


def test_finished_tree_eta_is_zero() -> None:
    root = NodeCounters(id=ROOT, state=BatchState.SUCCEEDED, total=1, ok=1)
    child = NodeCounters(id=PAGES, parent_id=ROOT, state=BatchState.SUCCEEDED, total=3, ok=3)
    assert compute_progress([root, child])[ROOT].eta == timedelta(0)


@pytest.mark.parametrize("window", [timedelta(0), timedelta(seconds=-1), 60])
def test_eta_window_is_validated(window: object) -> None:
    with pytest.raises(ConfigurationError, match="eta_window"):
        ProgressSettings(eta_window=cast("timedelta", window))
