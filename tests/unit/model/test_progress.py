"""Математика прогресса: счётчики, правило ``expected``, оценка Кнута (ARCHITECTURE §9.3-9.4)."""

from __future__ import annotations

from typing import cast
from uuid import UUID, uuid4

import pytest

from tallyho.model.errors import ConfigurationError
from tallyho.model.progress import (
    NodeCounters,
    ProgressSettings,
    compute_progress,
    estimate_threshold,
)
from tallyho.model.states import BatchState

ROOT = UUID(int=1)
PAGES = UUID(int=2)
CARDS = UUID(int=3)
PDFS = UUID(int=4)


# --- счётчики ------------------------------------------------------------------------


def test_counts_found_done_pending_queued() -> None:
    node = NodeCounters(
        id=ROOT,
        total=100,
        ok=50,
        skip=5,
        error=3,
        cancelled=2,
        in_flight=10,
        duplicates=7,
        skipped_by_limit=4,
    )
    p = compute_progress([node])[ROOT]
    assert (p.found, p.done, p.pending, p.queued, p.in_flight) == (100, 60, 40, 30, 10)
    assert (p.duplicates, p.skipped_by_limit) == (7, 4)
    assert p.final is False


def test_queued_never_negative() -> None:
    # lease может пережить завершение Item на мгновение: queued не уходит в минус.
    p = compute_progress([NodeCounters(id=ROOT, total=3, ok=2, in_flight=5)])[ROOT]
    assert p.queued == 0


TERMINAL = {
    BatchState.SUCCEEDED,
    BatchState.COMPLETED_WITH_ERRORS,
    BatchState.FAILED,
    BatchState.CANCELLED,
}


@pytest.mark.parametrize("state", list(BatchState))
def test_final_is_terminal_state(state: BatchState) -> None:
    assert compute_progress([NodeCounters(id=ROOT, state=state)])[ROOT].final is (state in TERMINAL)


# --- правило expected (таблица §9.4) -------------------------------------------------


@pytest.mark.parametrize("state", [s for s in BatchState if s is not BatchState.OPEN])
def test_closed_batch_expected_is_found(state: BatchState) -> None:
    node = NodeCounters(id=ROOT, state=state, total=12, ok=3, expected_total=50, fed_by=())
    p = compute_progress([node])[ROOT]
    assert (p.expected, p.expected_is_estimate, p.estimate_basis) == (12, False, None)


def test_expected_total_is_estimate_until_sealed() -> None:
    p = compute_progress([NodeCounters(id=ROOT, total=10, expected_total=500)])[ROOT]
    assert (p.expected, p.expected_is_estimate) == (500, True)


def test_expected_total_never_below_found() -> None:
    p = compute_progress([NodeCounters(id=ROOT, total=700, expected_total=500)])[ROOT]
    assert (p.expected, p.expected_is_estimate) == (700, True)


def test_open_batch_without_hints_has_no_expected() -> None:
    p = compute_progress([NodeCounters(id=ROOT, total=10, ok=3)])[ROOT]
    assert (p.expected, p.expected_is_estimate, p.estimate_basis) == (None, False, None)


def test_expected_total_wins_over_fed_by() -> None:
    # §12.6: send стартует до конца expand, итог известен сразу из expected_total.
    expand = NodeCounters(id=PAGES, parent_id=ROOT, total=1, ok=1)
    send = NodeCounters(
        id=CARDS, parent_id=ROOT, total=2, ok=2, fed_by=(PAGES,), expected_total=10_000
    )
    root = NodeCounters(id=ROOT, state=BatchState.SEALED, total=2)
    p = compute_progress([root, expand, send])[CARDS]
    assert p.done > 0
    assert p.expected == 10_000
    assert p.expected_is_estimate


# --- оценка Кнута по fed_by ----------------------------------------------------------


def test_threshold_is_min_of_basis_and_share() -> None:
    settings = ProgressSettings()
    assert estimate_threshold(24, settings) == pytest.approx(1.2)
    assert estimate_threshold(10_000, settings) == pytest.approx(20)


def _pages(*, done: int, expected: int = 24, state: BatchState = BatchState.OPEN) -> NodeCounters:
    return NodeCounters(
        id=PAGES, parent_id=ROOT, state=state, total=expected, ok=done, expected_total=expected
    )


def test_no_estimate_below_threshold_but_basis_reported() -> None:
    # §13.3 t1: выборка 1 страница меньше min(20, 5% * 24 = 1,2).
    cards = NodeCounters(id=CARDS, parent_id=ROOT, total=30, fed_by=(PAGES,))
    p = compute_progress([_pages(done=1), cards])[CARDS]
    assert (p.expected, p.expected_is_estimate, p.estimate_basis) == (None, False, 1)


def test_estimate_appears_at_threshold() -> None:
    cards = NodeCounters(id=CARDS, parent_id=ROOT, total=60, fed_by=(PAGES,))
    p = compute_progress([_pages(done=2), cards])[CARDS]
    assert (p.expected, p.expected_is_estimate, p.estimate_basis) == (720, True, 2)


def test_large_source_needs_twenty_parents() -> None:
    audience = NodeCounters(id=PAGES, total=10_000, ok=19, expected_total=10_000)
    send = NodeCounters(id=CARDS, total=19, fed_by=(PAGES,))
    assert compute_progress([audience, send])[CARDS].expected is None
    audience = NodeCounters(id=PAGES, total=10_000, ok=20, expected_total=10_000)
    send = NodeCounters(id=CARDS, total=20, fed_by=(PAGES,))
    assert compute_progress([audience, send])[CARDS].expected == 10_000


def test_estimate_is_recursive_and_rounded() -> None:
    # §13.3 t3: cards закрыт (712), pdfs оценивается по 600 карточкам: 1540/600 * 712 ≈ 1 827.
    cards = NodeCounters(
        id=CARDS, parent_id=ROOT, state=BatchState.SEALED, total=712, ok=600, fed_by=(PAGES,)
    )
    pdfs = NodeCounters(id=PDFS, parent_id=ROOT, total=1540, ok=1300, fed_by=(CARDS,))
    pages = _pages(done=24, state=BatchState.SUCCEEDED)
    p = compute_progress([pages, cards, pdfs])[PDFS]
    assert (p.expected, p.expected_is_estimate, p.estimate_basis) == (1827, True, 600)


def test_unknown_source_expected_gives_no_estimate() -> None:
    source = NodeCounters(id=PAGES, total=100, ok=100)
    target = NodeCounters(id=CARDS, total=300, fed_by=(PAGES,))
    p = compute_progress([source, target])[CARDS]
    assert (p.expected, p.estimate_basis) == (None, 100)


def test_several_sources_are_summed() -> None:
    a = NodeCounters(id=PAGES, state=BatchState.SEALED, total=10, ok=10)
    b = NodeCounters(id=PDFS, total=5, ok=0, expected_total=30)
    target = NodeCounters(id=CARDS, total=50, fed_by=(PAGES, PDFS))
    p = compute_progress([a, b, target])[CARDS]
    # 50 / 10 * (10 + 30) = 200
    assert (p.expected, p.estimate_basis) == (200, 10)


def test_estimate_rounds_to_nearest() -> None:
    source = NodeCounters(id=PAGES, total=40, ok=30, expected_total=40)
    target = NodeCounters(id=CARDS, total=10, fed_by=(PAGES,))
    settings = ProgressSettings(estimate_min_basis=1)
    # 10 / 30 * 40 = 13,33 -> 13; 20 / 30 * 40 = 26,67 -> 27
    assert compute_progress([source, target], settings=settings)[CARDS].expected == 13
    target = NodeCounters(id=CARDS, total=20, fed_by=(PAGES,))
    assert compute_progress([source, target], settings=settings)[CARDS].expected == 27


def test_empty_closed_source_gives_found() -> None:
    source = NodeCounters(id=PAGES, state=BatchState.SUCCEEDED)
    target = NodeCounters(id=CARDS, total=0, fed_by=(PAGES,))
    p = compute_progress([source, target])[CARDS]
    assert (p.expected, p.expected_is_estimate, p.estimate_basis) == (0, True, 0)


def test_fed_by_cycle_is_rejected() -> None:
    a = NodeCounters(id=PAGES, fed_by=(CARDS,))
    b = NodeCounters(id=CARDS, fed_by=(PAGES,))
    with pytest.raises(ConfigurationError, match="цикл"):
        compute_progress([a, b])


def test_unknown_source_is_rejected() -> None:
    target = NodeCounters(id=CARDS, fed_by=(uuid4(),))
    with pytest.raises(ConfigurationError, match="не входит"):
        compute_progress([target])


def test_duplicate_node_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="дважды"):
        compute_progress([NodeCounters(id=ROOT), NodeCounters(id=ROOT)])


@pytest.mark.parametrize(
    ("basis", "share", "match"),
    [
        (-1, 0.05, "estimate_min_basis"),
        (True, 0.05, "estimate_min_basis"),
        (20, 1.5, "estimate_min_share"),
        (20, False, "estimate_min_share"),
    ],
)
def test_settings_are_validated(basis: object, share: object, match: str) -> None:
    with pytest.raises(ConfigurationError, match=match):
        ProgressSettings(
            estimate_min_basis=cast("int", basis), estimate_min_share=cast("float", share)
        )
