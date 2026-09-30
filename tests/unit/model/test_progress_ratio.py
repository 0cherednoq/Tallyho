"""Доля по весам и сквозной пример §13.3 (конвейер парсинга, моменты t1-t5)."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tallyho.model.progress import NodeCounters, compute_progress
from tallyho.model.states import BatchState

ROOT = UUID(int=1)
PAGES = UUID(int=2)
CARDS = UUID(int=3)
PDFS = UUID(int=4)

# Веса §13.3: страница 1, карточка 2, PDF 4.
W_PAGE, W_CARD, W_PDF = 1, 2, 4


def _stage(
    node_id: UUID,
    counts: tuple[int, int, BatchState],
    *,
    weight: int,
    fed_by: tuple[UUID, ...] = (),
    expected_total: int | None = None,
) -> NodeCounters:
    found, done, state = counts
    return NodeCounters(
        id=node_id,
        parent_id=ROOT,
        state=state,
        total=found,
        ok=done,
        w_total=found * weight,
        w_done=done * weight,
        fed_by=fed_by,
        expected_total=expected_total,
    )


def _catalog(
    *,
    root: BatchState = BatchState.SEALED,
    root_ok: int = 0,
    pages: tuple[int, int, BatchState],
    cards: tuple[int, int, BatchState],
    pdfs: tuple[int, int, BatchState],
) -> list[NodeCounters]:
    # Корень закрыт при выходе из `async with th.batch(...)`; три виртуальных Item — этапы, вес 0.
    return [
        NodeCounters(id=ROOT, state=root, total=3, ok=root_ok),
        _stage(PAGES, pages, weight=W_PAGE, expected_total=24),
        _stage(CARDS, cards, weight=W_CARD, fed_by=(PAGES,)),
        _stage(PDFS, pdfs, weight=W_PDF, fed_by=(CARDS,)),
    ]


OPEN, SEALED, OK = BatchState.OPEN, BatchState.SEALED, BatchState.SUCCEEDED


@dataclass(frozen=True, slots=True)
class Row:
    """Строка таблицы §13.3: (done, expected, is_estimate) по этапам и общий %."""

    moment: str
    tree: list[NodeCounters]
    pages: tuple[int, int | None, bool]
    cards: tuple[int, int | None, bool]
    pdfs: tuple[int, int | None, bool]
    percent: int | None


TABLE = [
    Row(
        "t1: разобрана страница 1",
        _catalog(pages=(24, 1, OPEN), cards=(30, 0, OPEN), pdfs=(0, 0, OPEN)),
        pages=(1, 24, True),
        cards=(0, None, False),
        pdfs=(0, None, False),
        percent=None,
    ),
    Row(
        "t2",
        _catalog(pages=(24, 12, OPEN), cards=(350, 200, OPEN), pdfs=(510, 300, OPEN)),
        pages=(12, 24, True),
        cards=(200, 700, True),
        pdfs=(300, 1785, True),
        percent=19,
    ),
    Row(
        "t3: pages финализирован",
        _catalog(root_ok=1, pages=(24, 24, OK), cards=(712, 600, SEALED), pdfs=(1540, 1300, OPEN)),
        pages=(24, 24, False),
        cards=(600, 712, False),
        pdfs=(1300, 1827, True),
        percent=73,
    ),
    Row(
        "t4: cards финализирован",
        _catalog(root_ok=2, pages=(24, 24, OK), cards=(712, 712, OK), pdfs=(1810, 1700, SEALED)),
        pages=(24, 24, False),
        cards=(712, 712, False),
        pdfs=(1700, 1810, False),
        percent=95,
    ),
    Row(
        "t5",
        _catalog(
            root=OK, root_ok=3, pages=(24, 24, OK), cards=(712, 712, OK), pdfs=(1810, 1810, OK)
        ),
        pages=(24, 24, False),
        cards=(712, 712, False),
        pdfs=(1810, 1810, False),
        percent=100,
    ),
]


@pytest.mark.parametrize("row", TABLE, ids=[row.moment for row in TABLE])
def test_catalog_pipeline_table(row: Row) -> None:
    result = compute_progress(row.tree)
    for node_id, (done, expected, estimate) in (
        (PAGES, row.pages),
        (CARDS, row.cards),
        (PDFS, row.pdfs),
    ):
        p = result[node_id]
        assert (p.done, p.expected, p.expected_is_estimate) == (done, expected, estimate)
    ratio = result[ROOT].ratio
    if row.percent is None:
        assert ratio is None
    else:
        assert ratio is not None
        assert round(ratio * 100) == row.percent


def test_t2_root_ratio_matches_hand_calculation() -> None:
    result = compute_progress(TABLE[1].tree)
    # (12*1 + 200*2 + 300*4) / (24*1 + 700*2 + 1785*4) = 1 612 / 8 564
    assert result[ROOT].ratio == pytest.approx(1612 / 8564)
    # Доля этапа: w_done / (w_total / found * expected).
    assert result[CARDS].ratio == pytest.approx(400 / (700 / 350 * 700))
    assert result[PDFS].ratio == pytest.approx(1200 / (2040 / 510 * 1785))


def test_t5_everything_final() -> None:
    result = compute_progress(TABLE[4].tree)
    assert all(p.final for p in result.values())
    assert result[ROOT].ratio == pytest.approx(1.0)


# --- доля: частные случаи ------------------------------------------------------------


def test_leaf_without_expected_has_no_ratio() -> None:
    node = NodeCounters(id=ROOT, total=10, ok=5, w_total=10, w_done=5)
    assert compute_progress([node])[ROOT].ratio is None


def test_empty_sealed_batch_is_complete() -> None:
    assert compute_progress([NodeCounters(id=ROOT, state=BatchState.SEALED)])[
        ROOT
    ].ratio == pytest.approx(1.0)


def test_empty_open_batch_with_zero_expected_has_no_ratio() -> None:
    node = NodeCounters(id=ROOT, expected_total=0)
    assert compute_progress([node])[ROOT].ratio is None


def test_expected_before_first_item_uses_unit_weight() -> None:
    node = NodeCounters(id=ROOT, expected_total=40)
    p = compute_progress([node])[ROOT]
    assert (p.expected, p.ratio) == (40, 0.0)


def test_parent_with_own_items_and_unknown_expected_blocks_root_ratio() -> None:
    root = NodeCounters(id=ROOT, total=5, ok=1, w_total=4, w_done=1)  # 4 своих Item + 1 виртуальный
    child = NodeCounters(id=PAGES, parent_id=ROOT, state=BatchState.SEALED, total=2, w_total=2)
    assert compute_progress([root, child])[ROOT].ratio is None


def test_parent_own_items_and_children_are_summed() -> None:
    root = NodeCounters(id=ROOT, state=BatchState.SEALED, total=5, ok=3, w_total=4, w_done=3)
    child = NodeCounters(
        id=PAGES, parent_id=ROOT, state=BatchState.SEALED, total=4, ok=1, w_total=4, w_done=1
    )
    # Доля: (3 + 1) из (4 + 4).
    assert compute_progress([root, child])[ROOT].ratio == pytest.approx(0.5)


# --- свойства (hypothesis) -----------------------------------------------------------


@st.composite
def _node(draw: st.DrawFn, node_id: UUID, fed_by: tuple[UUID, ...]) -> NodeCounters:
    total = draw(st.integers(0, 500))
    ok = draw(st.integers(0, total))
    error = draw(st.integers(0, total - ok))
    weight = draw(st.integers(1, 5))
    done = ok + error
    return NodeCounters(
        id=node_id,
        parent_id=ROOT,
        state=draw(st.sampled_from(BatchState)),
        total=total,
        ok=ok,
        error=error,
        w_total=total * weight,
        w_done=draw(st.integers(0, done * weight)),
        in_flight=draw(st.integers(0, total - done)),
        expected_total=draw(st.none() | st.integers(0, 1_000)),
        fed_by=fed_by,
    )


@st.composite
def _trees(draw: st.DrawFn) -> list[NodeCounters]:
    root = NodeCounters(id=ROOT, state=draw(st.sampled_from(BatchState)), total=3)
    return [
        root,
        draw(_node(PAGES, ())),
        draw(_node(CARDS, (PAGES,))),
        draw(_node(PDFS, (CARDS, PAGES))),
    ]


@given(_trees())
def test_properties(tree: list[NodeCounters]) -> None:
    result = compute_progress(tree)
    for node in tree:
        p = result[node.id]
        if p.expected is not None:
            assert p.expected >= p.found
        if node.state is not BatchState.OPEN:
            assert p.expected == p.found
            assert not p.expected_is_estimate
        if p.ratio is not None:
            assert 0.0 <= p.ratio <= 1.0
        if node.state is not BatchState.OPEN and node.id != ROOT:  # закрытый лист
            assert p.ratio is not None
        assert 0 <= p.queued <= p.pending
