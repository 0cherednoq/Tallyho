"""Значения счётчиков: ``CounterDelta`` и ``CounterTotals`` без БД."""

from __future__ import annotations

from tallyho.storage.counters import COUNTER_FIELDS, DELTA_FIELDS, CounterDelta, CounterTotals
from tallyho.storage.tables import build_metadata


def test_fields_match_counter_columns() -> None:
    tables = build_metadata()
    counter_columns = [c.name for c in tables.counter.columns if c.name not in {"batch_id", "slot"}]
    delta_columns = [
        c.name.removeprefix("d_")
        for c in tables.counter_delta.columns
        if c.name not in {"id", "batch_id"}
    ]
    assert list(COUNTER_FIELDS) == counter_columns
    assert list(DELTA_FIELDS) == delta_columns
    assert list(CounterDelta().as_dict()) == counter_columns


def test_delta_arithmetic() -> None:
    a = CounterDelta(total=3, ok=1, w_total=5, tree_total=3)
    b = CounterDelta(total=1, error=2, duplicates=4, skipped_by_limit=1, dispatched=2, w_done=7)
    s = a + b
    assert s == CounterDelta(
        total=4,
        ok=1,
        error=2,
        dispatched=2,
        w_total=5,
        w_done=7,
        duplicates=4,
        skipped_by_limit=1,
        tree_total=3,
    )
    assert s - b == a
    assert -a + a == CounterDelta()
    assert CounterDelta(skip=1, cancelled=2) - CounterDelta(skip=1, cancelled=2) == CounterDelta()


def test_is_zero() -> None:
    assert CounterDelta().is_zero
    assert not CounterDelta(tree_total=1).is_zero
    assert not CounterDelta(cancelled=-1).is_zero


def test_every_counter_has_delta_column() -> None:
    # Путь B записывает любое поле (D-029).
    assert DELTA_FIELDS == COUNTER_FIELDS


def test_totals_done_and_pending() -> None:
    totals = CounterTotals(total=10, ok=3, skip=2, error=1, cancelled=1, w_total=20)
    assert totals.done == 7
    assert totals.pending == 3


def test_totals_plus_delta() -> None:
    totals = CounterTotals(total=10, ok=3, dispatched=5)
    assert totals + CounterDelta(ok=1, dispatched=1) == CounterTotals(total=10, ok=4, dispatched=6)
    assert CounterTotals() + totals.as_delta() == totals
