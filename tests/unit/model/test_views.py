"""Value-объекты чтения: производные поля и неизменяемость."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest

from tallyho.model.states import BatchState, CancelReason, ItemState
from tallyho.model.views import BatchSummary, BatchView, InFlightItem, ItemView, Progress

BATCH_ID = UUID(int=1)
CHILD_ID = UUID(int=2)
NOW = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)


def _summary(**children: BatchSummary) -> BatchSummary:
    return BatchSummary(
        id=BATCH_ID,
        kind="campaign_deliveries",
        key="campaign:1",
        state=BatchState.SEALED,
        progress=Progress(found=10, ok=3),
        labels={"sent": 3},
        metrics={"bytes": 100},
        children=children,
        seq=4,
    )


def _view(**children: BatchView) -> BatchView:
    return BatchView(
        id=BATCH_ID,
        kind="k",
        key=None,
        state=BatchState.OPEN,
        progress=Progress(),
        labels={},
        metrics={},
        children=children,
    )


def test_progress_defaults_are_empty() -> None:
    progress = Progress()
    assert (progress.found, progress.done, progress.pending) == (0, 0, 0)
    assert progress.expected is None
    assert progress.ratio is None
    assert progress.eta is None
    assert progress.final is False


def test_progress_done_and_pending() -> None:
    progress = Progress(found=20, ok=5, skip=4, error=3, cancelled=2, in_flight=1, queued=5)
    assert progress.done == 14
    assert progress.pending == 6


def test_progress_is_frozen() -> None:
    progress = Progress()
    name = "found"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(progress, name, 1)


def test_summary_children_by_key() -> None:
    send = dataclasses.replace(_summary(), id=CHILD_ID, key="send")
    root = _summary(send=send)
    assert root.children["send"].progress.ok == 3
    assert root.reason is None
    assert root.finished_at is None


def test_summary_mappings_are_read_only_copies() -> None:
    labels = {"sent": 1}
    summary = dataclasses.replace(_summary(), labels=labels)
    labels["sent"] = 99
    assert summary.labels == {"sent": 1}
    with pytest.raises(TypeError):
        cast("dict[str, int]", summary.labels)["sent"] = 2
    with pytest.raises(TypeError):
        cast("dict[str, BatchSummary]", summary.children)["x"] = summary


def test_summary_equality_by_value() -> None:
    assert _summary() == _summary()
    assert dataclasses.replace(_summary(), reason=CancelReason.DEADLINE) != _summary()


def test_view_flags() -> None:
    view = _view()
    assert view.paused is False
    assert view.cancel_requested is False
    flagged = dataclasses.replace(view, paused_at=NOW, cancel_requested_at=NOW)
    assert flagged.paused is True
    assert flagged.cancel_requested is True


def test_view_children_are_read_only() -> None:
    child = _view()
    view = _view(expand=child)
    assert view.children["expand"] is child
    with pytest.raises(TypeError):
        cast("dict[str, int]", view.metrics)["x"] = 1


def test_item_view_defaults() -> None:
    item = ItemView(id=CHILD_ID, batch_id=BATCH_ID, state=ItemState.ERROR, task_name="t")
    assert item.label is None
    assert item.weight == 1
    assert item.result is None
    assert item.child_batch_id is None


def test_in_flight_item_fields() -> None:
    item = InFlightItem(
        id=CHILD_ID,
        batch_id=BATCH_ID,
        worker_id="w1",
        attempt=2,
        lease_until=NOW,
        age=timedelta(seconds=5),
    )
    assert item.progress_done is None
    assert item.progress_total is None
    assert dataclasses.replace(item, progress_done=3, progress_total=10).progress_done == 3
