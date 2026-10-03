"""Виртуальные Items под-батчей в счётчиках родителя (ARCHITECTURE §9.3, D-024)."""

from __future__ import annotations

from uuid import UUID

import pytest

from tallyho.model.progress import NodeCounters, compute_progress
from tallyho.model.states import BatchState

__all__: list[str] = []

ROOT, SEND, EXPAND = UUID(int=1), UUID(int=2), UUID(int=3)


def test_pipeline_root_counts_stages_but_ratio_counts_their_work() -> None:
    # Корень конвейера без своих Items: два виртуальных Item (вес 0), один этап финализирован.
    tree = [
        NodeCounters(id=ROOT, state=BatchState.SEALED, total=2, ok=1),
        NodeCounters(
            id=EXPAND,
            parent_id=ROOT,
            state=BatchState.SUCCEEDED,
            total=1,
            ok=1,
            w_total=1,
            w_done=1,
        ),
        NodeCounters(
            id=SEND,
            parent_id=ROOT,
            state=BatchState.SEALED,
            total=9,
            ok=3,
            w_total=9,
            w_done=3,
        ),
    ]

    root = compute_progress(tree)[ROOT]

    # found/done/pending — собственные Items узла, виртуальные включительно (§9.3, UC-06).
    assert (root.found, root.done, root.pending, root.expected) == (2, 1, 1, 2)
    # Доля — по работе поддерева, виртуальные Items вычтены (D-024): (1 + 3) / (1 + 9), а не 1/2.
    assert root.ratio == pytest.approx(0.4)
