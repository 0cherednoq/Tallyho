"""Имена строк ``th_metric``: метки итога отдельно от метрик (ARCHITECTURE §11.2)."""

from __future__ import annotations

import pytest

from tallyho.model.errors import ConfigurationError
from tallyho.storage.metric_names import (
    METRIC_PREFIX,
    check_counter_name,
    metric_rows,
    split_metric_rows,
)

__all__: list[str] = []


def test_metric_rows_prefix_names_and_are_idempotent() -> None:
    rows = metric_rows({"sent": 2, "bytes": 5})
    assert rows == {METRIC_PREFIX + "sent": 2, METRIC_PREFIX + "bytes": 5}
    assert metric_rows(rows) == rows
    # Уже переведённое и исходное имя — одна строка: значения складываются.
    assert metric_rows({"x": 1, METRIC_PREFIX + "x": 2}) == {METRIC_PREFIX + "x": 3}


def test_split_separates_label_and_metric_of_same_name() -> None:
    labels, metrics = split_metric_rows({"sent": 4, METRIC_PREFIX + "sent": 40, "ok": 1})
    assert labels == {"sent": 4, "ok": 1}
    assert metrics == {"sent": 40}


@pytest.mark.parametrize("name", ["sent", "", "a\x1f", " \x1f"])
def test_regular_names_are_accepted(name: str) -> None:
    check_counter_name(name, what="метки")


def test_reserved_first_character_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="U\\+001F"):
        check_counter_name(METRIC_PREFIX + "sent", what="метрики")
