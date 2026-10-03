"""Эффективный лимит повторов Item: опция вызова, умолчание адаптера, 0 (D-012)."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from typing_extensions import override

from tallyho.engine.retry_limits import effective_max_retries
from tallyho.protocols.broker import RetryLimits

__all__: list[str] = []


@dataclass(frozen=True)
class _Limits(RetryLimits):
    value: int

    @override
    def max_retries(self, task_name: str) -> int:
        return self.value if task_name == "send" else 0


_Case = tuple[dict[str, object], str, RetryLimits | None]
CASES: list[tuple[_Case, int]] = [
    (({"max_retries": 5}, "send", _Limits(2)), 5),
    (({}, "send", _Limits(2)), 2),
    (({}, "other", _Limits(2)), 0),
    (({}, "send", None), 0),
    (({"max_retries": 4}, "send", None), 4),
    (({"max_retries": True}, "send", _Limits(2)), 0),
    (({"max_retries": -1}, "send", None), 0),
    (({"max_retries": "3"}, "send", None), 0),
    (({}, "send", _Limits(-3)), 0),
]


@pytest.mark.parametrize(("case", "expected"), CASES)
def test_effective_max_retries(case: _Case, expected: int) -> None:
    options, task_name, limits = case
    assert effective_max_retries(options, task_name, limits) == expected
