"""Итоги-значения ``Ok``/``Skip``/``Error``/``Defer`` (UC-18, A-WI-05)."""

from __future__ import annotations

import dataclasses
import itertools
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

import tallyho
from tallyho.model.errors import ConfigurationError
from tallyho.model.outcomes import Defer, Error, Ok, Skip

if TYPE_CHECKING:
    from collections.abc import Callable

    from tallyho.model.outcomes import Outcome

__all__: list[str] = []

_MOMENT = datetime(2030, 1, 1, tzinfo=UTC)


def test_outcomes_are_exported_from_package() -> None:
    assert (tallyho.Ok, tallyho.Skip, tallyho.Error, tallyho.Defer) == (Ok, Skip, Error, Defer)
    assert {"Ok", "Skip", "Error", "Defer", "Outcome"} <= set(tallyho.__all__)


@pytest.mark.parametrize(
    "outcome",
    [
        Ok(),
        Ok("chunk", result={"checked": 50}, mark=True),
        Skip("duplicate"),
        Error("invalid_domain", detail={"code": 7}),
        Defer(reason="no_accounts", delay=timedelta(seconds=10)),
        Defer(reason="quota", until=_MOMENT),
    ],
)
def test_outcome_is_frozen_slotted_and_comparable(outcome: Outcome) -> None:
    assert outcome == dataclasses.replace(outcome)
    assert not hasattr(outcome, "__dict__")
    first = dataclasses.fields(outcome)[0].name
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(outcome, first, "другое")


def test_distinct_outcomes_are_not_equal() -> None:
    assert Ok("chunk") != Ok("chunk", result=1)
    assert Defer(reason="quota") != Defer(reason="no_accounts")
    # Классы итогов не взаимозаменяемы: одна метка не делает их равными.
    same_label: list[object] = [Ok("chunk"), Skip("chunk"), Error("chunk")]
    for left, right in itertools.combinations(same_label, 2):
        assert left != right


def test_defaults_match_imperative_facade() -> None:
    assert Ok() == Ok(None, result=None, mark=None)
    assert Skip() == Skip(None, mark=None)
    assert Error() == Error(None, detail=None, mark=None)
    assert Defer(reason="x") == Defer("x", None, None)


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: Defer(reason=""), "reason"),
        (lambda: Defer(reason="x", delay=timedelta(seconds=1), until=_MOMENT), "delay или until"),
        (lambda: Defer(reason="x", delay=timedelta(seconds=-1)), "отрицательным"),
    ],
)
def test_defer_validates_reason_and_deadline(build: Callable[[], Defer], match: str) -> None:
    with pytest.raises(ConfigurationError, match=match):
        _ = build()


def test_defer_accepts_zero_delay() -> None:
    assert Defer(reason="now", delay=timedelta(0)).delay == timedelta(0)
