"""Снимок кодов состояний (D-005) и правила терминальности."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tallyho.model.states import (
    TERMINAL_THRESHOLD,
    BatchState,
    CancelReason,
    ItemState,
    OnFeederFailed,
    OutboxKind,
    ResultClass,
)

if TYPE_CHECKING:
    from enum import Enum

# Снимок: значения хранятся в БД. Менять только вместе с миграцией.
SNAPSHOT: dict[type[Enum], dict[str, int | str]] = {
    BatchState: {
        "OPEN": 0,
        "SEALED": 1,
        "FINALIZING": 2,
        "SUCCEEDED": 10,
        "COMPLETED_WITH_ERRORS": 11,
        "FAILED": 12,
        "CANCELLED": 13,
    },
    ItemState: {"ACTIVE": 0, "OK": 10, "SKIP": 11, "ERROR": 12, "CANCELLED": 13},
    ResultClass: {"OK": 10, "SKIP": 11, "ERROR": 12, "CANCELLED": 13},
    OnFeederFailed: {"SEAL": 0, "CANCEL": 1},
    OutboxKind: {"ITEM": 0, "CALLBACK": 1},
    CancelReason: {
        "CANCEL": "cancel",
        "DEADLINE": "deadline",
        "FAIL_FAST": "fail_fast",
        "POLICY": "policy",
    },
}


@pytest.mark.parametrize("enum_type", list(SNAPSHOT), ids=lambda t: t.__name__)
def test_enum_values_match_snapshot(enum_type: type[Enum]) -> None:
    actual = {member.name: member.value for member in enum_type}
    assert actual == SNAPSHOT[enum_type]


def test_terminal_threshold_is_ten() -> None:
    assert TERMINAL_THRESHOLD == 10


@pytest.mark.parametrize("state", list(BatchState))
def test_batch_state_terminality(state: BatchState) -> None:
    expected = state in {
        BatchState.SUCCEEDED,
        BatchState.COMPLETED_WITH_ERRORS,
        BatchState.FAILED,
        BatchState.CANCELLED,
    }
    assert state.is_terminal is expected


@pytest.mark.parametrize("state", list(ItemState))
def test_item_state_terminality(state: ItemState) -> None:
    assert state.is_terminal is (state is not ItemState.ACTIVE)


def test_active_item_has_no_result_class() -> None:
    assert ItemState.ACTIVE.result_class is None


@pytest.mark.parametrize("result", list(ResultClass))
def test_result_class_round_trips_through_item_state(result: ResultClass) -> None:
    state = result.item_state
    assert state.name == result.name
    assert state.is_terminal
    assert state.result_class is result


@pytest.mark.parametrize(
    ("reason", "state"),
    [
        (CancelReason.CANCEL, BatchState.CANCELLED),
        (CancelReason.DEADLINE, BatchState.FAILED),
        (CancelReason.FAIL_FAST, BatchState.FAILED),
        (CancelReason.POLICY, BatchState.FAILED),
    ],
)
def test_cancel_reason_terminal_state(reason: CancelReason, state: BatchState) -> None:
    assert reason.terminal_state is state
    assert state.is_terminal


def test_lookup_by_api_names() -> None:
    assert OnFeederFailed["SEAL"] is OnFeederFailed.SEAL
    assert CancelReason("fail_fast") is CancelReason.FAIL_FAST
