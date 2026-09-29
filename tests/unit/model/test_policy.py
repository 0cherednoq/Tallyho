"""FailurePolicy: граничные значения, min_processed, фильтр labels, монотонность."""

from __future__ import annotations

from typing import Literal, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tallyho.model.errors import ConfigurationError
from tallyho.model.policy import (
    FailurePolicy,
    PolicyAction,
    PolicyBreach,
    PolicyKind,
    PolicyVerdict,
)
from tallyho.model.states import CancelReason
from tallyho.model.views import Progress

NO_LABELS: dict[str, int] = {}


def _counts(*, ok: int = 0, skip: int = 0, error: int = 0, cancelled: int = 0) -> Progress:
    return Progress(ok=ok, skip=skip, error=error, cancelled=cancelled)


# --- continue / fail_fast ------------------------------------------------------------


def test_continue_never_breaches() -> None:
    verdict = FailurePolicy.continue_().evaluate(_counts(error=100), NO_LABELS)
    assert verdict.breached is False
    assert verdict.action is None
    assert verdict.reason is None
    assert verdict.ratio == pytest.approx(1.0)


def test_fail_fast_without_errors_does_not_breach() -> None:
    verdict = FailurePolicy.fail_fast().evaluate(_counts(ok=10, skip=5), NO_LABELS)
    assert verdict.breached is False


def test_fail_fast_on_first_error() -> None:
    verdict = FailurePolicy.fail_fast().evaluate(_counts(ok=999, error=1), NO_LABELS)
    assert verdict.action is PolicyAction.FAIL
    assert verdict.reason is CancelReason.FAIL_FAST
    assert (verdict.processed, verdict.failed) == (1000, 1)


# --- threshold: граничные значения ---------------------------------------------------


@pytest.mark.parametrize(
    ("errors", "breached"),
    [(4, False), (5, False), (6, True)],
    ids=["below", "equal", "above"],
)
def test_threshold_is_strictly_greater(*, errors: int, breached: bool) -> None:
    policy = FailurePolicy.threshold(ratio=0.05)
    verdict = policy.evaluate(_counts(ok=100 - errors, error=errors), NO_LABELS)
    assert verdict.breached is breached


def test_threshold_default_action_is_fail_with_policy_reason() -> None:
    verdict = FailurePolicy.threshold(ratio=0.0).evaluate(_counts(error=1), NO_LABELS)
    assert verdict.action is PolicyAction.FAIL
    assert verdict.reason is CancelReason.POLICY


def test_threshold_pause_has_no_cancel_reason() -> None:
    policy = FailurePolicy.threshold(ratio=0.0, action="pause")
    verdict = policy.evaluate(_counts(error=1), NO_LABELS)
    assert verdict.action is PolicyAction.PAUSE
    assert verdict.reason is None


def test_threshold_ratio_one_never_breaches() -> None:
    verdict = FailurePolicy.threshold(ratio=1).evaluate(_counts(error=10), NO_LABELS)
    assert verdict.breached is False
    assert verdict.ratio == pytest.approx(1.0)


def test_threshold_without_processed_items() -> None:
    verdict = FailurePolicy.threshold(ratio=0.0).evaluate(_counts(cancelled=5), NO_LABELS)
    assert verdict.breached is False
    assert (verdict.processed, verdict.ratio) == (0, 0.0)


# --- min_processed -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("processed", "breached"),
    [(499, False), (500, True), (501, True)],
)
def test_min_processed_boundary(*, processed: int, breached: bool) -> None:
    policy = FailurePolicy.threshold(ratio=0.05, min_processed=500)
    errors = processed // 2
    verdict = policy.evaluate(_counts(ok=processed - errors, error=errors), NO_LABELS)
    assert verdict.breached is breached


def test_cancelled_items_are_not_processed() -> None:
    policy = FailurePolicy.threshold(ratio=0.05, min_processed=10)
    verdict = policy.evaluate(_counts(ok=5, error=4, cancelled=100), NO_LABELS)
    assert verdict.processed == 9
    assert verdict.breached is False


def test_skip_counts_as_processed() -> None:
    policy = FailurePolicy.threshold(ratio=0.5)
    assert policy.evaluate(_counts(skip=2, error=2), NO_LABELS).breached is False
    assert policy.evaluate(_counts(skip=1, error=2), NO_LABELS).breached is True


# --- фильтр labels -------------------------------------------------------------------


def test_labels_filter_counts_only_listed_labels() -> None:
    policy = FailurePolicy.threshold(ratio=0.05, min_processed=500, labels=["hard_bounce"])
    counts = _counts(ok=900, error=100)
    labels = {"sent": 900, "hard_bounce": 50, "rejected": 50}
    verdict = policy.evaluate(counts, labels)
    assert verdict.failed == 50
    assert verdict.ratio == pytest.approx(0.05)
    assert verdict.breached is False
    labels["hard_bounce"] = 51
    assert policy.evaluate(counts, labels).breached is True


def test_labels_filter_sums_several_labels_and_ignores_missing() -> None:
    policy = FailurePolicy.threshold(ratio=0.1, labels=("a", "b", "absent"))
    verdict = policy.evaluate(_counts(ok=80, error=20), {"a": 6, "b": 5, "c": 9})
    assert verdict.failed == 11
    assert verdict.breached is True


def test_labels_are_deduplicated_in_order() -> None:
    policy = FailurePolicy.threshold(ratio=0.1, labels=["b", "a", "b"])
    assert policy.labels == ("b", "a")


def test_auto_pause_breach_for_hook() -> None:
    """Сценарий §12.4: 8% hard_bounce после 500 обработанных → пауза."""
    policy = FailurePolicy.threshold(
        ratio=0.05, min_processed=500, labels=["hard_bounce"], action="pause"
    )
    verdict = policy.evaluate(_counts(ok=460, error=40), {"sent": 460, "hard_bounce": 40})
    breach = verdict.breach(batch_key="send", labels=policy.labels or ())
    assert breach == PolicyBreach(
        batch_key="send", labels=["hard_bounce"], ratio=0.08, action=PolicyAction.PAUSE
    )
    assert f"{breach.labels} rate {breach.ratio:.1%}" == "['hard_bounce'] rate 8.0%"


def test_breach_of_not_breached_verdict_is_error() -> None:
    verdict = PolicyVerdict(action=None, ratio=0.0, processed=0, failed=0)
    with pytest.raises(ConfigurationError):
        verdict.breach(batch_key=None, labels=[])


# --- валидация -----------------------------------------------------------------------


@pytest.mark.parametrize("ratio", [-0.01, 1.01, float("nan"), float("inf"), True, "0.1", None])
def test_invalid_ratio(ratio: object) -> None:
    with pytest.raises(ConfigurationError, match="ratio"):
        FailurePolicy.threshold(ratio=cast("float", ratio))


@pytest.mark.parametrize("min_processed", [-1, 1.5, False])
def test_invalid_min_processed(min_processed: object) -> None:
    with pytest.raises(ConfigurationError, match="min_processed"):
        FailurePolicy.threshold(ratio=0.1, min_processed=cast("int", min_processed))


@pytest.mark.parametrize("labels", [[], "hard_bounce", [""], [1]])
def test_invalid_labels(labels: object) -> None:
    with pytest.raises(ConfigurationError):
        FailurePolicy.threshold(ratio=0.1, labels=cast("list[str]", labels))


@pytest.mark.parametrize("action", ["stop", "FAIL", None])
def test_invalid_action(action: object) -> None:
    with pytest.raises(ConfigurationError, match="action"):
        FailurePolicy.threshold(ratio=0.1, action=cast("Literal['fail']", action))


# --- JSON для th_batch.options -------------------------------------------------------


@pytest.mark.parametrize(
    "policy",
    [
        FailurePolicy.continue_(),
        FailurePolicy.fail_fast(),
        FailurePolicy.threshold(ratio=0.05),
        FailurePolicy.threshold(ratio=0.05, min_processed=500, labels=["x"], action="pause"),
    ],
    ids=["continue", "fail_fast", "threshold", "threshold_full"],
)
def test_json_round_trip(policy: FailurePolicy) -> None:
    data = policy.to_json()
    assert data["kind"] == policy.kind.value
    assert FailurePolicy.from_json(data) == policy


def test_json_threshold_defaults() -> None:
    policy = FailurePolicy.from_json({"kind": "threshold", "ratio": 0})
    assert policy == FailurePolicy.threshold(ratio=0.0)
    assert policy.kind is PolicyKind.THRESHOLD


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"kind": "unknown"},
        {"kind": "threshold"},
        {"kind": "threshold", "ratio": 0.1, "labels": "x"},
        {"kind": "threshold", "ratio": 0.1, "action": 1},
    ],
)
def test_json_invalid(data: dict[str, object]) -> None:
    with pytest.raises(ConfigurationError):
        FailurePolicy.from_json(data)


# --- свойства ------------------------------------------------------------------------

COUNT = st.integers(min_value=0, max_value=10_000)
RATIO = st.floats(min_value=0, max_value=1, allow_nan=False)


@given(
    ok=COUNT,
    skip=COUNT,
    errors=st.tuples(COUNT, COUNT).map(sorted),
    ratio=RATIO,
    min_processed=COUNT,
)
def test_threshold_is_monotonic_in_errors(
    *, ok: int, skip: int, errors: list[int], ratio: float, min_processed: int
) -> None:
    policy = FailurePolicy.threshold(ratio=ratio, min_processed=min_processed)
    fewer, more = errors
    before = policy.evaluate(_counts(ok=ok, skip=skip, error=fewer), NO_LABELS)
    after = policy.evaluate(_counts(ok=ok, skip=skip, error=more), NO_LABELS)
    assert before.ratio <= after.ratio
    assert not before.breached or after.breached


@given(
    ok=COUNT,
    other=COUNT,
    labelled=st.tuples(COUNT, COUNT).map(sorted),
    ratio=RATIO,
    min_processed=COUNT,
)
def test_threshold_is_monotonic_in_labelled_errors(
    *, ok: int, other: int, labelled: list[int], ratio: float, min_processed: int
) -> None:
    policy = FailurePolicy.threshold(ratio=ratio, min_processed=min_processed, labels=["hb"])
    fewer, more = labelled

    def verdict(n: int) -> PolicyVerdict:
        return policy.evaluate(_counts(ok=ok, error=other + n), {"hb": n, "other": other})

    assert not verdict(fewer).breached or verdict(more).breached


@given(ok=COUNT, skip=COUNT, error=COUNT, ratio=RATIO, min_processed=COUNT)
def test_verdict_ratio_is_share_of_processed(
    *, ok: int, skip: int, error: int, ratio: float, min_processed: int
) -> None:
    policy = FailurePolicy.threshold(ratio=ratio, min_processed=min_processed)
    verdict = policy.evaluate(_counts(ok=ok, skip=skip, error=error), NO_LABELS)
    assert 0.0 <= verdict.ratio <= 1.0
    assert verdict.breached == (verdict.processed >= min_processed and verdict.ratio > ratio)
