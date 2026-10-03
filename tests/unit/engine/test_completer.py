"""Completer без БД: настройки и значения."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tallyho.engine.completer import (
    ClaimOutcome,
    ClaimResult,
    CompleterSettings,
    ExpectRequest,
    FinishResult,
    ItemRef,
    SpawnRequest,
    SubBatchRequest,
)
from tallyho.engine.producer import SubBatchSpec
from tallyho.engine.spawn import SpawnRoute
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import ResultClass
from tallyho.storage.metric_names import METRIC_PREFIX

if TYPE_CHECKING:
    from collections.abc import Callable


def test_settings_defaults_follow_architecture() -> None:
    settings = CompleterSettings(worker_id="w")
    assert settings.tick == timedelta(milliseconds=20)
    assert settings.max_batch == 500
    assert settings.backpressure == 10_000
    assert settings.lease_ttl == timedelta(seconds=60)
    assert settings.slot == 0


INVALID: dict[str, Callable[[], CompleterSettings]] = {
    "worker": lambda: CompleterSettings(worker_id=""),
    "slot": lambda: CompleterSettings(worker_id="w", slot=-1),
    "tick": lambda: CompleterSettings(worker_id="w", tick=timedelta(0)),
    "lease_ttl": lambda: CompleterSettings(worker_id="w", lease_ttl=timedelta(seconds=-1)),
    "max_batch": lambda: CompleterSettings(worker_id="w", max_batch=0),
    "backpressure": lambda: CompleterSettings(worker_id="w", max_batch=10, backpressure=9),
}


@pytest.mark.parametrize("make", INVALID.values(), ids=INVALID.keys())
def test_settings_reject_invalid(make: Callable[[], CompleterSettings]) -> None:
    with pytest.raises(ConfigurationError):
        _ = make()


def test_only_claimed_runs() -> None:
    assert ClaimResult(outcome=ClaimOutcome.CLAIMED).run
    others = set(ClaimOutcome) - {ClaimOutcome.CLAIMED}
    assert not any(ClaimResult(outcome=outcome).run for outcome in others)


def test_item_ref_is_hashable_value() -> None:
    item_id, batch_id = uuid4(), uuid4()
    assert ItemRef(item_id, batch_id) == ItemRef(item_id, batch_id)
    assert len({ItemRef(item_id, batch_id), ItemRef(item_id, batch_id)}) == 1


@pytest.mark.parametrize("value", [-1, True])
def test_expect_rejects_invalid_total(value: int) -> None:
    source = uuid4()
    route = SpawnRoute(source_id=source, target_id=source, root_id=source)
    with pytest.raises(ConfigurationError, match="expected"):
        _ = ExpectRequest(route=route, total=value)


def test_finish_result_freezes_dynamic_buffers() -> None:
    source = uuid4()
    route = SpawnRoute(source_id=source, target_id=source, root_id=source)
    spawn = SpawnRequest(route=route, call=TaskCall(task_name="child"))
    expect = ExpectRequest(route=route, total=2)
    sub = SubBatchRequest(spec=SubBatchSpec(key="parts"), calls=[TaskCall(task_name="part")])
    metrics = {"bytes": 1}
    spawns = [spawn]
    expects = [expect]
    subs = [sub]
    value = FinishResult(
        result_class=ResultClass.OK,
        metrics=metrics,
        spawns=spawns,
        expects=expects,
        sub_batches=subs,
    )
    metrics["bytes"] = 2
    spawns.clear()
    expects.clear()
    subs.clear()
    # Имена метрик уже переведены в имена строк th_metric.
    assert dict(value.metrics) == {METRIC_PREFIX + "bytes": 1}
    assert value.spawns == (spawn,)
    assert value.expects == (expect,)
    assert value.sub_batches == (sub,)
    assert sub.calls == (TaskCall(task_name="part"),)
