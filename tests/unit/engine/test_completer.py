"""Completer без БД: настройки и значения."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tallyho.engine.completer import ClaimOutcome, ClaimResult, CompleterSettings, ItemRef
from tallyho.model.errors import ConfigurationError

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
