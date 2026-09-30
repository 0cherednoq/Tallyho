"""Relay без БД: параметры."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho.engine.relay import RelaySettings
from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable


def test_settings_defaults_match_section_15() -> None:
    settings = RelaySettings()
    assert settings.claim_ttl == timedelta(seconds=30)
    assert settings.grace == timedelta(seconds=5)
    assert settings.chunk == 1000
    assert settings.slot == 0


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: RelaySettings(claim_ttl=timedelta(0)), "relay_claim_ttl"),
        (lambda: RelaySettings(grace=timedelta(seconds=-1)), "relay_grace"),
        (lambda: RelaySettings(chunk=0), "chunk"),
        (lambda: RelaySettings(slot=-1), "slot"),
    ],
)
def test_settings_validation(build: Callable[[], RelaySettings], match: str) -> None:
    with pytest.raises(ConfigurationError, match=match):
        _ = build()


def test_zero_grace_is_allowed() -> None:
    assert RelaySettings(grace=timedelta(0)).grace == timedelta(0)
