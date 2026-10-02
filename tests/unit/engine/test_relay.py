"""Relay без БД: параметры и ключ advisory-блокировки окна."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from tallyho.engine.relay import RelaySettings, window_lock_key
from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable


def test_settings_defaults_match_section_15() -> None:
    settings = RelaySettings()
    assert settings.claim_ttl == timedelta(seconds=30)
    assert settings.grace == timedelta(seconds=5)
    assert settings.chunk == 1000
    assert settings.slot == 0
    assert settings.scan_interval == timedelta(seconds=5)


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: RelaySettings(claim_ttl=timedelta(0)), "relay_claim_ttl"),
        (lambda: RelaySettings(grace=timedelta(seconds=-1)), "relay_grace"),
        (lambda: RelaySettings(chunk=0), "chunk"),
        (lambda: RelaySettings(slot=-1), "slot"),
        (lambda: RelaySettings(scan_interval=timedelta(0)), "scan"),
    ],
)
def test_settings_validation(build: Callable[[], RelaySettings], match: str) -> None:
    with pytest.raises(ConfigurationError, match=match):
        _ = build()


def test_zero_grace_is_allowed() -> None:
    assert RelaySettings(grace=timedelta(0)).grace == timedelta(0)


def test_window_lock_key_is_stable_signed_int64() -> None:
    first = UUID("01234567-89ab-7def-8123-456789abcdef")
    second = UUID("01234567-89ab-7def-8123-456789abcdee")
    key = window_lock_key(first)
    assert key == window_lock_key(first)
    assert key != window_lock_key(second)
    assert -(2**63) <= key < 2**63
