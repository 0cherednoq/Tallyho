"""Пауза повтора упавшего ``on_finalized`` в настройках Sweeper (ARCHITECTURE §7.3)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from tallyho.engine.sweeper import SweeperSettings
from tallyho.model.errors import ConfigurationError

__all__: list[str] = []


@pytest.mark.parametrize(
    ("attempts", "expected"),
    [
        (0, timedelta(seconds=3)),
        (1, timedelta(seconds=3)),
        (2, timedelta(seconds=6)),
        (4, timedelta(seconds=24)),
        (5, timedelta(seconds=40)),
        (10_000, timedelta(seconds=40)),
    ],
)
def test_hook_backoff_doubles_from_initial_up_to_max(attempts: int, expected: timedelta) -> None:
    settings = SweeperSettings(
        hook_backoff_initial=timedelta(seconds=3), hook_backoff_max=timedelta(seconds=40)
    )
    assert settings.hook_backoff(attempts) == expected


def test_hook_backoff_defaults_match_architecture() -> None:
    settings = SweeperSettings()
    assert [settings.hook_backoff(n).total_seconds() for n in (1, 2, 3, 9, 10)] == [
        1.0,
        2.0,
        4.0,
        256.0,
        300.0,
    ]


@pytest.mark.parametrize(
    ("initial", "maximum"),
    [
        (timedelta(0), timedelta(minutes=5)),
        (timedelta(minutes=6), timedelta(minutes=5)),
        (timedelta(seconds=1), timedelta(0)),
    ],
)
def test_hook_backoff_bounds_are_validated(initial: timedelta, maximum: timedelta) -> None:
    with pytest.raises(ConfigurationError):
        _ = SweeperSettings(hook_backoff_initial=initial, hook_backoff_max=maximum)
