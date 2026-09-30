"""Управляемые часы публичного testing API."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho.model.errors import ConfigurationError
from tallyho.testing import FakeClock

if TYPE_CHECKING:
    from tallyho.protocols.clock import Clock

__all__: list[str] = []


def test_fake_clock_advances_calendar_and_monotonic_time() -> None:
    start = datetime(2026, 10, 1, 9, tzinfo=UTC)
    clock: Clock = FakeClock(start)
    assert clock.now() == start
    assert clock.monotonic() == 0

    fake = clock
    assert isinstance(fake, FakeClock)
    assert fake.advance(timedelta(seconds=2), minutes=1) == start + timedelta(seconds=62)
    assert fake.now() == start + timedelta(seconds=62)
    assert fake.monotonic() == 62


def test_fake_clock_rejects_naive_time_and_backwards_advance() -> None:
    with pytest.raises(ConfigurationError, match="часовым поясом"):
        _ = FakeClock(datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None))

    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ConfigurationError, match="только вперёд"):
        _ = clock.advance(seconds=-1)


def test_fake_clock_wraps_bad_timedelta_arguments() -> None:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ConfigurationError):
        _ = clock.advance(unknown=1)
