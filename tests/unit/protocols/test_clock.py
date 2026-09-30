"""Clock: системные часы отдают «сейчас» БД, фейки проходят проверку протокола."""

from __future__ import annotations

from datetime import UTC, datetime

from tallyho.protocols.clock import Clock, SystemClock


class _FixedClock:
    def __init__(self, at: datetime) -> None:
        self.at: datetime = at

    def now(self) -> datetime:
        return self.at

    def monotonic(self) -> float:
        return 0.0


class _NoMonotonic:
    def now(self) -> datetime | None:
        return None


def test_system_clock_delegates_now_to_database() -> None:
    assert SystemClock().now() is None


def test_system_clock_monotonic_does_not_decrease() -> None:
    clock = SystemClock()
    first = clock.monotonic()
    assert clock.monotonic() >= first


def test_clock_protocol_accepts_implementations() -> None:
    fixed: Clock = _FixedClock(datetime(2026, 10, 1, 9, 0, tzinfo=UTC))
    system: Clock = SystemClock()
    assert isinstance(fixed, Clock)
    assert isinstance(system, Clock)
    assert fixed.now() == datetime(2026, 10, 1, 9, 0, tzinfo=UTC)


def test_clock_protocol_rejects_incomplete_fake() -> None:
    assert not isinstance(_NoMonotonic(), Clock)
