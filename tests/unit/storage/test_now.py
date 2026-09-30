"""``sql_now``: ``now()`` БД или bind-параметр времени из часов (D-002)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import BindParameter, DateTime, create_mock_engine, select
from typing_extensions import override

from tallyho.protocols import Clock, SystemClock
from tallyho.storage.now import sql_now

MOMENT = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
PG = create_mock_engine("postgresql+asyncpg://", executor=print).dialect


class FrozenClock(Clock):
    """Часы в духе ``FakeClock``: «сейчас» задаётся тестом."""

    def __init__(self, now: datetime | None) -> None:
        self.value: datetime | None = now

    @override
    def now(self) -> datetime | None:
        return self.value

    @override
    def monotonic(self) -> float:
        return 0.0


def _sql(clock: Clock) -> str:
    return str(select(sql_now(clock)).compile(dialect=PG))


def test_system_clock_uses_database_now() -> None:
    assert _sql(SystemClock()) == "SELECT now() AS now_1"


def test_fake_time_is_bound_as_timestamptz() -> None:
    expr = sql_now(FrozenClock(MOMENT))

    assert isinstance(expr, BindParameter)
    assert expr.value == MOMENT
    assert isinstance(expr.type, DateTime)
    assert expr.type.timezone
    assert "now()" not in _sql(FrozenClock(MOMENT))


def test_value_is_read_on_every_call() -> None:
    clock = FrozenClock(MOMENT)
    first = sql_now(clock)
    clock.value = MOMENT + timedelta(seconds=5)

    second = sql_now(clock)

    assert isinstance(first, BindParameter)
    assert isinstance(second, BindParameter)
    assert first.value == MOMENT
    assert second.value == MOMENT + timedelta(seconds=5)


def test_non_utc_offset_is_accepted() -> None:
    moment = datetime(2026, 9, 30, 15, 0, tzinfo=timezone(timedelta(hours=3)))

    expr = sql_now(FrozenClock(moment))

    assert isinstance(expr, BindParameter)
    assert expr.value == MOMENT


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(TypeError, match="naive"):
        _ = sql_now(FrozenClock(MOMENT.replace(tzinfo=None)))
