"""``sql_now`` на PostgreSQL: время транзакции БД или время тестовых часов."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from tallyho.protocols import SystemClock
from tallyho.storage.now import sql_now

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection

MOMENT = datetime(2001, 2, 3, 4, 5, 6, tzinfo=UTC)


class FixedClock:
    """Структурно подходит под ``NowSource`` без наследования ``Clock``."""

    def now(self) -> datetime | None:
        return MOMENT


async def test_system_clock_is_transaction_start(connection: AsyncConnection) -> None:
    async with connection.begin():
        ours = await connection.scalar(select(sql_now(SystemClock())))
        database = await connection.scalar(select(func.now()))

    assert ours is not None
    assert ours == database


async def test_fixed_clock_value_round_trips(connection: AsyncConnection) -> None:
    async with connection.begin():
        value = await connection.scalar(select(sql_now(FixedClock())))

    assert value == MOMENT
