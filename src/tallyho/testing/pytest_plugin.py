"""Pytest-плагин с готовой фикстурой ``tallyho_env``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest_asyncio

from tallyho.api.client import Tallyho
from tallyho.testing.broker import InlineBroker
from tallyho.testing.clock import FakeClock
from tallyho.testing.environment import TallyhoTestEnv

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["tallyho_env"]


@pytest_asyncio.fixture
async def tallyho_env(engine: AsyncEngine, schema: str) -> AsyncGenerator[TallyhoTestEnv]:
    """Создать мигрированную установку с InlineBroker и FakeClock.

    Yields:
        Изолированное тестовое окружение в переданной схеме.
    """
    clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
    broker = InlineBroker(seed=0)
    th = Tallyho(engine, schema=schema, clock=clock)
    th.install(broker.adapter)
    await th.migrate()
    environment = TallyhoTestEnv(th, broker, clock, engine, schema)
    try:
        yield environment
    finally:
        await environment.close()
