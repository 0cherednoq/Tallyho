"""Пауза повтора упавшего ``on_finalized`` задаётся настройками клиента (ARCHITECTURE §7.3)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from tallyho.model.views import BatchSummary

__all__: list[str] = []


async def noop(value: int) -> None:
    """Задача без итога: Item завершается ``ok``."""
    _ = value
    await asyncio.sleep(0)


async def test_hook_backoff_initial_reaches_sweeper(engine: AsyncEngine, schema: str) -> None:
    clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
    broker = InlineBroker(seed=1)
    th = Tallyho(
        engine,
        schema=schema,
        clock=clock,
        hook_backoff_initial=timedelta(minutes=1),
        hook_backoff_max=timedelta(minutes=10),
    )
    th.install(broker.adapter)
    _ = await th.migrate()
    broken = [True]

    @th.on_finalized("backoff")
    async def finalized(_session: AsyncSession, _summary: BatchSummary) -> None:
        await asyncio.sleep(0)
        if broken[0]:
            message = "hook failed"
            raise RuntimeError(message)

    _ = finalized
    try:
        async with th.batch("backoff", key="one") as root:
            await root.add(noop, 1)
        _ = await broker.drain()
        _ = await th.run_maintenance_once()
        failed = (await root.handle.view()).hook_attempts
        assert failed >= 1

        # Пауза после первой неудачи — hook_backoff_initial (1 мин), а не 1 с по умолчанию.
        clock.advance(seconds=30)
        _ = await th.run_maintenance_once()
        assert (await root.handle.view()).hook_attempts == failed

        broken[0] = False
        clock.advance(minutes=10)
        _ = await th.run_maintenance_once()
        view = await root.handle.view()
    finally:
        await th.aclose()

    assert view.state is BatchState.SUCCEEDED
