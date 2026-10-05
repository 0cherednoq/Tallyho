"""Операции ``BatchHandle`` над деревом после retention бросают ``BatchPurged`` (Fix-30)."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho import Tallyho, item
from tallyho.model.errors import BatchPurged
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.api.batch import BatchHandle

__all__: list[str] = []

_LATER = datetime(2030, 1, 1, tzinfo=UTC)

OPERATIONS: dict[str, Callable[[BatchHandle], Awaitable[object]]] = {
    "retry_failed": lambda handle: handle.retry_failed(),
    "cancel": lambda handle: handle.cancel(),
    "pause": lambda handle: handle.pause(),
    "resume": lambda handle: handle.resume(),
    "reschedule": lambda handle: handle.reschedule(_LATER),
    "retry_finalize": lambda handle: handle.retry_finalize(),
    "release": lambda handle: handle.release(),
}


@asynccontextmanager
async def client(
    engine: AsyncEngine, schema: str
) -> AsyncGenerator[tuple[Tallyho, InlineBroker, FakeClock]]:
    clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
    broker = InlineBroker(seed=1)
    th = Tallyho(engine, schema=schema, clock=clock)
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        yield th, broker, clock
    finally:
        await th.aclose()


async def bounce(value: int) -> None:
    await asyncio.sleep(0)
    item.error(f"bounce:{value}")


async def assert_purged(name: str, handle: BatchHandle) -> None:
    with pytest.raises(BatchPurged) as caught:
        _ = await OPERATIONS[name](handle)
    assert caught.value.batch_id == handle.id


@pytest.mark.parametrize("name", sorted(OPERATIONS))
async def test_handle_operation_after_retention_raises_batch_purged(
    engine: AsyncEngine, schema: str, name: str
) -> None:
    async with client(engine, schema) as (th, broker, clock):
        async with th.batch(
            "purge", key="one", retention=timedelta(days=1), release_required=True
        ) as root:
            send = root.sub_batch("send")
            await send.add(bounce, 1)
        _ = await broker.drain()
        _ = await th.run_maintenance_once()
        child = await root.handle.child("send")
        assert (await root.handle.view()).state is BatchState.COMPLETED_WITH_ERRORS
        await root.handle.release()

        clock.advance(days=2)
        # Retention истёк, sweeper ещё не прошёл: дерево уже считается удалённым.
        await assert_purged(name, root.handle)
        await assert_purged(name, child)
        view = await root.handle.view()
        assert view.state is BatchState.COMPLETED_WITH_ERRORS

        _ = await th.run_maintenance_once()
        with pytest.raises(BatchPurged):
            _ = await root.handle.view()
        await assert_purged(name, root.handle)
        await assert_purged(name, child)
