"""Потоковое добавление UC-02: ``th.batch(..., seal=False)`` и повторный вход по ключу."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.errors import SealError
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []

KIND = "import"
KEY = "file:1"


@asynccontextmanager
async def make_client(
    engine: AsyncEngine, schema: str
) -> AsyncGenerator[tuple[Tallyho, InlineBroker]]:
    broker = InlineBroker()
    th = Tallyho(engine, schema=schema, clock=FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC)))
    th.install(broker.adapter)
    await th.migrate()
    try:
        yield th, broker
    finally:
        await th.aclose()


async def load_row(value: int) -> None:
    """Задача импорта одной строки."""
    await asyncio.sleep(0)
    item.ok("loaded" if value % 2 else "even")


async def test_chunks_run_before_seal_and_last_entry_seals(
    engine: AsyncEngine, schema: str
) -> None:
    async with make_client(engine, schema) as (th, broker):
        async with th.batch(KIND, key=KEY, seal=False) as first:
            await first.map(load_row, [1, 2])
        batch_id = first.handle.id

        # Первая порция уже выполняется, хотя продюсер ещё читает источник.
        assert await broker.drain() == 2
        view = await first.handle.view()
        assert view.state is BatchState.OPEN  # pending = 0, но батч открыт — финализации нет
        assert (view.progress.found, view.progress.ok) == (2, 2)

        async with th.batch(KIND, key=KEY, seal=False) as second:
            await second.map(load_row, [3, 4])
        assert second.handle.id == batch_id

        async with th.batch(KIND, key=KEY) as last:  # вход без seal=False закрывает батч
            await last.add(load_row, 5)
        assert last.handle.id == batch_id

        assert await broker.drain() == 3
        view = await th.handle(batch_id).view()
        assert view.state is BatchState.SUCCEEDED
        assert (view.progress.found, view.progress.ok) == (5, 5)
        assert (view.labels["loaded"], view.labels["even"]) == (3, 2)


async def test_streaming_in_user_session_and_explicit_seal(
    engine: AsyncEngine, schema: str
) -> None:
    async with make_client(engine, schema) as (th, broker):
        async with AsyncSession(engine) as session:
            async with th.batch(KIND, key=KEY, seal=False, session=session) as first:
                await first.add(load_row, 1)
                part = first.sub_batch("part")
                await part.add(load_row, 2)
            await session.commit()
        view = await first.handle.view()
        assert view.state is BatchState.OPEN
        assert view.children["part"].state is BatchState.OPEN  # seal=False — на всё дерево

        async with th.batch(KIND, key=KEY, seal=False) as closing:
            part = closing.sub_batch("part")  # тот же под-батч: ключ идемпотентен в дереве
            await part.seal()
            await closing.seal()

        assert await broker.drain() == 2
        view = await th.handle(first.handle.id).view()
        assert view.state is BatchState.SUCCEEDED
        assert view.children["part"].state is BatchState.SUCCEEDED


async def test_sealed_batch_rejects_next_chunk(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker):
        async with th.batch(KIND, key=KEY) as batch:
            await batch.add(load_row, 1)

        with pytest.raises(SealError):
            async with th.batch(KIND, key=KEY, seal=False) as again:
                await again.add(load_row, 2)
        view = await batch.handle.view()
        assert view.progress.found == 1
