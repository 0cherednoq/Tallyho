"""Публичный th.demand: спрос активных батчей через клиент (RM4.1)."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from tallyho import Tallyho
from tallyho.model.errors import ConfigurationError
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []

START = datetime(2026, 10, 1, 9, tzinfo=UTC)


@asynccontextmanager
async def make_client(engine: AsyncEngine, schema: str) -> AsyncGenerator[Tallyho]:
    th = Tallyho(engine, schema=schema, clock=FakeClock(START))
    th.install(InlineBroker(seed=1).adapter)
    _ = await th.migrate()
    try:
        yield th
    finally:
        await th.aclose()


async def noop(value: int) -> None:
    _ = value
    await asyncio.sleep(0)


async def test_demand_matches_view_and_filters_by_attributes(
    engine: AsyncEngine, schema: str
) -> None:
    async with make_client(engine, schema) as th:
        async with th.batch("pool", key="de", attributes={"region": "DE"}, seal=False) as de:
            for index in range(4):
                await de.add(noop, index)
        async with th.batch("pool", key="fr", attributes={"region": "FR"}, seal=False) as fr:
            await fr.add(noop, 0)

        rows = await th.demand(["pool"])
        assert {r.key for r in rows} == {"de", "fr"}
        de_view = await de.handle.view()
        de_row = next(r for r in rows if r.key == "de")
        assert de_row.pending == de_view.progress.found - de_view.progress.ok
        assert de_row.in_flight == de_view.progress.in_flight
        assert de_row.max_in_flight is None
        assert de_row.paused is False

        # Фильтр по атрибутам корня нормализуется клиентом и сужает выдачу.
        only_de = await th.demand(["pool"], attributes={"region": "DE"})
        assert {r.key for r in only_de} == {"de"}
        assert only_de[0].attributes == {"region": "DE"}


async def test_demand_rejects_empty_kinds(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as th:
        with pytest.raises(ConfigurationError, match="kind"):
            _ = await th.demand([])
