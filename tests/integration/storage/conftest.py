"""Фикстуры интеграционных тестов storage: установленная схема tallyho."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tallyho.storage.migrations import migrate
from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.storage.tables import Tables


@pytest.fixture
async def tables(engine: AsyncEngine, schema: str) -> Tables:
    """Таблицы tallyho, установленные ``migrate`` в схему теста."""
    _ = await migrate(engine, schema)
    return build_metadata()
