"""PostgreSQL для интеграционных тестов.

Источник БД по приоритету:
1. ``TALLYHO_TEST_DSN`` (например, в CI с service-контейнером);
2. testcontainers — поднимает ``postgres`` в Docker на сессию.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tests.helpers.db import temporary_schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlalchemy.ext.asyncio import AsyncEngine

POSTGRES_IMAGE = "postgres:16-alpine"


HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Хук видит все тесты сессии, поэтому помечаем только тесты из этой папки.
    for item in items:
        if item.path.is_relative_to(HERE):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    if dsn := os.environ.get("TALLYHO_TEST_DSN"):
        yield dsn
        return
    from testcontainers.community.postgres import PostgresContainer  # ruff: ignore[import-outside-top-level]  # тяжёлый импорт только при нужде

    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture
async def engine(postgres_dsn: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(postgres_dsn)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture
async def schema(engine: AsyncEngine) -> AsyncIterator[str]:
    """Пустая схема, уникальная для теста; после теста — ``DROP SCHEMA ... CASCADE``."""
    async with temporary_schema(engine) as name:
        yield name
