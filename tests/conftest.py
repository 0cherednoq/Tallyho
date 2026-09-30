"""Публичные pytest-фикстуры tallyho для собственного тестового набора."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from tests.helpers.db import temporary_schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

pytest_plugins = ["tallyho.testing.pytest_plugin"]

__all__ = ["pytest_plugins"]

POSTGRES_IMAGE = "postgres:16-alpine"


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    """Provide PostgreSQL from CI or a lazy testcontainer."""
    if dsn := os.environ.get("TALLYHO_TEST_DSN"):
        yield dsn
        return
    from testcontainers.community.postgres import PostgresContainer  # ruff: ignore[import-outside-top-level]  # optional Docker dependency is loaded only for integration tests

    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture
async def engine(postgres_dsn: str) -> AsyncIterator[AsyncEngine]:
    """Create an async engine for one PostgreSQL-backed test."""
    value = create_async_engine(postgres_dsn)
    try:
        yield value
    finally:
        await value.dispose()


@pytest.fixture
async def schema(engine: AsyncEngine) -> AsyncIterator[str]:
    """Create and remove an isolated PostgreSQL schema."""
    async with temporary_schema(engine) as name:
        yield name


@pytest.fixture
async def connection(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """Open a connection without retaining a transaction after the test."""
    async with engine.connect() as value:
        yield value


@pytest.fixture
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Open a user-style SQLAlchemy session."""
    async with AsyncSession(engine, expire_on_commit=False) as value:
        yield value
