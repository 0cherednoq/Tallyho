"""Application fixture for the catalog pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.examples.catalog.app import CatalogApp

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["catalog_app"]


@pytest.fixture
async def catalog_app(engine: AsyncEngine, schema: str) -> AsyncIterator[CatalogApp]:
    """Create one isolated catalog application."""
    app = await CatalogApp.create(engine, schema)
    try:
        yield app
    finally:
        await app.close()
