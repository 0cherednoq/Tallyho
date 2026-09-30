"""Isolated application fixture for the mailing scenarios."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.examples.mailing.app import MailingApp

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["mailing_app"]


@pytest.fixture
async def mailing_app(engine: AsyncEngine, schema: str) -> AsyncIterator[MailingApp]:
    """Create one real PostgreSQL-backed user application."""
    app = await MailingApp.create(engine, schema)
    try:
        yield app
    finally:
        await app.close()
