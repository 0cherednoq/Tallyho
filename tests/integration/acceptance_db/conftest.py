"""Shared fixtures for database acceptance scenarios."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from tests.examples.mailing.app import MailingApp
from tests.integration.engine.conftest import env, registry

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import AsyncIterator, Callable

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["env", "mailing_app", "registry"]


def pytest_asyncio_loop_factories(
    config: pytest.Config,
    item: pytest.Item,
) -> dict[str, Callable[[], AbstractEventLoop]]:
    """Use a loop supported by psycopg async, including on Windows."""
    del config, item
    return {"selector": asyncio.SelectorEventLoop}


@pytest.fixture
async def mailing_app(engine: AsyncEngine, schema: str) -> AsyncIterator[MailingApp]:
    """Build the real mailing domain for atomic hook acceptance tests."""
    app = await MailingApp.create(engine, schema)
    try:
        yield app
    finally:
        await app.close()
