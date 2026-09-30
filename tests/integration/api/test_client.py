"""Миграция через публичный Tallyho API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select

from tallyho import Tallyho

if TYPE_CHECKING:
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


async def test_migrate_is_idempotent_in_selected_schema(env: Env) -> None:
    client = Tallyho(env.engine, schema=env.schema)
    assert await client.migrate() == 2
    assert await client.migrate() == 2
    async with env.connection() as conn:
        assert await conn.scalar(select(func.count()).select_from(env.tables.meta)) == 1
