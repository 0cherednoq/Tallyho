from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


async def test_postgres_is_reachable(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        version = await conn.scalar(text("SHOW server_version_num"))
    assert version is not None
    assert int(version) >= 140000
