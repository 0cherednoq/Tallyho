"""Таблица-зонд для интеграционных тестов транзакций: видно, что закоммичено."""

from __future__ import annotations

import contextlib
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from sqlalchemy import Column, Integer, MetaData, Table, TypedColumns, insert, select
from sqlalchemy.orm import registry

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = [
    "ProbeColumns",
    "ProbeRow",
    "committed_ids",
    "create_probe",
    "insert_id",
    "mapped_probe",
    "seeded",
]


@final
class ProbeColumns(TypedColumns):
    """Одна колонка ``id``."""

    id = Column(Integer(), primary_key=True)


async def create_probe(engine: AsyncEngine, schema: str) -> Table[ProbeColumns]:
    """Создаёт пустую таблицу ``probe`` в схеме теста."""
    probe = Table("probe", MetaData(schema=schema), ProbeColumns)
    async with engine.begin() as conn:
        await conn.run_sync(probe.create)
    return probe


async def insert_id(conn: AsyncConnection, probe: Table[ProbeColumns], value: int) -> None:
    """Вставляет строку в текущей транзакции соединения."""
    _ = await conn.execute(insert(probe).values(id=value))


async def committed_ids(engine: AsyncEngine, probe: Table[ProbeColumns]) -> list[int]:
    """Закоммиченные ``id`` по возрастанию (читает отдельным соединением)."""
    async with engine.connect() as conn:
        return list(await conn.scalars(select(probe.c.id).order_by(probe.c.id)))


def seeded(seed: int) -> random.Random:
    """Детерминированный источник джиттера для тестов повторов."""
    return random.Random(seed)  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # джиттер, не криптография


@dataclass
class ProbeRow:
    """ORM-объект пользователя поверх таблицы-зонда."""

    id: int


@contextlib.contextmanager
def mapped_probe(probe: Table[ProbeColumns]) -> Generator[None]:
    """Временно отображает :class:`ProbeRow` на таблицу теста (ORM пользователя)."""
    orm = registry()
    _ = orm.map_imperatively(ProbeRow, probe)
    try:
        yield
    finally:
        orm.dispose()
