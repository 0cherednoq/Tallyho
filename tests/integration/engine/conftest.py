"""Фикстуры интеграционных тестов engine: установленная схема и продюсер."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

import pytest
from sqlalchemy import TypedColumns, func, select

from tallyho.engine.producer import Producer
from tallyho.hooks.registry import HookRegistry
from tallyho.protocols.clock import SystemClock
from tallyho.protocols.ids import UuidV7Factory
from tallyho.protocols.serialization import SerializerCodec
from tallyho.storage.counters import read_counters
from tallyho.storage.migrations import migrate
from tallyho.storage.tables import build_metadata
from tests.helpers.db import schema_connection, schema_transaction

if TYPE_CHECKING:
    import contextlib
    from uuid import UUID

    from sqlalchemy import RowMapping, Table
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.storage.counters import CounterTotals
    from tallyho.storage.tables import Tables

C = TypeVar("C", bound=TypedColumns)

PRODUCER_SLOT = 3
"""Слот продюсера в тестах: не 0, чтобы было видно, что слот берётся из параметра."""


@dataclass(frozen=True, slots=True)
class Env:
    """Установленная схема теста и продюсер над ней."""

    engine: AsyncEngine
    schema: str
    tables: Tables
    producer: Producer

    def transaction(self) -> contextlib.AbstractAsyncContextManager[AsyncConnection]:
        """Транзакция в схеме теста; commit на выходе."""
        return schema_transaction(self.engine, self.schema)

    def connection(self) -> contextlib.AbstractAsyncContextManager[AsyncConnection]:
        """Соединение в схеме теста без commit."""
        return schema_connection(self.engine, self.schema)

    async def batch(self, batch_id: UUID) -> RowMapping:
        """Строка ``th_batch``."""
        batch = self.tables.batch
        async with self.connection() as conn:
            result = await conn.execute(select(batch).where(batch.c.id == batch_id))
            return result.mappings().one()

    async def count(self, table: Table[C]) -> int:
        """Число строк таблицы."""
        async with self.connection() as conn:
            return int(await conn.scalar(select(func.count()).select_from(table)) or 0)

    async def counters(self, batch_id: UUID) -> CounterTotals:
        """Точные счётчики батча."""
        async with self.connection() as conn:
            return (await read_counters(conn, self.tables, [batch_id]))[batch_id]


@pytest.fixture
def registry() -> HookRegistry:
    """Пустой реестр tx-хуков; тест регистрирует нужные сам."""
    return HookRegistry()


@pytest.fixture
async def env(engine: AsyncEngine, schema: str, registry: HookRegistry) -> Env:
    """Схема с таблицами tallyho и продюсер: системные часы, UUIDv7, JSON-кодек."""
    _ = await migrate(engine, schema)
    tables = build_metadata()
    producer = Producer(
        tables=tables,
        clock=SystemClock(),
        ids=UuidV7Factory(),
        codec=SerializerCodec(),
        hooks=registry,
        slot=PRODUCER_SLOT,
    )
    return Env(engine=engine, schema=schema, tables=tables, producer=producer)
