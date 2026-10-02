"""Fix-12: запросы tallyho на соединении пользователя находят схему установки.

Пользователь передаёт ``AsyncSession`` или ``AsyncConnection`` от своего движка:
без ``schema_translate_map`` и с ``search_path`` по умолчанию, а установка
лежит в другой схеме. Библиотека адресует свои таблицы сама и не трогает
настройки чужого соединения; доменные таблицы пользователя — и в его
транзакции, и в tx-хуке — ищутся так же, как в остальном его коде.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal, final

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    TypedColumns,
    insert,
    select,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import registry

from tallyho import Tallyho, item
from tallyho.model.errors import BatchPurged
from tallyho.model.states import BatchState
from tallyho.storage.tables import build_metadata
from tallyho.testing import InlineBroker
from tests.helpers.db import temporary_schema

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Mapping

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.model.views import BatchSummary

__all__: list[str] = []

Driver = Literal["asyncpg", "psycopg"]
Target = Literal["session", "connection"]

pytestmark = [
    pytest.mark.parametrize("driver", ["asyncpg", "psycopg"]),
    pytest.mark.parametrize("target", ["session", "connection"]),
]


@final
class _NoteColumns(TypedColumns):
    """Доменная таблица пользователя."""

    id = Column(Integer(), primary_key=True)
    status = Column(String(32), nullable=False)


@dataclass
class _Note:
    """ORM-объект пользователя: проверяет flush сессии хука."""

    id: int
    status: str


@dataclass(frozen=True, slots=True)
class _Stand:
    """Установка в своей схеме и отдельный движок пользователя без настроек схемы."""

    th: Tallyho
    broker: InlineBroker
    engine: AsyncEngine
    user_engine: AsyncEngine
    schema: str
    notes: Table[_NoteColumns]

    async def statuses(self, notes: Table[_NoteColumns] | None = None) -> dict[int, str]:
        """Закоммиченные строки доменной таблицы (по умолчанию — из ``public``)."""
        table = self.notes if notes is None else notes
        async with self.engine.connect() as conn:
            rows = await conn.execute(select(table.c.id, table.c.status))
            return {int(row.id): str(row.status) for row in rows}


def _notes() -> Table[_NoteColumns]:
    # Имя уникально: таблица без схемы лежит в общей для тестов схеме public.
    return Table(f"fix12_notes_{uuid.uuid4().hex[:12]}", MetaData(), _NoteColumns)


@pytest.fixture
async def stand(postgres_dsn: str, driver: Driver) -> AsyncIterator[_Stand]:
    """Tallyho в отдельной схеме; у пользователя свой движок на тот же DSN."""
    url = make_url(postgres_dsn).set(drivername=f"postgresql+{driver}")
    engine = create_async_engine(url)
    user_engine = create_async_engine(url)
    notes = _notes()
    broker = InlineBroker()
    try:
        async with temporary_schema(engine) as schema:
            th = Tallyho(engine, schema=schema)
            th.install(broker.adapter)
            _ = await th.migrate()
            async with user_engine.begin() as conn:
                await conn.run_sync(notes.create)
            try:
                yield _Stand(th, broker, engine, user_engine, schema, notes)
            finally:
                await broker.close()
                await th.aclose()
                async with user_engine.begin() as conn:
                    await conn.run_sync(notes.drop)
    finally:
        await user_engine.dispose()
        await engine.dispose()


@contextlib.asynccontextmanager
async def _user_tx(
    engine: AsyncEngine, target: Target
) -> AsyncGenerator[AsyncSession | AsyncConnection]:
    """Транзакция пользователя: commit и rollback делает сам тест."""
    if target == "connection":
        async with engine.connect() as connection:
            yield connection
        return
    async with AsyncSession(engine) as session:
        yield session


async def _options(target: AsyncSession | AsyncConnection) -> Mapping[str, object]:
    conn = await target.connection() if isinstance(target, AsyncSession) else target
    sync = conn.sync_connection
    assert sync is not None
    return dict(sync.get_execution_options())


async def _search_path(target: AsyncSession | AsyncConnection) -> str:
    return str((await target.execute(text("SHOW search_path"))).scalar_one())


async def _noop(value: int) -> None:
    await asyncio.sleep(0)
    assert value >= 0


async def test_batch_in_user_transaction(stand: _Stand, target: Target) -> None:
    """``th.batch(session=)``: add и seal на чужом соединении, commit — пользователя."""
    th, notes = stand.th, stand.notes
    async with _user_tx(stand.user_engine, target) as tx:
        options = await _options(tx)
        search_path = await _search_path(tx)
        _ = await tx.execute(insert(notes).values(id=1, status="created"))
        async with th.batch("fix12-batch", key="commit", session=tx) as batch:
            await batch.add(_noop, 1)
            await batch.add(_noop, 2)
        # Состояние соединения пользователя не изменилось, его запросы работают.
        assert await _options(tx) == options
        assert await _search_path(tx) == search_path
        assert stand.schema not in search_path
        _ = await tx.execute(insert(notes).values(id=2, status="created"))
        assert stand.broker.pending == 0
        await tx.commit()

    # after_commit транзакции пользователя подтолкнул relay: сообщения ушли в брокер.
    assert await stand.broker.drain() == 2
    assert (await batch.handle.view()).state is BatchState.SUCCEEDED
    assert await stand.statuses() == {1: "created", 2: "created"}


async def test_batch_rolls_back_with_user_transaction(stand: _Stand, target: Target) -> None:
    """Откат пользователя убирает и батч, и доменную строку; в брокер ничего не уходит."""
    async with _user_tx(stand.user_engine, target) as tx:
        _ = await tx.execute(insert(stand.notes).values(id=1, status="created"))
        async with stand.th.batch("fix12-batch", key="rollback", session=tx) as batch:
            await batch.add(_noop, 1)
        await tx.rollback()

    with pytest.raises(BatchPurged):
        _ = await batch.handle.view()
    assert await stand.broker.drain() == 0
    assert await stand.statuses() == {}


async def test_handle_operations_in_user_transaction(stand: _Stand, target: Target) -> None:
    """pause, resume, reschedule, retry_finalize, cancel и release с ``session=``."""
    th = stand.th
    start_at = datetime(2035, 1, 1, tzinfo=UTC)
    async with th.batch("fix12-handle", key="operations", start_at=start_at) as batch:
        await batch.add(_noop, 1)
    handle = batch.handle

    async with _user_tx(stand.user_engine, target) as tx:
        await handle.pause(session=tx)
        await tx.commit()
    assert (await handle.view()).paused_at is not None

    async with _user_tx(stand.user_engine, target) as tx:
        await handle.resume(session=tx)
        await tx.commit()
    assert (await handle.view()).paused_at is None

    moved = start_at + timedelta(hours=1)
    async with _user_tx(stand.user_engine, target) as tx:
        assert await handle.reschedule(moved, session=tx) == 0
        await handle.retry_finalize(session=tx)
        await tx.commit()
    assert (await handle.view()).start_at == moved

    async with _user_tx(stand.user_engine, target) as tx:
        await handle.cancel(session=tx)
        await tx.commit()
    assert (await handle.wait(timeout=5)).state is BatchState.CANCELLED

    async with _user_tx(stand.user_engine, target) as tx:
        await handle.release(session=tx)
        await tx.commit()
    batches = build_metadata(schema=stand.schema).batch
    async with stand.engine.connect() as conn:
        released = await conn.scalar(select(batches.c.released_at).where(batches.c.id == handle.id))
    assert released is not None


async def test_retry_failed_in_user_transaction(stand: _Stand, target: Target) -> None:
    """``retry_failed(session=)`` возвращает упавший Item в работу после commit."""
    attempts = 0

    async def flaky() -> None:
        nonlocal attempts
        await asyncio.sleep(0)
        attempts += 1
        if attempts == 1:
            item.error("unlucky")

    async with stand.th.batch("fix12-retry", key="failed") as batch:
        await batch.add(flaky)
    _ = await stand.broker.drain()
    assert (await batch.handle.view()).state is BatchState.COMPLETED_WITH_ERRORS

    async with _user_tx(stand.user_engine, target) as tx:
        assert await batch.handle.retry_failed(session=tx) == 1
        await tx.commit()
    _ = await stand.broker.drain()

    assert attempts == 2
    assert (await batch.handle.view()).state is BatchState.SUCCEEDED


async def test_complete_in_user_transaction(stand: _Stand, target: Target) -> None:
    """``item.complete_in``: итог задачи и доменная строка — одним коммитом пользователя."""
    notes = stand.notes

    async def store(note_id: int) -> None:
        async with _user_tx(stand.user_engine, target) as tx:
            _ = await tx.execute(insert(notes).values(id=note_id, status="stored"))
            item.ok("stored")
            await item.complete_in(tx)
            await tx.commit()

    async with stand.th.batch("fix12-complete", key="tasks") as batch:
        await batch.add(store, 1)
        await batch.add(store, 2)
    _ = await stand.broker.drain()

    view = await batch.handle.wait(timeout=5)
    assert view.state is BatchState.SUCCEEDED
    assert view.labels == {"stored": 2}
    assert await stand.statuses() == {1: "stored", 2: "stored"}


async def test_hook_resolves_domain_table_like_user_engine(stand: _Stand, target: Target) -> None:
    """tx-хук: таблица без схемы ищется по ``search_path``, а не в схеме установки."""
    del target
    notes = stand.notes
    orm = registry()
    _ = orm.map_imperatively(_Note, notes)

    @stand.th.on_finalized("fix12-hook")
    async def save(session: AsyncSession, summary: BatchSummary) -> None:
        _ = await session.execute(insert(notes).values(id=1, status=summary.state.name.lower()))
        session.add(_Note(id=2, status="orm"))  # уходит в БД на flush сессии хука

    try:
        async with stand.th.batch("fix12-hook", key="public") as batch:
            pass
        view = await batch.handle.wait(timeout=5)
    finally:
        orm.dispose()

    assert view.hook_error is None
    assert view.state is BatchState.SUCCEEDED
    assert await stand.statuses() == {1: "succeeded", 2: "orm"}


async def test_user_translate_map_keeps_working(stand: _Stand, target: Target) -> None:
    """Своя ``schema_translate_map`` пользователя действует на его таблицы, не на наши."""
    broker = InlineBroker()
    async with temporary_schema(stand.engine) as domain_schema:
        mapping = {None: domain_schema}
        user_engine = stand.user_engine.execution_options(schema_translate_map=mapping)
        notes = _notes()  # без схемы: адрес задаёт отображение пользователя
        async with user_engine.begin() as conn:
            await conn.run_sync(notes.create)
        th = Tallyho(user_engine, schema=stand.schema)
        th.install(broker.adapter)

        @th.on_finalized("fix12-map")
        async def save(session: AsyncSession, summary: BatchSummary) -> None:
            _ = await session.execute(insert(notes).values(id=3, status=summary.state.name.lower()))

        async def store() -> None:
            async with _user_tx(user_engine, target) as tx:
                _ = await tx.execute(insert(notes).values(id=2, status="stored"))
                await item.complete_in(tx)
                await tx.commit()

        try:
            async with _user_tx(user_engine, target) as tx:
                _ = await tx.execute(insert(notes).values(id=1, status="created"))
                async with th.batch("fix12-map", key="mapped", session=tx) as batch:
                    await batch.add(store)
                assert (await _options(tx))["schema_translate_map"] == mapping
                await tx.commit()
            _ = await broker.drain()
            view = await batch.handle.wait(timeout=5)
        finally:
            await broker.close()
            await th.aclose()

        qualified = notes.to_metadata(MetaData(), schema=domain_schema)
        assert view.hook_error is None
        assert view.state is BatchState.SUCCEEDED
        assert await stand.statuses(qualified) == {1: "created", 2: "stored", 3: "succeeded"}


async def test_schema_with_special_characters_is_quoted(stand: _Stand, target: Target) -> None:
    """A-NF-03: имя схемы со спецсимволами экранируется и на соединении пользователя."""
    odd = f'Fix12 "odd".{uuid.uuid4().hex[:8]};--'
    quoted = '"' + odd.replace('"', '""') + '"'
    broker = InlineBroker()
    th = Tallyho(stand.engine, schema=odd)
    th.install(broker.adapter)
    try:
        _ = await th.migrate()
        async with _user_tx(stand.user_engine, target) as tx:
            async with th.batch("fix12-odd", key="quoted", session=tx) as batch:
                await batch.add(_noop, 1)
            await tx.commit()
        assert await broker.drain() == 1
        assert (await batch.handle.view()).state is BatchState.SUCCEEDED
    finally:
        await broker.close()
        await th.aclose()
        async with stand.engine.begin() as conn:
            _ = await conn.execute(text(f"DROP SCHEMA IF EXISTS {quoted} CASCADE"))
