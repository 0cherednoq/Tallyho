"""Сверка с DLQ через полный путь producer → relay → tracked → maintenance (Fix-6, UC-15).

Сценарий дефекта: PostgreSQL недоступен на claim дольше, чем брокер повторяет
джобу. Джоба уходит в DLQ, итог Item никто не записал: Item остаётся ``active``
без lease, outbox и джобы, батч не финализируется. Отказ claim имитируется
переименованием ``th_lease``: групповая транзакция Completer падает, остальные
таблицы доступны.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, text, update

from tallyho import Tallyho
from tallyho.engine.dead_letters import CURSOR_KEY
from tallyho.engine.maintenance import MaintenanceResult
from tallyho.model.states import BatchState, ItemState
from tallyho.storage.tables import build_metadata
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho import BatchHandle

__all__: list[str] = []

TABLES = build_metadata()


@asynccontextmanager
async def make_client(
    engine: AsyncEngine, schema: str, *, max_retries: int = 1
) -> AsyncGenerator[tuple[Tallyho, InlineBroker]]:
    clock = FakeClock(datetime(2026, 10, 2, 9, tzinfo=UTC))
    broker = InlineBroker(max_retries=max_retries)
    th = Tallyho(engine, schema=schema, clock=clock)
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        yield th, broker
    finally:
        await broker.close()


@asynccontextmanager
async def claim_outage(engine: AsyncEngine, schema: str) -> AsyncGenerator[None]:
    """Пока контекст открыт, claim падает с ``CompleterError``: таблицы lease «нет»."""
    quoted = f'"{schema}"'
    async with engine.begin() as conn:
        _ = await conn.execute(text(f"ALTER TABLE {quoted}.th_lease RENAME TO th_lease_down"))
    try:
        yield
    finally:
        async with engine.begin() as conn:
            _ = await conn.execute(text(f"ALTER TABLE {quoted}.th_lease_down RENAME TO th_lease"))


async def items(
    engine: AsyncEngine, schema: str, handle: BatchHandle
) -> list[tuple[int, str | None, int]]:
    """``(state, label, generation)`` Items батча по порядку id."""
    item = TABLES.item
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        rows = await conn.execute(
            select(item.c.state, item.c.label, item.c.generation)
            .where(item.c.batch_id == handle.id)
            .order_by(item.c.id)
        )
        return [(int(state), label, int(generation)) for state, label, generation in rows]


async def stored_cursor(engine: AsyncEngine, schema: str) -> str | None:
    meta = TABLES.meta
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        return await conn.scalar(select(meta.c.value).where(meta.c.key == CURSOR_KEY))


async def set_cursor(engine: AsyncEngine, schema: str, value: str) -> None:
    meta = TABLES.meta
    async with engine.begin() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        _ = await conn.execute(update(meta).where(meta.c.key == CURSOR_KEY).values(value=value))


async def maintenance_pass(th: Tallyho) -> MaintenanceResult:
    result = await th.run_maintenance_once()
    assert isinstance(result, MaintenanceResult)
    return result


async def lost_dead_letters(
    th: Tallyho, broker: InlineBroker, *, engine: AsyncEngine, schema: str, calls: list[int]
) -> BatchHandle:
    """Батч из двух Items, джобы которых умерли на claim и не записали итог."""

    async def work(value: int) -> None:
        await asyncio.sleep(0)
        calls.append(value)

    async with th.batch("dlq-lost", key="one") as batch:
        await batch.add(work, 1)
        await batch.add(work, 2)
    async with claim_outage(engine, schema):
        # Две доставки на Item: первая попытка и один ретрай брокера.
        assert await broker.drain() == 4
    return batch.handle


async def test_lost_dead_letter_is_reconciled_by_one_maintenance_pass(
    engine: AsyncEngine, schema: str
) -> None:
    calls: list[int] = []
    async with make_client(engine, schema) as (th, broker):
        handle = await lost_dead_letters(th, broker, engine=engine, schema=schema, calls=calls)

        # Дефект: брокер сдался, задача не выполнялась, итог Items никто не записал.
        assert calls == []
        assert len(broker.dead_letters) == 2
        assert await items(engine, schema, handle) == [(int(ItemState.ACTIVE), None, 0)] * 2
        view = await handle.view()
        assert view.state is BatchState.SEALED
        assert (view.progress.error, view.progress.in_flight) == (0, 0)
        assert await stored_cursor(engine, schema) is None

        # Один проход maintenance завершает Items и финализирует батч.
        result = await maintenance_pass(th)

        assert result.dead_letters == 2
        assert await items(engine, schema, handle) == [(int(ItemState.ERROR), "exhausted", 0)] * 2
        view = await handle.view()
        assert view.state is BatchState.COMPLETED_WITH_ERRORS
        assert (view.progress.error, view.progress.ok) == (2, 0)
        assert view.labels == {"exhausted": 2}
        failed = [item async for item in handle.items(labels=["exhausted"])]
        assert len(failed) == 2
        assert await stored_cursor(engine, schema) == "2"

        # Повторный проход — no-op, курсор не откатывается.
        again = await maintenance_pass(th)
        assert again.dead_letters == 0
        assert await stored_cursor(engine, schema) == "2"
        assert (await handle.view()).progress.error == 2
        assert calls == []


async def test_retried_item_is_not_finished_by_dead_letter_of_previous_dispatch(
    engine: AsyncEngine, schema: str
) -> None:
    calls: list[int] = []
    async with make_client(engine, schema) as (th, broker):
        handle = await lost_dead_letters(th, broker, engine=engine, schema=schema, calls=calls)
        assert (await maintenance_pass(th)).dead_letters == 2

        # Повтор упавших: новая отправка — новое поколение.
        await handle.retry_failed()
        assert await items(engine, schema, handle) == [(int(ItemState.ACTIVE), None, 1)] * 2
        # Сверка отстала и разбирает записи DLQ прошлой отправки заново, когда
        # новые джобы уже ушли в брокер: по данным tallyho Items выглядят
        # осиротевшими (active без lease и outbox).
        assert await broker.step(0) == 0
        await set_cursor(engine, schema, "")

        late = await maintenance_pass(th)

        assert late.dead_letters == 0
        assert await items(engine, schema, handle) == [(int(ItemState.ACTIVE), None, 1)] * 2
        assert await stored_cursor(engine, schema) == "2"

        assert await broker.drain() == 2
        assert sorted(calls) == [1, 2]
        view = await handle.view()
        assert view.state is BatchState.SUCCEEDED
        assert view.progress.ok == 2


async def test_dead_letter_of_current_dispatch_finishes_retried_item(
    engine: AsyncEngine, schema: str
) -> None:
    calls: list[int] = []
    async with make_client(engine, schema, max_retries=0) as (th, broker):

        async def work(value: int) -> None:
            await asyncio.sleep(0)
            calls.append(value)

        async with th.batch("dlq-again", key="one") as batch:
            await batch.add(work, 1)
        handle = batch.handle
        async with claim_outage(engine, schema):
            assert await broker.drain() == 1
        assert (await maintenance_pass(th)).dead_letters == 1

        await handle.retry_failed()
        async with claim_outage(engine, schema):
            assert await broker.drain() == 1
        # Умерла и джоба повторной отправки: её запись DLQ несёт текущее поколение.
        generations = [message.generation for message in broker.dead_letters]
        assert generations == [0, 1]
        dead = await broker.reconcile_dead("1")
        assert [entry.generation for entry in dead.entries] == [1]

        assert (await maintenance_pass(th)).dead_letters == 1

        assert await items(engine, schema, handle) == [(int(ItemState.ERROR), "exhausted", 1)]
        assert (await handle.view()).state is BatchState.COMPLETED_WITH_ERRORS
        assert calls == []
        assert await stored_cursor(engine, schema) == "2"
        assert {message.batch_id for message in broker.dead_letters} == {handle.id}
