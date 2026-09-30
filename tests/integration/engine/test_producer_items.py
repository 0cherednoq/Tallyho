"""Продюсер: добавление Items (UC-01, UC-02) — th_item, th_outbox, счётчики, дедуп."""

from __future__ import annotations

import contextlib
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho.engine.producer import ITEM_CHUNK, AddResult, RootSpec, SubBatchSpec
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError, NotFoundError, SealError, SpawnTargetError
from tallyho.model.states import BatchState, ItemState, OutboxKind
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterTotals
from tallyho.storage.tx import resolve_connection
from tests.integration.engine.conftest import PRODUCER_SLOT

if TYPE_CHECKING:
    from collections.abc import Generator

    from tests.integration.engine.conftest import Env

KIND = "invoices"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
START = datetime(2030, 1, 1, tzinfo=UTC)


class FixedClock(SystemClock):
    """Часы, у которых «сейчас» в SQL — фиксированное значение."""

    @override
    def now(self) -> datetime | None:
        return NOW


def call(n: int, *, key: str | None = None, weight: int = 1) -> TaskCall:
    return TaskCall(task_name="render", args=(n,), kwargs={"x": "y"}, key=key, weight=weight)


async def set_batch(env: Env, batch_id: UUID, **values: object) -> None:
    async with env.transaction() as conn:
        batch = env.tables.batch
        _ = await conn.execute(update(batch).where(batch.c.id == batch_id).values(values))


async def test_add_items_writes_items_outbox_and_counters(env: Env) -> None:
    producer = replace(env.producer, clock=FixedClock())
    async with env.transaction() as conn:
        root = await producer.create_root(conn, RootSpec(kind=KIND))
        result = await producer.add_items(
            conn, root.id, [call(1, key="a", weight=2), call(2), call(3, weight=5)]
        )
    assert result == AddResult(found=3, duplicates=0)
    item = env.tables.item
    outbox = env.tables.outbox
    async with env.connection() as conn:
        items = (await conn.execute(select(item).order_by(item.c.id))).mappings().all()
        queued = (await conn.execute(select(outbox).order_by(outbox.c.id))).mappings().all()
    assert [row["key"] for row in items] == ["a", None, None]
    assert [row["weight"] for row in items] == [2, 1, 5]
    first = items[0]
    assert first["batch_id"] == root.id
    assert first["state"] == ItemState.ACTIVE
    assert first["task_name"] == "render"
    assert (first["attempt"], first["depth"]) == (0, 0)
    assert first["created_at"] == NOW
    assert first["child_batch_id"] is None
    assert producer.codec.decode("render", first["payload"]) == ((1,), {"x": "y"})
    # Запись outbox на каждый Item, id записи = id Item, payload берётся из th_item.
    assert [row["id"] for row in queued] == [row["id"] for row in items]
    assert [row["item_id"] for row in queued] == [row["id"] for row in items]
    assert {row["kind"] for row in queued} == {OutboxKind.ITEM}
    assert {row["batch_id"] for row in queued} == {root.id}
    assert {row["task_name"] for row in queued} == {"render"}
    assert {row["payload"] for row in queued} == {None}
    assert {row["available_at"] for row in queued} == {NOW}
    assert {row["attempts"] for row in queued} == {0}
    assert await env.counters(root.id) == CounterTotals(total=3, w_total=8, tree_total=3)
    counter = env.tables.counter
    async with env.connection() as conn:
        assert list(await conn.scalars(select(counter.c.slot))) == [PRODUCER_SLOT]


async def test_duplicates_by_key(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        first = await env.producer.add_items(
            conn, root.id, [call(1, key="a"), call(2, key="b"), call(3, key="a"), call(4)]
        )
    async with env.transaction() as conn:
        second = await env.producer.add_items(
            conn, root.id, [call(5, key="b"), call(6, key="c", weight=3)]
        )
    assert first == AddResult(found=3, duplicates=1)
    assert second == AddResult(found=1, duplicates=1)
    assert await env.counters(root.id) == CounterTotals(
        total=4, w_total=6, duplicates=2, tree_total=4
    )
    assert await env.count(env.tables.item) == 4
    assert await env.count(env.tables.outbox) == 4
    item = env.tables.item
    async with env.connection() as conn:
        payload = await conn.scalar(select(item.c.payload).where(item.c.key == "a"))
    assert payload is not None
    # Первый вызов с ключом выигрывает.
    assert env.producer.codec.decode("render", payload) == ((1,), {"x": "y"})


async def test_sub_batch_items_count_in_root_tree(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        _ = await env.producer.add_items(conn, pages.id, [call(1), call(2, weight=4)])
        _ = await env.producer.add_items(conn, root.id, [call(3)])
    assert await env.counters(pages.id) == CounterTotals(total=2, w_total=5)
    # Корень: виртуальный Item (weight 0) + свой Item; tree_total — Items всего дерева.
    assert await env.counters(root.id) == CounterTotals(total=2, w_total=1, tree_total=3)


async def test_start_at_and_pause_set_available_at(env: Env) -> None:
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        delayed = await env.producer.create_root(conn, RootSpec(kind=KIND, start_at=START))
        paused = await env.producer.create_root(conn, RootSpec(kind=KIND, start_at=START))
    await set_batch(env, paused.id, paused_at=NOW)
    async with env.transaction() as conn:
        _ = await env.producer.add_items(conn, delayed.id, [call(1)])
        _ = await env.producer.add_items(conn, paused.id, [call(2)])
    async with env.connection() as conn:
        at = await conn.scalar(select(outbox.c.available_at).where(outbox.c.batch_id == delayed.id))
        finite = await conn.scalar(
            select(func.isfinite(outbox.c.available_at)).where(outbox.c.batch_id == paused.id)
        )
    assert at == START
    assert finite is False


async def test_empty_add_writes_nothing(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        assert await env.producer.add_items(conn, root.id, []) == AddResult(found=0, duplicates=0)
    assert await env.count(env.tables.counter) == 0


@pytest.mark.parametrize(
    "values",
    [
        {"state": BatchState.SEALED},
        {"state": BatchState.CANCELLED},
        {"cancel_requested_at": NOW},
    ],
)
async def test_add_needs_open_batch(env: Env, values: dict[str, object]) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
    await set_batch(env, root.id, **values)
    async with env.transaction() as conn:
        with pytest.raises(SealError):
            _ = await env.producer.add_items(conn, root.id, [call(1)])


async def test_producer_cannot_add_into_stage(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        with pytest.raises(SpawnTargetError):
            _ = await env.producer.add_items(conn, cards.id, [call(1)])
        with pytest.raises(NotFoundError):
            _ = await env.producer.add_items(conn, UUID(int=1), [call(1)])


async def test_payload_limit(env: Env) -> None:
    producer = replace(env.producer, max_payload_bytes=64)
    async with env.transaction() as conn:
        root = await producer.create_root(conn, RootSpec(kind=KIND))
        _ = await producer.add_items(conn, root.id, [call(1)])
        with pytest.raises(ConfigurationError, match="предел 64"):
            _ = await producer.add_items(conn, root.id, [TaskCall(task_name="t", args=("x" * 80,))])


async def test_call_options_stored_in_item(env: Env) -> None:
    # D-033: queue и опции брокера живут в th_item.options; без опций — NULL.
    calls = [
        call(1),
        call(2).opts(queue="mail"),
        call(3).opts(priority=7, expires=30.5, metadata='{"a": 1}'),
        call(4).opts(queue="bulk", priority=1),
    ]
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        _ = await env.producer.add_items(conn, root.id, calls)
    item = env.tables.item
    async with env.connection() as conn:
        stored = list(await conn.scalars(select(item.c.options).order_by(item.c.id)))
    assert stored == [
        None,
        {"queue": "mail"},
        {"priority": 7, "expires": 30.5, "metadata": '{"a": 1}'},
        {"priority": 1, "queue": "bulk"},
    ]


async def test_call_options_must_be_json(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        with pytest.raises(ConfigurationError, match="JSON"):
            _ = await env.producer.add_items(conn, root.id, [call(1).opts(when=object())])


async def test_user_rollback_leaves_nothing(env: Env) -> None:
    tables = env.tables
    async with AsyncSession(env.engine) as session:
        conn = await resolve_connection(session)
        conn = await conn.execution_options(schema_translate_map={None: env.schema})
        root = await env.producer.create_root(conn, RootSpec(kind=KIND, key="k"))
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        _ = await env.producer.add_items(conn, pages.id, [call(1, key="a"), call(2)])
        await env.producer.expect(conn, cards.id, 10)
        await session.rollback()
    for table in (
        tables.batch,
        tables.item,
        tables.outbox,
        tables.feed,
        tables.counter,
        tables.counter_delta,
    ):
        assert await env.count(table) == 0


@contextlib.contextmanager
def statements_of(env: Env) -> Generator[list[str]]:
    """Запросы, отправленные движком теста, пока контекст открыт."""
    seen: list[str] = []

    def record(*args: object) -> None:
        # Третий аргумент события before_cursor_execute — текст запроса.
        seen.append(str(args[2]))

    sync_engine = env.engine.sync_engine
    event.listen(sync_engine, "before_cursor_execute", record)
    try:
        yield seen
    finally:
        event.remove(sync_engine, "before_cursor_execute", record)


async def test_100k_items_in_chunked_queries(env: Env) -> None:
    total = 100_000
    unique_keys = 99_000
    calls = (call(n, key=f"k{n % unique_keys}") for n in range(total))
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        with statements_of(env) as seen:
            result = await env.producer.add_items(conn, root.id, calls)
        executed = len(seen)

    chunks = total // ITEM_CHUNK
    # Блокировка батча + по запросу на чанк + upsert счётчиков.
    assert executed <= chunks + 3
    assert result == AddResult(found=unique_keys, duplicates=total - unique_keys)
    assert await env.counters(root.id) == CounterTotals(
        total=unique_keys,
        w_total=unique_keys,
        duplicates=total - unique_keys,
        tree_total=unique_keys,
    )
    assert await env.count(env.tables.item) == unique_keys
    assert await env.count(env.tables.outbox) == unique_keys
