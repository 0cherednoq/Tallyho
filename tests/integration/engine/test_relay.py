"""Relay: захват outbox, отправка, подтверждение, fast-path, start_at, пауза (T4.2)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import insert, select, text, update
from typing_extensions import override

from tallyho.engine.producer import RootSpec
from tallyho.engine.relay import RelaySettings
from tallyho.model.calls import TaskCall
from tallyho.model.states import ItemState, OutboxKind
from tallyho.protocols.observer import NullObserver
from tests.helpers.relay import RecordingDispatcher, relay_env

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from tests.helpers.relay import RelayEnv
    from tests.integration.engine.conftest import Env

NOW = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
GRACE = timedelta(seconds=5)
TTL = timedelta(seconds=30)


@pytest.fixture
def rel(env: Env) -> RelayEnv:
    return relay_env(env, NOW)


def call(n: int, task: str = "render") -> TaskCall:
    return TaskCall(task_name=task, args=(n,))


async def add_batch(rel: RelayEnv, calls: Sequence[TaskCall], spec: RootSpec | None = None) -> UUID:
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(conn, spec or RootSpec(kind="k"))
        _ = await rel.producer.add_items(conn, root.id, calls)
    return root.id


async def outbox_rows(rel: RelayEnv) -> list[tuple[UUID, datetime, int]]:
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        result = await conn.execute(
            select(outbox.c.id, outbox.c.available_at, outbox.c.attempts).order_by(outbox.c.id)
        )
        return [(row_id, available_at, attempts) for row_id, available_at, attempts in result]


async def test_scan_dispatches_items_with_payload_and_options(rel: RelayEnv) -> None:
    calls = [call(1), call(2).opts(queue="mail", priority=3), call(3)]
    batch_id = await add_batch(rel, calls)
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 3
    messages = rel.dispatcher.messages
    assert len(rel.dispatcher.batches) == 1
    assert {message.batch_id for message in messages} == {batch_id}
    assert {message.kind for message in messages} == {OutboxKind.ITEM}
    assert {message.task_name for message in messages} == {"render"}
    decoded = [rel.producer.codec.decode("render", message.payload) for message in messages]
    assert decoded == [((1,), {}), ((2,), {}), ((3,), {})]
    assert [dict(message.options) for message in messages] == [
        {},
        {"queue": "mail", "priority": 3},
        {},
    ]
    item = rel.tables.item
    async with rel.env.connection() as conn:
        item_ids = list(await conn.scalars(select(item.c.id).order_by(item.c.id)))
    assert [message.id for message in messages] == item_ids
    assert await outbox_rows(rel) == []
    assert (await rel.env.counters(batch_id)).dispatched == 3
    # Всё отправлено: повторный проход ничего не шлёт.
    assert await rel.relay.scan_once() == 0


async def test_dispatch_groups_by_task_name(rel: RelayEnv) -> None:
    _ = await add_batch(rel, [call(1, "b"), call(2, "a"), call(3, "b")])
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 3
    assert [[m.task_name for m in batch] for batch in rel.dispatcher.batches] == [
        ["a"],
        ["b", "b"],
    ]


async def test_scan_waits_for_grace_kick_does_not(rel: RelayEnv) -> None:
    first = await add_batch(rel, [call(1)])
    second = await add_batch(rel, [call(2)])
    assert await rel.relay.scan_once() == 0
    rel.relay.kick([first])
    assert await rel.relay.flush_kicked() == 1
    assert [message.batch_id for message in rel.dispatcher.messages] == [first]
    assert await rel.relay.flush_kicked() == 0
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 1
    assert [message.batch_id for message in rel.dispatcher.messages] == [first, second]


async def test_kick_with_no_batches_does_nothing(rel: RelayEnv) -> None:
    rel.relay.kick([])
    assert await rel.relay.flush_kicked() == 0


async def test_failed_dispatch_is_retried_after_claim_ttl(rel: RelayEnv) -> None:
    # Падение между захватом и подтверждением: записи ждут claim_ttl, потом уходят снова.
    batch_id = await add_batch(rel, [call(1), call(2)])
    rel.clock.advance(GRACE)
    rel.dispatcher.fail_times = 1
    assert await rel.relay.scan_once() == 0
    claimed_until = rel.clock.current + TTL
    assert [(at, attempts) for _, at, attempts in await outbox_rows(rel)] == [
        (claimed_until, 1),
        (claimed_until, 1),
    ]
    assert (await rel.env.counters(batch_id)).dispatched == 0
    rel.clock.advance(TTL - timedelta(seconds=1))
    assert await rel.relay.scan_once() == 0
    rel.clock.advance(timedelta(seconds=1) + GRACE)
    assert await rel.relay.scan_once() == 2
    assert await outbox_rows(rel) == []
    assert (await rel.env.counters(batch_id)).dispatched == 2


async def test_failed_group_does_not_block_others(
    rel: RelayEnv, caplog: pytest.LogCaptureFixture
) -> None:
    _ = await add_batch(rel, [call(1, "bad"), call(2, "good")])
    rel.clock.advance(GRACE)
    rel.dispatcher.fail_tasks = frozenset({"bad"})
    with caplog.at_level(logging.ERROR, logger="tallyho.engine.relay"):
        assert await rel.relay.scan_once() == 1
    assert [m.task_name for m in rel.dispatcher.messages] == ["good"]
    assert "dispatch 1" in caplog.text
    assert len(await outbox_rows(rel)) == 1


async def test_parallel_scans_do_not_send_same_record(env: Env) -> None:
    rel = relay_env(env, NOW, RelaySettings(chunk=50))
    rel.dispatcher.delay = 0.005
    batch_id = await add_batch(rel, [call(n) for n in range(600)])
    rel.clock.advance(GRACE)
    other = RecordingDispatcher(delay=0.005)
    sent = await asyncio.gather(
        rel.relay.scan_once(), rel.another_relay(other).scan_once(), rel.relay.scan_once()
    )
    ids = rel.dispatcher.ids + other.ids
    assert sum(sent) == 600
    assert len(ids) == len(set(ids)) == 600
    assert other.ids
    assert await outbox_rows(rel) == []
    assert (await rel.env.counters(batch_id)).dispatched == 600


async def test_start_at_in_future_waits(rel: RelayEnv) -> None:
    start = NOW + timedelta(hours=1)
    batch_id = await add_batch(rel, [call(1), call(2)], RootSpec(kind="k", start_at=start))
    rel.relay.kick([batch_id])
    assert await rel.relay.flush_kicked() == 0
    rel.clock.advance(timedelta(minutes=59))
    assert await rel.relay.scan_once() == 0
    rel.clock.current = start
    assert await rel.relay.scan_once() == 0  # ещё grace
    rel.relay.kick([batch_id])
    assert await rel.relay.flush_kicked() == 2


async def test_paused_batch_is_parked(rel: RelayEnv) -> None:
    batch_id = await add_batch(rel, [call(1), call(2)])
    batch = rel.tables.batch
    async with rel.env.transaction() as conn:
        _ = await conn.execute(update(batch).where(batch.c.id == batch_id).values(paused_at=NOW))
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 0
    assert rel.dispatcher.messages == []
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        result = await conn.execute(
            select(outbox.c.available_at == text("'infinity'"), outbox.c.attempts)
        )
        assert sorted((bool(is_parked), attempts) for is_parked, attempts in result) == [
            (True, 0),
            (True, 0),
        ]


async def test_expires_writes_th_expiry(rel: RelayEnv) -> None:
    calls = [
        call(1).opts(expires=60),
        call(2),
        call(3).opts(expires=1.5),
        call(4).opts(expires=True),
    ]
    _ = await add_batch(rel, calls)
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 4
    ids = rel.dispatcher.ids
    expiry = rel.tables.expiry
    async with rel.env.connection() as conn:
        result = await conn.execute(select(expiry.c.item_id, expiry.c.expires_at))
        rows = {row.item_id: row.expires_at for row in result}
    sent_at = rel.clock.current
    assert rows == {
        ids[0]: sent_at + timedelta(seconds=60),
        ids[2]: sent_at + timedelta(seconds=1.5),
    }


async def test_redispatch_moves_expiry(rel: RelayEnv) -> None:
    _ = await add_batch(rel, [call(1).opts(expires=10)])
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 1
    item_id = rel.dispatcher.ids[0]
    outbox = rel.tables.outbox
    item = rel.tables.item
    # Sweeper вернул Item в outbox (lease истёк) — срок считается от новой отправки.
    async with rel.env.transaction() as conn:
        batch_id = await conn.scalar(select(item.c.batch_id).where(item.c.id == item_id))
        _ = await conn.execute(
            insert(outbox).values(
                id=item_id,
                kind=int(OutboxKind.ITEM),
                batch_id=batch_id,
                item_id=item_id,
                task_name="render",
                available_at=rel.clock.current,
            )
        )
    rel.clock.advance(timedelta(minutes=1))
    assert await rel.relay.scan_once() == 1
    expiry = rel.tables.expiry
    async with rel.env.connection() as conn:
        expires_at = await conn.scalar(select(expiry.c.expires_at))
    assert expires_at == rel.clock.current + timedelta(seconds=10)


async def test_callback_record_uses_own_payload_and_options(rel: RelayEnv) -> None:
    batch_id = await add_batch(rel, [])
    callback_id = rel.producer.ids.new_id()
    outbox = rel.tables.outbox
    async with rel.env.transaction() as conn:
        _ = await conn.execute(
            insert(outbox).values(
                id=callback_id,
                kind=int(OutboxKind.CALLBACK),
                batch_id=batch_id,
                task_name="notify",
                payload=b"cb",
                options={"queue": "hooks"},
                available_at=NOW,
            )
        )
    rel.relay.kick([batch_id])
    assert await rel.relay.flush_kicked() == 1
    (message,) = rel.dispatcher.messages
    assert (message.id, message.kind, message.payload) == (callback_id, OutboxKind.CALLBACK, b"cb")
    assert dict(message.options) == {"queue": "hooks"}
    assert await outbox_rows(rel) == []
    # Колбэк — не Item: dispatched батча не растёт.
    assert (await rel.env.counters(batch_id)).dispatched == 0


async def test_records_without_live_item_are_dropped(
    rel: RelayEnv, caplog: pytest.LogCaptureFixture
) -> None:
    batch_id = await add_batch(rel, [call(1), call(2), call(3)])
    item = rel.tables.item
    outbox = rel.tables.outbox
    async with rel.env.transaction() as conn:
        ids = list(await conn.scalars(select(item.c.id).order_by(item.c.id)))
        # Первый Item уже завершён (отменён), второго нет (retention), колбэк без payload.
        _ = await conn.execute(
            update(item).where(item.c.id == ids[0]).values(state=int(ItemState.CANCELLED))
        )
        _ = await conn.execute(item.delete().where(item.c.id == ids[1]))
        _ = await conn.execute(
            insert(outbox).values(
                id=rel.producer.ids.new_id(),
                kind=int(OutboxKind.CALLBACK),
                batch_id=batch_id,
                task_name="notify",
                available_at=NOW,
            )
        )
    rel.clock.advance(GRACE)
    with caplog.at_level(logging.WARNING, logger="tallyho.engine.relay"):
        assert await rel.relay.scan_once() == 1
    assert rel.dispatcher.ids == [ids[2]]
    assert await outbox_rows(rel) == []
    assert "удалено 3" in caplog.text


class ExplodingObserver(NullObserver):
    def __init__(self) -> None:
        self.calls: list[int] = []

    @override
    def relay_dispatched(self, *, messages: int, duration: float) -> None:
        self.calls.append(messages)
        raise RuntimeError


async def test_observer_error_does_not_break_dispatch(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    rel = relay_env(env, NOW)
    observer = ExplodingObserver()
    relay = rel.another_relay()
    relay.observer = observer
    _ = await add_batch(rel, [call(1), call(2)])
    rel.clock.advance(GRACE)
    with caplog.at_level(logging.ERROR, logger="tallyho.engine.relay"):
        assert await relay.scan_once() == 2
    assert observer.calls == [2]
    assert "Observer.relay_dispatched" in caplog.text
    assert await outbox_rows(rel) == []


async def test_run_loop_sends_on_kick(rel: RelayEnv) -> None:
    batch_id = await add_batch(rel, [call(1)])
    task = asyncio.create_task(rel.relay.run())
    try:
        rel.relay.kick([batch_id])
        _ = await asyncio.wait_for(rel.dispatcher.dispatched.wait(), timeout=10)
    finally:
        _ = task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert [message.batch_id for message in rel.dispatcher.messages] == [batch_id]


async def test_run_loop_survives_failed_pass(env: Env, caplog: pytest.LogCaptureFixture) -> None:
    rel = relay_env(env, NOW)
    broken = rel.another_relay()
    # Схема, которой нет: проход падает с ошибкой БД, цикл продолжает ждать.
    broken.engine = env.engine.execution_options(schema_translate_map={None: "no_such_schema"})
    task = asyncio.create_task(broken.run())
    try:
        with caplog.at_level(logging.ERROR, logger="tallyho.engine.relay"):
            broken.kick([rel.producer.ids.new_id()])
            for _ in range(200):
                if "fast-path" in caplog.text:
                    break
                await asyncio.sleep(0.05)
        assert "fast-path" in caplog.text
        assert not task.done()
    finally:
        _ = task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
