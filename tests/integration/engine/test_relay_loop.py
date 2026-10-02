"""Фоновый цикл relay: ленивый старт по kick, страховочный scan, остановка (Fix-10)."""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select

from tallyho.engine.producer import RootSpec
from tallyho.engine.relay import RelaySettings
from tallyho.model.calls import TaskCall
from tests.helpers.relay import RecordingDispatcher, relay_env

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from uuid import UUID

    from tallyho.engine.relay import Relay
    from tests.helpers.relay import RelayEnv
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

NOW = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
GRACE = timedelta(seconds=5)
FAST_SCAN = timedelta(milliseconds=40)
NEVER_SCAN = timedelta(hours=1)
TASK_NAME = "tallyho-relay"


@pytest.fixture
async def rel(env: Env) -> AsyncGenerator[RelayEnv]:
    """Relay, который сам не сканирует: в тесте работает только fast-path."""
    value = relay_env(env, NOW, RelaySettings(scan_interval=NEVER_SCAN))
    yield value
    await value.relay.close()


async def add_batch(rel: RelayEnv, items: int = 1) -> UUID:
    calls = [TaskCall(task_name="render", args=(n,)) for n in range(items)]
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(conn, RootSpec(kind="k"))
        _ = await rel.producer.add_items(conn, root.id, calls)
    return root.id


async def outbox_size(rel: RelayEnv) -> int:
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        return int(await conn.scalar(select(func.count()).select_from(outbox)) or 0)


async def eventually(condition: Callable[[], bool], *, deadline: float = 10.0) -> None:
    async with asyncio.timeout(deadline):
        for _ in itertools.count():
            if condition():
                return
            await asyncio.sleep(0.01)


def loop_tasks() -> list[asyncio.Task[object]]:
    return [task for task in asyncio.all_tasks() if task.get_name() == TASK_NAME]


async def test_kick_starts_loop_and_sends_without_grace(rel: RelayEnv) -> None:
    batch_id = await add_batch(rel)
    assert loop_tasks() == []

    rel.relay.kick([batch_id])

    assert rel.relay.running
    # Запись свежее relay_grace, scan в этом тесте не работает: отправил fast-path.
    _ = await asyncio.wait_for(rel.dispatcher.dispatched.wait(), timeout=10)
    assert [message.batch_id for message in rel.dispatcher.messages] == [batch_id]
    await eventually(lambda: len(loop_tasks()) == 1)

    await rel.relay.stop()

    assert loop_tasks() == []
    assert await outbox_size(rel) == 0


async def test_repeated_kicks_reuse_one_loop(rel: RelayEnv) -> None:
    first = await add_batch(rel)
    second = await add_batch(rel)

    rel.relay.kick([first])
    rel.relay.kick([second])
    rel.relay.start()

    await eventually(lambda: len(rel.dispatcher.messages) == 2)
    assert len(loop_tasks()) == 1


async def test_stop_delivers_already_kicked_batches(rel: RelayEnv) -> None:
    batch_id = await add_batch(rel, items=3)

    rel.relay.kick([batch_id])
    await rel.relay.stop()

    assert len(rel.dispatcher.messages) == 3
    assert await outbox_size(rel) == 0


async def test_stopped_loop_restarts_on_next_kick(rel: RelayEnv) -> None:
    rel.relay.start()
    await rel.relay.stop()
    await rel.relay.stop()
    batch_id = await add_batch(rel)

    rel.relay.kick([batch_id])

    await eventually(lambda: len(rel.dispatcher.messages) == 1)
    assert rel.relay.running


async def test_close_is_final(rel: RelayEnv) -> None:
    rel.relay.start()
    await rel.relay.close()
    batch_id = await add_batch(rel)

    rel.relay.kick([batch_id])
    rel.relay.start(scan_now=True)

    assert not rel.relay.running
    assert loop_tasks() == []
    # id накоплен: ручной проход (или scan другого процесса) его отправит.
    assert await rel.relay.flush_kicked() == 1


async def test_manual_mode_does_not_send_behind_the_owner(rel: RelayEnv) -> None:
    manual = replace(rel.relay, autostart=False)
    batch_id = await add_batch(rel)

    manual.kick([batch_id])
    await asyncio.sleep(0.2)

    assert not manual.running
    assert rel.dispatcher.messages == []
    assert await manual.flush_kicked() == 1


async def test_manual_mode_runs_only_while_started_explicitly(rel: RelayEnv) -> None:
    manual = replace(rel.relay, autostart=False)
    manual.start()
    try:
        batch_id = await add_batch(rel)
        manual.kick([batch_id])
        await eventually(lambda: len(rel.dispatcher.messages) == 1)
    finally:
        await manual.stop()

    later = await add_batch(rel)
    manual.kick([later])
    await asyncio.sleep(0.1)
    assert not manual.running
    assert len(rel.dispatcher.messages) == 1


async def test_lost_kick_is_picked_up_by_scan(env: Env) -> None:
    rel = relay_env(env, NOW, RelaySettings(grace=GRACE, scan_interval=FAST_SCAN))
    batch_id = await add_batch(rel, items=2)  # commit без kick: процесс «упал» до отправки
    rel.relay.start()
    try:
        await asyncio.sleep(5 * FAST_SCAN.total_seconds())
        assert rel.dispatcher.messages == []  # запись ещё моложе relay_grace

        rel.clock.advance(GRACE)

        await eventually(lambda: len(rel.dispatcher.messages) == 2)
        assert {message.batch_id for message in rel.dispatcher.messages} == {batch_id}
    finally:
        await rel.relay.close()
    assert await outbox_size(rel) == 0


async def test_scan_now_does_not_wait_for_interval(rel: RelayEnv) -> None:
    first = await add_batch(rel)
    rel.clock.advance(GRACE)

    rel.relay.start(scan_now=True)
    await eventually(lambda: len(rel.dispatcher.messages) == 1)

    # Цикл уже работает: повторный start только просит внеочередной scan.
    second = await add_batch(rel)
    rel.clock.advance(GRACE)
    rel.relay.start(scan_now=True)
    await eventually(lambda: len(rel.dispatcher.messages) == 2)

    assert [message.batch_id for message in rel.dispatcher.messages] == [first, second]
    assert len(loop_tasks()) == 1


async def test_loop_survives_failed_passes(env: Env, caplog: pytest.LogCaptureFixture) -> None:
    rel = relay_env(env, NOW, RelaySettings(scan_interval=FAST_SCAN))
    broken = rel.another_relay()
    # Схема, которой нет: и fast-path, и scan падают с ошибкой БД, цикл продолжает ждать.
    broken.engine = env.engine.execution_options(schema_translate_map={None: "no_such_schema"})
    try:
        with caplog.at_level(logging.ERROR, logger="tallyho.engine.relay"):
            broken.kick([rel.producer.ids.new_id()])
            await eventually(
                lambda: "проход fast-path" in caplog.text and "проход scan" in caplog.text
            )
        assert broken.running
    finally:
        await broken.close()


async def test_two_processes_do_not_send_one_message_twice(env: Env) -> None:
    settings = RelaySettings(grace=timedelta(0), scan_interval=timedelta(milliseconds=5), chunk=7)
    rel = relay_env(env, NOW, settings)
    rel.dispatcher.delay = 0.002
    other_dispatcher = RecordingDispatcher(delay=0.002)
    other = rel.another_relay(other_dispatcher)
    batches = 12
    per_batch = 9
    try:
        for _ in range(batches):
            batch_id = await add_batch(rel, items=per_batch)
            # Оба «процесса» узнали о батче и к тому же сканируют outbox.
            rel.relay.kick([batch_id])
            other.kick([batch_id])
        total = batches * per_batch
        await eventually(lambda: len(rel.dispatcher.ids) + len(other_dispatcher.ids) >= total)
        await asyncio.sleep(0.1)  # лишние проходы не должны ничего добавить
    finally:
        await rel.relay.close()
        await other.close()

    ids = rel.dispatcher.ids + other_dispatcher.ids
    assert len(ids) == len(set(ids)) == total
    assert await outbox_size(rel) == 0


async def test_kick_from_thread_wakes_loop_of_another_thread(rel: RelayEnv) -> None:
    rel.relay.start()
    batch_id = await add_batch(rel)

    await asyncio.to_thread(rel.relay.kick, [batch_id])

    await eventually(lambda: len(rel.dispatcher.messages) == 1)
    assert len(loop_tasks()) == 1


async def test_kick_without_event_loop_only_accumulates(rel: RelayEnv) -> None:
    batch_id = await add_batch(rel)

    await asyncio.to_thread(rel.relay.kick, [batch_id])

    assert not rel.relay.running
    assert await rel.relay.flush_kicked() == 1


def _start_in_loop_that_dies(relay: Relay) -> None:
    async def starter() -> None:
        relay.start()
        await asyncio.sleep(0)

    # Как при аварийной остановке приложения: event loop закрыт, задачу цикла
    # никто не отменил и не дождался.
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(starter())
    finally:
        loop.close()


async def test_loop_of_finished_event_loop_is_replaced(rel: RelayEnv) -> None:
    await asyncio.to_thread(_start_in_loop_that_dies, rel.relay)
    assert not rel.relay.running
    batch_id = await add_batch(rel)

    rel.relay.kick([batch_id])

    await eventually(lambda: len(rel.dispatcher.messages) == 1)
    assert len(loop_tasks()) == 1


async def test_stop_from_another_event_loop_only_requests_stop(rel: RelayEnv) -> None:
    rel.relay.start()
    await eventually(lambda: len(loop_tasks()) == 1)

    await asyncio.to_thread(asyncio.run, rel.relay.stop())

    await eventually(lambda: loop_tasks() == [])
    assert not rel.relay.running
