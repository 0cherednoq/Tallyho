"""Закрытие установки ``await th.aclose()`` (Fix-11, ARCHITECTURE §11.1).

После закрытия в event loop нет незавершённых задач библиотеки, удержанные
Items возвращены в outbox, а повторный вызов ничего не делает.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from tallyho import Tallyho
from tallyho.engine.shutdown import run_in
from tallyho.model.errors import ClosedError
from tallyho.model.states import BatchState
from tallyho.testing.broker import InlineBroker
from tests.helpers.loops import LoopThread, library_tasks
from tests.helpers.relay import RecordingDispatcher

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from uuid import UUID

    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

HEARTBEAT = timedelta(seconds=30)
"""Heartbeat в этих тестах не успевает сработать: задача просто ждёт своего срока."""


async def quick(number: int) -> None:
    """Задача, которая в этих тестах не исполняется или завершается сразу."""
    _ = number
    await asyncio.sleep(0)


async def eventually(condition: Callable[[], bool], *, deadline: float = 10.0) -> None:
    async with asyncio.timeout(deadline):
        for _ in itertools.count():
            if condition():
                return
            await asyncio.sleep(0.01)


async def outbox_items(env: Env) -> list[UUID]:
    outbox = env.tables.outbox
    async with env.connection() as conn:
        return list(await conn.scalars(select(outbox.c.item_id).order_by(outbox.c.item_id)))


async def item_attempts(env: Env) -> dict[UUID, int]:
    item = env.tables.item
    async with env.connection() as conn:
        return {row[0]: row[1] for row in await conn.execute(select(item.c.id, item.c.attempt))}


@pytest.fixture
async def installed(env: Env) -> AsyncGenerator[tuple[Tallyho, InlineBroker]]:
    """Установка с воркером ``InlineBroker`` над схемой теста."""
    broker = InlineBroker(seed=0)
    th = Tallyho(env.engine, schema=env.schema, heartbeat_every=HEARTBEAT)
    th.install(broker.adapter)
    try:
        yield th, broker
    finally:
        await th.aclose()


async def test_aclose_leaves_no_library_tasks_and_returns_held_items(
    env: Env, installed: tuple[Tallyho, InlineBroker]
) -> None:
    th, broker = installed
    started = asyncio.Event()
    hook_states: list[BatchState] = []

    async def long_running(number: int) -> None:
        _ = number
        started.set()
        _ = await asyncio.Event().wait()

    @th.on_finalized("closing-empty")
    async def slow_hook(_session: AsyncSession, summary: BatchSummary) -> None:
        await asyncio.sleep(0.2)
        hook_states.append(summary.state)

    async with th.batch(kind="closing", key="held") as batch:
        await batch.add(long_running, 1)
        await batch.add(long_running, 2)
    # Один Item взят «упавшим» воркером, второй выполняется: оба lease держит этот процесс.
    broker.kill_worker_after(1)
    assert await broker.step(1) == 1
    running = asyncio.create_task(broker.step(1))
    _ = await asyncio.wait_for(started.wait(), timeout=10)
    maintenance = asyncio.create_task(th.maintenance().run())
    await eventually(lambda: "tallyho-relay" in library_tasks())
    async with th.batch(kind="closing-empty", key="one") as empty:
        pass
    assert await env.count(env.tables.lease) == 2
    assert await outbox_items(env) == []
    before = set(library_tasks())
    assert {"tallyho-completer", "tallyho-relay", "tallyho-api-finalize"} <= before
    assert any(name.startswith("tallyho-heartbeat-") for name in before)

    await th.aclose()

    assert library_tasks() == []
    # Удержанные Items вернулись в outbox, lease удалены, попытка не потрачена.
    assert await env.count(env.tables.lease) == 0
    attempts = await item_attempts(env)
    assert await outbox_items(env) == sorted(attempts)
    assert set(attempts.values()) == {0}
    # Финализация, начатая до закрытия, доведена до конца вместе с хуком.
    assert hook_states == [BatchState.SUCCEEDED]
    assert (await empty.handle.view()).state is BatchState.SUCCEEDED
    # Maintenance получил просьбу остановиться; дожидается его тот, кто запустил.
    await asyncio.wait_for(maintenance, timeout=10)

    await th.aclose()  # повторный вызов — no-op

    assert library_tasks() == []
    assert await env.count(env.tables.lease) == 0
    assert len(await outbox_items(env)) == 2
    # Недоработавшая задача при отмене не подменяет CancelledError ошибкой закрытия.
    _ = running.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await running


async def test_closed_installation_rejects_writes_and_serves_reads(
    installed: tuple[Tallyho, InlineBroker],
) -> None:
    th, _broker = installed
    async with th.batch(kind="closed", key="one") as batch:
        await batch.add(quick, 1)
    handle = batch.handle

    await th.aclose()

    with pytest.raises(ClosedError, match="aclose"):
        async with th.batch(kind="closed", key="two"):
            pass
    later = datetime.now(UTC) + timedelta(hours=1)
    for operation in (
        handle.pause,
        handle.resume,
        handle.cancel,
        handle.retry_failed,
        handle.retry_finalize,
        handle.release,
        lambda: handle.reschedule(later),
    ):
        with pytest.raises(ClosedError):
            _ = await operation()
    with pytest.raises(ClosedError):
        _ = th.maintenance()
    with pytest.raises(ClosedError):
        _ = await th.run_maintenance_once()
    # Чтение фоновой работы не создаёт и после закрытия доступно.
    view = await handle.view()
    assert (view.state, view.progress.found) == (BatchState.SEALED, 1)
    assert (await th.find("closed", "one")).id == handle.id
    assert [info.id for info in (await th.list_batches()).items] == [handle.id]
    assert library_tasks() == []


async def test_commit_after_close_starts_no_background_work(
    env: Env, installed: tuple[Tallyho, InlineBroker]
) -> None:
    th, _broker = installed
    scoped = env.engine.execution_options(schema_translate_map={None: env.schema})
    async with th.batch(kind="late", key="cancelled") as early:
        await early.add(quick, 1)

    async with AsyncSession(scoped) as session:
        async with th.batch(kind="late", key="sealed", session=session) as late:
            pass
        await early.handle.cancel(session=session)
        await th.aclose()
        # Транзакция начата до закрытия: commit проходит, after-commit действия не бросают.
        await session.commit()
    await asyncio.sleep(0)

    assert library_tasks() == []
    # Финализацию не запустили: её выполнит sweeper другого процесса.
    assert (await late.handle.view()).state is BatchState.SEALED
    assert (await early.handle.view()).state is BatchState.SEALED


async def test_aclose_cancels_work_that_misses_close_timeout(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    dispatcher = RecordingDispatcher(delay=3600)  # брокер завис: проход relay не закончится
    th = Tallyho(env.engine, schema=env.schema, close_timeout=timedelta(seconds=0.5))
    th.install(dispatcher)
    entered = asyncio.Event()

    @th.on_finalized("stuck")
    async def stuck_hook(_session: AsyncSession, _summary: BatchSummary) -> None:
        entered.set()
        _ = await asyncio.Event().wait()

    async with th.batch(kind="stuck", key="hook") as stuck:
        pass
    _ = await asyncio.wait_for(entered.wait(), timeout=10)
    async with th.batch(kind="mail", key="dispatch") as mail:
        await mail.add(quick, 1)
    await eventually(lambda: "tallyho-relay" in library_tasks())

    started = monotonic()
    with caplog.at_level(logging.WARNING, logger="tallyho"):
        await th.aclose()
    elapsed = monotonic() - started

    assert elapsed < 5
    assert library_tasks() == []
    assert "close_timeout" in caplog.text
    assert "tallyho-api-finalize" in caplog.text
    assert "relay: цикл не остановился" in caplog.text
    # Ничего не потеряно: батч остался sealed, запись — в outbox; их подберут sweeper и scan.
    assert (await stuck.handle.view()).state is BatchState.SEALED
    assert len(await outbox_items(env)) == 1
    assert dispatcher.messages == []


@pytest.mark.parametrize("stopped", [True, False], ids=["stopped-loop", "running-loop"])
async def test_aclose_from_another_loop_closes_worker_parts_in_their_loop(
    env: Env, postgres_dsn: str, *, stopped: bool
) -> None:
    # Воркер flexiq исполняет задачи в своём event loop и останавливает его, не закрывая,
    # при выходе из run_worker; закрывают установку уже из другого loop.
    worker = LoopThread()
    engine = create_async_engine(postgres_dsn)
    broker = InlineBroker(seed=0)
    th = Tallyho(engine, schema=env.schema, heartbeat_every=HEARTBEAT)
    th.install(broker.adapter)

    async def work() -> None:
        async with th.batch(kind="worker", key="held") as batch:
            await batch.add(quick, 1)
        broker.kill_worker_after(1)  # lease взят в loop воркера и не отпущен
        _ = await broker.step(1)

    try:
        await worker.run(work())
        assert await env.count(env.tables.lease) == 1
        assert "tallyho-completer" in library_tasks(worker.loop)
        if stopped:
            worker.stop()

        await th.aclose()

        assert library_tasks(worker.loop) == []
        assert library_tasks() == []
        assert await env.count(env.tables.lease) == 0
        assert len(await outbox_items(env)) == 1
        await th.aclose()
    finally:
        # Соединения пула принадлежат loop воркера: закрываем их там же.
        await run_in(worker.loop, engine.dispose, patience=10)
        worker.close()
