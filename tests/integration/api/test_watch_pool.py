"""Fix-17: ``wait()`` и ``watch()`` возвращают соединение в пул без подписки.

Соединение, на котором был ``LISTEN th_progress``, должно вернуться в пул после
``UNLISTEN`` — при успехе ``wait()``, по таймауту, при отмене и при закрытии
``watch()`` посреди итерации. Иначе подписка копится на соединениях пула, а
прерванный запрос asyncpg может оставить на нём неявную транзакцию: следующая
транзакция видит устаревший ``now()``, и fast-path relay не берёт только что
созданную запись outbox (``available_at <= now()`` ложно).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal, cast, final

import pytest
from sqlalchemy import event, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import QueuePool

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker
from tests.helpers.db import temporary_schema

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.api.batch import BatchHandle
    from tallyho.model.views import BatchView

__all__: list[str] = []

Driver = Literal["asyncpg", "psycopg"]

_WATCH_TASKS = frozenset({"ProgressWatcher._produce", "_PsycopgSubscription._pump"})
"""Корутины задач ``watch()``, которые берут соединения пула."""

pytestmark = pytest.mark.parametrize(
    "driver",
    [
        "asyncpg",
        pytest.param(
            "psycopg",
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="psycopg async requires selector loop"
            ),
        ),
    ],
)


@final
@dataclass(frozen=True, slots=True)
class _Stand:
    th: Tallyho
    broker: InlineBroker
    engine: AsyncEngine
    probe: AsyncEngine
    holders: dict[int, asyncio.Task[object] | None]
    """Занятые соединения пула: id записи пула → задача, которая его взяла."""


@pytest.fixture
async def stand(postgres_dsn: str, driver: Driver) -> AsyncIterator[_Stand]:
    """Установка на выбранном драйвере и отдельный движок для замера времени БД."""
    url = make_url(postgres_dsn).set(drivername=f"postgresql+{driver}")
    engine = create_async_engine(url)
    probe = create_async_engine(postgres_dsn)
    holders: dict[int, asyncio.Task[object] | None] = {}

    def checkout(_dbapi: object, record: object, _proxy: object) -> None:
        holders[id(record)] = asyncio.current_task()

    def checkin(_dbapi: object, record: object) -> None:
        _ = holders.pop(id(record), None)

    event.listen(engine.sync_engine.pool, "checkout", checkout)
    event.listen(engine.sync_engine.pool, "checkin", checkin)
    broker = InlineBroker()
    try:
        async with temporary_schema(engine) as schema:
            th = Tallyho(engine, schema=schema)
            th.install(broker.adapter)
            _ = await th.migrate()
            try:
                yield _Stand(th, broker, engine, probe, holders)
            finally:
                await broker.close()
                await th.aclose()
    finally:
        await probe.dispose()
        await engine.dispose()


async def _pending(stand: _Stand, key: str) -> BatchHandle:
    """Батч с невыполненным Item: ``wait()`` на нём не завершится сам."""
    async with stand.th.batch("fix17-pending", key=key) as batch:
        await batch.add(_noop)
    return batch.handle


async def _noop() -> None:
    await asyncio.sleep(0)


def _held_by_watch(stand: _Stand) -> list[asyncio.Task[object]]:
    """Соединения, которые держит ``watch()``; остальные — фоновые задачи библиотеки."""
    return [
        task
        for task in stand.holders.values()
        if task is not None and getattr(task.get_coro(), "__qualname__", "") in _WATCH_TASKS
    ]


async def _quiesce(stand: _Stand) -> None:
    """Дождаться, пока фоновые after-commit действия вернут соединения в пул."""
    async with asyncio.timeout(10):
        for _ in itertools.count():
            if not stand.holders:
                return
            await asyncio.sleep(0.01)


async def _session_state(conn: AsyncConnection) -> tuple[list[str], datetime]:
    result = await conn.execute(text("SELECT pg_listening_channels()"))
    channels: list[object] = list(result.scalars())
    now = await conn.scalar(select(func.now()))
    assert isinstance(now, datetime)
    return [str(channel) for channel in channels], now


async def _assert_pool_clean(stand: _Stand) -> None:
    """Соединение подписки уже в пуле; в пуле нет LISTEN, новая транзакция видит текущее время.

    Финализация после seal идёт фоновой задачей библиотеки и может держать своё
    соединение в момент проверки, поэтому занятость пула проверяется только
    для задач ``watch()``, а состояние соединений — после того как пул затих.
    """
    assert _held_by_watch(stand) == []
    await _quiesce(stand)
    pool = stand.engine.pool
    assert isinstance(pool, QueuePool)
    idle = pool.checkedin()
    assert idle > 0
    async with stand.probe.connect() as probe:
        started = await probe.scalar(select(func.clock_timestamp()))
    assert isinstance(started, datetime)
    async with contextlib.AsyncExitStack() as stack:
        conns: list[AsyncConnection] = [
            await stack.enter_async_context(stand.engine.connect()) for _ in range(idle)
        ]
        for conn in conns:
            channels, now = await _session_state(conn)
            assert channels == []
            # Транзакция, оставшаяся открытой на соединении, дала бы now() из прошлого.
            assert now >= started
            await conn.rollback()


async def test_wait_success_returns_clean_connection(stand: _Stand) -> None:
    async with stand.th.batch("fix17-done", key="done") as batch:
        await batch.add(_noop)
    _ = await stand.broker.drain()
    view = await batch.handle.wait(timeout=5)

    assert view.state is BatchState.SUCCEEDED
    await _assert_pool_clean(stand)


async def test_wait_timeout_returns_clean_connection(stand: _Stand) -> None:
    handle = await _pending(stand, "timeout")

    with pytest.raises(TimeoutError):
        _ = await handle.wait(timeout=0.3)

    await _assert_pool_clean(stand)


async def test_cancelled_wait_returns_clean_connection(stand: _Stand) -> None:
    handle = await _pending(stand, "cancel")
    waiting = asyncio.create_task(handle.wait())
    await asyncio.sleep(0.3)

    _ = waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await waiting

    await _assert_pool_clean(stand)


@pytest.mark.parametrize("pause", [0.0, 0.001, 0.005, 0.02])
async def test_watch_closed_mid_iteration_returns_clean_connection(
    stand: _Stand, pause: float
) -> None:
    handle = await _pending(stand, f"close-{pause}")
    # Публичный тип watch() — AsyncIterator; закрыть поток явно можно через aclose генератора.
    stream = cast("AsyncGenerator[BatchView]", handle.watch())
    async with contextlib.aclosing(stream):
        async for view in stream:
            assert not view.state.is_terminal
            await asyncio.sleep(pause)
            break

    await _assert_pool_clean(stand)


async def test_cancelled_watch_consumer_returns_clean_connection(stand: _Stand) -> None:
    handle = await _pending(stand, "consumer")
    first = asyncio.Event()

    async def consume() -> None:
        async for _ in handle.watch():
            first.set()

    consumer = asyncio.create_task(consume())
    _ = await first.wait()
    _ = consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    await _assert_pool_clean(stand)


async def test_wait_then_retry_in_user_session_is_relayed_on_fast_path(stand: _Stand) -> None:
    """Сценарий отчёта Fix-12: после ``wait()`` повтор уходит через fast-path, а не scan."""
    attempts = 0

    async def flaky() -> None:
        nonlocal attempts
        await asyncio.sleep(0)
        attempts += 1
        if attempts == 1:
            item.error("unlucky")

    async with stand.th.batch("fix17-retry", key="retry") as batch:
        await batch.add(flaky)
    _ = await stand.broker.drain()
    await _quiesce(stand)
    assert (await batch.handle.wait(timeout=5)).state is BatchState.COMPLETED_WITH_ERRORS

    async with AsyncSession(stand.engine) as session:
        assert await batch.handle.retry_failed(session=session) == 1
        await session.commit()
    assert await stand.broker.drain() == 1

    assert attempts == 2
    await _quiesce(stand)
    assert (await batch.handle.wait(timeout=5)).state is BatchState.SUCCEEDED
    await _assert_pool_clean(stand)


async def test_listening_connection_is_reused_without_subscription(stand: _Stand) -> None:
    """Повторные ``wait()`` не копят подписки: в пуле нет ни одного LISTEN."""
    async with stand.th.batch("fix17-repeat", key="repeat") as batch:
        await batch.add(_noop)
    _ = await stand.broker.drain()

    for _ in range(5):
        _ = await batch.handle.wait(timeout=5)
    async with stand.engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM pg_listening_channels()"))
        ).scalar() == 0
