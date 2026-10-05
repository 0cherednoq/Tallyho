"""Путь B при закрытии из другого потока: loop владельца остановлен посреди запроса (Fix-25).

Воркер flexiq останавливает loop исполнителя, не дожидаясь задач библиотеки, и
его поток завершается; ``aclose`` докручивает этот loop в служебном потоке
(ARCHITECTURE §11.1). Запрос SQLAlchemy, прерванный остановкой, привязан к
greenlet умершего потока и возобновиться не может. После-коммитная транзакция
``complete_in`` (lease, дельты, слот метрик) идемпотентна, поэтому Completer
повторяет её в потоке закрытия: lease снимается сразу, а не через ``lease_ttl``.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho.engine.completer import Completer, CompleterTriggers, FinishResult
from tallyho.engine.completion import complete_in
from tallyho.engine.shutdown import run_in
from tallyho.model.states import ResultClass
from tallyho.storage.tx import RetryPolicy, TxSettings, deliver_committed
from tests.helpers.after_commit import pause_commit_polling
from tests.helpers.loops import LoopThread
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    SETTINGS,
    MovableClock,
    lease_row,
    open_completer,
    seed,
)

if TYPE_CHECKING:
    import pytest
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.engine.completer import ItemRef
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

WITH_METRICS = FinishResult(result_class=ResultClass.OK, label="sent", metrics={"bytes": 42})
SETTLE_FAILED = "обработка complete_in после commit упала"
RETHREADED = "прервана остановкой event loop, повтор"


async def _blocked_by(env: Env, pid: int) -> bool:
    """Ждёт ли какой-нибудь backend блокировку, которую держит ``pid``."""
    query = text("SELECT count(*) FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid))")
    async with env.connection() as conn:
        return bool(await conn.scalar(query, {"pid": pid}))


async def _wait_blocked(env: Env, pid: int) -> None:
    async with asyncio.timeout(10):
        for _ in itertools.count():
            if await _blocked_by(env, pid):
                return
            await asyncio.sleep(0.01)


async def _lock_item(holder: AsyncConnection, env: Env, ref: ItemRef) -> int:
    item = env.tables.item
    _ = await holder.execute(select(item.c.id).where(item.c.id == ref.id).with_for_update())
    return int(await holder.scalar(text("SELECT pg_backend_pid()")) or 0)


async def test_settle_frozen_by_stopped_owner_loop_is_redone_in_closing_thread(
    env: Env,
    postgres_dsn: str,
    *,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pause_commit_polling(monkeypatch)
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    worker = LoopThread()
    # Соединения этого движка принадлежат loop воркера, как у flexiq.
    engine = create_async_engine(postgres_dsn).execution_options(
        schema_translate_map={None: env.schema}
    )
    completer = Completer(
        tables=env.tables,
        engine=engine,
        clock=MovableClock(),
        settings=SETTINGS,
        triggers=CompleterTriggers(producer=env.producer),
    )

    async def finish() -> None:
        assert (await completer.claim(ref)).run
        async with engine.begin() as conn:
            assert await complete_in(conn, ref, WITH_METRICS, completer=completer, attempt=0)

    try:
        await worker.run(finish())
        async with env.connection() as holder:
            pid = await _lock_item(holder, env, ref)
            # После-коммитная транзакция начата в потоке воркера и ждёт строку Item.
            _ = worker.loop.call_soon_threadsafe(deliver_committed)
            await _wait_blocked(env, pid)
            worker.stop()  # flexiq: loop остановлен посреди запроса, поток завершился
            await holder.rollback()

        with caplog.at_level(logging.WARNING, logger="tallyho"):
            await run_in(worker.loop, lambda: completer.close(requeue_held=True), patience=10)

        assert SETTLE_FAILED not in caplog.text
        assert RETHREADED in caplog.text
        assert await lease_row(env, ref.id) is None
        assert await env.count(env.tables.counter_delta) == 0
        metric = env.tables.metric
        async with env.connection() as conn:
            slots = set(await conn.scalars(select(metric.c.slot)))
        assert slots == {COMPLETER_SLOT}
        assert (await env.counters(seeded.batch_id)).ok == 1
    finally:
        await run_in(worker.loop, engine.dispose, patience=10)
        worker.close()


async def test_settle_failure_in_same_thread_is_not_repeated(
    env: Env, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Повтор — только при смене потока: обычный отказ логируется, хвосты убирает sweeper."""
    pause_commit_polling(monkeypatch)
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    impatient = replace(
        SETTINGS,
        tx=TxSettings(lock_timeout=timedelta(milliseconds=100)),
        retry=RetryPolicy(attempts=1),
    )
    async with open_completer(env, settings=impatient) as completer:
        assert (await completer.claim(ref)).run
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, WITH_METRICS, completer=completer, attempt=0)
        async with env.connection() as holder:
            _ = await _lock_item(holder, env, ref)
            with caplog.at_level(logging.WARNING, logger="tallyho"):
                deliver_committed()
                await completer.settled()
            await holder.rollback()

    assert SETTLE_FAILED in caplog.text
    assert RETHREADED not in caplog.text
    assert await lease_row(env, ref.id) is not None
    assert await env.count(env.tables.counter_delta) == 1
