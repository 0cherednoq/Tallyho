"""InlineBroker выполняет полный producer → relay → tracked → completer путь."""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho import Tallyho, callback, item
from tallyho.model.states import BatchState, ItemState
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho import BatchHandle
    from tallyho.testing import TallyhoTestEnv

__all__: list[str] = []


class RetryableError(Exception):
    """Управляемая ошибка пользовательской задачи."""


@asynccontextmanager
async def make_client(
    engine: AsyncEngine,
    schema: str,
    *,
    duplicates: float = 0.0,
    lease_seconds: float = 60,
    max_retries: int = 0,
) -> AsyncGenerator[tuple[Tallyho, InlineBroker, FakeClock]]:
    clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
    broker = InlineBroker(duplicate_delivery_rate=duplicates, seed=42, max_retries=max_retries)
    th = Tallyho(
        engine,
        schema=schema,
        clock=clock,
        lease_ttl=timedelta(seconds=lease_seconds),
        heartbeat_every=timedelta(seconds=max(lease_seconds / 3, 0.001)),
    )
    th.install(broker.adapter)
    await th.migrate()
    try:
        yield th, broker, clock
    finally:
        await broker.close()


async def test_duplicates_are_delivered_but_task_side_effect_runs_once(
    engine: AsyncEngine, schema: str
) -> None:
    calls: list[int] = []
    async with make_client(engine, schema, duplicates=1.0) as (th, broker, _clock):

        async def record(value: int) -> None:
            calls.append(value)
            await asyncio.sleep(0)

        async with th.batch("inline-duplicates", key="one") as batch:
            await batch.add(record, 7)

        assert await broker.drain() == 2
        assert calls == [7]
        view = await batch.handle.view()
        assert view.state is BatchState.SUCCEEDED
        assert view.progress.ok == 1


async def test_retry_and_dlq_follow_max_retries(engine: AsyncEngine, schema: str) -> None:
    attempts: Counter[int] = Counter()
    async with make_client(engine, schema) as (th, broker, _clock):

        async def flaky(value: int) -> None:
            await asyncio.sleep(0)
            attempts[value] += 1
            if value == 1 and attempts[value] == 1:
                raise RetryableError
            if value == 2:
                raise RetryableError

        async with th.batch("inline-retries", key="one") as batch:
            await batch.add_calls(
                [
                    th.call(flaky, 1).opts(key="ok", max_retries=1),
                    th.call(flaky, 2).opts(key="dead", max_retries=1),
                ]
            )

        assert await broker.drain() == 4
        assert attempts == Counter({1: 2, 2: 2})
        assert len(broker.dead_letters) == 1
        dead = await broker.reconcile_dead(None)
        assert dead.item_ids == (broker.dead_letters[0].id,)
        view = await batch.handle.view()
        assert view.state is BatchState.COMPLETED_WITH_ERRORS
        assert (view.progress.ok, view.progress.error) == (1, 1)


async def test_step_bounds_work_and_drain_finishes_rest(engine: AsyncEngine, schema: str) -> None:
    seen: list[int] = []
    async with make_client(engine, schema) as (th, broker, _clock):

        async def record(value: int) -> None:
            await asyncio.sleep(0)
            seen.append(value)

        async with th.batch("inline-step", key="one") as batch:
            await batch.map(record, range(4))

        assert await broker.step(2) == 2
        assert len(seen) == 2
        assert not (await batch.handle.view()).progress.final
        assert await broker.drain() == 2
        assert sorted(seen) == [0, 1, 2, 3]
        assert (await batch.handle.view()).progress.final


async def test_concurrent_drain_executes_a_worker_pool(engine: AsyncEngine, schema: str) -> None:
    active = 0
    maximum = 0
    release = asyncio.Event()
    started = asyncio.Event()
    async with make_client(engine, schema) as (th, broker, _clock):

        async def record() -> None:
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            if active == 4:
                started.set()
            await release.wait()
            active -= 1

        async with th.batch("inline-concurrent", key="one") as batch:
            await batch.add_calls([th.call(record) for _ in range(4)])

        draining = asyncio.create_task(broker.drain(concurrency=4))
        await started.wait()
        release.set()
        assert await draining == 4
        assert maximum == 4
        assert (await batch.handle.view()).state is BatchState.SUCCEEDED


async def test_kill_leaves_lease_until_maintenance_then_redelivers(
    engine: AsyncEngine, schema: str
) -> None:
    calls: list[int] = []
    async with make_client(engine, schema, lease_seconds=1) as (th, broker, clock):

        async def record(value: int) -> None:
            await asyncio.sleep(0)
            calls.append(value)

        async with th.batch("inline-kill", key="one") as batch:
            await batch.add_calls([th.call(record, 1).opts(max_retries=1)])

        broker.kill_worker_after(1)
        assert await broker.step() == 1
        assert calls == []
        assert len(await batch.handle.in_flight()) == 1

        _ = clock.advance(seconds=2)
        _ = await th.run_maintenance_once()
        assert await broker.drain() >= 1
        assert calls == [1]
        assert (await batch.handle.view()).state is BatchState.SUCCEEDED


async def test_broker_default_retries_bound_lease_recovery(
    engine: AsyncEngine, schema: str
) -> None:
    """Fix-5: лимит задан только настройкой брокера, у вызова опции нет."""
    calls: list[int] = []
    async with make_client(engine, schema, lease_seconds=1, max_retries=2) as (th, broker, clock):

        async def record(value: int) -> None:
            await asyncio.sleep(0)
            calls.append(value)

        async with th.batch("inline-default-retries", key="one") as batch:
            await batch.add_calls([th.call(record, 1)])

        async def lose_worker() -> list[tuple[str, int, str | None]]:
            broker.kill_worker_after(1)
            _ = await broker.drain()
            assert len(await batch.handle.in_flight()) == 1
            _ = clock.advance(seconds=2)
            _ = await th.run_maintenance_once()
            return [
                (view.state.name, view.attempt, view.label)
                async for view in batch.handle.items(states=[ItemState.ACTIVE, ItemState.ERROR])
            ]

        # Первые два истёкших lease возвращают Item в outbox и тратят попытку.
        assert await lose_worker() == [("ACTIVE", 1, None)]
        assert await lose_worker() == [("ACTIVE", 2, None)]
        assert (await batch.handle.view()).state is BatchState.SEALED
        # Попытки исчерпаны: третий истёкший lease — окончательная ошибка.
        assert await lose_worker() == [("ERROR", 2, "lease_expired")]
        # Брокер ещё раз доставит потерянное сообщение, но Item уже терминален.
        assert await broker.drain() == 1
        assert calls == []
        assert (await batch.handle.view()).state is BatchState.COMPLETED_WITH_ERRORS


async def test_broker_default_retries_apply_to_task_errors(
    engine: AsyncEngine, schema: str
) -> None:
    runs = 0
    async with make_client(engine, schema, max_retries=1) as (th, broker, _clock):

        async def flaky() -> None:
            nonlocal runs
            await asyncio.sleep(0)
            runs += 1
            raise RetryableError

        async with th.batch("inline-default-errors", key="one") as batch:
            await batch.add(flaky)

        assert await broker.drain() == 2
        assert runs == 2
        assert len(broker.dead_letters) == 1
        assert (await batch.handle.view()).state is BatchState.COMPLETED_WITH_ERRORS


async def test_kill_callback_requeues_it_without_item_lease(
    engine: AsyncEngine, schema: str
) -> None:
    called: list[object] = []
    async with make_client(engine, schema) as (th, broker, _clock):

        async def work() -> None:
            await asyncio.sleep(0)

        async def finalized() -> None:
            await asyncio.sleep(0)
            called.append(callback.current())

        async with th.batch(
            "inline-callback",
            key="one",
            on_succeeded=th.call(finalized),
        ) as batch:
            await batch.add(work)

        assert await broker.step() == 1
        broker.kill_worker_after(1)
        assert await broker.step() == 1
        assert called == []
        assert await broker.drain() == 1
        assert len(called) == 1
        assert called[0] is not None


async def test_killing_duplicate_with_terminal_item_does_not_leave_crash(
    engine: AsyncEngine, schema: str
) -> None:
    calls = 0
    async with make_client(engine, schema, duplicates=1.0) as (th, broker, _clock):

        async def work() -> None:
            nonlocal calls
            await asyncio.sleep(0)
            calls += 1

        async with th.batch("inline-kill-duplicate", key="one") as batch:
            await batch.add(work)

        assert await broker.step() == 1
        broker.kill_worker_after(1)
        assert await broker.step() == 1
        assert await broker.drain() == 0
        assert calls == 1


async def test_caller_cancellation_is_not_swallowed(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, broker, _clock):

        async def cancelled() -> None:
            await asyncio.sleep(0)
            raise asyncio.CancelledError

        async with th.batch("inline-cancelled", key="one") as batch:
            await batch.add(cancelled)

        with pytest.raises(asyncio.CancelledError):
            _ = await broker.step()
        view = await batch.handle.view()
        assert view.progress.pending == 1


async def test_pytest_fixture_is_installed_and_ready(tallyho_env: TallyhoTestEnv) -> None:
    seen: list[str] = []

    async def record(value: str) -> None:
        await asyncio.sleep(0)
        seen.append(value)

    wrapped = tallyho_env.broker.wrap(record)
    await wrapped("plain")

    async with tallyho_env.th.batch("fixture", key="ready") as batch:
        await batch.add(record, "ok")

    assert await tallyho_env.step() == 1
    _ = await tallyho_env.run_maintenance_once()
    assert await tallyho_env.drain() == 0
    assert seen == ["plain", "ok"]
    assert (await batch.handle.view()).state is BatchState.SUCCEEDED


async def test_drain_waits_for_post_commit_finalization_and_callback(
    engine: AsyncEngine, schema: str
) -> None:
    """Один drain доводит дерево до колбэка, даже если итог пишет ``complete_in``."""
    settled: list[int] = []
    async with make_client(engine, schema) as (th, broker, _clock):
        scoped = engine.execution_options(schema_translate_map={None: schema})

        async def in_user_transaction(value: int) -> None:
            async with scoped.begin() as connection:
                item.ok("done", result={"value": value})
                await item.complete_in(connection)

        async def boom(value: int) -> None:
            await asyncio.sleep(0)
            raise RetryableError(value)

        async def on_done(index: int) -> None:
            await asyncio.sleep(0)
            settled.append(index)

        handles: list[BatchHandle] = []
        for index in range(12):
            async with th.batch(
                "inline-settled",
                key=f"tree:{index}",
                on_finalized_task=th.call(on_done, index),
            ) as batch:
                stage = batch.sub_batch("stage")
                await stage.add(in_user_transaction, index)
                await stage.add_calls([th.call(boom, index).opts(max_retries=1)])
            handles.append(batch.handle)

        _ = await broker.drain()

        states = [(await handle.view()).state for handle in handles]
    assert states == [BatchState.COMPLETED_WITH_ERRORS] * 12
    assert sorted(settled) == list(range(12))
