"""InlineBroker выполняет полный producer → relay → tracked → completer путь."""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho import Tallyho, callback
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

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
) -> AsyncGenerator[tuple[Tallyho, InlineBroker, FakeClock]]:
    clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
    broker = InlineBroker(duplicate_delivery_rate=duplicates, seed=42)
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
