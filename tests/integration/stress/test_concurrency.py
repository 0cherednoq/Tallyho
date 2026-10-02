"""Deadlock, randomized-pipeline and duplicate-delivery stress tests."""

from __future__ import annotations

import asyncio
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol, final

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, TypedColumns, insert, select, text, update
from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

from tallyho import Tallyho, item
from tallyho.model.states import BatchState, OnFeederFailed
from tallyho.protocols.observer import NullObserver
from tallyho.testing import FakeClock, InlineBroker
from tests.helpers.db import DEADLOCK_SQLSTATE, deadlocks, record_db_errors, schema_connection

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from tallyho import BatchHandle
    from tallyho.model.views import BatchSummary
    from tests.helpers.db import DbError

__all__: list[str] = []

pytestmark = pytest.mark.timeout(900)

PIPELINES_PER_SEED = 1_000
PIPELINE_CHUNK = 50
STRESS_SEEDS = (104_729, 130_363, 155_921)


class _SourceError(Exception):
    pass


@final
class _HookProbeColumns(TypedColumns):
    id = Column(Integer, primary_key=True)
    calls = Column(Integer, nullable=False)


@final
class _Finalizations(NullObserver):
    def __init__(self) -> None:
        self.counts: Counter[UUID] = Counter()
        self.retries: Counter[str] = Counter()

    @override
    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        del kind, state
        self.counts[batch_id] += 1

    @override
    def transaction_retry(self, *, sqlstate: str) -> None:
        self.retries[sqlstate] += 1


@dataclass(frozen=True, slots=True)
class _Scenario:
    spawn_middle: bool
    spawn_sink: bool
    fail_source: bool
    feeder_failure: OnFeederFailed


class _SourceTask(Protocol):
    def __call__(
        self,
        run: int,
        *,
        spawn_middle: bool,
        spawn_sink: bool,
        fail: bool,
    ) -> Awaitable[None]: ...


class _MiddleTask(Protocol):
    def __call__(self, run: int, *, spawn_sink: bool) -> Awaitable[None]: ...


@dataclass(frozen=True, slots=True)
class _PipelineRuntime:
    th: Tallyho
    broker: InlineBroker
    clock: FakeClock
    observer: _Finalizations
    source: _SourceTask
    middle: _MiddleTask
    sink: Callable[[int], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _DeadlockHarness:
    engine: AsyncEngine
    th: Tallyho
    broker: InlineBroker
    observer: _Finalizations
    hook_probe: Table[_HookProbeColumns]


@dataclass(slots=True)
class _BlockingWork:
    root_count: int
    started: int = 0
    all_started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def run(self, root_index: int, source_index: int) -> None:
        assert root_index >= 0
        assert source_index in {0, 1}
        self.started += 1
        if self.started == self.root_count * 2:
            self.all_started.set()
        await self.release.wait()


def _random(seed: int) -> random.Random:
    return random.Random(seed)  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # deterministic stress schedule


def _pipeline_runtime(
    engine: AsyncEngine,
    schema: str,
    *,
    seed: int,
    duplicate_delivery_rate: float = 0.0,
) -> _PipelineRuntime:
    clock = FakeClock(datetime(2035, 1, 1, tzinfo=UTC))
    observer = _Finalizations()
    broker = InlineBroker(duplicate_delivery_rate=duplicate_delivery_rate, seed=seed)
    th = Tallyho(
        engine,
        schema=schema,
        clock=clock,
        observer=observer,
        lease_ttl=timedelta(seconds=1),
        heartbeat_every=timedelta(milliseconds=20),
        finalize_grace=timedelta(0),
    )
    th.install(broker.adapter)

    async def sink(run: int) -> None:
        await asyncio.sleep(0)
        assert run >= 0

    async def middle(run: int, *, spawn_sink: bool) -> None:
        await asyncio.sleep(0)
        if spawn_sink:
            item.spawn_call(th.call(sink, run), into="sink")

    async def source(
        run: int,
        *,
        spawn_middle: bool,
        spawn_sink: bool,
        fail: bool,
    ) -> None:
        await asyncio.sleep(0)
        if fail:
            message = f"source {run} failed"
            raise _SourceError(message)
        if spawn_middle:
            item.spawn_call(th.call(middle, run, spawn_sink=spawn_sink), into="middle")

    return _PipelineRuntime(th, broker, clock, observer, source, middle, sink)


async def _create_pipeline(
    runtime: _PipelineRuntime,
    run: int,
    scenario: _Scenario,
) -> BatchHandle:
    async with runtime.th.batch("stress-pipeline", key=str(run)) as root:
        source = root.sub_batch("source")
        middle = root.sub_batch(
            "middle",
            fed_by=[source],
            on_feeder_failed=scenario.feeder_failure,
        )
        _ = root.sub_batch(
            "sink",
            fed_by=[middle],
            on_feeder_failed=scenario.feeder_failure,
        )
        await source.add_calls(
            [
                runtime.th.call(
                    runtime.source,
                    run,
                    spawn_middle=scenario.spawn_middle,
                    spawn_sink=scenario.spawn_sink,
                    fail=scenario.fail_source,
                ).opts(max_retries=0 if scenario.fail_source else 1)
            ]
        )
    return root.handle


def _scenario(rng: random.Random) -> _Scenario:
    return _Scenario(
        spawn_middle=rng.random() >= 0.25,
        spawn_sink=rng.random() >= 0.25,
        fail_source=rng.random() < 0.20,
        feeder_failure=(OnFeederFailed.SEAL if rng.random() < 0.5 else OnFeederFailed.CANCEL),
    )


async def _assert_terminal_once(
    handles: Sequence[BatchHandle],
    observer: _Finalizations,
) -> None:
    views = await asyncio.gather(*(handle.view() for handle in handles))
    for view in views:
        assert view.state.is_terminal
        assert len(view.children) == 3
        batch_ids = [view.id, *(child.id for child in view.children.values())]
        assert all(child.state.is_terminal for child in view.children.values())
        assert all(observer.counts[batch_id] == 1 for batch_id in batch_ids)


async def _deadlock_harness(engine: AsyncEngine, schema: str) -> _DeadlockHarness:
    stress_engine = create_async_engine(
        engine.url,
        connect_args={"server_settings": {"deadlock_timeout": "100ms"}},
    )
    observer = _Finalizations()
    broker = InlineBroker()
    th = Tallyho(
        stress_engine,
        schema=schema,
        clock=FakeClock(datetime(2035, 1, 1, tzinfo=UTC)),
        observer=observer,
        heartbeat_every=timedelta(milliseconds=20),
    )
    th.install(broker.adapter)
    await th.migrate()
    hook_probe = Table(
        "stress_hook_probe",
        MetaData(),
        _HookProbeColumns,
        schema=schema,
    )
    async with stress_engine.begin() as connection:
        await connection.run_sync(hook_probe.create)
        _ = await connection.execute(insert(hook_probe).values(id=1, calls=0))

    @th.on_finalized("stress-deadlock")
    async def serialize_hook(session: AsyncSession, summary: BatchSummary) -> None:
        del summary
        _ = await session.execute(
            update(hook_probe).where(hook_probe.c.id == 1).values(calls=hook_probe.c.calls + 1)
        )

    return _DeadlockHarness(stress_engine, th, broker, observer, hook_probe)


async def _create_deadlock_roots(
    th: Tallyho,
    work: Callable[[int, int], Awaitable[None]],
    *,
    roots: int,
) -> list[BatchHandle]:
    handles: list[BatchHandle] = []
    for root_index in range(roots):
        async with th.batch("stress-deadlock", key=str(root_index)) as root:
            left = root.sub_batch("left")
            right = root.sub_batch("right")
            _ = root.sub_batch("target", fed_by=[left, right])
            await left.add(work, root_index, 0)
            await right.add(work, root_index, 1)
        handles.append(root.handle)
    return handles


def _assert_no_deadlocks(errors: list[DbError], observer: _Finalizations) -> None:
    # Любой 40P01 на соединениях сценария — нарушение порядка блокировок (§9.2, §14),
    # даже если библиотека повторила транзакцию: повтор виден в retries.
    assert deadlocks(errors) == []
    assert observer.retries[DEADLOCK_SQLSTATE] == 0


async def _run_pipelines(runtime: _PipelineRuntime, rng: random.Random) -> None:
    for offset in range(0, PIPELINES_PER_SEED, PIPELINE_CHUNK):
        handles = [
            await _create_pipeline(runtime, run, _scenario(rng))
            for run in range(offset, offset + PIPELINE_CHUNK)
        ]
        inject_kill = rng.random() < 0.75
        if inject_kill:
            runtime.broker.kill_worker_after(rng.randint(1, PIPELINE_CHUNK))
        _ = await runtime.broker.drain(concurrency=PIPELINE_CHUNK)
        if inject_kill:
            _ = runtime.clock.advance(seconds=2)
            _ = await runtime.th.run_maintenance_once()
            _ = await runtime.broker.drain(concurrency=PIPELINE_CHUNK)
        _ = await runtime.th.run_maintenance_once()
        _ = await runtime.broker.drain(concurrency=PIPELINE_CHUNK)
        await _assert_terminal_once(handles, runtime.observer)


@pytest.mark.slow
@pytest.mark.parametrize("seed", STRESS_SEEDS)
async def test_randomized_pipelines_close_every_stage_once(
    engine: AsyncEngine,
    schema: str,
    seed: int,
) -> None:
    runtime = _pipeline_runtime(engine, schema, seed=seed)
    await runtime.th.migrate()
    try:
        with record_db_errors(engine) as errors:
            await _run_pipelines(runtime, _random(seed))
        _assert_no_deadlocks(errors, runtime.observer)
    finally:
        await runtime.th.aclose()


@pytest.mark.slow
async def test_ten_percent_duplicate_delivery_finalizes_once(
    engine: AsyncEngine,
    schema: str,
) -> None:
    runtime = _pipeline_runtime(engine, schema, seed=42, duplicate_delivery_rate=0.10)
    calls: Counter[int] = Counter()

    async def record(index: int) -> None:
        await asyncio.sleep(0)
        calls[index] += 1

    await runtime.th.migrate()
    try:
        with record_db_errors(engine) as errors:
            async with runtime.th.batch("stress-duplicates", key="ten-percent") as batch:
                await batch.add_calls([runtime.th.call(record, index) for index in range(200)])
            _ = await runtime.broker.drain(concurrency=100)

        view = await batch.handle.view()
        assert view.state is BatchState.SUCCEEDED
        assert view.progress.ok == 200
        assert runtime.broker.deliveries > 200
        assert calls == Counter(dict.fromkeys(range(200), 1))
        assert runtime.observer.counts[view.id] == 1
        _assert_no_deadlocks(errors, runtime.observer)
    finally:
        await runtime.th.aclose()


@pytest.mark.slow
async def test_parallel_pause_cancel_sources_and_hooks_have_no_deadlocks(
    engine: AsyncEngine,
    schema: str,
) -> None:
    harness = await _deadlock_harness(engine, schema)
    roots = 16
    work = _BlockingWork(roots)
    draining: asyncio.Task[int] | None = None
    try:
        async with harness.engine.connect() as connection:
            assert await connection.scalar(text("SHOW deadlock_timeout")) == "100ms"

        # Дедлоки считаются по своим соединениям: pg_stat_database общий на БД и
        # публикуется с задержкой, в него попадают намеренные дедлоки других тестов.
        with record_db_errors(harness.engine) as errors:
            handles = await _create_deadlock_roots(harness.th, work.run, roots=roots)
            draining = asyncio.create_task(harness.broker.drain(concurrency=roots * 2))
            await asyncio.wait_for(work.all_started.wait(), timeout=10)
            operations = [
                handle.pause() if index % 2 == 0 else handle.cancel()
                for index, handle in enumerate(handles)
            ]
            _ = await asyncio.gather(*operations)
            work.release.set()
            _ = await asyncio.wait_for(draining, timeout=30)
            await _assert_terminal_once(handles, harness.observer)

        async with schema_connection(harness.engine, schema) as connection:
            hook_calls = await connection.scalar(
                select(harness.hook_probe.c.calls).where(harness.hook_probe.c.id == 1)
            )
        _assert_no_deadlocks(errors, harness.observer)
        assert hook_calls == roots
    finally:
        work.release.set()
        if draining is not None:
            _ = await asyncio.gather(draining, return_exceptions=True)
        await harness.th.aclose()
        await harness.engine.dispose()
