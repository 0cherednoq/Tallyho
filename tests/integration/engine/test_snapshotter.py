"""Snapshotter: расписание, EMA и атомарный CAS tx-хука прогресса."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, update
from typing_extensions import override

from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.snapshotter import Snapshotter, SnapshotterSettings
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState
from tallyho.protocols.clock import SystemClock
from tallyho.protocols.observer import NullObserver
from tallyho.storage.counters import CounterDelta, upsert_slots
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class ManualClock(SystemClock):
    """Управляемое монотонное время расписания Snapshotter."""

    value: float = 0.0

    @override
    def monotonic(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        """Сдвинуть расписание вперёд."""
        self.value += seconds


class ExplodingObserver(NullObserver):
    """Проверяет, что сбой наблюдаемости не влияет на Snapshotter."""

    missing_calls: int = 0
    failed_calls: int = 0

    @override
    def hook_missing(self, *, batch_id: UUID, kind: str, hook: str) -> None:
        del batch_id, kind, hook
        self.missing_calls += 1
        message = "observer missing"
        raise RuntimeError(message)

    @override
    def hook_failed(
        self,
        *,
        batch_id: UUID,
        kind: str,
        hook: str,
        attempt: int,
        error: BaseException,
    ) -> None:
        del batch_id, kind, hook, attempt, error
        self.failed_calls += 1
        message = "observer failed"
        raise RuntimeError(message)


def snapshotter(env: Env, registry: HookRegistry, clock: ManualClock) -> Snapshotter:
    """Snapshotter над схемой теста."""
    return Snapshotter(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock,
        hooks=registry,
    )


async def test_tick_writes_only_changes_with_strict_seq_and_eta(
    env: Env, registry: HookRegistry
) -> None:
    clock = ManualClock()
    seen: list[BatchSummary] = []

    @registry.on_progress("work", every=timedelta(seconds=10))
    async def save(_session: AsyncSession, summary: BatchSummary) -> None:
        await asyncio.sleep(0)
        seen.append(summary)

    async with env.transaction() as conn:
        first = await env.producer.create_root(
            conn, RootSpec(kind="work", key="first", expected_total=2)
        )
        second = await env.producer.create_root(
            conn, RootSpec(kind="work", key="second", expected_total=1)
        )
        _ = await env.producer.add_items(
            conn,
            first.id,
            [TaskCall(task_name="task", args=(index,), kwargs={}) for index in range(2)],
        )
        _ = await env.producer.add_items(
            conn, second.id, [TaskCall(task_name="task", args=(1,), kwargs={})]
        )

    subject = snapshotter(env, registry, clock)
    assert await subject.tick() == 2
    assert {(summary.id, summary.seq) for summary in seen} == {
        (first.id, 1),
        (second.id, 1),
    }

    async with env.transaction() as conn:
        await upsert_slots(
            conn,
            env.tables,
            {(first.id, 7): CounterDelta(ok=1, w_done=1)},
        )
    # Изменение до every не вызывает ранний снимок.
    assert await subject.tick() == 0
    clock.advance(10)
    assert await subject.tick() == 1
    latest = seen[-1]
    assert (latest.id, latest.seq, latest.progress.done) == (first.id, 2, 1)
    assert latest.progress.eta is not None
    assert (await env.batch(first.id))["snap_seq"] == 2
    assert (await env.batch(second.id))["snap_seq"] == 1
    clock.advance(10)
    assert await subject.tick() == 0
    assert len(seen) == 3


async def test_finalization_wins_after_hook_and_rolls_back_domain_write(
    env: Env, registry: HookRegistry
) -> None:
    clock = ManualClock()
    probe = await create_probe(env.engine, env.schema)
    entered = asyncio.Event()
    release = asyncio.Event()

    @registry.on_progress("race", every=timedelta(seconds=1))
    async def save(session: AsyncSession, _summary: BatchSummary) -> None:
        await insert_id(await session.connection(), probe, 1)
        entered.set()
        await release.wait()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="race"))

    running = asyncio.create_task(snapshotter(env, registry, clock).tick())
    await entered.wait()
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(
                state=int(BatchState.SUCCEEDED),
                snap_seq=env.tables.batch.c.snap_seq + 1,
                finished_at=func.now(),
            )
        )
    release.set()
    assert await running == 0
    assert await committed_ids(env.engine, probe) == []
    row = await env.batch(root.id)
    assert row["snap_seq"] == 1
    assert row["state"] == int(BatchState.SUCCEEDED)


async def test_two_snapshotters_commit_one_strict_sequence(
    env: Env, registry: HookRegistry
) -> None:
    clock = ManualClock()
    ready = asyncio.Event()
    release = asyncio.Event()
    entered = 0

    @registry.on_progress("concurrent", every=timedelta(seconds=1))
    async def save(_session: AsyncSession, _summary: BatchSummary) -> None:
        nonlocal entered
        entered += 1
        if entered == 2:
            ready.set()
        await release.wait()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="concurrent"))

    first = asyncio.create_task(snapshotter(env, registry, clock).tick())
    second = asyncio.create_task(snapshotter(env, registry, clock).tick())
    await ready.wait()
    release.set()
    assert sorted(await asyncio.gather(first, second)) == [0, 1]
    assert (await env.batch(root.id))["snap_seq"] == 1


async def test_hook_failure_rolls_back_and_is_retried(env: Env, registry: HookRegistry) -> None:
    clock = ManualClock()
    probe = await create_probe(env.engine, env.schema)
    fail = True

    @registry.on_progress("flaky", every=timedelta(seconds=1))
    async def save(session: AsyncSession, summary: BatchSummary) -> None:
        nonlocal fail
        await insert_id(await session.connection(), probe, summary.seq)
        if fail:
            message = "boom"
            raise RuntimeError(message)

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="flaky"))

    observer = ExplodingObserver()
    subject = Snapshotter(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock,
        hooks=registry,
        observer=observer,
    )
    assert await subject.tick() == 0
    assert observer.failed_calls == 1
    assert await committed_ids(env.engine, probe) == []
    assert (await env.batch(root.id))["snap_seq"] == 0

    fail = False
    clock.advance(1)
    assert await subject.tick() == 1
    assert await committed_ids(env.engine, probe) == [1]
    assert (await env.batch(root.id))["snap_seq"] == 1


async def test_overlapping_tree_rates_and_missing_hook_are_safe(
    env: Env, registry: HookRegistry
) -> None:
    clock = ManualClock()

    @registry.on_progress("tree", every=timedelta(seconds=1))
    async def save(_session: AsyncSession, _summary: BatchSummary) -> None:
        await asyncio.sleep(0)

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="tree"))
        _ = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="child", kind="tree")
        )

    assert await snapshotter(env, registry, clock).tick() == 2

    observer = ExplodingObserver()
    empty_registry = type(registry)()
    missing = Snapshotter(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock,
        hooks=empty_registry,
        observer=observer,
    )
    assert await missing.tick() == 0
    assert await missing.tick() == 0
    assert observer.missing_calls == 2


def test_settings_validation() -> None:
    with pytest.raises(ConfigurationError):
        _ = SnapshotterSettings(batch_size=0)
    with pytest.raises(ConfigurationError):
        _ = SnapshotterSettings(hook_timeout=timedelta(0))
