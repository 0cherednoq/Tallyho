"""Maintenance leadership, deterministic passes, and LISTEN-based watch."""

from __future__ import annotations

import asyncio
import itertools
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho.engine.maintenance import (
    Maintenance,
    MaintenanceSettings,
    ProgressNotifier,
    ProgressWatcher,
    run_maintenance_once,
)
from tallyho.engine.producer import RootSpec
from tallyho.engine.reads import Reads
from tallyho.engine.sweeper import SweepResult
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState
from tallyho.protocols.clock import SystemClock
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.integration.engine.conftest import Env

__all__: list[str] = []


@dataclass
class RecordingRelay:
    events: list[str]
    calls: int = 0
    starts: list[bool] = field(default_factory=list[bool])
    stops: int = 0

    def start(self, *, scan_now: bool = False) -> None:
        self.starts.append(scan_now)

    async def stop(self) -> None:
        await asyncio.sleep(0)
        self.stops += 1

    async def scan_once(self) -> int:
        self.events.append("relay")
        self.calls += 1
        return 2


@dataclass
class RecordingSweeper:
    events: list[str]
    calls: int = 0

    async def sweep(self) -> SweepResult:
        self.events.append("sweeper")
        self.calls += 1
        return SweepResult(leases=3)


@dataclass
class RecordingSnapshotter:
    events: list[str]
    calls: int = 0

    async def tick(self) -> int:
        self.events.append("snapshotter")
        self.calls += 1
        return 4


@dataclass
class FlakySweeper:
    subject: Maintenance | None = None
    calls: int = 0

    async def sweep(self) -> SweepResult:
        self.calls += 1
        if self.calls == 1:
            message = "synthetic sweeper failure"
            raise RuntimeError(message)
        assert self.subject is not None
        self.subject.stop()
        return SweepResult()


@dataclass(frozen=True, slots=True)
class Services:
    relay: RecordingRelay
    sweeper: RecordingSweeper
    snapshotter: RecordingSnapshotter

    @property
    def calls(self) -> int:
        """Сколько раз работали сервисы лидера; relay к лидерству не привязан."""
        return self.sweeper.calls + self.snapshotter.calls


def services() -> Services:
    events: list[str] = []
    return Services(
        relay=RecordingRelay(events),
        sweeper=RecordingSweeper(events),
        snapshotter=RecordingSnapshotter(events),
    )


def maintenance(env: Env, owned: Services, *, identity: str | None) -> Maintenance:
    return Maintenance(
        engine=schema_engine(env),
        relay=owned.relay,
        sweeper=owned.sweeper,
        snapshotter=owned.snapshotter,
        settings=MaintenanceSettings(
            sweep_interval=timedelta(milliseconds=250),
            snapshot_tick=timedelta(milliseconds=50),
        ),
        lock_identity=identity,
    )


async def eventually(
    condition: Callable[[], bool],
    *,
    deadline: float = 2.0,
) -> None:
    async with asyncio.timeout(deadline):
        for _ in itertools.count():
            if condition():
                return
            await asyncio.sleep(0.01)


async def stop_all(*pairs: tuple[Maintenance, asyncio.Task[None]]) -> None:
    for subject, _task in pairs:
        subject.stop()
    await asyncio.gather(*(task for _subject, task in pairs))


async def test_run_once_has_deterministic_complete_order(env: Env) -> None:
    owned = services()
    subject = maintenance(env, owned, identity="once")

    result = await run_maintenance_once(subject)

    assert result.relayed == 2
    assert result.swept.leases == 3
    assert result.snapshots == 4
    assert owned.relay.events == ["relay", "sweeper", "snapshotter"]


async def test_exactly_one_leader_and_second_takes_over_after_backend_loss(env: Env) -> None:
    first_services, second_services = services(), services()
    identity = f"{env.schema}:failover"
    first = maintenance(env, first_services, identity=identity)
    second = maintenance(env, second_services, identity=identity)
    first_task = asyncio.create_task(first.run())
    second_task = asyncio.create_task(second.run())
    pairs = ((first, first_task), (second, second_task))
    try:
        await eventually(lambda: first.is_leader != second.is_leader)
        leader, loser = (first, second) if first.is_leader else (second, first)
        leader_services, loser_services = (
            (first_services, second_services)
            if first.is_leader
            else (second_services, first_services)
        )
        await eventually(lambda: leader_services.calls > 0)
        assert loser_services.calls == 0
        # Relay запущен в обоих процессах: страховочный scan не ждёт лидерства.
        assert first_services.relay.starts == [True]
        assert second_services.relay.starts == [True]
        assert first_services.relay.calls == second_services.relay.calls == 0
        backend_pid = leader.leader_backend_pid
        assert backend_pid is not None

        started = monotonic()
        leader.stop()
        async with env.engine.begin() as conn:
            assert await conn.scalar(select(func.pg_terminate_backend(backend_pid)))
        await eventually(lambda: loser.is_leader)

        assert monotonic() - started <= 2 * first.settings.sweep_interval.total_seconds()
        await eventually(lambda: loser_services.calls > 0)
    finally:
        await stop_all(*pairs)
        # asyncpg may leave the server-terminated physical connection in the
        # SQLAlchemy pool until the next checkout; discard that chaos artifact.
        await env.engine.dispose()


async def test_graceful_stop_unlocks_pooled_leader_connection(env: Env) -> None:
    first_services, second_services = services(), services()
    first = maintenance(env, first_services, identity=None)
    first_task = asyncio.create_task(first.run())
    await eventually(lambda: first.is_leader)
    await eventually(lambda: first_services.snapshotter.calls >= 3)
    assert first_services.sweeper.calls == 1
    assert first_services.relay.starts == [True]
    assert first_services.relay.stops == 0
    first.stop()
    await first_task
    assert first_services.relay.stops == 1

    second = maintenance(env, second_services, identity=None)
    second_task = asyncio.create_task(second.run())
    try:
        await eventually(lambda: second.is_leader)
        await eventually(lambda: second_services.calls > 0)
    finally:
        await stop_all((second, second_task))


async def test_service_failure_relinquishes_and_retries_leadership(env: Env) -> None:
    events: list[str] = []
    sweeper = FlakySweeper()
    relay = RecordingRelay(events)
    subject = Maintenance(
        engine=schema_engine(env),
        relay=relay,
        sweeper=sweeper,
        snapshotter=RecordingSnapshotter(events),
        settings=MaintenanceSettings(
            sweep_interval=timedelta(milliseconds=100),
            snapshot_tick=timedelta(milliseconds=20),
        ),
        lock_identity=f"{env.schema}:flaky",
    )
    sweeper.subject = subject

    await subject.run()

    assert sweeper.calls == 2
    assert relay.starts == [True]
    assert relay.stops == 1


async def test_cancelled_run_still_stops_relay(env: Env) -> None:
    owned = services()
    subject = maintenance(env, owned, identity=f"{env.schema}:cancelled")
    task = asyncio.create_task(subject.run())
    await eventually(lambda: subject.is_leader)

    _ = task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert owned.relay.stops == 1
    assert not subject.is_leader


async def test_process_without_broker_runs_leader_services_only(env: Env) -> None:
    events: list[str] = []
    sweeper = RecordingSweeper(events)
    snapshotter = RecordingSnapshotter(events)
    subject = Maintenance(
        engine=schema_engine(env),
        sweeper=sweeper,
        snapshotter=snapshotter,
        settings=MaintenanceSettings(
            sweep_interval=timedelta(milliseconds=100),
            snapshot_tick=timedelta(milliseconds=20),
        ),
        lock_identity=f"{env.schema}:no-broker",
    )

    result = await run_maintenance_once(subject)
    assert result.relayed == 0
    assert events == ["sweeper", "snapshotter"]

    task = asyncio.create_task(subject.run())
    try:
        await eventually(lambda: sweeper.calls >= 2 and snapshotter.calls >= 3)
    finally:
        await stop_all((subject, task))


async def test_watch_is_throttled_and_never_loses_final_state(env: Env) -> None:
    engine = schema_engine(env)
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="watch"))
    notifier = ProgressNotifier(engine=engine, throttle=timedelta(seconds=1))
    watcher = ProgressWatcher(
        engine=engine,
        reads=Reads(engine, env.tables, SystemClock()),
        throttle=timedelta(milliseconds=80),
    )
    stream = watcher.watch(root.id)
    moments: list[float] = []

    initial = await anext(stream)
    moments.append(monotonic())
    assert initial.state is BatchState.OPEN

    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(expected_total=1)
        )
    assert await notifier.notify([root.id]) == 1
    assert await notifier.notify([root.id]) == 0
    changed = await anext(stream)
    moments.append(monotonic())
    assert changed.progress.expected == 1

    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(state=int(BatchState.SUCCEEDED))
        )
    assert await notifier.notify([root.id], final=True) == 1
    final = await anext(stream)
    moments.append(monotonic())

    assert final.state is BatchState.SUCCEEDED
    assert final.progress.final
    assert all(right - left >= 0.06 for left, right in itertools.pairwise(moments))
    with pytest.raises(StopAsyncIteration):
        _ = await anext(stream)


@pytest.mark.skipif(sys.platform == "win32", reason="psycopg async requires selector loop")
async def test_watch_supports_psycopg_listener(env: Env, postgres_dsn: str) -> None:
    url = make_url(postgres_dsn).set(drivername="postgresql+psycopg")
    engine = create_async_engine(url).execution_options(schema_translate_map={None: env.schema})
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="psycopg-watch"))
    watcher = ProgressWatcher(
        engine=engine,
        reads=Reads(engine, env.tables, SystemClock()),
        throttle=timedelta(milliseconds=50),
    )
    notifier = ProgressNotifier(engine=engine, throttle=timedelta(seconds=1))
    stream = watcher.watch(root.id)
    try:
        assert (await anext(stream)).state is BatchState.OPEN
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(env.tables.batch)
                .where(env.tables.batch.c.id == root.id)
                .values(state=int(BatchState.SUCCEEDED))
            )
        assert await notifier.notify([root.id], final=True) == 1
        assert (await anext(stream)).state is BatchState.SUCCEEDED
        with pytest.raises(StopAsyncIteration):
            _ = await anext(stream)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("field", ["sweep_interval", "snapshot_tick", "watch_throttle"])
def test_settings_reject_non_positive_intervals(field: str) -> None:
    values = {
        "sweep_interval": timedelta(seconds=1),
        "snapshot_tick": timedelta(seconds=1),
        "watch_throttle": timedelta(seconds=1),
    }
    values[field] = timedelta(0)
    with pytest.raises(ConfigurationError):
        _ = MaintenanceSettings(**values)
