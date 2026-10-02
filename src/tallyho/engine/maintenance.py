"""Leader-elected maintenance loops and progress watching (UC-13, UC-15)."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final, Protocol, cast, runtime_checkable

from sqlalchemy import func, literal, select
from sqlalchemy.exc import DBAPIError

from tallyho.model.errors import ConfigurationError
from tallyho.protocols.clock import SystemClock

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.engine.reads import Reads
    from tallyho.engine.sweeper import SweepResult
    from tallyho.model.views import BatchView
    from tallyho.protocols.clock import Clock

__all__ = [
    "Maintenance",
    "MaintenanceResult",
    "MaintenanceSettings",
    "ProgressNotifier",
    "ProgressWatcher",
    "run_maintenance_once",
]

_log = logging.getLogger(__name__)

_CHANNEL: Final = "th_progress"
_LOCK_PERSON: Final = b"tallyho.maint"
_POSITIVE_INTERVALS: Final = "maintenance intervals must be positive"
_LISTENER_UNSUPPORTED: Final = "PostgreSQL driver does not expose LISTEN notifications"


class _Relay(Protocol):
    def start(self, *, scan_now: bool = False) -> None: ...

    async def stop(self) -> None: ...

    async def scan_once(self) -> int: ...


class _Sweeper(Protocol):
    async def sweep(self) -> SweepResult: ...


class _Snapshotter(Protocol):
    async def tick(self) -> int: ...


class _DeadLetters(Protocol):
    async def reconcile_once(self) -> int: ...


@runtime_checkable
class _AsyncpgListener(Protocol):
    async def add_listener(
        self,
        channel: str,
        callback: Callable[[object, int, str, str], None],
    ) -> None: ...

    async def remove_listener(
        self,
        channel: str,
        callback: Callable[[object, int, str, str], None],
    ) -> None: ...


class _Notify(Protocol):
    payload: str


@runtime_checkable
class _PsycopgListener(Protocol):
    def notifies(
        self,
        *,
        timeout: float | None = None,
        stop_after: int | None = None,
    ) -> AsyncIterator[_Notify]: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class MaintenanceSettings:
    """Cadence for leader recovery work and progress delivery."""

    sweep_interval: timedelta = timedelta(seconds=5)
    snapshot_tick: timedelta = timedelta(milliseconds=500)
    watch_throttle: timedelta = timedelta(milliseconds=500)

    def __post_init__(self) -> None:
        """Validate positive loop intervals.

        Raises:
            ConfigurationError: An interval is zero or negative.
        """
        if min(self.sweep_interval, self.snapshot_tick, self.watch_throttle) <= timedelta(0):
            raise ConfigurationError(_POSITIVE_INTERVALS)


@dataclass(frozen=True, slots=True)
class MaintenanceResult:
    """Results of one deterministic maintenance pass."""

    relayed: int
    swept: SweepResult
    snapshots: int
    dead_letters: int = 0
    """Items finished by the broker DLQ reconciliation (0 without a broker adapter)."""


def _lock_key(identity: str) -> int:
    digest = hashlib.blake2b(identity.encode(), digest_size=8, person=_LOCK_PERSON).digest()
    return int.from_bytes(digest, "big", signed=True)


def _installation_identity(engine: AsyncEngine) -> str:
    options = engine.get_execution_options()
    mapping: object = options.get("schema_translate_map")
    schema = "public"
    if isinstance(mapping, Mapping):
        typed_mapping = cast("Mapping[object, object]", mapping)
        translated = typed_mapping.get(None)
        if isinstance(translated, str):
            schema = translated
    return f"{schema}:maintenance"


@dataclass(eq=False, kw_only=True)
class Maintenance:
    """Run recovery services only while this process owns the installation lock.

    The relay is not leader-bound (ARCHITECTURE §3.2): while :meth:`run` works,
    the relay loop of this process is kept running, leader or not. ``relay`` is
    ``None`` in a process without a broker adapter; such a process never claims
    the outbox.

    The broker DLQ reconciliation (``dead_letters``, UC-15) needs the adapter
    too and is therefore not leader-bound either: the relay loop runs it after
    every safety scan, and :meth:`run_once` runs it explicitly. It is ``None``
    in a process without a broker adapter.
    """

    engine: AsyncEngine
    sweeper: _Sweeper
    snapshotter: _Snapshotter
    relay: _Relay | None = None
    dead_letters: _DeadLetters | None = None
    settings: MaintenanceSettings = field(default_factory=MaintenanceSettings)
    lock_identity: str | None = None
    _stop: asyncio.Event = field(init=False, default_factory=asyncio.Event)
    _leader: bool = field(init=False, default=False)
    _leader_backend_pid: int | None = field(init=False, default=None)

    @property
    def is_leader(self) -> bool:
        """Whether the current run owns its session-level advisory lock."""
        return self._leader

    @property
    def leader_backend_pid(self) -> int | None:
        """PostgreSQL backend holding leadership, for diagnostics and chaos tests."""
        return self._leader_backend_pid

    async def run_once(self) -> MaintenanceResult:
        """Run relay scan, DLQ reconciliation, all sweeper passes, and one snapshot tick once.

        Returns:
            Counts and sweep details from the pass.
        """
        return await run_maintenance_once(self)

    async def run(self) -> None:
        """Compete for leadership and run until :meth:`stop` is requested.

        The advisory lock is session scoped and held by ``leader_conn`` only.
        A ping before every tick makes a broken backend immediately revoke local
        leadership; the outer loop then obtains a fresh connection and competes
        again.

        The relay loop of this process is started with an immediate safety scan
        and stopped on exit; it does not depend on leadership.

        Raises:
            asyncio.CancelledError: The owner cancels the maintenance task.
        """
        self._stop.clear()
        retry = self.settings.snapshot_tick.total_seconds()
        relay = self.relay
        if relay is not None:
            relay.start(scan_now=True)
        try:
            while not self._stop.is_set():
                try:
                    await self._compete(retry)
                except asyncio.CancelledError:
                    raise
                except Exception:  # ruff: ignore[blind-except]  # connection loss relinquishes leadership and retries
                    self._leader = False
                    _log.exception("maintenance leader connection failed")
                    await self._wait(retry)
        finally:
            if relay is not None:
                await relay.stop()

    async def _compete(self, retry: float) -> None:
        async with self.engine.connect() as leader_conn:
            lock_key = self._lock_value()
            if not await self._acquire(leader_conn, lock_key):
                await self._wait(retry)
                return
            self._leader = True
            try:
                self._leader_backend_pid = int(
                    await leader_conn.scalar(select(func.pg_backend_pid())) or 0
                )
                await self._leader_loop(leader_conn)
            except asyncio.CancelledError:
                raise
            except DBAPIError:
                await leader_conn.invalidate()
                raise
            finally:
                self._leader = False
                self._leader_backend_pid = None
                if not leader_conn.invalidated:
                    try:
                        _ = await leader_conn.scalar(select(func.pg_advisory_unlock(lock_key)))
                    except Exception:
                        await leader_conn.invalidate()
                        raise

    def stop(self) -> None:
        """Request graceful loop termination and wake a sleeping contender."""
        self._stop.set()

    def _lock_value(self) -> int:
        identity = self.lock_identity or _installation_identity(self.engine)
        return _lock_key(identity)

    @staticmethod
    async def _acquire(conn: AsyncConnection, lock_key: int) -> bool:
        acquired = await conn.scalar(select(func.pg_try_advisory_lock(lock_key)))
        return bool(acquired)

    async def _leader_loop(self, conn: AsyncConnection) -> None:
        sweep_every = self.settings.sweep_interval.total_seconds()
        tick_every = self.settings.snapshot_tick.total_seconds()
        next_sweep = 0.0
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            _ = await conn.scalar(select(literal(1)))
            now = loop.time()
            if now >= next_sweep:
                _ = await self.sweeper.sweep()
                next_sweep = now + sweep_every
            _ = await self.snapshotter.tick()
            await self._wait(tick_every)

    async def _wait(self, seconds: float) -> None:
        if self._stop.is_set():
            return
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(seconds):
                await self._stop.wait()


async def run_maintenance_once(maintenance: Maintenance) -> MaintenanceResult:
    """Run one complete pass without leader election, primarily for deterministic tests.

    A process without a broker adapter has no relay and no DLQ reconciliation:
    the outbox is left untouched and the broker DLQ is not read.

    Returns:
        Counts and sweep details from the pass.
    """
    relay = maintenance.relay
    relayed = 0 if relay is None else await relay.scan_once()
    dead_letters = maintenance.dead_letters
    reconciled = 0 if dead_letters is None else await dead_letters.reconcile_once()
    swept = await maintenance.sweeper.sweep()
    snapshots = await maintenance.snapshotter.tick()
    return MaintenanceResult(
        relayed=relayed, swept=swept, snapshots=snapshots, dead_letters=reconciled
    )


@dataclass(eq=False, kw_only=True)
class ProgressNotifier:
    """Publish transaction-aware ``th_progress`` notifications with per-batch throttling."""

    engine: AsyncEngine
    throttle: timedelta = timedelta(milliseconds=500)
    clock: Clock = field(default_factory=SystemClock)
    _sent_at: dict[UUID, float] = field(default_factory=dict, init=False)
    _final: set[UUID] = field(default_factory=set, init=False)
    _guard: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        """Validate a positive throttle interval.

        Raises:
            ConfigurationError: The interval is zero or negative.
        """
        if self.throttle <= timedelta(0):
            raise ConfigurationError(_POSITIVE_INTERVALS)

    async def notify(self, batch_ids: Iterable[UUID], *, final: bool = False) -> int:
        """Publish eligible ids in a short owned transaction.

        Final notifications bypass the time gate once, ensuring a terminal state
        is never hidden behind a recently published intermediate update.

        Returns:
            Number of notifications queued for commit.
        """
        async with self.engine.begin() as conn:
            return await self.notify_in(conn, batch_ids, final=final)

    async def notify_in(
        self,
        conn: AsyncConnection,
        batch_ids: Iterable[UUID],
        *,
        final: bool = False,
    ) -> int:
        """Queue notifications on ``conn``; PostgreSQL delivers them on commit.

        Returns:
            Number of notifications queued for commit.
        """
        async with self._guard:
            now = self.clock.monotonic()
            due: list[UUID] = []
            threshold = self.throttle.total_seconds()
            for batch_id in sorted(set(batch_ids)):
                if batch_id in self._final:
                    continue
                last = self._sent_at.get(batch_id)
                if final or last is None or now - last >= threshold:
                    due.append(batch_id)
                    self._sent_at[batch_id] = now
                    if final:
                        self._final.add(batch_id)
            for batch_id in due:
                _ = await conn.scalar(select(func.pg_notify(_CHANNEL, str(batch_id))))
            return len(due)


@dataclass(eq=False, kw_only=True)
class ProgressWatcher:
    """Turn PostgreSQL notifications into throttled, lossless ``BatchView`` streams."""

    engine: AsyncEngine
    reads: Reads
    throttle: timedelta = timedelta(milliseconds=500)

    def __post_init__(self) -> None:
        """Validate a positive throttle interval.

        Raises:
            ConfigurationError: The interval is zero or negative.
        """
        if self.throttle <= timedelta(0):
            raise ConfigurationError(_POSITIVE_INTERVALS)

    async def watch(self, batch_id: UUID) -> AsyncIterator[BatchView]:
        """Yield the initial view, changed updates, and exactly one terminal view.

        LISTEN is installed before the initial read. A timeout read also closes
        the delivery gap caused by a listener connection loss or an emitter that
        died immediately before publishing its notification.
        """
        output: asyncio.Queue[BatchView | None] = asyncio.Queue()
        task = asyncio.create_task(self._produce(batch_id, output))
        try:
            while (view := await output.get()) is not None:
                yield view
            await task
        finally:
            if not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _produce(
        self,
        batch_id: UUID,
        output: asyncio.Queue[BatchView | None],
    ) -> None:
        queue = asyncio.Event()

        def received(_connection: object, _pid: int, _channel: str, payload: str) -> None:
            if payload == str(batch_id):
                queue.set()

        try:
            async with self.engine.connect() as conn:
                raw = await conn.get_raw_connection()
                driver = cast("object", raw.driver_connection)
                if isinstance(driver, _AsyncpgListener):
                    await self._with_asyncpg(
                        driver,
                        received=received,
                        batch_id=batch_id,
                        queue=queue,
                        output=output,
                    )
                elif isinstance(driver, _PsycopgListener):
                    await self._with_psycopg(
                        driver,
                        conn=conn,
                        batch_id=batch_id,
                        queue=queue,
                        output=output,
                    )
                else:
                    raise ConfigurationError(_LISTENER_UNSUPPORTED)
        finally:
            await output.put(None)

    async def _with_asyncpg(
        self,
        driver: _AsyncpgListener,
        *,
        received: Callable[[object, int, str, str], None],
        batch_id: UUID,
        queue: asyncio.Event,
        output: asyncio.Queue[BatchView | None],
    ) -> None:
        await driver.add_listener(_CHANNEL, received)
        try:
            await self._poll(batch_id, queue, output)
        finally:
            await driver.remove_listener(_CHANNEL, received)

    async def _with_psycopg(
        self,
        driver: _PsycopgListener,
        *,
        conn: AsyncConnection,
        batch_id: UUID,
        queue: asyncio.Event,
        output: asyncio.Queue[BatchView | None],
    ) -> None:
        _ = await conn.exec_driver_sql(f"LISTEN {_CHANNEL}")
        await conn.commit()
        task = asyncio.create_task(self._pump_psycopg(driver, batch_id, queue))
        try:
            await self._poll(batch_id, queue, output)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    @staticmethod
    async def _pump_psycopg(
        driver: _PsycopgListener,
        batch_id: UUID,
        queue: asyncio.Event,
    ) -> None:
        async for notification in driver.notifies():
            if notification.payload == str(batch_id):
                queue.set()

    async def _poll(
        self,
        batch_id: UUID,
        queue: asyncio.Event,
        output: asyncio.Queue[BatchView | None],
    ) -> None:
        previous = await self.reads.view(batch_id)
        await output.put(previous)
        if previous.state.is_terminal:
            return
        delay = self.throttle.total_seconds()
        loop = asyncio.get_running_loop()
        last_yield = loop.time()
        while True:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(delay):
                    await queue.wait()
            remaining = delay - (loop.time() - last_yield)
            if remaining > 0:
                await asyncio.sleep(remaining)
            queue.clear()
            current = await self.reads.view(batch_id)
            if current != previous:
                await output.put(current)
                previous = current
                last_yield = loop.time()
            if current.state.is_terminal:
                return
