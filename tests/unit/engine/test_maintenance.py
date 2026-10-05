"""Driver-independent branches of maintenance progress delivery."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast
from uuid import uuid4

import pytest
from typing_extensions import override

from tallyho.engine.maintenance import ProgressNotifier, ProgressWatcher
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState
from tallyho.model.views import BatchView, Progress
from tallyho.protocols.clock import SystemClock

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.engine.reads import Reads
    from tallyho.model.progress import RateTracker

__all__: list[str] = []


def view(batch_id: UUID, state: BatchState) -> BatchView:
    return BatchView(
        id=batch_id,
        kind="test",
        key=None,
        state=state,
        progress=Progress(final=state.is_terminal),
        labels={},
        metrics={},
        children={},
    )


@dataclass
class ManualClock(SystemClock):
    value: float = 0.0

    @override
    def monotonic(self) -> float:
        return self.value


@dataclass(frozen=True, slots=True)
class Notification:
    payload: str


class ListenerError(Exception):
    """Сбой LISTEN/UNLISTEN или чтения уведомлений в фейковом драйвере."""


class PsycopgDriver:
    payloads: list[str]
    broken: bool

    def __init__(self, payloads: list[str], *, broken: bool = False) -> None:
        self.payloads = payloads
        self.broken = broken

    async def notifies(
        self,
        *,
        timeout: float | None = None,  # ruff: ignore[async-function-with-timeout]  # mirrors psycopg
        stop_after: int | None = None,
    ) -> AsyncIterator[Notification]:
        del timeout, stop_after
        for payload in self.payloads:
            yield Notification(payload)
        if self.broken:
            raise ListenerError
        await asyncio.Event().wait()


class AsyncpgDriver:
    """asyncpg: LISTEN и UNLISTEN идут по сети, их можно прервать посреди запроса."""

    events: list[str]
    delay: float
    fail: str | None
    sent: dict[str, asyncio.Event]

    def __init__(self, *, delay: float = 0.0, fail: str | None = None) -> None:
        self.events = []
        self.delay = delay
        self.fail = fail
        self.sent = {"LISTEN": asyncio.Event(), "UNLISTEN": asyncio.Event()}

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[object, int, str, str], None],
    ) -> None:
        del callback
        await self._roundtrip("LISTEN", channel)

    async def remove_listener(
        self,
        channel: str,
        callback: Callable[[object, int, str, str], None],
    ) -> None:
        del callback
        await self._roundtrip("UNLISTEN", channel)

    async def _roundtrip(self, command: str, channel: str) -> None:
        statement = f"{command} {channel}"
        self.events.append(f"sent {statement}")
        self.sent[command].set()
        await asyncio.sleep(self.delay)
        if command == self.fail:
            raise ListenerError
        self.events.append(statement)


@dataclass(frozen=True, slots=True)
class Raw:
    driver_connection: object


class Connection:
    driver: object
    commands: list[str]
    commits: int
    notifications: list[str]

    invalidated: bool

    def __init__(self, driver: object) -> None:
        self.driver = driver
        self.commands = []
        self.commits = 0
        self.notifications = []
        self.invalidated = False

    async def invalidate(self) -> None:
        self.invalidated = True

    async def get_raw_connection(self) -> Raw:
        return Raw(self.driver)

    async def exec_driver_sql(self, statement: str) -> object:
        self.commands.append(statement)
        return object()

    async def commit(self) -> None:
        self.commits += 1

    async def scalar(self, statement: object) -> object:
        self.notifications.append(str(statement))
        return None


class Engine:
    connection: Connection
    begins: int

    def __init__(self, driver: object) -> None:
        self.connection = Connection(driver)
        self.begins = 0

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncGenerator[Connection]:
        yield self.connection

    @contextlib.asynccontextmanager
    async def begin(self) -> AsyncGenerator[Connection]:
        self.begins += 1
        yield self.connection


class SequenceReads:
    values: list[BatchView]
    index: int

    def __init__(self, values: list[BatchView]) -> None:
        self.values = values
        self.index = 0

    async def view(self, _batch_id: UUID, *, rates: RateTracker | None = None) -> BatchView:
        # watch() ведёт скорость по своему потоку: трекер передаётся в каждое чтение.
        assert rates is not None
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        return value


def watcher(engine: Engine, reads: SequenceReads, *, throttle: float = 0.001) -> ProgressWatcher:
    return ProgressWatcher(
        engine=cast("AsyncEngine", cast("object", engine)),
        reads=cast("Reads", cast("object", reads)),
        throttle=timedelta(seconds=throttle),
    )


async def test_psycopg_notifications_drive_watch_and_cleanup_pump() -> None:
    batch_id = uuid4()
    driver = PsycopgDriver([str(uuid4()), str(batch_id)])
    engine = Engine(driver)
    subject = watcher(
        engine,
        SequenceReads([view(batch_id, BatchState.OPEN), view(batch_id, BatchState.SUCCEEDED)]),
    )

    assert [item.state async for item in subject.watch(batch_id)] == [
        BatchState.OPEN,
        BatchState.SUCCEEDED,
    ]
    # psycopg и пул SQLAlchemy подписку не снимают: UNLISTEN — забота watch (Fix-17).
    assert engine.connection.commands == ["LISTEN th_progress", "UNLISTEN th_progress"]
    assert engine.connection.commits == 2
    assert not engine.connection.invalidated


async def test_unsupported_listener_is_explicit_error() -> None:
    batch_id = uuid4()
    subject = watcher(Engine(object()), SequenceReads([view(batch_id, BatchState.OPEN)]))

    with pytest.raises(ConfigurationError):
        _ = await anext(subject.watch(batch_id))


async def test_closing_watch_cancels_and_awaits_listener_task() -> None:
    batch_id = uuid4()
    subject = watcher(
        Engine(PsycopgDriver([])),
        SequenceReads([view(batch_id, BatchState.OPEN)]),
        throttle=60,
    )
    stream = subject.watch(batch_id)

    assert (await anext(stream)).state is BatchState.OPEN
    await stream.aclose()


async def test_watch_skips_unchanged_poll_and_finishes_without_notification() -> None:
    batch_id = uuid4()
    opened = view(batch_id, BatchState.OPEN)
    subject = watcher(
        Engine(PsycopgDriver([])),
        SequenceReads([opened, opened, view(batch_id, BatchState.SUCCEEDED)]),
    )

    assert [item.state async for item in subject.watch(batch_id)] == [
        BatchState.OPEN,
        BatchState.SUCCEEDED,
    ]


async def test_watch_does_not_emit_eta_only_changes() -> None:
    batch_id = uuid4()
    child = view(uuid4(), BatchState.OPEN)
    opened = replace(view(batch_id, BatchState.OPEN), children={"stage": child})
    drifted = replace(
        opened,
        progress=replace(opened.progress, eta=timedelta(seconds=5)),
        children={"stage": replace(child, progress=Progress(eta=timedelta(seconds=7)))},
    )
    subject = watcher(
        Engine(PsycopgDriver([])),
        SequenceReads([opened, drifted, view(batch_id, BatchState.SUCCEEDED)]),
    )

    # ETA при простое растёт с каждым чтением; без других изменений это не обновление.
    assert [item.state async for item in subject.watch(batch_id)] == [
        BatchState.OPEN,
        BatchState.SUCCEEDED,
    ]


async def test_terminal_initial_view_does_not_wait_for_notifications() -> None:
    batch_id = uuid4()
    subject = watcher(
        Engine(PsycopgDriver([])),
        SequenceReads([view(batch_id, BatchState.CANCELLED)]),
    )

    assert [item.state async for item in subject.watch(batch_id)] == [BatchState.CANCELLED]


async def test_notifier_throttles_reopens_and_sends_final_once() -> None:
    clock = ManualClock()
    engine = Engine(object())
    subject = ProgressNotifier(
        engine=cast("AsyncEngine", cast("object", engine)),
        throttle=timedelta(seconds=1),
        clock=clock,
    )
    first, second = uuid4(), uuid4()

    assert await subject.notify([first, first, second]) == 2
    assert await subject.notify([first]) == 0
    clock.value = 1.0
    assert await subject.notify([first]) == 1
    assert await subject.notify([first], final=True) == 1
    assert await subject.notify([first], final=True) == 0
    assert len(engine.connection.notifications) == 4


async def test_notifier_opens_no_transaction_when_everything_is_throttled() -> None:
    # Completer зовёт notify после каждой групповой транзакции: пустой BEGIN/COMMIT
    # на каждый flush — лишний round-trip (T11.6, Perf-1).
    clock = ManualClock()
    engine = Engine(object())
    subject = ProgressNotifier(
        engine=cast("AsyncEngine", cast("object", engine)),
        throttle=timedelta(seconds=1),
        clock=clock,
    )
    batch_id = uuid4()

    assert await subject.notify([batch_id]) == 1
    assert engine.begins == 1
    assert await subject.notify([batch_id]) == 0
    assert await subject.notify([]) == 0
    assert engine.begins == 1
    clock.value = 1.0
    assert await subject.notify([batch_id]) == 1
    assert engine.begins == 2


@pytest.mark.parametrize("kind", ["notifier", "watcher"])
def test_progress_services_reject_non_positive_throttle(kind: str) -> None:
    engine = cast("AsyncEngine", cast("object", Engine(object())))
    with pytest.raises(ConfigurationError):
        _ = invalid_service(kind, engine)


def invalid_service(kind: str, engine: AsyncEngine) -> object:
    if kind == "notifier":
        return ProgressNotifier(engine=engine, throttle=timedelta(0))
    reads = cast("Reads", cast("object", SequenceReads([])))
    return ProgressWatcher(engine=engine, reads=reads, throttle=timedelta(0))


LISTENED: Final = ["sent LISTEN th_progress", "LISTEN th_progress"]
UNLISTENED: Final = ["sent UNLISTEN th_progress", "UNLISTEN th_progress"]


async def test_asyncpg_listener_is_removed_after_terminal_view() -> None:
    batch_id = uuid4()
    driver = AsyncpgDriver()
    engine = Engine(driver)
    subject = watcher(engine, SequenceReads([view(batch_id, BatchState.SUCCEEDED)]))

    assert [item.state async for item in subject.watch(batch_id)] == [BatchState.SUCCEEDED]
    assert driver.events == LISTENED + UNLISTENED
    assert not engine.connection.invalidated


async def test_closing_watch_during_unlisten_waits_for_it() -> None:
    """Fix-17: ``wait()`` выходит из цикла сразу, поток закрывают, пока идёт UNLISTEN."""
    batch_id = uuid4()
    driver = AsyncpgDriver(delay=0.02)
    engine = Engine(driver)
    subject = watcher(engine, SequenceReads([view(batch_id, BatchState.SUCCEEDED)]))
    stream = subject.watch(batch_id)

    assert (await anext(stream)).state is BatchState.SUCCEEDED
    _ = await driver.sent["UNLISTEN"].wait()
    await stream.aclose()

    assert driver.events == LISTENED + UNLISTENED
    assert not engine.connection.invalidated


@pytest.mark.parametrize("cancels", [1, 2])
async def test_cancelled_consumer_finishes_listen_and_unlisten(cancels: int) -> None:
    """Отмена посреди LISTEN (в том числе повторная) не оставляет подписку на соединении."""
    batch_id = uuid4()
    driver = AsyncpgDriver(delay=0.02)
    engine = Engine(driver)
    subject = watcher(engine, SequenceReads([view(batch_id, BatchState.OPEN)]), throttle=60)

    consumer = asyncio.create_task(anext(subject.watch(batch_id)))
    _ = await driver.sent["LISTEN"].wait()
    for _ in range(cancels):
        _ = consumer.cancel()
        await asyncio.sleep(0.005)
    with pytest.raises(asyncio.CancelledError):
        _ = await consumer

    assert driver.events == LISTENED + UNLISTENED
    assert not engine.connection.invalidated


@pytest.mark.parametrize(
    ("fail", "expected"),
    [
        ("LISTEN", ["sent LISTEN th_progress"]),
        ("UNLISTEN", [*LISTENED, "sent UNLISTEN th_progress"]),
    ],
)
async def test_failed_listener_step_invalidates_connection(fail: str, expected: list[str]) -> None:
    batch_id = uuid4()
    driver = AsyncpgDriver(fail=fail)
    engine = Engine(driver)
    subject = watcher(engine, SequenceReads([view(batch_id, BatchState.SUCCEEDED)]))

    with pytest.raises(ListenerError):
        _ = [item async for item in subject.watch(batch_id)]

    assert driver.events == expected
    assert engine.connection.invalidated


async def test_psycopg_pump_failure_invalidates_connection() -> None:
    batch_id = uuid4()
    engine = Engine(PsycopgDriver([], broken=True))
    subject = watcher(engine, SequenceReads([view(batch_id, BatchState.OPEN)]), throttle=60)
    stream = subject.watch(batch_id)

    assert (await anext(stream)).state is BatchState.OPEN
    await asyncio.sleep(0.01)
    with pytest.raises(ListenerError):
        await stream.aclose()

    assert engine.connection.commands == ["LISTEN th_progress"]
    assert engine.connection.invalidated
