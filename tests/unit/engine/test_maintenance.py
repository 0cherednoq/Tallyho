"""Driver-independent branches of maintenance progress delivery."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from typing_extensions import override

from tallyho.engine.maintenance import ProgressNotifier, ProgressWatcher
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState
from tallyho.model.views import BatchView, Progress
from tallyho.protocols.clock import SystemClock

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.engine.reads import Reads

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


class PsycopgDriver:
    payloads: list[str]

    def __init__(self, payloads: list[str]) -> None:
        self.payloads = payloads

    async def notifies(
        self,
        *,
        timeout: float | None = None,  # ruff: ignore[async-function-with-timeout]  # mirrors psycopg
        stop_after: int | None = None,
    ) -> AsyncIterator[Notification]:
        del timeout, stop_after
        for payload in self.payloads:
            yield Notification(payload)
        await asyncio.Event().wait()


@dataclass(frozen=True, slots=True)
class Raw:
    driver_connection: object


class Connection:
    driver: object
    commands: list[str]
    commits: int
    notifications: list[str]

    def __init__(self, driver: object) -> None:
        self.driver = driver
        self.commands = []
        self.commits = 0
        self.notifications = []

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

    def __init__(self, driver: object) -> None:
        self.connection = Connection(driver)

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncGenerator[Connection]:
        yield self.connection

    @contextlib.asynccontextmanager
    async def begin(self) -> AsyncGenerator[Connection]:
        yield self.connection


class SequenceReads:
    values: list[BatchView]
    index: int

    def __init__(self, values: list[BatchView]) -> None:
        self.values = values
        self.index = 0

    async def view(self, _batch_id: UUID) -> BatchView:
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
    assert engine.connection.commands == ["LISTEN th_progress"]
    assert engine.connection.commits == 1


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
    stream = cast("AsyncGenerator[BatchView, None]", subject.watch(batch_id))

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
