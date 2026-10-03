"""Закрытие установки без БД: срок, чужие event loop, отмена опоздавших (ARCHITECTURE §11.1)."""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, NoReturn

import pytest
from typing_extensions import override

from tallyho.engine.shutdown import Budget, close_services, drain_tasks, run_in
from tallyho.model.errors import CompleterError
from tallyho.testing.clock import FakeClock
from tests.helpers.loops import LoopThread

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterator

__all__: list[str] = []

START = datetime(2026, 10, 2, 12, tzinfo=UTC)
PATIENCE = 5.0
MESSAGE = "закрытие сорвалось"


def budget(seconds: float = 5.0) -> Budget:
    return Budget.start(FakeClock(START), timedelta(seconds=seconds))


@pytest.fixture
def foreign() -> Iterator[LoopThread]:
    value = LoopThread()
    try:
        yield value
    finally:
        value.close()


@dataclass
class Probe:
    """Корутина, которая запоминает, в каком loop её выполнили."""

    loops: list[asyncio.AbstractEventLoop] = field(default_factory=list[asyncio.AbstractEventLoop])

    async def run(self) -> None:
        await asyncio.sleep(0)
        self.loops.append(asyncio.get_running_loop())


@dataclass
class FakeCompleter:
    """Completer, чьё закрытие можно задержать или уронить."""

    loop: asyncio.AbstractEventLoop | None = None
    delay: float = 0.0
    error: Exception | None = None
    calls: list[str] = field(default_factory=list[str])

    async def close(self, *, requeue_held: bool = False) -> None:
        self.calls.append(f"close(requeue_held={requeue_held})")
        await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        self.calls.append("closed")

    async def abort(self) -> None:
        await asyncio.sleep(0)
        self.calls.append("abort")


@dataclass
class FakeRelay:
    """Relay, записывающий порядок закрытия относительно Completer."""

    completer: FakeCompleter
    loop: asyncio.AbstractEventLoop | None = None
    seen: list[tuple[list[str], float | None]] = field(
        default_factory=list[tuple[list[str], float | None]]
    )

    async def close(self, *, grace: float | None = None) -> None:
        await asyncio.sleep(0)
        self.seen.append((list(self.completer.calls), grace))


def test_budget_counts_down_on_the_installation_clock() -> None:
    clock = FakeClock(START)
    value = Budget.start(clock, timedelta(seconds=10))

    assert value.left == pytest.approx(10.0)
    _ = clock.advance(seconds=4)
    assert value.left == pytest.approx(6.0)
    _ = clock.advance(seconds=60)
    assert value.left == pytest.approx(0.0)


async def test_run_in_own_or_unbound_loop_awaits_directly() -> None:
    probe = Probe()

    await run_in(None, probe.run, patience=PATIENCE)
    await run_in(asyncio.get_running_loop(), probe.run, patience=PATIENCE)

    assert probe.loops == [asyncio.get_running_loop()] * 2


async def test_run_in_running_foreign_loop_executes_there(foreign: LoopThread) -> None:
    probe = Probe()

    await run_in(foreign.loop, probe.run, patience=PATIENCE)

    assert probe.loops == [foreign.loop]


async def test_run_in_stopped_foreign_loop_drives_it(foreign: LoopThread) -> None:
    # flexiq останавливает loop исполнителя, не закрывая: закрытие докручивает его само.
    foreign.stop()
    probe = Probe()

    await run_in(foreign.loop, probe.run, patience=PATIENCE)

    assert probe.loops == [foreign.loop]
    assert not foreign.loop.is_running()


async def test_run_in_closed_loop_is_skipped_with_warning(
    foreign: LoopThread, caplog: pytest.LogCaptureFixture
) -> None:
    foreign.close()
    probe = Probe()

    with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
        await run_in(foreign.loop, probe.run, patience=PATIENCE)

    assert probe.loops == []
    assert "уже закрыт" in caplog.text


async def test_run_in_gives_up_on_unresponsive_foreign_loop(
    foreign: LoopThread, caplog: pytest.LogCaptureFixture
) -> None:
    release = threading.Event()
    # Loop «работает», но занят синхронным кодом и до нашей корутины не дойдёт.
    _ = foreign.loop.call_soon_threadsafe(release.wait)
    made: list[str] = []

    def make() -> Coroutine[object, object, None]:
        made.append("made")
        return asyncio.sleep(0)

    try:
        with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
            await run_in(foreign.loop, make, patience=0.05)
    finally:
        release.set()
    # Loop освободился и разобрал очередь: опоздавшую корутину он так и не создал.
    await foreign.run(asyncio.sleep(0))

    assert made == []
    assert "не ответил" in caplog.text


async def test_run_in_cancels_started_coroutine_it_gave_up_on(
    foreign: LoopThread, caplog: pytest.LogCaptureFixture
) -> None:
    cancelled = threading.Event()

    async def stuck() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    # Срок с запасом: свободный loop успевает начать корутину, она не завершается.
    with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
        await run_in(foreign.loop, stuck, patience=0.5)

    assert await asyncio.to_thread(cancelled.wait, PATIENCE)
    assert "не ответил" in caplog.text


async def test_run_in_raises_what_foreign_coroutine_raised(foreign: LoopThread) -> None:
    async def failing() -> None:
        await asyncio.sleep(0)
        raise CompleterError(MESSAGE)

    with pytest.raises(CompleterError, match=MESSAGE):
        await run_in(foreign.loop, failing, patience=PATIENCE)


async def test_run_in_raises_when_factory_fails_in_foreign_loop(foreign: LoopThread) -> None:
    def make() -> Coroutine[object, object, None]:
        raise CompleterError(MESSAGE)

    foreign.loop.set_exception_handler(lambda _loop, _context: None)

    with pytest.raises(CompleterError, match=MESSAGE):
        await run_in(foreign.loop, make, patience=PATIENCE)


async def test_run_in_tolerates_foreign_loop_closed_while_waiting(
    foreign: LoopThread, caplog: pytest.LogCaptureFixture
) -> None:
    started = threading.Event()

    async def stuck() -> None:
        started.set()
        await asyncio.Event().wait()

    async def close_when_started() -> None:
        assert await asyncio.to_thread(started.wait, PATIENCE)
        foreign.close()

    # Брошенная задача закрытого loop пишет «Task was destroyed»: здесь это ожидаемо.
    foreign.loop.set_exception_handler(lambda _loop, _context: None)
    closer = asyncio.create_task(close_when_started())
    with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
        await run_in(foreign.loop, stuck, patience=0.5)
    await closer

    assert "задачу не отменить" in caplog.text


class BusyLoop(asyncio.SelectorEventLoop):
    """Остановленный loop, который успели занять, пока закрытие шло к нему."""

    @override
    def run_until_complete(self, future: object) -> NoReturn:
        _ = future
        message = "This event loop is already running"
        raise RuntimeError(message)


async def test_stopped_loop_taken_by_someone_else_is_reported(
    caplog: pytest.LogCaptureFixture,
) -> None:
    owner = BusyLoop()
    probe = Probe()

    try:
        with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
            await run_in(owner, probe.run, patience=PATIENCE)
    finally:
        owner.close()

    assert probe.loops == []
    assert "недоступен" in caplog.text


async def test_drain_tasks_waits_for_tasks_that_fit_the_budget() -> None:
    done: list[int] = []

    async def work(number: int) -> None:
        await asyncio.sleep(0.01)
        done.append(number)

    tasks = [asyncio.create_task(work(number)) for number in range(3)]
    finished = asyncio.create_task(asyncio.sleep(0))
    await finished

    await drain_tasks([*tasks, finished], budget())
    await drain_tasks([], budget())

    assert sorted(done) == [0, 1, 2]
    assert all(task.done() and not task.cancelled() for task in tasks)


async def test_drain_tasks_cancels_latecomers_and_waits_for_them(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cleaned: list[str] = []

    async def stuck() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)  # отмена доставлена, задача успевает прибрать за собой
            cleaned.append("stuck")

    task = asyncio.create_task(stuck(), name="tallyho-api-finalize")
    await asyncio.sleep(0)

    with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
        await drain_tasks([task], budget(0.05))

    assert task.cancelled()
    assert cleaned == ["stuck"]
    assert "tallyho-api-finalize" in caplog.text


async def test_close_services_stops_relay_after_completer_and_tasks() -> None:
    completer = FakeCompleter(delay=0.01)
    relay = FakeRelay(completer)
    task = asyncio.create_task(asyncio.sleep(0.01))

    await close_services(budget=budget(7), tasks=[task], completer=completer, relay=relay)

    assert task.done()
    assert completer.calls == ["close(requeue_held=True)", "closed"]
    # Relay остановлен последним и получил остаток общего срока.
    [(completer_calls, grace)] = relay.seen
    assert completer_calls == ["close(requeue_held=True)", "closed"]
    assert grace == pytest.approx(7.0)


async def test_close_services_without_worker_or_broker_is_a_noop() -> None:
    await close_services(budget=budget(), tasks=[], completer=None, relay=None)


async def test_close_services_aborts_completer_that_missed_the_budget(
    caplog: pytest.LogCaptureFixture,
) -> None:
    completer = FakeCompleter(delay=30)

    with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
        await close_services(budget=budget(0.05), tasks=[], completer=completer, relay=None)

    assert completer.calls == ["close(requeue_held=True)", "abort"]
    assert "lease_ttl" in caplog.text


async def test_close_services_logs_failed_requeue_and_goes_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    completer = FakeCompleter(error=CompleterError("нет БД"))
    relay = FakeRelay(completer)

    with caplog.at_level(logging.ERROR, logger="tallyho.engine.shutdown"):
        await close_services(budget=budget(), tasks=[], completer=completer, relay=relay)

    assert "не возвращены в outbox" in caplog.text
    assert len(relay.seen) == 1


async def test_close_services_closes_each_part_in_its_own_loop(foreign: LoopThread) -> None:
    seen: list[asyncio.AbstractEventLoop] = []

    async def remember() -> None:
        await asyncio.sleep(0.01)
        seen.append(asyncio.get_running_loop())

    foreign_task = _spawn_in(foreign.loop, remember)
    own_task = asyncio.create_task(remember())
    completer = FakeCompleter(loop=foreign.loop)

    await close_services(
        budget=budget(), tasks=[foreign_task, own_task], completer=completer, relay=None
    )

    assert foreign_task.done()
    assert own_task.done()
    assert sorted(seen, key=id) == sorted([foreign.loop, asyncio.get_running_loop()], key=id)
    assert completer.calls == ["close(requeue_held=True)", "closed"]


async def test_close_services_drives_a_stopped_loop_once_for_all_its_parts(
    foreign: LoopThread, caplog: pytest.LogCaptureFixture
) -> None:
    done: list[str] = []

    async def finish() -> None:
        await asyncio.sleep(0.01)
        done.append("task")

    foreign_task = _spawn_in(foreign.loop, finish)
    completer = FakeCompleter(loop=foreign.loop, delay=0.01)
    foreign.stop()

    with caplog.at_level(logging.WARNING, logger="tallyho.engine.shutdown"):
        await close_services(budget=budget(), tasks=[foreign_task], completer=completer, relay=None)

    # Задачи и Completer одного остановленного loop закрыты одним проходом, без гонки потоков.
    assert not caplog.records
    assert done == ["task"]
    assert completer.calls == ["close(requeue_held=True)", "closed"]


def _spawn_in(
    loop: asyncio.AbstractEventLoop, work: Callable[[], Coroutine[object, object, None]]
) -> asyncio.Task[None]:
    created: concurrent.futures.Future[asyncio.Task[None]] = concurrent.futures.Future()
    _ = loop.call_soon_threadsafe(lambda: created.set_result(loop.create_task(work())))
    return created.result(5)
