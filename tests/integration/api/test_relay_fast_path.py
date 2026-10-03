"""Отправка после commit в каждом процессе с адаптером, без процесса maintenance (Fix-10).

Тесты работают через публичный ``Tallyho`` с настоящим ``Dispatcher``-фейком:
``InlineBroker`` здесь не годится, он сам вызывает проходы relay.
"""

from __future__ import annotations

import asyncio
import itertools
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, final

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.protocols.broker import DeadLetter, DeadLetters, Verdict
from tests.helpers.relay import RecordingDispatcher

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

RELAY_GRACE = timedelta(seconds=5)
"""Умолчание ``relay_grace``: раньше этого срока запись отправляет только fast-path."""
SWEEP_INTERVAL = timedelta(seconds=5)
"""Умолчание ``sweep_interval``: период страховочного scan."""
NOTICEABLY_FASTER = RELAY_GRACE.total_seconds() / 2


@final
class CrashedBeforeDispatch(RecordingDispatcher):
    """Процесс, который закоммитил батч и «упал»: его relay ничего не отправляет."""

    @property
    def relay_autostart(self) -> bool:
        return False

    @override
    async def dispatch(self, messages: object) -> None:
        raise AssertionError(messages)


@final
class BrokerWithDeadLetters(RecordingDispatcher):
    """Брокер, который принял сообщения и отправил все их джобы в DLQ, не выполнив."""

    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        return fn

    def retry_verdict(self, exc: BaseException) -> Verdict:
        _ = exc
        return Verdict.FINAL

    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        await asyncio.sleep(0)
        offset = 0 if since is None else int(since)
        dead = self.messages
        entries = tuple(DeadLetter(message.id, message.generation) for message in dead[offset:])
        return DeadLetters(entries, str(len(dead)))


async def send_email(address: str) -> None:
    """Задача примера; в этих тестах не исполняется."""
    _ = address
    await asyncio.sleep(0)


class Clients(Protocol):
    """Фабрика «процессов» с настоящим адаптером над схемой теста."""

    def __call__(
        self,
        dispatcher: RecordingDispatcher,
        *,
        relay_grace: timedelta = RELAY_GRACE,
        sweep_interval: timedelta = SWEEP_INTERVAL,
    ) -> Tallyho:
        """Создать и установить клиент."""
        ...


@pytest.fixture
async def clients(env: Env) -> AsyncGenerator[Clients]:
    """Фабрика «процессов» над одной схемой; relay каждого остановлен до DROP SCHEMA."""
    created: list[Tallyho] = []

    def make(
        dispatcher: RecordingDispatcher,
        *,
        relay_grace: timedelta = RELAY_GRACE,
        sweep_interval: timedelta = SWEEP_INTERVAL,
    ) -> Tallyho:
        value = Tallyho(
            env.engine, schema=env.schema, relay_grace=relay_grace, sweep_interval=sweep_interval
        )
        value.install(dispatcher)
        created.append(value)
        return value

    yield make
    for value in created:
        await value.aclose()


async def create_batch(th: Tallyho, key: str, addresses: int) -> UUID:
    async with th.batch(kind="mailing", key=key) as batch:
        for number in range(addresses):
            await batch.add(send_email, f"user{number}@example.test")
    return batch.handle.id


async def outbox_size(env: Env) -> int:
    return await env.count(env.tables.outbox)


async def eventually(condition: Callable[[], bool], *, deadline: float = 15.0) -> None:
    async with asyncio.timeout(deadline):
        for _ in itertools.count():
            if condition():
                return
            await asyncio.sleep(0.01)


async def test_commit_dispatches_noticeably_faster_than_relay_grace(
    env: Env, clients: Clients
) -> None:
    dispatcher = RecordingDispatcher()
    th = clients(dispatcher)  # настройки по умолчанию: relay_grace = 5 с, maintenance не запущен

    started = monotonic()
    batch_id = await create_batch(th, "fast", addresses=3)
    await eventually(lambda: len(dispatcher.messages) == 3, deadline=NOTICEABLY_FASTER)
    elapsed = monotonic() - started

    assert elapsed < NOTICEABLY_FASTER
    assert {message.batch_id for message in dispatcher.messages} == {batch_id}
    await th.aclose()
    assert await outbox_size(env) == 0
    assert (await env.counters(batch_id)).dispatched == 3


def received(dispatcher: RecordingDispatcher, count: int) -> Callable[[], bool]:
    """Условие «брокер принял ``count`` сообщений»."""
    return lambda: len(dispatcher.messages) == count


async def test_kick_after_own_commit_dispatches_without_scan(env: Env, clients: Clients) -> None:
    """Fix-16: kick своей транзакции приходит после COMMIT, scan для отправки не нужен."""
    dispatcher = RecordingDispatcher()
    # Scan и grace — час: всё, что отправлено за секунды, отправил fast-path.
    hour = timedelta(hours=1)
    th = clients(dispatcher, relay_grace=hour, sweep_interval=hour)

    for number in range(1, 21):
        batch_id = await create_batch(th, f"kick:{number}", addresses=1)
        await eventually(received(dispatcher, number), deadline=2.0)
        assert dispatcher.messages[-1].batch_id == batch_id

    await th.aclose()
    assert await outbox_size(env) == 0


async def test_kick_after_user_connection_commit_dispatches_without_scan(
    env: Env, clients: Clients
) -> None:
    """Fix-16: то же для ``AsyncConnection`` пользователя: kick после его COMMIT."""
    dispatcher = RecordingDispatcher()
    hour = timedelta(hours=1)
    th = clients(dispatcher, relay_grace=hour, sweep_interval=hour)

    for number in range(1, 21):
        async with env.engine.connect() as conn:
            async with th.batch(kind="mailing", key=f"conn:{number}", session=conn) as batch:
                await batch.add(send_email, "user@example.test")
            await conn.commit()
        await eventually(received(dispatcher, number), deadline=2.0)
        assert dispatcher.messages[-1].batch_id == batch.handle.id

    await th.aclose()
    assert await outbox_size(env) == 0


async def test_commit_of_user_session_dispatches_after_commit_only(
    env: Env, clients: Clients
) -> None:
    dispatcher = RecordingDispatcher()
    th = clients(dispatcher)
    scoped = env.engine.execution_options(schema_translate_map={None: env.schema})

    async with AsyncSession(scoped) as session:
        async with th.batch(kind="mailing", key="rolled-back", session=session) as batch:
            await batch.add(send_email, "nobody@example.test")
        await session.rollback()
        async with th.batch(kind="mailing", key="committed", session=session) as batch:
            await batch.add(send_email, "user@example.test")
        await asyncio.sleep(0.2)
        assert dispatcher.messages == []  # до commit пользователя в брокер не уходит ничего
        await session.commit()

    await eventually(lambda: len(dispatcher.messages) == 1, deadline=NOTICEABLY_FASTER)
    assert dispatcher.messages[0].batch_id == batch.handle.id


async def test_kick_lost_by_crashed_process_is_recovered_by_scan_elsewhere(
    env: Env, clients: Clients
) -> None:
    grace = timedelta(milliseconds=300)
    crashed = clients(CrashedBeforeDispatch(), relay_grace=grace)
    survivor_dispatcher = RecordingDispatcher()
    survivor = clients(
        survivor_dispatcher, relay_grace=grace, sweep_interval=timedelta(milliseconds=50)
    )

    # Живой процесс уже что-то отправлял: его цикл relay работает.
    own = await create_batch(survivor, "own", addresses=1)
    await eventually(lambda: len(survivor_dispatcher.messages) == 1)
    lost = await create_batch(crashed, "lost", addresses=2)

    await eventually(lambda: len(survivor_dispatcher.messages) == 3)

    assert [message.batch_id for message in survivor_dispatcher.messages] == [own, lost, lost]
    await survivor.aclose()
    assert await outbox_size(env) == 0


async def test_two_processes_dispatch_each_message_once(env: Env, clients: Clients) -> None:
    scan_every = timedelta(milliseconds=10)
    first_dispatcher = RecordingDispatcher(delay=0.002)
    second_dispatcher = RecordingDispatcher(delay=0.002)
    first = clients(first_dispatcher, relay_grace=timedelta(0), sweep_interval=scan_every)
    second = clients(second_dispatcher, relay_grace=timedelta(0), sweep_interval=scan_every)
    per_batch = 5
    rounds = 10

    for number in range(rounds):
        # Каждый процесс отправляет своё fast-path'ом и сканирует чужое.
        _ = await asyncio.gather(
            create_batch(first, f"first-{number}", per_batch),
            create_batch(second, f"second-{number}", per_batch),
        )
    total = 2 * rounds * per_batch
    await eventually(lambda: len(first_dispatcher.ids) + len(second_dispatcher.ids) >= total)
    await asyncio.sleep(0.2)  # лишние проходы scan не должны ничего добавить
    await first.aclose()
    await second.aclose()

    ids = first_dispatcher.ids + second_dispatcher.ids
    assert len(ids) == len(set(ids)) == total
    assert await outbox_size(env) == 0
    item = env.tables.item
    async with env.connection() as conn:
        assert await conn.scalar(select(func.count()).select_from(item)) == total


async def test_maintenance_in_process_with_adapter_scans_immediately(
    env: Env, clients: Clients
) -> None:
    crashed = clients(CrashedBeforeDispatch(), relay_grace=timedelta(0))
    lost = await create_batch(crashed, "lost", addresses=2)
    dispatcher = RecordingDispatcher()
    # sweep_interval по умолчанию: без немедленного scan при старте тест не уложится в срок.
    th = clients(dispatcher, relay_grace=timedelta(0))
    runner = th.maintenance()
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: len(dispatcher.messages) == 2, deadline=NOTICEABLY_FASTER)
    finally:
        runner.stop()
        await task

    assert {message.batch_id for message in dispatcher.messages} == {lost}
    assert await outbox_size(env) == 0


async def test_dead_letters_are_reconciled_by_process_with_adapter_not_by_leader(
    env: Env, clients: Clients
) -> None:
    """Fix-6: лидер maintenance без адаптера DLQ не читает — сверяет процесс с брокером."""
    leader = Tallyho(env.engine, schema=env.schema, sweep_interval=timedelta(milliseconds=50))
    leader.install(None)
    runner = leader.maintenance()
    task = asyncio.create_task(runner.run())
    broker = BrokerWithDeadLetters()
    th = clients(broker, sweep_interval=timedelta(milliseconds=50))
    try:
        try:
            # Commit запускает цикл relay процесса: fast-path отправляет Items, а
            # после каждого scan идёт сверка с DLQ.
            batch_id = await create_batch(th, "dead", addresses=3)
            handle = th.handle(batch_id)
            view = await handle.wait(timeout=timedelta(seconds=15))
        finally:
            runner.stop()
            await task

        assert len(broker.messages) == 3
        assert view.state is BatchState.COMPLETED_WITH_ERRORS
        assert (view.progress.error, view.labels) == (3, {"exhausted": 3})
        # В процессе без адаптера сверки нет вовсе; проверяем до закрытия установки.
        assert getattr(await leader.run_maintenance_once(), "dead_letters", None) == 0
    finally:
        await leader.aclose()
