"""Событие flexiq ``JOB_DEAD`` идёт по правилу сверки с DLQ (ARCHITECTURE UC-15, D-051).

Событие о мёртвой джобе завершает Item, только если джоба — текущее поколение
отправки Item, а у Item нет ни lease, ни записи outbox. Живой lease того же
поколения получает ``redelivered = true``. Адаптер — настоящий
``FlexiqAdapter`` над очередью-фейком: событие вызывается так же, как его
вызывает пул ``flexiq-events``.
"""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, cast, final

import pytest
from flexiq import EventType
from sqlalchemy import func, insert, select, update

from tallyho import Tallyho
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.model.states import ItemState, OutboxKind
from tests.helpers.loops import library_tasks

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from uuid import UUID

    from flexiq import Queue

    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class _Fn(Protocol):
    def __call__(self, *args: object, **kwargs: object) -> object: ...


@dataclass(slots=True)
class _StoredJob:
    task_name: str
    payload_bytes: bytes


@dataclass(slots=True)
class _Job:
    _py_job: _StoredJob


@dataclass(slots=True)
class _Page:
    items: list[object] = field(default_factory=list[object])
    next_cursor: str | None = None


@final
class _Queue:
    """Очередь flexiq в памяти: принимает отправку, хранит джобы и подписки."""

    def __init__(self) -> None:
        self.events: dict[object, Callable[[object, object], None]] = {}
        self.payloads: dict[bytes, tuple[tuple[object, ...], dict[str, object]]] = {}
        self.jobs: dict[str, _Job] = {}
        self.sent: list[dict[str, object]] = []

    def task(self, **options: object) -> Callable[[object], object]:
        name = str(options["name"])

        def decorate(fn: object) -> object:
            return _Task(name, cast("_Fn", fn))

        return decorate

    def enqueue_many(self, **options: object) -> object:
        self.sent.append(dict(options))
        return []

    def enqueue(self, **options: object) -> object:
        self.sent.append(dict(options))
        return object()

    def on_event(self, event_type: object, callback: Callable[[object, object], None]) -> None:
        self.events[event_type] = callback

    async def aget_job(self, job_id: str) -> object | None:
        await asyncio.sleep(0)
        return self.jobs.get(job_id)

    async def adead_letters_after(self, *, limit: int, after: str | None) -> object:
        _ = (limit, after)
        await asyncio.sleep(0)
        return _Page()

    def _encode_payload(  # pyright: ignore[reportUnusedFunction]  # flexiq SPI вызывается адаптером динамически
        self, task_name: str, args: tuple[object, ...], kwargs: dict[str, object]
    ) -> bytes:
        key = f"{task_name}:{len(self.payloads)}".encode()
        self.payloads[key] = (args, dict(kwargs))
        return key

    def _deserialize_payload(  # pyright: ignore[reportUnusedFunction]  # flexiq SPI вызывается адаптером динамически
        self, task_name: str, payload: bytes
    ) -> object:
        assert payload.startswith(task_name.encode())
        return self.payloads[payload]


@final
class _Task:
    def __init__(self, name: str, fn: _Fn) -> None:
        self.name = name
        self._fn = fn

    def __call__(self, *args: object, **kwargs: object) -> object:
        return self._fn(*args, **kwargs)


async def echo(value: str) -> str:
    """Задача примера; в тесте вызывается только для привязки loop адаптера."""
    await asyncio.sleep(0)
    return value


@dataclass
class Worker:
    th: Tallyho
    adapter: FlexiqAdapter
    queue: _Queue
    task: Callable[[str], Awaitable[str]]


@pytest.fixture
async def worker(env: Env) -> AsyncGenerator[Worker]:
    """Процесс воркера: установка с адаптером, loop исполнителя привязан."""
    queue = _Queue()
    adapter = FlexiqAdapter(cast("Queue", cast("object", queue)))
    th = Tallyho(env.engine, schema=env.schema)
    th.install(adapter)
    task = adapter.task(name="echo")(echo)
    # Вызов без маркера _th не трогает Completer, но запоминает loop исполнителя.
    _ = await task("bind-loop")
    try:
        yield Worker(th, adapter, queue, task)
    finally:
        await th.aclose()
        await adapter.close()


async def _eventually(condition: Callable[[], bool]) -> None:
    async with asyncio.timeout(15):
        for _ in itertools.count():
            if condition():
                return
            await asyncio.sleep(0.01)


async def _dispatched_item(env: Env, worker: Worker, key: str) -> tuple[UUID, UUID]:
    async with worker.th.batch(kind="dlq-event", key=key) as batch:
        await batch.add(worker.task, "payload")
    # Relay отправил Item во flexiq: записи outbox нет.
    async with asyncio.timeout(15):
        for _ in itertools.count():
            if not await env.count(env.tables.outbox):
                break
            await asyncio.sleep(0.01)
    item = env.tables.item
    async with env.connection() as conn:
        item_id = await conn.scalar(select(item.c.id).where(item.c.batch_id == batch.handle.id))
    assert item_id is not None
    return item_id, batch.handle.id


async def _job_dead(worker: Worker, item_id: UUID, batch_id: UUID, *, generation: int) -> None:
    marker: dict[str, object] = {"i": str(item_id), "b": str(batch_id), "r": 0}
    if generation:
        marker["g"] = generation
    payload = worker.adapter.encode("echo", ("payload",), {"_th": marker})
    worker.queue.jobs["dead-job"] = _Job(_StoredJob("echo", payload))
    worker.queue.events[EventType.JOB_DEAD](
        EventType.JOB_DEAD, {"job_id": "dead-job", "error": "boom"}
    )
    await asyncio.sleep(0)
    await _eventually(lambda: "tallyho-flexiq-dlq" not in library_tasks())


async def _state(env: Env, item_id: UUID) -> tuple[ItemState, str | None, object]:
    item = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(item.c.state, item.c.label, item.c.error).where(item.c.id == item_id)
            )
        ).one()
    return ItemState(row[0]), row[1], row[2]


async def test_orphan_of_current_generation_is_exhausted(env: Env, worker: Worker) -> None:
    item_id, batch_id = await _dispatched_item(env, worker, "orphan")

    await _job_dead(worker, item_id, batch_id, generation=0)

    assert await _state(env, item_id) == (
        ItemState.ERROR,
        "exhausted",
        {"type": "FlexiqDeadLetter", "message": "boom"},
    )


async def test_orphan_of_cancelling_batch_is_cancelled(env: Env, worker: Worker) -> None:
    item_id, batch_id = await _dispatched_item(env, worker, "cancelling")
    batch = env.tables.batch
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(batch).where(batch.c.id == batch_id).values(cancel_requested_at=func.now())
        )

    await _job_dead(worker, item_id, batch_id, generation=0)

    state, label, _ = await _state(env, item_id)
    assert (state, label) == (ItemState.CANCELLED, "cancelled")


async def test_dead_job_of_previous_generation_does_not_touch_item(
    env: Env, worker: Worker
) -> None:
    item_id, batch_id = await _dispatched_item(env, worker, "resent")
    item = env.tables.item
    async with env.transaction() as conn:
        # Item вернулся в outbox и отправлен заново: за него отвечает новая джоба.
        _ = await conn.execute(update(item).where(item.c.id == item_id).values(generation=1))

    await _job_dead(worker, item_id, batch_id, generation=0)

    assert await _state(env, item_id) == (ItemState.ACTIVE, None, None)


async def test_dead_job_with_queued_outbox_does_not_touch_item(env: Env, worker: Worker) -> None:
    item_id, batch_id = await _dispatched_item(env, worker, "queued")
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(outbox).values(
                id=item_id,
                kind=int(OutboxKind.ITEM),
                batch_id=batch_id,
                item_id=item_id,
                task_name="echo",
                # Запаркована: relay теста её не заберёт.
                available_at=func.now() + timedelta(days=1),
            )
        )

    await _job_dead(worker, item_id, batch_id, generation=0)

    assert await _state(env, item_id) == (ItemState.ACTIVE, None, None)
    assert await env.count(outbox) == 1


async def test_dead_job_with_live_lease_marks_redelivery(env: Env, worker: Worker) -> None:
    item_id, batch_id = await _dispatched_item(env, worker, "running")
    lease = env.tables.lease
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(lease).values(
                item_id=item_id,
                batch_id=batch_id,
                lease_until=func.now() + timedelta(minutes=1),
                worker_id="other-worker",
                attempt=0,
            )
        )

    await _job_dead(worker, item_id, batch_id, generation=0)

    # Выполнение идёт: итог запишет оно само, release вернёт Item в outbox (UC-04).
    assert await _state(env, item_id) == (ItemState.ACTIVE, None, None)
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(lease.c.worker_id, lease.c.redelivered).where(lease.c.item_id == item_id)
            )
        ).one()
    assert tuple(row) == ("other-worker", True)
