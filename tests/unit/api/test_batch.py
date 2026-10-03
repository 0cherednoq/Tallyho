"""Ветвления публичных BatchBuilder и BatchHandle без PostgreSQL."""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ParamSpec, cast
from uuid import UUID

import pytest
from typing_extensions import override

from tallyho import BatchBuilder, BatchHandle
from tallyho.engine.public import BatchDefinition, BatchReference, BatchWriter, EngineFacade
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState, ItemState, OnFeederFailed
from tallyho.model.views import BatchPage
from tallyho.protocols.broker import Dispatcher

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Callable,
        Collection,
        Mapping,
        Sequence,
    )
    from types import TracebackType

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from tallyho.engine.public import MaintenanceRunner
    from tallyho.model.attributes import AttributeValue
    from tallyho.model.views import BatchView, InFlightItem, ItemView
    from tallyho.protocols.broker import Message, WorkerFactory

__all__: list[str] = []

P = ParamSpec("P")
ROOT_ID = UUID(int=1)
CHILD_ID = UUID(int=2)
_CREATE_FAILED = "create failed"
_SEAL_FAILED = "seal failed"


class Adapter(Dispatcher):
    """Стабильное разрешение имён тестовых функций."""

    @override
    def task_name(self, fn: Callable[P, object]) -> str:
        return fn.__name__

    @override
    async def dispatch(self, messages: Sequence[Message]) -> None:
        _ = messages


@dataclass
class Writer(BatchWriter):
    """Записывающий fake writer."""

    roots: list[BatchDefinition] = field(default_factory=list[BatchDefinition])
    children: list[tuple[UUID, BatchDefinition]] = field(
        default_factory=list[tuple[UUID, BatchDefinition]]
    )
    additions: list[tuple[UUID, tuple[TaskCall, ...]]] = field(
        default_factory=list[tuple[UUID, tuple[TaskCall, ...]]]
    )
    expected: list[tuple[UUID, int]] = field(default_factory=list[tuple[UUID, int]])
    sealed: list[UUID] = field(default_factory=list[UUID])
    fail_create: bool = False
    fail_seal: bool = False

    @override
    async def create_root(self, spec: BatchDefinition) -> BatchReference:
        if self.fail_create:
            raise ConfigurationError(_CREATE_FAILED)
        self.roots.append(spec)
        return BatchReference(ROOT_ID, ROOT_ID, created=True)

    @override
    async def create_child(self, parent_id: UUID, spec: BatchDefinition) -> BatchReference:
        child_id = UUID(int=len(self.children) + CHILD_ID.int)
        self.children.append((parent_id, spec))
        return BatchReference(child_id, ROOT_ID, created=True)

    @override
    async def add(self, batch_id: UUID, calls: Sequence[TaskCall]) -> None:
        self.additions.append((batch_id, tuple(calls)))

    @override
    async def expect(self, batch_id: UUID, total: int) -> None:
        self.expected.append((batch_id, total))

    @override
    async def seal(self, batch_id: UUID) -> None:
        if self.fail_seal:
            raise ConfigurationError(_SEAL_FAILED)
        self.sealed.append(batch_id)


@dataclass
class WriterContext(AbstractAsyncContextManager[BatchWriter]):
    """Fake async context с записью параметров выхода."""

    writer: Writer
    exits: list[type[BaseException] | None] = field(
        default_factory=list[type[BaseException] | None]
    )

    @override
    async def __aenter__(self) -> BatchWriter:
        return self.writer

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        _ = exc_value, traceback
        self.exits.append(exc_type)
        return False


@dataclass(frozen=True, slots=True)
class View:
    """Минимальный объект для проверки ``wait``."""

    state: BatchState


async def views(values: Sequence[View]) -> AsyncGenerator[BatchView]:
    """Преобразовать минимальные view в тестовый поток."""
    await asyncio.sleep(0)
    for value in values:
        yield cast("BatchView", cast("object", value))


async def no_items() -> AsyncIterator[ItemView]:
    """Пустой поток Items."""
    await asyncio.sleep(0)
    for value in cast("tuple[ItemView, ...]", ()):
        yield value


@dataclass  # ruff: ignore[too-many-public-methods]  # fake повторяет весь протокол EngineFacade
class Facade(EngineFacade):
    """Полный записывающий fake чистой engine-границы."""

    writer_value: Writer = field(default_factory=Writer)
    context: WriterContext = field(init=False)
    calls: list[tuple[str, object]] = field(default_factory=list[tuple[str, object]])
    views: list[View] = field(default_factory=lambda: [View(BatchState.SUCCEEDED)])

    def __post_init__(self) -> None:
        self.context = WriterContext(self.writer_value)

    @override
    def install(self, adapter: Dispatcher | None, worker_factory: WorkerFactory) -> None:
        _ = adapter
        _ = worker_factory

    @override
    async def migrate(self) -> int:
        return 2

    @override
    def maintenance(self) -> MaintenanceRunner | None:
        return None

    @override
    async def run_maintenance_once(self) -> object:
        return None

    @override
    async def close(self) -> None:
        self.calls.append(("close", None))

    @override
    def writer(
        self, target: AsyncSession | AsyncConnection | None
    ) -> AbstractAsyncContextManager[BatchWriter]:
        self.calls.append(("writer", target))
        return self.context

    @override
    async def view(self, batch_id: UUID) -> BatchView:
        self.calls.append(("view", batch_id))
        return cast("BatchView", cast("object", self.views[-1]))

    @override
    async def in_flight(self, batch_id: UUID, limit: int) -> list[InFlightItem]:
        self.calls.append(("in_flight", (batch_id, limit)))
        return []

    @override
    def items(
        self,
        batch_id: UUID,
        *,
        states: Collection[ItemState] | None = None,
        labels: Collection[str] | None = None,
    ) -> AsyncIterator[ItemView]:
        self.calls.append(("items", (batch_id, states, labels)))
        return no_items()

    @override
    async def list_batches(
        self,
        *,
        kinds: Collection[str] | None = None,
        states: Collection[BatchState] | None = None,
        attributes: Mapping[str, AttributeValue] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> BatchPage:
        self.calls.append(
            (
                "list_batches",
                (kinds, states, attributes, created_after, created_before, limit, cursor),
            )
        )
        return BatchPage(items=())

    @override
    async def find(self, kind: str, key: str) -> UUID:
        self.calls.append(("find", (kind, key)))
        return ROOT_ID

    @override
    async def child(self, batch_id: UUID, key: str) -> UUID:
        self.calls.append(("child", (batch_id, key)))
        return CHILD_ID

    @override
    def watch(self, batch_id: UUID) -> AsyncGenerator[BatchView]:
        self.calls.append(("watch", batch_id))
        return views(self.views)

    @override
    async def pause(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        self.calls.append(("pause", (target, batch_id)))

    @override
    async def resume(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        self.calls.append(("resume", (target, batch_id)))

    @override
    async def cancel(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        self.calls.append(("cancel", (target, batch_id)))

    @override
    async def reschedule(
        self,
        target: AsyncSession | AsyncConnection | None,
        batch_id: UUID,
        start_at: datetime,
    ) -> int:
        self.calls.append(("reschedule", (target, batch_id, start_at)))
        return 3

    @override
    async def retry_failed(
        self,
        target: AsyncSession | AsyncConnection | None,
        batch_id: UUID,
        labels: Sequence[str] | None,
    ) -> int:
        self.calls.append(("retry_failed", (target, batch_id, labels)))
        return 4

    @override
    async def retry_finalize(
        self, target: AsyncSession | AsyncConnection | None, batch_id: UUID
    ) -> None:
        self.calls.append(("retry_finalize", (target, batch_id)))

    @override
    async def release(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        self.calls.append(("release", (target, batch_id)))


async def task(value: int) -> None:
    """Сигнатура задачи для преобразования в TaskCall."""
    _ = value
    await asyncio.sleep(0)


def builder(facade: Facade | None = None) -> tuple[BatchBuilder, Facade]:
    """Новый корневой builder и его fake facade."""
    value = facade or Facade()
    return BatchBuilder(value, Adapter(), BatchDefinition(kind="root")), value


async def test_builder_records_calls_children_callbacks_and_manual_seal() -> None:
    root, facade = builder()
    callback = TaskCall(task_name="done")
    async with root:
        feeder = root.sub_batch("feeder", on_feeder_failed=OnFeederFailed.SEAL)
        stage = root.sub_batch(
            "stage",
            fed_by=[feeder],
            on_feeder_failed="cancel",
            on_succeeded=callback,
            on_completed_with_errors=callback,
            on_failed=callback,
            on_cancelled=callback,
            on_finalized_task=callback,
        )
        await feeder.map(task, [1, 2])
        await feeder.add_calls([TaskCall(task_name="other")])
        await stage.expect(5)
        async with feeder:
            await feeder.add(task, 3)
        await feeder.seal()

    assert root.handle.id == ROOT_ID
    assert len(facade.writer_value.additions) == 3
    assert facade.writer_value.expected == [(UUID(int=3), 5)]
    assert facade.writer_value.sealed.count(CHILD_ID) == 1
    assert facade.writer_value.sealed[-1] == ROOT_ID
    stage_spec = facade.writer_value.children[1][1]
    assert stage_spec.on_feeder_failed is OnFeederFailed.CANCEL
    assert stage_spec.fed_by == (CHILD_ID,)
    assert set(stage_spec.callbacks) == {
        "on_succeeded",
        "on_completed_with_errors",
        "on_failed",
        "on_cancelled",
        "on_finalized_task",
    }


async def test_builder_guards_and_foreign_feeder() -> None:
    root, _ = builder()
    foreign, _ = builder()
    with pytest.raises(ConfigurationError, match="async with"):
        _ = root.handle
    with pytest.raises(ConfigurationError, match="async with"):
        await root.add(task, 1)
    with pytest.raises(ConfigurationError, match="fed_by"):
        _ = root.sub_batch("bad", fed_by=[foreign.sub_batch("source")])
    with pytest.raises(ConfigurationError, match="on_feeder_failed"):
        _ = root.sub_batch("bad-policy", on_feeder_failed="unknown")
    with pytest.raises(ConfigurationError, match="async with"):
        await root.__aexit__(None, None, None)

    async with root:
        await root.seal()
        with pytest.raises(ConfigurationError, match="закрыт"):
            await root.expect(1)
        with pytest.raises(ConfigurationError, match="закрыт"):
            _ = root.sub_batch("late")


async def test_child_context_with_feeder_does_not_seal_stage() -> None:
    root, facade = builder()
    async with root:
        feeder = root.sub_batch("feeder")
        stage = root.sub_batch("stage", fed_by=[feeder])
        async with stage:
            pass
        assert stage.handle.id == UUID(int=3)
        assert UUID(int=3) not in facade.writer_value.sealed


async def test_streaming_builder_commits_without_sealing_the_tree() -> None:
    facade = Facade()
    root = BatchBuilder(facade, Adapter(), BatchDefinition(kind="root"), _auto_seal=False)
    async with root:
        await root.add(task, 1)
        part = root.sub_batch("part")
        async with part:
            await part.add(task, 2)
    # Под-батч создан и наполнен, но ни он, ни корень не закрыты; транзакция завершена.
    assert facade.writer_value.children[0][1].key == "part"
    assert len(facade.writer_value.additions) == 2
    assert facade.writer_value.sealed == []
    assert facade.context.exits == [None]

    closing = BatchBuilder(facade, Adapter(), BatchDefinition(kind="root"), _auto_seal=False)
    async with closing:
        await closing.seal()  # явный seal работает и при seal=False
    assert facade.writer_value.sealed == [ROOT_ID]


async def test_enter_and_exit_failures_close_writer_context() -> None:
    failed_create = Facade(writer_value=Writer(fail_create=True))
    root, _ = builder(failed_create)
    with pytest.raises(ConfigurationError, match="create failed"):
        async with root:
            pass
    assert failed_create.context.exits == [ConfigurationError]

    failed_seal = Facade(writer_value=Writer(fail_seal=True))
    root, _ = builder(failed_seal)
    with pytest.raises(ConfigurationError, match="seal failed"):
        async with root:
            pass
    assert failed_seal.context.exits == [ConfigurationError]


async def test_user_exception_is_forwarded_to_writer_context() -> None:
    root, facade = builder()
    with pytest.raises(ZeroDivisionError):
        async with root:
            _ = 1 / 0
    assert facade.context.exits == [ZeroDivisionError]


async def test_handle_delegates_reads_wait_and_mutations() -> None:
    facade = Facade(views=[View(BatchState.OPEN), View(BatchState.SUCCEEDED)])
    handle = BatchHandle(facade, ROOT_ID)
    assert (await handle.view()).state is BatchState.SUCCEEDED
    assert (await handle.wait(timedelta(seconds=1))).state is BatchState.SUCCEEDED
    assert await handle.in_flight(7) == []
    assert [item async for item in handle.items(labels=["bad"])] == []
    assert [item async for item in handle.items(states={ItemState.CANCELLED})] == []
    assert ("items", (ROOT_ID, {ItemState.CANCELLED}, None)) in facade.calls
    assert ("items", (ROOT_ID, None, ["bad"])) in facade.calls
    assert (await handle.child("part")).id == CHILD_ID
    at = datetime.now(UTC)
    assert await handle.reschedule(at) == 3
    await handle.pause()
    await handle.resume()
    await handle.cancel()
    assert await handle.retry_failed(labels=["bad"]) == 4
    await handle.retry_finalize()
    await handle.release()
    assert {name for name, _ in facade.calls} >= {
        "view",
        "watch",
        "in_flight",
        "items",
        "child",
        "reschedule",
        "pause",
        "resume",
        "cancel",
        "retry_failed",
        "retry_finalize",
        "release",
    }


async def test_wait_falls_back_to_view_if_stream_ends_before_terminal() -> None:
    """Потерявшийся watcher всё равно даёт последний атомарный снимок."""
    facade = Facade(views=[View(BatchState.OPEN)])
    value = await BatchHandle(facade, ROOT_ID).wait()
    assert value.state is BatchState.OPEN
    assert facade.calls[-1] == ("view", ROOT_ID)
