"""Безопасное поведение runtime-фасадов вне tracked-задачи."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest

from tallyho.engine.completer import ItemRef
from tallyho.engine.spawn import TreeNode, TreeSnapshot
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import ResultClass
from tallyho.runtime import (
    CallbackContext,
    ItemContext,
    TaskRuntime,
    bind_runtime,
    callback,
    item,
    tracked,
)
from tallyho.runtime.context import RuntimeSubBatch, activate_callback, activate_item

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from tallyho.engine.completer import Completer, FinishResult
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import Dispatcher, Runtime

__all__: list[str] = []


@dataclass
class FakeCompleter:
    changed: bool = True
    calls: list[FinishResult] = field(default_factory=list["FinishResult"])

    async def complete_in(
        self,
        _target: AsyncSession | AsyncConnection,
        _item: ItemRef,
        value: FinishResult,
    ) -> bool:
        self.calls.append(value)
        return self.changed


def _context() -> tuple[ItemContext, FakeCompleter]:
    batch_id = uuid4()
    node = TreeNode(id=batch_id, root_id=batch_id, parent_id=None, key=None)
    tree = TreeSnapshot(root_id=batch_id, nodes={batch_id: node}, by_key={})
    completer = FakeCompleter()

    def make_call(fn: object, args: tuple[object, ...], kwargs: Mapping[str, object]) -> TaskCall:
        _ = fn
        return TaskCall(task_name="task", args=args, kwargs=kwargs)

    context = ItemContext(
        ref=ItemRef(uuid4(), batch_id),
        attempt=2,
        depth=3,
        tree=tree,
        make_call=make_call,
        completer=cast("Completer", cast("object", completer)),
    )
    return context, completer


def test_facades_are_safe_outside_task() -> None:
    assert item.current() is None
    assert item.id() is None
    assert item.cancelled() is False
    assert callback.current() is None
    item.incr("ignored")
    item.progress(1, 2)
    item.ok("ignored")
    item.skip("ignored")
    item.error("ignored")
    item.expect(10)
    item.spawn(lambda: None)


def test_sub_batch_outside_task_is_explicit_error() -> None:
    with pytest.raises(ConfigurationError, match="только внутри"):
        _ = item.sub_batch("parts")


def test_tracked_rejects_sync_function() -> None:
    def sync_task() -> None:
        return None

    with pytest.raises(ConfigurationError, match="async def"):
        _ = tracked(cast("Callable[[], Awaitable[None]]", sync_task))


def test_item_context_buffers_all_finish_data() -> None:
    context, _ = _context()
    context.spawn(object(), 1, key="child", name="x")
    context.expect(7)
    context.progress(2, 5)
    context.incr("rows", 2)
    context.incr("rows", -1)
    context.ok("sent", result={"id": 1}, mark=True)

    value = context.finish_result()
    assert (context.id, context.batch_id, context.attempt, context.depth) == (
        context.ref.id,
        context.ref.batch_id,
        2,
        3,
    )
    assert value.result_class is ResultClass.OK
    assert value.effective_label == "sent"
    assert value.result == {"id": 1}
    assert value.metrics == {"rows": 1}
    assert value.effective_mark is True
    assert value.spawns[0].call.key == "child"
    assert value.expects[0].total == 7

    context.skip("duplicate")
    assert context.finish_result().result_class is ResultClass.SKIP
    context.error("bad", detail={"why": "x"})
    assert context.finish_result().error == {"why": "x"}


@pytest.mark.parametrize(("done", "total"), [(True, 1), (-1, 1), (1, True), (1, -1)])
def test_progress_rejects_invalid_values(done: int, total: int) -> None:
    context, _ = _context()
    with pytest.raises(ConfigurationError, match="неотрицательные"):
        context.progress(done, total)


@pytest.mark.parametrize(("name", "value"), [("", 1), ("x", True)])
def test_metric_rejects_invalid_values(name: str, value: int) -> None:
    context, _ = _context()
    with pytest.raises(ConfigurationError, match="метрики"):
        context.incr(name, value)


async def test_runtime_sub_batch_buffers_only_successful_context_manager() -> None:
    context, _ = _context()
    builder = context.sub_batch("parts", expected_total=2)
    async with builder as entered:
        assert entered is builder
        builder.add(object(), 1)
        builder.add_calls([TaskCall(task_name="other")])
        builder.map(object(), [2, 3])
        builder.expect(4)
    assert len(context.sub_batches) == 1
    assert context.sub_batches[0].spec.expected_total == 4
    assert len(context.sub_batches[0].calls) == 4
    builder.seal()
    with pytest.raises(ConfigurationError, match="закрыт"):
        builder.add_call(TaskCall(task_name="late"))

    failed = RuntimeSubBatch(context, context.sub_batch("failed").spec)
    with pytest.raises(RuntimeError):
        async with failed:
            raise RuntimeError
    assert len(context.sub_batches) == 1


def test_facades_delegate_inside_scoped_context() -> None:
    context, completer = _context()
    callback_context = CallbackContext(uuid4(), context.batch_id, {"ok": 1})
    with activate_item(context):
        assert item.current() is context
        assert item.id() == context.id
        item.progress(1)
        item.incr("n")
        item.spawn(object())
        item.spawn_call(TaskCall(task_name="prepared"))
        item.expect(3)
        item.skip("nope", mark=True)
        context.cancel_requested = True
        assert item.cancelled()
    assert item.current() is None
    assert not context.completed_in_user_tx
    assert completer.calls == []

    with activate_callback(callback_context):
        assert callback.current() is callback_context
    assert callback.current() is None


def _runtime() -> TaskRuntime:
    dependency: object = object()
    return TaskRuntime(
        completer=cast("Completer", dependency),
        broker=cast("Runtime", dependency),
        dispatcher=cast("Dispatcher", dependency),
        tree_cache=cast("TreeCache", dependency),
        heartbeat_every=timedelta(seconds=1),
    )


def test_task_runtime_validates_settings_and_sync_wrap() -> None:
    dependency: object = object()
    with pytest.raises(ConfigurationError, match="heartbeat_every"):
        _ = TaskRuntime(
            completer=cast("Completer", dependency),
            broker=cast("Runtime", dependency),
            dispatcher=cast("Dispatcher", dependency),
            tree_cache=cast("TreeCache", dependency),
            heartbeat_every=timedelta(0),
        )

    def sync_task() -> None:
        return None

    with pytest.raises(ConfigurationError, match="async def"):
        _ = _runtime().wrap(cast("Callable[[], Awaitable[None]]", sync_task))


async def test_wrap_passthrough_rejects_bad_markers_and_module_binding() -> None:
    runtime = _runtime()

    async def task(value: int, **kwargs: object) -> int:
        await asyncio.sleep(0)
        return value + len(kwargs)

    wrapped = runtime.wrap(task)
    assert await wrapped(2) == 2
    with pytest.raises(ConfigurationError, match="_th"):
        await wrapped(2, _th=3)
    with pytest.raises(ConfigurationError, match="_th"):
        await wrapped(2, _th={"i": "bad", "b": str(uuid4())})

    bind_runtime(runtime)
    module_wrapped = tracked(task)
    assert await module_wrapped(4) == 4
