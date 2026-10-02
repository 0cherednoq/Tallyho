"""Around-обёртка tracked-задач: claim, heartbeat и терминальный исход."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
from collections.abc import Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, cast, final
from uuid import UUID

from sqlalchemy import select

from tallyho.engine.completer import FinishResult, ItemRef
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ClosedError, ConfigurationError
from tallyho.model.states import ResultClass
from tallyho.protocols.broker import CancellationClassifier, Verdict
from tallyho.runtime.context import (
    CallbackContext,
    ItemContext,
    activate_callback,
    activate_item,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from datetime import timedelta

    from tallyho.engine.completer import Completer
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import Dispatcher, Runtime

P = ParamSpec("P")
R = TypeVar("R")

__all__ = ["TaskRuntime", "bind_runtime", "build_runtime", "current_runtime", "tracked"]

_runtime: ContextVar[TaskRuntime | None] = ContextVar("tallyho_runtime", default=None)
_installed: list[TaskRuntime] = []
_NOT_INSTALLED = "tallyho runtime не установлен"
_BAD_MARKER = "служебный аргумент _th имеет неверный формат"


class _AsyncTask(Protocol):
    async def __call__(self, *args: object, **kwargs: object) -> object:
        """Вызвать задачу с динамической сигнатурой."""
        ...


@final
class TaskRuntime:
    """Зависимости и жизненный цикл одной установки runtime."""

    completer: Completer
    broker: Runtime
    dispatcher: Dispatcher
    tree_cache: TreeCache
    heartbeat_every: timedelta

    def __init__(
        self,
        *,
        completer: Completer,
        broker: Runtime,
        dispatcher: Dispatcher,
        tree_cache: TreeCache,
        heartbeat_every: timedelta,
    ) -> None:
        """Собрать runtime вокруг engine и адаптера брокера.

        Raises:
            ConfigurationError: heartbeat имеет неположительный интервал.
        """
        if heartbeat_every.total_seconds() <= 0:
            message = "heartbeat_every должен быть > 0"
            raise ConfigurationError(message)
        self.completer = completer
        self.broker = broker
        self.dispatcher = dispatcher
        self.tree_cache = tree_cache
        self.heartbeat_every = heartbeat_every

    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R | None]]:
        """Обернуть только ``async def`` и сохранить метаданные.

        Returns:
            Async around-обёртка задачи.

        Raises:
            ConfigurationError: передана не ``async def`` функция.
        """
        if not inspect.iscoroutinefunction(fn):
            message = "th.tracked поддерживает только async def"
            raise ConfigurationError(message)
        task = cast("_AsyncTask", fn)

        async def erased(*args: object, **kwargs: object) -> object:
            marker = kwargs.pop("_th", None)
            if marker is None:
                return await task(*args, **kwargs)
            if not isinstance(marker, Mapping):
                raise ConfigurationError(_BAD_MARKER)
            normalized = cast("Mapping[object, object]", marker)
            token = _runtime.set(self)
            try:
                if "c" in normalized:
                    return await self._callback(task, args, kwargs, marker=normalized)
                return await self._item(task, args, kwargs, marker=normalized)
            finally:
                _runtime.reset(token)

        wrapped = functools.update_wrapper(erased, fn)
        return cast("Callable[P, Awaitable[R | None]]", wrapped)

    @staticmethod
    async def _callback(
        task: _AsyncTask,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        marker: Mapping[object, object],
    ) -> object:
        context = CallbackContext(
            callback_id=_uuid(marker.get("c")),
            batch_id=_uuid(marker.get("b")),
            summary=marker.get("s"),
        )
        with activate_callback(context):
            return await task(*args, **kwargs)

    async def _item(
        self,
        task: _AsyncTask,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        marker: Mapping[object, object],
    ) -> object:
        ref = ItemRef(id=_uuid(marker.get("i")), batch_id=_uuid(marker.get("b")))
        claim = await self.completer.claim(ref)
        if not claim.run:
            return None
        tree = self.tree_cache.get(ref.batch_id)
        if tree is None:
            async with self.completer.engine.connect() as conn:
                tree = await self.tree_cache.load(conn, self.completer.tables, ref.batch_id)
        context = ItemContext(
            ref=ref,
            attempt=claim.attempt,
            depth=claim.depth,
            tree=tree,
            make_call=self._make_call,
            completer=self.completer,
        )
        heartbeat = asyncio.create_task(
            self._heartbeat(context), name=f"tallyho-heartbeat-{ref.id}"
        )
        # Закрытие установки отменит heartbeat: продлевать lease будет некому.
        self.completer.attach(heartbeat)
        try:
            with activate_item(context):
                return await self._invoke(task, args, kwargs, context=context)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _invoke(
        self,
        task: _AsyncTask,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        context: ItemContext,
    ) -> object:
        try:
            result = await task(*args, **kwargs)
        except asyncio.CancelledError:
            # Установка уже закрыта — Item вернул в outbox aclose; отмену не подменяем.
            with contextlib.suppress(ClosedError):
                _ = await self.completer.release(context.ref)
            raise
        except BaseException as exc:
            if isinstance(self.broker, CancellationClassifier) and self.broker.is_cancelled(exc):
                value = FinishResult(
                    result_class=ResultClass.CANCELLED,
                    label="cancelled",
                    error={"type": type(exc).__name__, "message": str(exc)},
                    metrics=context.metrics,
                )
                _ = await self.completer.finish(context.ref, value)
            elif self.broker.retry_verdict(exc) is Verdict.RETRY:
                _ = await self.completer.release(context.ref)
            else:
                value = FinishResult(
                    result_class=ResultClass.ERROR,
                    label="exhausted",
                    error={"type": type(exc).__name__, "message": str(exc)},
                    metrics=context.metrics,
                )
                _ = await self.completer.finish(context.ref, value)
            raise
        if not context.completed_in_user_tx:
            _ = await self.completer.finish(context.ref, context.finish_result())
        return result

    async def _heartbeat(self, context: ItemContext) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_every.total_seconds())
            alive = await self.completer.heartbeat(
                context.ref,
                progress_done=context.progress_done,
                progress_total=context.progress_total,
            )
            if not alive:
                return
            async with self.completer.engine.connect() as conn:
                requested_at = await conn.scalar(
                    select(self.completer.tables.batch.c.cancel_requested_at).where(
                        self.completer.tables.batch.c.id == context.batch_id
                    )
                )
            context.cancel_requested = requested_at is not None

    def _make_call(
        self, fn: object, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> TaskCall:
        callable_fn = cast("Callable[[], object]", fn)
        return TaskCall(task_name=self.dispatcher.task_name(callable_fn), args=args, kwargs=kwargs)


def bind_runtime(runtime: TaskRuntime) -> None:
    """Установить runtime для последующих вызовов модульного ``tracked``."""
    _installed.clear()
    _installed.append(runtime)


def build_runtime(
    *,
    completer: object,
    broker: Runtime,
    dispatcher: Dispatcher,
    tree_cache: object,
    heartbeat_every: timedelta,
) -> TaskRuntime:
    """Собрать и активировать runtime из непрозрачных engine-зависимостей.

    Returns:
        Runtime, готовый для передачи broker adapter через ``WorkerServices``.
    """
    runtime = TaskRuntime(
        completer=cast("Completer", completer),
        broker=broker,
        dispatcher=dispatcher,
        tree_cache=cast("TreeCache", tree_cache),
        heartbeat_every=heartbeat_every,
    )
    bind_runtime(runtime)
    return runtime


def current_runtime() -> TaskRuntime:
    """Вернуть runtime текущей задачи или установленный runtime.

    Returns:
        Активная установка runtime.

    Raises:
        ConfigurationError: runtime ещё не установлен.
    """
    runtime = _runtime.get() or (_installed[0] if _installed else None)
    if runtime is None:
        raise ConfigurationError(_NOT_INSTALLED)
    return runtime


def tracked(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R | None]]:
    """Обернуть задачу установленным runtime.

    Returns:
        Async around-обёртка с сохранённой сигнатурой.

    Raises:
        ConfigurationError: передана не ``async def`` функция.
    """
    if not inspect.iscoroutinefunction(fn):
        message = "th.tracked поддерживает только async def"
        raise ConfigurationError(message)
    task = cast("_AsyncTask", fn)

    async def erased(*args: object, **kwargs: object) -> object:
        wrapped = current_runtime().wrap(task)
        return await wrapped(*args, **kwargs)

    wrapped = functools.update_wrapper(erased, fn)
    return cast("Callable[P, Awaitable[R | None]]", wrapped)


def _uuid(value: object) -> UUID:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError as exc:
            raise ConfigurationError(_BAD_MARKER) from exc
    raise ConfigurationError(_BAD_MARKER)
