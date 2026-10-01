"""Публичные builder и handle для деревьев батчей."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, Self, TypeVar, cast

from tallyho.engine.public import BatchDefinition
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import OnFeederFailed

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Callable,
        Collection,
        Iterable,
        Mapping,
        Sequence,
    )
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime
    from types import TracebackType
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from tallyho.engine.public import BatchWriter, EngineFacade
    from tallyho.model.policy import FailurePolicy
    from tallyho.model.states import ItemState
    from tallyho.model.views import BatchView, InFlightItem, ItemView
    from tallyho.protocols.broker import Dispatcher

__all__ = ["BatchBuilder", "BatchHandle"]

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

_NOT_ENTERED = "BatchBuilder ещё не вошёл в async with"
_CLOSED = "BatchBuilder уже закрыт"
_FOREIGN_FEEDER = "fed_by должен ссылаться на под-батчи того же дерева"


def _callbacks(
    *,
    on_succeeded: TaskCall | None,
    on_completed_with_errors: TaskCall | None,
    on_failed: TaskCall | None,
    on_cancelled: TaskCall | None,
    on_finalized_task: TaskCall | None,
) -> dict[str, TaskCall]:
    values = {
        "on_succeeded": on_succeeded,
        "on_completed_with_errors": on_completed_with_errors,
        "on_failed": on_failed,
        "on_cancelled": on_cancelled,
        "on_finalized_task": on_finalized_task,
    }
    return {name: call for name, call in values.items() if call is not None}


def _feeder_policy(value: OnFeederFailed | str) -> OnFeederFailed:
    if isinstance(value, OnFeederFailed):
        return value
    try:
        return {"seal": OnFeederFailed.SEAL, "cancel": OnFeederFailed.CANCEL}[value]
    except KeyError as exc:
        message = "on_feeder_failed должен быть 'seal' или 'cancel'"
        raise ConfigurationError(message) from exc


@dataclass(eq=False, slots=True)
class BatchBuilder:
    """Транзакционный конструктор корня или под-батча."""

    _engine: EngineFacade
    _adapter: Dispatcher
    _spec: BatchDefinition
    _target: AsyncSession | AsyncConnection | None = None
    _parent: BatchBuilder | None = None
    _feeders: tuple[BatchBuilder, ...] = ()
    _children: list[BatchBuilder] = field(default_factory=list)
    _writer_context: AbstractAsyncContextManager[BatchWriter] | None = None
    _writer: BatchWriter | None = None
    _id: UUID | None = None
    _root_id: UUID | None = None
    _sealed: bool = False

    async def __aenter__(self) -> Self:
        """Открыть writer и создать батч в его транзакции.

        Returns:
            Этот builder.
        """
        if self._parent is not None:
            await self._ensure_created()
            return self
        context = self._engine.writer(self._target)
        self._writer_context = context
        self._writer = await context.__aenter__()
        try:
            await self._ensure_created()
        except BaseException:
            await context.__aexit__(*self._exc_info())
            self._writer = None
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Закрыть батчи при успехе и завершить writer-транзакцию.

        Returns:
            ``False``: исключения пользователя не подавляются.

        Raises:
            ConfigurationError: корневой builder не был открыт.
        """
        if self._parent is not None:
            if exc_type is None and not self._feeders:
                await self.seal()
            return False
        context = self._writer_context
        if context is None:
            raise ConfigurationError(_NOT_ENTERED)
        try:
            if exc_type is None:
                for child in self._walk_children():
                    await child._ensure_created()  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
                for child in reversed(self._walk_children()):
                    if not child._feeders:  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
                        await child.seal()
                await self.seal()
        except BaseException as close_exc:
            _ = await context.__aexit__(type(close_exc), close_exc, close_exc.__traceback__)
            raise
        finally:
            self._writer = None
        return bool(await context.__aexit__(exc_type, exc, traceback))

    async def add(
        self,
        fn: Callable[P, Awaitable[R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """Добавить один вызов задачи."""
        await self.add_calls([self._make_call(fn, args, kwargs)])

    async def map(self, fn: Callable[[T], Awaitable[R]], values: Iterable[T]) -> None:
        """Добавить ``fn(value)`` для каждого элемента."""
        await self.add_calls(self._make_call(fn, (value,), {}) for value in values)

    async def add_calls(self, calls: Iterable[TaskCall]) -> None:
        """Добавить подготовленные вызовы в батч."""
        self._check_open()
        await self._ensure_created()
        writer = self._require_writer()
        await writer.add(self._require_id(), tuple(calls))

    def sub_batch(  # ruff: ignore[too-many-arguments]  # публичный API задан ARCHITECTURE §11.2
        self,
        key: str,
        *,
        kind: str | None = None,
        fed_by: Sequence[BatchBuilder] = (),
        on_feeder_failed: OnFeederFailed | str = OnFeederFailed.SEAL,
        start_at: datetime | None = None,
        deadline: datetime | timedelta | None = None,
        on_succeeded: TaskCall | None = None,
        on_completed_with_errors: TaskCall | None = None,
        on_failed: TaskCall | None = None,
        on_cancelled: TaskCall | None = None,
        on_finalized_task: TaskCall | None = None,
        failure_policy: FailurePolicy | None = None,
        max_in_flight: int | None = None,
        expected_total: int | None = None,
        max_depth: int | None = None,
    ) -> BatchBuilder:
        """Зарегистрировать дочерний батч; запись выполняется лениво.

        Returns:
            Builder нового или идемпотентно существующего под-батча.

        Raises:
            ConfigurationError: builder закрыт или feeder относится к другому дереву.
        """
        self._check_open()
        feeders = tuple(fed_by)
        root = self._root()
        if any(
            feeder._root() is not root  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
            for feeder in feeders
        ):
            raise ConfigurationError(_FOREIGN_FEEDER)
        child = BatchBuilder(
            self._engine,
            self._adapter,
            BatchDefinition(
                kind=kind,
                key=key,
                start_at=start_at,
                deadline=deadline,
                callbacks=_callbacks(
                    on_succeeded=on_succeeded,
                    on_completed_with_errors=on_completed_with_errors,
                    on_failed=on_failed,
                    on_cancelled=on_cancelled,
                    on_finalized_task=on_finalized_task,
                ),
                failure_policy=failure_policy,
                max_in_flight=max_in_flight,
                expected_total=expected_total,
                on_feeder_failed=_feeder_policy(on_feeder_failed),
                max_depth=max_depth,
            ),
            _parent=self,
            _feeders=feeders,
        )
        self._children.append(child)
        return child

    async def expect(self, total: int) -> None:
        """Монотонно повысить ожидаемое число Items."""
        self._check_open()
        await self._ensure_created()
        await self._require_writer().expect(self._require_id(), total)

    async def seal(self) -> None:
        """Закрыть батч вручную; повторный вызов безопасен."""
        if self._sealed:
            return
        await self._ensure_created()
        await self._require_writer().seal(self._require_id())
        self._sealed = True

    @property
    def handle(self) -> BatchHandle:
        """Handle уже созданного батча."""
        return BatchHandle(self._engine, self._require_id())

    async def _ensure_created(self) -> None:
        if self._id is not None:
            return
        if self._parent is None:
            reference = await self._require_writer().create_root(self._spec)
        else:
            await self._parent._ensure_created()  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
            for feeder in self._feeders:
                await feeder._ensure_created()  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
            reference = await self._require_writer().create_child(
                self._parent._require_id(),  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
                replace(
                    self._spec,
                    fed_by=tuple(
                        feeder._require_id()  # ruff: ignore[private-member-access]  # узлы одного builder-дерева
                        for feeder in self._feeders
                    ),
                ),
            )
        self._id = reference.id
        self._root_id = reference.root_id

    def _make_call(
        self, fn: object, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> TaskCall:
        callable_fn = cast("Callable[[], object]", fn)
        return TaskCall(task_name=self._adapter.task_name(callable_fn), args=args, kwargs=kwargs)

    def _require_writer(self) -> BatchWriter:
        writer = self._root()._writer  # ruff: ignore[private-member-access]  # writer принадлежит корню дерева
        if writer is None:
            raise ConfigurationError(_NOT_ENTERED)
        return writer

    def _require_id(self) -> UUID:
        if self._id is None:
            raise ConfigurationError(_NOT_ENTERED)
        return self._id

    def _check_open(self) -> None:
        if self._sealed:
            raise ConfigurationError(_CLOSED)

    def _root(self) -> BatchBuilder:
        node = self
        while node._parent is not None:
            node = node._parent
        return node

    def _walk_children(self) -> list[BatchBuilder]:
        found: list[BatchBuilder] = []
        for child in self._children:
            found.append(child)
            found.extend(child._walk_children())  # ruff: ignore[private-member-access]  # рекурсивный обход своего дерева
        return found

    @staticmethod
    def _exc_info() -> tuple[
        type[BaseException] | None, BaseException | None, TracebackType | None
    ]:
        import sys  # ruff: ignore[import-outside-top-level]  # только аварийный __aenter__

        return sys.exc_info()


@dataclass(frozen=True, slots=True)
class BatchHandle:
    """Ссылка на батч с чтением и управляющими операциями."""

    _engine: EngineFacade
    id: UUID

    async def view(self) -> BatchView:
        """Прочитать атомарный снимок поддерева.

        Returns:
            Текущий снимок.
        """
        return await self._engine.view(self.id)

    def watch(self) -> AsyncIterator[BatchView]:
        """Следить за изменениями до терминального снимка.

        Returns:
            Асинхронный поток снимков.
        """
        return self._engine.watch(self.id)

    async def wait(
        self,
        timeout: float | timedelta | None = None,  # ruff: ignore[async-function-with-timeout]  # имя задано публичным API
    ) -> BatchView:
        """Дождаться терминального состояния с необязательным таймаутом.

        Returns:
            Терминальный снимок.
        """
        seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        async with asyncio.timeout(seconds):
            async for value in self.watch():
                if value.state.is_terminal:
                    return value
        return await self.view()

    async def in_flight(self, limit: int = 100) -> list[InFlightItem]:
        """Вернуть выполняющиеся Items.

        Returns:
            Не более ``limit`` активных lease.
        """
        return await self._engine.in_flight(self.id, limit)

    def items(
        self,
        *,
        states: Collection[ItemState] | None = None,
        labels: Collection[str] | None = None,
    ) -> AsyncIterator[ItemView]:
        """Поток Items этого батча по состояниям, меткам или их пересечению.

        ``labels`` находит только помеченные Items (по умолчанию — ошибки),
        ``states`` — Items в любом состоянии, включая ``CANCELLED``. Хотя бы
        один фильтр обязателен; порядок выдачи контрактом не является
        (ARCHITECTURE §11.2).

        Вызов без фильтров, с пустым фильтром или со строкой вместо коллекции
        сразу даёт ``ConfigurationError``.

        Returns:
            Асинхронный поток ``ItemView``.
        """
        return self._engine.items(self.id, states=states, labels=labels)

    async def child(self, key: str) -> BatchHandle:
        """Найти прямого потомка по ключу.

        Returns:
            Handle потомка.
        """
        return BatchHandle(self._engine, await self._engine.child(self.id, key))

    async def reschedule(
        self,
        start_at: datetime,
        *,
        session: AsyncSession | AsyncConnection | None = None,
    ) -> int:
        """Перенести ещё не отправленные Items всего дерева.

        Returns:
            Число уже отправленных и потому не перенесённых Items.
        """
        return await self._engine.reschedule(session, self.id, start_at)

    async def pause(self, *, session: AsyncSession | AsyncConnection | None = None) -> None:
        """Поставить поддерево на паузу."""
        await self._engine.pause(session, self.id)

    async def resume(self, *, session: AsyncSession | AsyncConnection | None = None) -> None:
        """Снять паузу с поддерева."""
        await self._engine.resume(session, self.id)

    async def cancel(self, *, session: AsyncSession | AsyncConnection | None = None) -> None:
        """Запросить отмену поддерева."""
        await self._engine.cancel(session, self.id)

    async def retry_failed(
        self,
        *,
        labels: Sequence[str] | None = None,
        session: AsyncSession | AsyncConnection | None = None,
    ) -> int:
        """Вернуть выбранные ошибочные Items в active.

        Returns:
            Число повторно поставленных Items.
        """
        return await self._engine.retry_failed(session, self.id, labels)

    async def retry_finalize(
        self, *, session: AsyncSession | AsyncConnection | None = None
    ) -> None:
        """Сбросить backoff tx-хука и повторить финализацию."""
        await self._engine.retry_finalize(session, self.id)

    async def release(self, *, session: AsyncSession | AsyncConnection | None = None) -> None:
        """Разрешить retention удалить терминальное дерево."""
        await self._engine.release(session, self.id)
