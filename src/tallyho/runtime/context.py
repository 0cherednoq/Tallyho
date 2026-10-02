"""Контекст выполняемой Item и фасады ``th.item`` / ``th.callback``."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, ParamSpec, Protocol, Self, TypeVar, cast, overload

from tallyho.engine.completer import ExpectRequest, FinishResult, SpawnRequest, SubBatchRequest
from tallyho.engine.producer import SubBatchSpec
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError, LeaseLostError
from tallyho.model.states import OnFeederFailed, ResultClass
from tallyho.storage.tx import after_commit, after_commit_pending, resolve_connection

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator, Iterable, Mapping
    from datetime import datetime, timedelta
    from uuid import UUID

    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from tallyho.engine.completer import Completer, ItemRef
    from tallyho.engine.producer import CallbackName
    from tallyho.engine.spawn import TreeSnapshot
    from tallyho.model.policy import FailurePolicy

__all__ = [
    "CallFactory",
    "CallbackContext",
    "CallbackFacade",
    "ItemContext",
    "ItemFacade",
    "RuntimeSubBatch",
    "activate_callback",
    "activate_item",
    "callback",
    "item",
]

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")

_NO_CONTEXT = "операция th.item доступна только внутри отслеживаемой задачи"
_OTHER_TRANSACTION = (
    "complete_in уже вызван в другой, ещё не завершённой транзакции: "
    "Item завершается в одной транзакции"
)


class CallFactory(Protocol):
    """Преобразование Python-вызова во внутренний ``TaskCall``."""

    def __call__(
        self, fn: object, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> TaskCall:
        """Создать вызов задачи."""
        ...


@dataclass(frozen=True, slots=True)
class CallbackContext:
    """Служебная информация текущей callback-задачи."""

    callback_id: UUID
    batch_id: UUID
    summary: object = None


@dataclass(slots=True)
class RuntimeSubBatch:
    """Буфер динамического под-батча, добавляемый атомарно с finish Item."""

    context: ItemContext
    spec: SubBatchSpec
    calls: list[TaskCall] = field(default_factory=list[TaskCall])
    sealed: bool = False

    async def __aenter__(self) -> Self:
        """Вернуть этот builder.

        Returns:
            Текущий builder.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        """При успешном выходе добавить под-батч в буфер Item."""
        if exc_type is None:
            self.seal()

    def add(
        self,
        fn: Callable[P, Awaitable[R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """Добавить задачу в под-батч."""
        self.add_call(self.context.make_call(fn, args, kwargs))

    def add_call(self, call: TaskCall) -> None:
        """Добавить подготовленный вызов.

        Raises:
            ConfigurationError: builder уже закрыт.
        """
        if self.sealed:
            message = "динамический под-батч уже закрыт"
            raise ConfigurationError(message)
        self.calls.append(call)

    def add_calls(self, calls: Iterable[TaskCall]) -> None:
        """Добавить несколько подготовленных вызовов."""
        for call in calls:
            self.add_call(call)

    def map(self, fn: Callable[[T], Awaitable[R]], values: Iterable[T]) -> None:
        """Добавить ``fn(value)`` для каждого значения."""
        for value in values:
            self.add(fn, value)

    def expect(self, total: int) -> None:
        """Задать ожидаемое число Items под-батча."""
        self.spec = replace(self.spec, expected_total=total)

    def seal(self) -> None:
        """Закрыть builder и перенести его снимок в ItemContext."""
        if not self.sealed:
            self.sealed = True
            self.context.sub_batches.append(SubBatchRequest(spec=self.spec, calls=self.calls))


@dataclass(frozen=True, slots=True, kw_only=True)
class _Completion:
    """Запись ``complete_in`` в транзакции пользователя, ещё не закоммиченная."""

    target: AsyncSession | AsyncConnection
    connection: Connection | None
    committed: Callable[[], None]


@dataclass(slots=True, kw_only=True)
class ItemContext:
    """Буферы одной попытки пользовательской задачи."""

    ref: ItemRef
    attempt: int
    depth: int
    tree: TreeSnapshot
    make_call: CallFactory
    completer: Completer
    progress_done: int | None = None
    progress_total: int | None = None
    completed_in_user_tx: bool = False
    lease_lost: bool = False
    """``complete_in`` обнаружил, что попытка больше не владеет Item (UC-08)."""
    completion: _Completion | None = None
    """Последняя запись ``complete_in``; в силе, пока её колбэк ждёт commit."""
    cancel_requested: bool = False
    metrics: dict[str, int] = field(default_factory=dict[str, int])
    spawns: list[SpawnRequest] = field(default_factory=list[SpawnRequest])
    expects: list[ExpectRequest] = field(default_factory=list[ExpectRequest])
    sub_batches: list[SubBatchRequest] = field(default_factory=list[SubBatchRequest])
    _result_class: ResultClass = ResultClass.OK
    _label: str | None = None
    _result: object = None
    _error: object = None
    _mark: bool | None = None

    @property
    def id(self) -> UUID:
        """Идентификатор текущего Item."""
        return self.ref.id

    @property
    def batch_id(self) -> UUID:
        """Идентификатор батча текущего Item."""
        return self.ref.batch_id

    @overload
    def spawn(
        self,
        fn: Callable[P, Awaitable[R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[], Awaitable[R]],
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[T], Awaitable[R]],
        arg: T,
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[T, U], Awaitable[R]],
        arg1: T,
        arg2: U,
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[T, U, V], Awaitable[R]],
        arg1: T,
        arg2: U,
        arg3: V,
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    def spawn(
        self,
        fn: object,
        *args: object,
        **kwargs: object,
    ) -> None:
        """Добавить дочерний вызов в атомарный буфер finish."""
        into = cast("str | UUID | None", kwargs.pop("into", None))
        key = cast("str | None", kwargs.pop("key", None))
        call = self.make_call(fn, args, kwargs)
        self.spawn_call(call if key is None else call.opts(key=key), into=into)

    def spawn_call(self, call: TaskCall, *, into: str | UUID | None = None) -> None:
        """Добавить подготовленный дочерний вызов."""
        self.spawns.append(SpawnRequest(route=self.tree.route(self.batch_id, into), call=call))

    def sub_batch(  # ruff: ignore[too-many-arguments]  # API задан ARCHITECTURE §11.2
        self,
        key: str,
        *,
        kind: str | None = None,
        start_at: datetime | None = None,
        deadline: datetime | timedelta | None = None,
        callbacks: Mapping[CallbackName, TaskCall] | None = None,
        failure_policy: FailurePolicy | None = None,
        max_in_flight: int | None = None,
        expected_total: int | None = None,
        on_feeder_failed: OnFeederFailed = OnFeederFailed.SEAL,
        max_depth: int | None = None,
    ) -> RuntimeSubBatch:
        """Создать асинхронный builder динамического под-батча.

        Returns:
            Буфер под-батча.
        """
        return RuntimeSubBatch(
            self,
            SubBatchSpec(
                key=key,
                kind=kind,
                start_at=start_at,
                deadline=deadline,
                callbacks={} if callbacks is None else callbacks,
                failure_policy=failure_policy,
                max_in_flight=max_in_flight,
                expected_total=expected_total,
                on_feeder_failed=on_feeder_failed,
                max_depth=max_depth,
            ),
        )

    def expect(self, total: int, *, into: str | UUID | None = None) -> None:
        """Монотонно повысить expected целевого батча."""
        self.expects.append(ExpectRequest(route=self.tree.route(self.batch_id, into), total=total))

    def progress(self, done: int, total: int | None = None) -> None:
        """Обновить диагностический прогресс lease.

        Raises:
            ConfigurationError: значение отрицательное или является bool.
        """
        invalid_total = total is not None and (isinstance(total, bool) or total < 0)
        if isinstance(done, bool) or done < 0 or invalid_total:
            message = "progress требует неотрицательные целые done/total"
            raise ConfigurationError(message)
        self.progress_done = done
        self.progress_total = total

    def incr(self, name: str, value: int = 1) -> None:
        """Прибавить пользовательскую метрику.

        Raises:
            ConfigurationError: имя пусто или value не является целым.
        """
        if not name or isinstance(value, bool):
            message = "имя метрики должно быть непустым, value — целым"
            raise ConfigurationError(message)
        self.metrics[name] = self.metrics.get(name, 0) + value

    def ok(
        self, label: str | None = None, *, result: object = None, mark: bool | None = None
    ) -> None:
        """Установить успешный итог попытки."""
        self._set_result(ResultClass.OK, label, result=result, mark=mark)

    def skip(self, label: str | None = None, *, mark: bool | None = None) -> None:
        """Установить пропущенный итог попытки."""
        self._set_result(ResultClass.SKIP, label, mark=mark)

    def error(
        self, label: str | None = None, *, detail: object = None, mark: bool | None = None
    ) -> None:
        """Установить ошибочный итог без выбрасывания исключения."""
        self._set_result(ResultClass.ERROR, label, error=detail, mark=mark)

    async def complete_in(self, session: AsyncSession | AsyncConnection) -> None:
        """Атомарно завершить Item в транзакции пользователя (ARCHITECTURE UC-08).

        Записывает итог и буферы, накопленные к этому моменту, если попытка
        ещё владеет Item. Повторный вызов после commit и в той же транзакции,
        пока первая запись в силе, ничего не делает; после отката транзакции
        или savepoint записывает заново.

        Args:
            session: Открытая сессия или соединение пользователя.

        Raises:
            LeaseLostError: Item уже завершён без этой попытки или его lease
                перехвачен. Ничего не записано; исключение должно выйти из
                транзакции пользователя, чтобы та откатилась.
            ConfigurationError: Item уже завершается в другой, ещё открытой
                транзакции этой же попытки.
        """
        if self.completed_in_user_tx:
            return
        connection = (await resolve_connection(session)).sync_connection
        pending = self.completion
        if pending is not None and await after_commit_pending(pending.target, pending.committed):
            if pending.connection is not connection:
                raise ConfigurationError(_OTHER_TRANSACTION)
            return
        changed = await self.completer.complete_in(
            session, self.ref, self.finish_result(), attempt=self.attempt
        )
        if not changed:
            self.lease_lost = True
            raise LeaseLostError(self.id)

        def committed() -> None:
            self.completed_in_user_tx = True

        self.completion = _Completion(target=session, connection=connection, committed=committed)
        await after_commit(session, committed)

    def cancelled(self) -> bool:
        """Вернуть запрос кооперативной отмены, замеченный heartbeat.

        Returns:
            Флаг отмены батча.
        """
        return self.cancel_requested

    def finish_result(self) -> FinishResult:
        """Снять неизменяемый итог накопленных буферов.

        Returns:
            Значение для Completer.
        """
        return FinishResult(
            result_class=self._result_class,
            label=self._label,
            result=self._result,
            error=self._error,
            metrics=self.metrics,
            mark=self._mark,
            spawns=self.spawns,
            expects=self.expects,
            sub_batches=self.sub_batches,
        )

    def _set_result(
        self,
        result_class: ResultClass,
        label: str | None,
        *,
        result: object = None,
        error: object = None,
        mark: bool | None = None,
    ) -> None:
        self._result_class = result_class
        self._label = label
        self._result = result
        self._error = error
        self._mark = mark


_item_context: ContextVar[ItemContext | None] = ContextVar("tallyho_item", default=None)
_callback_context: ContextVar[CallbackContext | None] = ContextVar("tallyho_callback", default=None)


@contextmanager
def activate_item(context: ItemContext) -> Generator[None, None, None]:
    """Установить ItemContext на время пользовательской функции."""
    token = _item_context.set(context)
    try:
        yield
    finally:
        _item_context.reset(token)


@contextmanager
def activate_callback(context: CallbackContext) -> Generator[None, None, None]:
    """Установить CallbackContext на время callback-задачи."""
    token = _callback_context.set(context)
    try:
        yield
    finally:
        _callback_context.reset(token)


class ItemFacade:
    """Модуль-подобный фасад операций текущего Item."""

    @staticmethod
    def current() -> ItemContext | None:
        """Вернуть текущий контекст или ``None`` вне tracked-задачи.

        Returns:
            Активный ItemContext.
        """
        return _item_context.get()

    def id(self) -> UUID | None:
        """Вернуть идентификатор Item или ``None`` вне задачи.

        Returns:
            Идентификатор активного Item.
        """
        context = self.current()
        return context.id if context is not None else None

    @overload
    def spawn(
        self,
        fn: Callable[P, Awaitable[R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[], Awaitable[R]],
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[T], Awaitable[R]],
        arg: T,
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[T, U], Awaitable[R]],
        arg1: T,
        arg2: U,
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    @overload
    def spawn(
        self,
        fn: Callable[[T, U, V], Awaitable[R]],
        arg1: T,
        arg2: U,
        arg3: V,
        *,
        into: str | UUID | None = None,
        key: str | None = None,
    ) -> None: ...

    def spawn(
        self,
        fn: object,
        *args: object,
        **kwargs: object,
    ) -> None:
        """Делегировать ``spawn``; вне задачи — no-op."""
        if (context := self.current()) is not None:
            into = cast("str | UUID | None", kwargs.pop("into", None))
            key = cast("str | None", kwargs.pop("key", None))
            call = context.make_call(fn, args, kwargs)
            context.spawn_call(call if key is None else call.opts(key=key), into=into)

    def spawn_call(self, call: TaskCall, *, into: str | UUID | None = None) -> None:
        """Делегировать ``spawn_call``; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.spawn_call(call, into=into)

    def sub_batch(  # ruff: ignore[too-many-arguments]  # API задан ARCHITECTURE §11.2
        self,
        key: str,
        *,
        kind: str | None = None,
        start_at: datetime | None = None,
        deadline: datetime | timedelta | None = None,
        callbacks: Mapping[CallbackName, TaskCall] | None = None,
        failure_policy: FailurePolicy | None = None,
        max_in_flight: int | None = None,
        expected_total: int | None = None,
        on_feeder_failed: OnFeederFailed = OnFeederFailed.SEAL,
        max_depth: int | None = None,
    ) -> RuntimeSubBatch:
        """Создать под-батч.

        Returns:
            Буфер под-батча.

        Raises:
            ConfigurationError: вызов сделан вне tracked-задачи.
        """
        if (context := self.current()) is None:
            raise ConfigurationError(_NO_CONTEXT)
        return context.sub_batch(
            key,
            kind=kind,
            start_at=start_at,
            deadline=deadline,
            callbacks=callbacks,
            failure_policy=failure_policy,
            max_in_flight=max_in_flight,
            expected_total=expected_total,
            on_feeder_failed=on_feeder_failed,
            max_depth=max_depth,
        )

    def expect(self, total: int, *, into: str | UUID | None = None) -> None:
        """Делегировать ``expect``; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.expect(total, into=into)

    def progress(self, done: int, total: int | None = None) -> None:
        """Делегировать прогресс; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.progress(done, total)

    def incr(self, name: str, value: int = 1) -> None:
        """Делегировать метрику; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.incr(name, value)

    def ok(
        self, label: str | None = None, *, result: object = None, mark: bool | None = None
    ) -> None:
        """Делегировать успешный итог; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.ok(label, result=result, mark=mark)

    def skip(self, label: str | None = None, *, mark: bool | None = None) -> None:
        """Делегировать пропуск; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.skip(label, mark=mark)

    def error(
        self, label: str | None = None, *, detail: object = None, mark: bool | None = None
    ) -> None:
        """Делегировать ошибочный итог; вне задачи — no-op."""
        if (context := self.current()) is not None:
            context.error(label, detail=detail, mark=mark)

    async def complete_in(self, session: AsyncSession | AsyncConnection) -> None:
        """Делегировать ``complete_in``; вне задачи — no-op.

        Исключения — как у :meth:`ItemContext.complete_in`: ``LeaseLostError``,
        если попытка больше не владеет Item.
        """
        if (context := self.current()) is not None:
            await context.complete_in(session)

    def cancelled(self) -> bool:
        """Вернуть ``False`` вне задачи или флаг кооперативной отмены.

        Returns:
            Флаг отмены.
        """
        context = self.current()
        return context.cancelled() if context is not None else False


class CallbackFacade:
    """Фасад контекста callback-задачи."""

    @staticmethod
    def current() -> CallbackContext | None:
        """Вернуть текущий callback-контекст или ``None``.

        Returns:
            Активный CallbackContext.
        """
        return _callback_context.get()


item = ItemFacade()
callback = CallbackFacade()
