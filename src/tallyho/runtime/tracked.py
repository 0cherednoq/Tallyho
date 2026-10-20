"""Around-обёртка tracked-задач: claim, heartbeat и терминальный исход."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import logging
from collections.abc import Mapping
from contextvars import ContextVar
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, cast, final
from uuid import UUID

from sqlalchemy import select

from tallyho.engine.completer import FinishResult, ItemRef
from tallyho.model.calls import TaskCall
from tallyho.model.errors import (
    ClosedError,
    CompleterError,
    ConfigurationError,
    LeaseLostError,
    TallyhoError,
)
from tallyho.model.outcomes import Defer, Error, Ok, Skip
from tallyho.model.states import ResultClass
from tallyho.protocols.broker import CancellationClassifier, Verdict
from tallyho.runtime.context import (
    CallbackContext,
    WorkItem,
    activate_callback,
    activate_item,
)
from tallyho.runtime.exceptions import ExceptionPolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from tallyho.engine.completer import Completer
    from tallyho.engine.spawn import TreeCache, TreeSnapshot
    from tallyho.protocols.broker import Dispatcher, Runtime
    from tallyho.runtime.middleware import ItemMiddleware

P = ParamSpec("P")
R = TypeVar("R")

__all__ = ["TaskRuntime", "bind_runtime", "build_runtime", "current_runtime", "tracked"]

_log = logging.getLogger(__name__)

_runtime: ContextVar[TaskRuntime | None] = ContextVar("tallyho_runtime", default=None)
_installed: list[TaskRuntime] = []
_DEFAULT_DEFER_DELAY = timedelta(seconds=60)
_NOT_INSTALLED = "tallyho runtime не установлен"
_BAD_MARKER = "служебный аргумент _th имеет неверный формат"
_TREE_FAILED = "не удалось прочитать дерево батча после claim"


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
    defer_default_delay: timedelta
    middleware: tuple[ItemMiddleware, ...]

    def __init__(  # ruff: ignore[too-many-arguments]  # композиционный конструктор установки
        self,
        *,
        completer: Completer,
        broker: Runtime,
        dispatcher: Dispatcher,
        tree_cache: TreeCache,
        heartbeat_every: timedelta,
        defer_default_delay: timedelta = _DEFAULT_DEFER_DELAY,
        middleware: Sequence[ItemMiddleware] = (),
    ) -> None:
        """Собрать runtime вокруг engine и адаптера брокера.

        Raises:
            ConfigurationError: heartbeat имеет неположительный интервал или
                элемент ``middleware`` не вызываемый.
        """
        if heartbeat_every.total_seconds() <= 0:
            message = "heartbeat_every должен быть > 0"
            raise ConfigurationError(message)
        # Элементы приходят из composition root как непрозрачные объекты
        # (api не знает ItemMiddleware), поэтому проверяем их здесь.
        for entry in cast("Sequence[object]", middleware):
            if not callable(entry):
                message = "middleware должен быть вызываемым: async (work, call_next)"
                raise ConfigurationError(message)
        self.completer = completer
        self.broker = broker
        self.dispatcher = dispatcher
        self.tree_cache = tree_cache
        self.heartbeat_every = heartbeat_every
        self.defer_default_delay = defer_default_delay
        self.middleware = tuple(middleware)

    def wrap(
        self, fn: Callable[P, Awaitable[R]], *, policy: object = None
    ) -> Callable[P, Awaitable[R | None]]:
        """Обернуть только ``async def`` и сохранить метаданные.

        Args:
            fn: Пользовательская задача.
            policy: :class:`~tallyho.runtime.exceptions.ExceptionPolicy` этой
                задачи — судьба её исключений, объявленная при регистрации
                (UC-18). ``None`` — правил нет, путь прежний.

        Returns:
            Async around-обёртка задачи.

        Raises:
            ConfigurationError: передана не ``async def`` функция или
                ``policy`` не является политикой исключений.
        """
        if not inspect.iscoroutinefunction(fn):
            message = "th.tracked поддерживает только async def"
            raise ConfigurationError(message)
        if policy is not None and not isinstance(policy, ExceptionPolicy):
            message = "policy должен быть ExceptionPolicy"
            raise ConfigurationError(message)
        rules = policy if isinstance(policy, ExceptionPolicy) else None
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
                return await self._item(task, args, kwargs, marker=normalized, policy=rules)
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
        policy: ExceptionPolicy | None = None,
    ) -> object:
        ref = ItemRef(id=_uuid(marker.get("i")), batch_id=_uuid(marker.get("b")))
        claim = await self.completer.claim(ref)
        if not claim.run:
            return None
        tree = self.tree_cache.get(ref.batch_id)
        if tree is None:
            tree = await self._load_tree(ref, attempt=claim.attempt)
        context = WorkItem(
            ref=ref,
            attempt=claim.attempt,
            depth=claim.depth,
            defers=claim.defers,
            checkpoint_position=claim.checkpoint,
            tree=tree,
            make_call=self._make_call,
            completer=self.completer,
            defer_default_delay=self.defer_default_delay,
        )
        heartbeat = asyncio.create_task(
            self._heartbeat(context), name=f"tallyho-heartbeat-{ref.id}"
        )
        # Закрытие установки отменит heartbeat: продлевать lease будет некому.
        self.completer.attach(heartbeat)
        try:
            with activate_item(context):
                return await self._through_middleware(
                    task, args, kwargs, context=context, policy=policy
                )
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _load_tree(self, ref: ItemRef, *, attempt: int) -> TreeSnapshot:
        """Прочитать дерево батча захваченного Item до вызова задачи.

        Returns:
            Снимок дерева для ``spawn``/``sub_batch`` задачи.

        Raises:
            CompleterError: чтение не прошло (PostgreSQL недоступен, ошибка
                запроса); lease уже отпущен, как перед ретраем брокера (§10).
        """
        try:
            async with self.completer.engine.connect() as conn:
                return await self.tree_cache.load(conn, self.completer.tables, ref.batch_id)
        except BaseException as exc:
            # Задача не начиналась: lease отпускаем, чтобы повтор брокера не ждал lease_ttl.
            with contextlib.suppress(CompleterError, ClosedError):
                _ = await self.completer.release(ref, attempt=attempt)
            if isinstance(exc, TallyhoError) or not isinstance(exc, Exception):
                raise
            # Ошибка драйвера — подкласс TallyhoError из retry_on адаптера, а не DLQ сразу.
            raise CompleterError(_TREE_FAILED) from exc

    async def _through_middleware(
        self,
        task: _AsyncTask,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        context: WorkItem,
        policy: ExceptionPolicy | None = None,
    ) -> object:
        """Провести вызов задачи через цепочку middleware (ARCHITECTURE §11.2).

        Первый элемент списка — внешний. Пустая цепочка — прежний горячий путь
        без лишних кадров стека.

        Returns:
            Значение, которое вернула задача (или подменившая его middleware).
        """
        if not self.middleware:
            return await self._invoke(task, args, kwargs, context=context, policy=policy)

        async def call_task() -> object:
            return await self._invoke(task, args, kwargs, context=context, policy=policy)

        call_next: Callable[[], Awaitable[object]] = call_task
        for entry in reversed(self.middleware):
            call_next = functools.partial(entry, context, call_next)
        return await call_next()

    async def _invoke(
        self,
        task: _AsyncTask,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        context: WorkItem,
        policy: ExceptionPolicy | None = None,
    ) -> object:
        attempt = context.attempt
        try:
            result = await task(*args, **kwargs)
            if isinstance(result, Ok | Skip | Error | Defer):
                # Итог значением — тот же внутренний путь, что у item.ok/skip/error/defer.
                # Конфликт с императивным итогом обрабатывается как любое исключение задачи.
                context.adopt(result)
        except asyncio.CancelledError:
            if not context.lease_lost:
                # Установка уже закрыта — Item вернул в outbox aclose; отмену не подменяем.
                with contextlib.suppress(ClosedError):
                    _ = await self.completer.release(context.ref, attempt=attempt)
            raise
        except BaseException as exc:
            if context.lease_lost:
                # Item уже завершён или принадлежит другому исполнителю (UC-08):
                # finish и release задели бы чужую попытку, поэтому не пишем ничего.
                if isinstance(exc, LeaseLostError):
                    # Не ошибка задачи: брокеру — успех, как при DUPLICATE и TERMINAL.
                    _log.info("Item %s: попытка %d потеряла lease", context.id, attempt)
                    return None
                raise
            if self._translated(exc, context, policy):
                # Судьба исключения объявлена при регистрации (UC-18): это итог
                # попытки, а не её провал. Брокеру — успех, без ретрая и DLQ.
                _ = await self._record(context, attempt)
                return None
            if not await self._settle_failure(exc, context):
                # Путь A без lease (UC-03): итог не записан, ретрай и DLQ задели бы
                # Item, который теперь у другого исполнителя, — как LeaseLostError.
                _log.info("Item %s: попытка %d потеряла lease", context.id, attempt)
                return None
            raise
        if not (context.completed_in_user_tx or context.lease_lost):
            # Потерянный lease здесь ничего не меняет: брокеру — успех, итог не записан.
            _ = await self._record(context, attempt)
        return result

    async def _record(self, context: WorkItem, attempt: int) -> bool:
        """Записать итог попытки: отложение или терминальный итог.

        ``defer`` — такой же успешный выход, как итог: попытка не тратится,
        ретрая и DLQ нет (UC-15).

        Returns:
            ``True``, если запись применилась к этой попытке.
        """
        if context.deferred:
            return await self.completer.defer(
                context.ref,
                reason=context.defer_reason,
                until=context.defer_until,
                delay=context.defer_delay,
                attempt=attempt,
            )
        return await self.completer.finish(context.ref, context.finish_result(), attempt=attempt)

    @staticmethod
    def _translated(exc: BaseException, context: WorkItem, policy: ExceptionPolicy | None) -> bool:
        """Перевести исключение задачи в итог попытки по правилам регистрации.

        Пути владения не переводятся никогда: ``CancelledError`` обработан
        раньше, а ``LeaseLostError`` и прочие ошибки самой библиотеки
        принадлежат не задаче, а учёту работы (UC-03).

        Returns:
            ``True``, если итог заполнен правилом и его нужно записать.
        """
        if policy is None or not policy or isinstance(exc, TallyhoError):
            return False
        try:
            outcome = policy.outcome_for(exc)
        except Exception:  # ruff: ignore[blind-except]  # падение правила — обычная ошибка задачи
            _log.exception(
                "Item %s: правило для %s упало, исключение идёт обычным путём",
                context.id,
                type(exc).__name__,
            )
            return False
        if outcome is None:
            return False
        context.adopt(outcome)
        return True

    async def _settle_failure(self, exc: BaseException, context: WorkItem) -> bool:
        """Записать исход упавшей попытки: cancelled, release или error.

        Returns:
            ``False``, если попытка уже не владеет lease и ничего не записано.
        """
        if isinstance(self.broker, CancellationClassifier) and self.broker.is_cancelled(exc):
            value = FinishResult(
                result_class=ResultClass.CANCELLED,
                label="cancelled",
                error={"type": type(exc).__name__, "message": str(exc)},
                metrics=context.metrics,
            )
            return await self.completer.finish(context.ref, value, attempt=context.attempt)
        if self.broker.retry_verdict(exc) is Verdict.RETRY:
            return await self.completer.release(context.ref, attempt=context.attempt)
        value = FinishResult(
            result_class=ResultClass.ERROR,
            label="exhausted",
            error={"type": type(exc).__name__, "message": str(exc)},
            metrics=context.metrics,
        )
        return await self.completer.finish(context.ref, value, attempt=context.attempt)

    async def _heartbeat(self, context: WorkItem) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_every.total_seconds())
            alive = await self.completer.heartbeat(
                context.ref,
                attempt=context.attempt,
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


def build_runtime(  # ruff: ignore[too-many-arguments]  # граница композиции engine → runtime
    *,
    completer: object,
    broker: Runtime,
    dispatcher: Dispatcher,
    tree_cache: object,
    heartbeat_every: timedelta,
    defer_default_delay: timedelta,
    middleware: Sequence[object] = (),
) -> TaskRuntime:
    """Собрать и активировать runtime из непрозрачных engine-зависимостей.

    ``middleware`` приходит через composition root как ``Sequence[object]``:
    ни ``api``, ни ``engine`` не знают тип :class:`ItemMiddleware`, потому что
    он описан здесь, в верхнем слое (§3.3).

    Returns:
        Runtime, готовый для передачи broker adapter через ``WorkerServices``.
    """
    runtime = TaskRuntime(
        completer=cast("Completer", completer),
        broker=broker,
        dispatcher=dispatcher,
        tree_cache=cast("TreeCache", tree_cache),
        heartbeat_every=heartbeat_every,
        defer_default_delay=defer_default_delay,
        middleware=cast("Sequence[ItemMiddleware]", middleware),
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
