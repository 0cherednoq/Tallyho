"""Production-адаптер flexiq: outbox dispatch и tracked worker runtime."""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import logging
from collections import defaultdict
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from importlib.metadata import version
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, cast, final, runtime_checkable
from uuid import UUID

from flexiq import EventType, current_job
from flexiq.exceptions import TaskCancelledError
from flexiq.notes import validate_and_encode_notes
from typing_extensions import override

from tallyho.model.errors import (
    ClosedError,
    CompleterError,
    ConfigurationError,
    TallyhoError,
    UnsupportedOption,
)
from tallyho.model.states import OutboxKind
from tallyho.protocols.broker import (
    CallOptionsValidator,
    CancellationClassifier,
    DeadLetters,
    Dispatcher,
    RetryLimits,
    Runtime,
    Verdict,
    WorkerServices,
)
from tallyho.protocols.serialization import PayloadCodec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from flexiq import Queue

    from tallyho.protocols.broker import Message, WorkerRuntime
    from tallyho.protocols.serialization import CallArgs

__all__ = ["FlexiqAdapter"]

P = ParamSpec("P")
R = TypeVar("R")

_log = logging.getLogger(__name__)
_BAD_API = "несовместимый API flexiq; требуется flexiq>=2.0,<3"
_NOT_INSTALLED = "сначала передайте FlexiqAdapter в Tallyho.install()"
_NOT_REGISTERED = "задача не зарегистрирована через @FlexiqAdapter.task"
_SYNC_TASK = "FlexiqAdapter поддерживает только async def задачи"
_PREFORK = "tallyho v1 поддерживает только flexiq pool='thread'; prefork несовместим с lease"
_DISPATCH_FAILED = "flexiq не принял сообщения relay"
_BAD_OPTIONS = "опции задачи flexiq имеют неверный тип"
_FED_BY_HINT = "используйте под-батчи с fed_by вместо depends_on"
_DEBOUNCE_HINT = "debounce/batch объединяют jobs и нарушают правило «один Item — одна job»"
_CHUNK = 1_000
_FLEXIQ_MAJOR = 2
_PAIR_SIZE = 2
_BATCH_OPTION = "batch"
_ENCODE_PAYLOAD = "_encode_payload"
_DECODE_PAYLOAD = "_deserialize_payload"
_PY_JOB = "_py_job"
_INFRASTRUCTURE_ERRORS: tuple[type[Exception], ...] = (CompleterError, ClosedError)
"""Ошибки tallyho, которые брокер повторяет независимо от ``retry_on`` задачи.

``ClosedError`` получает задача, не доработавшая до закрытия установки: её
Item уже возвращён в outbox, и уводить джобу в DLQ нельзя.
"""

_CALL_OPTIONS = frozenset(
    {
        "delay",
        "expires",
        "idempotency_key",
        "idempotent",
        "max_retries",
        "metadata",
        "notes",
        "priority",
        "queue",
        "result_ttl",
        "timeout",
        "unique_key",
    }
)
_FORBIDDEN_CALL = frozenset(
    {
        "batch",
        "debounce",
        "debounce_key",
        "debounce_max_wait",
        "debounce_replace_payload",
        "depends_on",
    }
)


class _FlexiqDispatchError(TallyhoError):
    """Внешний flexiq отказал при dispatch или чтении DLQ."""


class _AsyncTask(Protocol):
    async def __call__(self, *args: object, **kwargs: object) -> object:
        """Выполнить задачу с динамической сигнатурой."""


@runtime_checkable
class _NamedTask(Protocol):
    @property
    def name(self) -> str:
        """Имя задачи в registry flexiq."""
        ...


@runtime_checkable
class _Queue(Protocol):
    def task(self, **options: object) -> Callable[[_AsyncTask], _NamedTask]:
        """Создать декоратор задачи."""
        ...

    def enqueue_many(self, **options: object) -> object:
        """Поставить атомарную пачку."""
        ...

    def enqueue(self, **options: object) -> object:
        """Поставить одно сообщение."""
        ...

    def on_event(self, event_type: object, callback: Callable[[object, object], None]) -> None:
        """Подписаться на событие воркера."""
        ...

    async def aget_job(self, job_id: str) -> object | None:
        """Прочитать job."""
        ...

    async def adead_letters_after(self, *, limit: int, after: str | None) -> object:
        """Прочитать страницу DLQ."""
        ...

    def _encode_payload(
        self, task_name: str, args: tuple[object, ...], kwargs: dict[str, object]
    ) -> bytes:
        """Закодировать payload штатным кодеком задачи."""
        ...

    def _deserialize_payload(self, task_name: str, payload: bytes) -> object:
        """Декодировать payload штатным кодеком задачи."""
        ...


class _StoredJob(Protocol):
    task_name: str

    @property
    def payload_bytes(self) -> bytes:
        """Сырые байты job."""
        ...


class _Page(Protocol):
    items: list[object]
    next_cursor: str | None


class _CurrentJob(Protocol):
    @property
    def retry_count(self) -> int:
        """Номер текущей попытки."""
        ...

    @property
    def task_name(self) -> str:
        """Имя текущей задачи."""
        ...


@dataclass(frozen=True, slots=True)
class _TaskConfig:
    priority: int
    queue: str
    max_retries: int
    timeout: int
    expires: float | None
    idempotent: bool
    retry_on: tuple[type[BaseException], ...]
    dont_retry_on: tuple[type[BaseException], ...]


@dataclass(frozen=True, slots=True)
class _EffectiveAttempt:
    max_retries: int
    config: _TaskConfig


@dataclass(frozen=True, slots=True)
class _Prepared:
    message: Message
    args: tuple[object, ...]
    kwargs: dict[str, object]
    priority: int
    queue: str
    max_retries: int
    timeout: int
    delay: float | None
    metadata: str | None
    notes: dict[str, object] | None
    expires: float | None
    result_ttl: int | None
    unique_key: str | None
    idempotency_key: str | None
    idempotent: bool

    @property
    def group(self) -> tuple[str, str, int, int, int, bool]:
        """Ключ совместимого вызова ``enqueue_many``."""
        return (
            self.message.task_name,
            self.queue,
            self.priority,
            self.max_retries,
            self.timeout,
            self.idempotent,
        )


@final
class FlexiqAdapter(
    Dispatcher, Runtime, PayloadCodec, CallOptionsValidator, CancellationClassifier, RetryLimits
):
    """Двусторонний адаптер одной пользовательской ``flexiq.Queue``."""

    def __init__(self, queue: Queue, *, pool: str = "thread") -> None:
        """Связать адаптер с Queue; проверка совместимости идёт при install."""
        self._raw_queue = cast("object", queue)
        self._queue = cast("_Queue", self._raw_queue)
        self._pool = pool
        self._runtime: WorkerRuntime | None = None
        self._services: WorkerServices | None = None
        self._tasks: dict[str, _TaskConfig] = {}
        self._attempt: contextvars.ContextVar[_EffectiveAttempt | None] = contextvars.ContextVar(
            "tallyho_flexiq_attempt", default=None
        )
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="tallyho-flexiq")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._background: set[asyncio.Task[None]] = set()

    def task(
        self, **options: object
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
        """Зарегистрировать tracked async-задачу в flexiq.

        Returns:
            Декоратор, сохраняющий сигнатуру исходной функции.

        """
        self._reject_decorated_options(options)

        def decorate(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            if not inspect.iscoroutinefunction(fn):
                raise ConfigurationError(_SYNC_TASK)
            runtime = self._require_runtime()
            config = _with_infrastructure_retries(self._task_config(options))
            broker_options = dict(options)
            if config.retry_on:
                broker_options["retry_on"] = list(config.retry_on)
            tracked = runtime.wrap(fn)
            dynamic = cast("_AsyncTask", tracked)

            async def invoke(*args: object, **kwargs: object) -> object:
                self._loop = asyncio.get_running_loop()
                maximum = _marker_retries(kwargs.get("_th"), config.max_retries)
                token = self._attempt.set(_EffectiveAttempt(maximum, config))
                try:
                    return await dynamic(*args, **kwargs)
                finally:
                    self._attempt.reset(token)

            wrapped = functools.update_wrapper(invoke, fn)
            try:
                registered = self._queue.task(**broker_options)(wrapped)
            except Exception as exc:
                raise ConfigurationError(_BAD_OPTIONS) from exc
            name = registered.name
            self._tasks[name] = config
            return cast("Callable[P, Awaitable[R]]", registered)

        return decorate

    @override
    def task_name(self, fn: Callable[P, object]) -> str:
        """Вернуть имя задачи, зарегистрированной через :meth:`task`.

        Returns:
            Имя flexiq.

        Raises:
            ConfigurationError: функция не зарегистрирована этим адаптером.
        """
        candidate = cast("object", fn)
        if isinstance(candidate, _NamedTask) and candidate.name in self._tasks:
            return candidate.name
        raise ConfigurationError(_NOT_REGISTERED)

    @override
    async def dispatch(self, messages: Sequence[Message]) -> None:
        """Сгруппировать и отправить сообщения чанками по 1000."""
        prepared = [self._prepare(message) for message in messages]
        groups: dict[tuple[str, str, int, int, int, bool], list[_Prepared]] = defaultdict(list)
        for item in prepared:
            groups[item.group].append(item)
        loop = asyncio.get_running_loop()
        pending: list[asyncio.Future[None]] = []
        for key in sorted(groups):
            values = groups[key]
            for start in range(0, len(values), _CHUNK):
                chunk = values[start : start + _CHUNK]
                pending.append(loop.run_in_executor(self._executor, self._send_chunk, chunk))
        if pending:
            _ = await asyncio.gather(*pending)

    @override
    def validate_options(self, options: Mapping[str, object]) -> None:
        """Validate call options synchronously at the producer boundary.

        Raises:
            ConfigurationError: an option has an invalid value or structured notes exceed limits.

        """
        self._validate_call_options(options)
        notes = _notes(options.get("notes"))
        try:
            _ = validate_and_encode_notes(notes)
        except Exception as exc:
            raise ConfigurationError(_BAD_OPTIONS) from exc

    @override
    def max_retries(self, task_name: str) -> int:
        """Отдать sweeper-у ``max_retries`` декоратора задачи (D-012).

        Returns:
            Лимит из ``@fq.task``; 0 для задачи, не зарегистрированной здесь.
        """
        config = self._tasks.get(task_name)
        return 0 if config is None else config.max_retries

    @override
    def is_cancelled(self, exc: BaseException) -> bool:
        """Recognize Flexiq's cooperative cancellation signal.

        Returns:
            Whether this is Flexiq's task-cancelled exception.
        """
        return isinstance(exc, TaskCancelledError)

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        """Обернуть функцию установленным worker runtime.

        Returns:
            Tracked async-функция с той же сигнатурой.
        """
        return cast("Callable[P, Awaitable[R]]", self._require_runtime().wrap(fn))

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        """Повторять только по лимиту job и фильтрам декоратора.

        Returns:
            Решение, совпадающее с retry-фильтрами flexiq.
        """
        effective = self._attempt.get()
        if effective is None:
            return Verdict.FINAL
        job = cast("_CurrentJob", cast("object", current_job))
        if job.retry_count >= effective.max_retries:
            return Verdict.FINAL
        config = effective.config
        if config.dont_retry_on and isinstance(exc, config.dont_retry_on):
            return Verdict.FINAL
        if config.retry_on and not isinstance(exc, config.retry_on):
            return Verdict.FINAL
        return Verdict.RETRY

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        """Прочитать одну страницу DLQ и извлечь Item ids из payload.

        Returns:
            Найденные Item и непрозрачный курсор следующей страницы.

        Raises:
            _FlexiqDispatchError: flexiq не прочитал DLQ либо payload повреждён.
            TallyhoError: payload DLQ не удалось декодировать.
        """
        try:
            raw_page = await self._queue.adead_letters_after(limit=_CHUNK, after=since)
        except TallyhoError:
            raise
        except Exception as exc:
            raise _FlexiqDispatchError(_DISPATCH_FAILED) from exc
        page = cast("_Page", raw_page)
        ids: list[UUID] = []
        for raw in page.items:
            entry = _mapping(raw)
            job_id = entry.get("original_job_id")
            if isinstance(job_id, str):
                identity = await self._job_identity(job_id)
                if identity is not None:
                    ids.append(identity[0])
        return DeadLetters(tuple(ids), page.next_cursor)

    @override
    def encode(
        self, task_name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> bytes:
        """Кодировать тем же serializer/codec chain, что flexiq.

        Returns:
            Байты flexiq payload.

        Raises:
            _FlexiqDispatchError: flexiq codec не смог закодировать payload.
        """
        try:
            encoder = cast(
                "Callable[[str, tuple[object, ...], dict[str, object]], bytes]",
                getattr(self._raw_queue, _ENCODE_PAYLOAD),
            )
            return encoder(task_name, args, dict(kwargs))
        except Exception as exc:
            raise _FlexiqDispatchError(_DISPATCH_FAILED) from exc

    @override
    def decode(self, task_name: str, data: bytes) -> CallArgs:
        """Декодировать штатный flexiq payload.

        Returns:
            Позиционные и именованные аргументы.

        Raises:
            _FlexiqDispatchError: flexiq codec вернул повреждённый payload.
            TallyhoError: flexiq codec не смог декодировать payload.
        """
        try:
            decoder = cast(
                "Callable[[str, bytes], object]",
                getattr(self._raw_queue, _DECODE_PAYLOAD),
            )
            return _call_args(decoder(task_name, data))
        except TallyhoError:
            raise
        except Exception as exc:
            raise _FlexiqDispatchError(_DISPATCH_FAILED) from exc

    def install_runtime(self, services: object) -> None:
        """Проверить flexiq и установить worker runtime + JOB_DEAD listener.

        Raises:
            ConfigurationError: pool/version/API несовместимы или services неверны.
        """
        if self._pool != "thread":
            raise ConfigurationError(_PREFORK)
        if _major_version() != _FLEXIQ_MAJOR or not isinstance(self._raw_queue, _Queue):
            raise ConfigurationError(_BAD_API)
        if not isinstance(services, WorkerServices):
            raise ConfigurationError(_BAD_API)
        self._runtime = services.runtime
        self._services = services
        self._queue.on_event(EventType.JOB_DEAD, self._on_dead)

    async def close(self) -> None:
        """Дождаться DLQ-задач и остановить собственный dispatch executor.

        Вызывается после ``Tallyho.aclose()``. DLQ-задачи живут в event loop
        исполнителя flexiq; из другого loop дождаться их нельзя, и они
        пропускаются — потерянное событие закрывает сверка ``reconcile_dead``.
        """
        current = asyncio.get_running_loop()
        own = [task for task in self._background if task.get_loop() is current]
        if own:
            _ = await asyncio.gather(*own)
        self._executor.shutdown(wait=True)

    def _prepare(self, message: Message) -> _Prepared:
        config = self._tasks.get(message.task_name)
        if config is None:
            raise ConfigurationError(_NOT_REGISTERED)
        options = dict(message.options)
        self._validate_call_options(options)
        args, kwargs = self.decode(message.task_name, message.payload)
        priority = _integer(options.get("priority", config.priority), "priority", minimum=0)
        maximum = _integer(options.get("max_retries", config.max_retries), "max_retries")
        timeout = _integer(options.get("timeout", config.timeout), "timeout", minimum=1)
        queue = _string(options.get("queue", config.queue), "queue")
        kwargs["_th"] = self._marker(message, maximum)
        return _Prepared(
            message=message,
            args=args,
            kwargs=kwargs,
            priority=priority,
            queue=queue,
            max_retries=maximum,
            timeout=timeout,
            delay=_number_or_none(options.get("delay"), "delay"),
            metadata=_string_or_none(options.get("metadata"), "metadata"),
            notes=_notes(options.get("notes")),
            expires=_number_or_none(options.get("expires", config.expires), "expires"),
            result_ttl=_integer_or_none(options.get("result_ttl"), "result_ttl"),
            unique_key=_string_or_none(options.get("unique_key"), "unique_key"),
            idempotency_key=_string_or_none(options.get("idempotency_key"), "idempotency_key"),
            idempotent=_boolean(options.get("idempotent", config.idempotent), "idempotent"),
        )

    def _send_chunk(self, chunk: list[_Prepared]) -> None:
        first = chunk[0]
        many = self._many_options(first, chunk)
        try:
            _ = self._queue.enqueue_many(**many)
        except RuntimeError as exc:
            lowered = str(exc).lower()
            if "duplicate" not in lowered or "key" not in lowered:
                raise _FlexiqDispatchError(_DISPATCH_FAILED) from exc
        except Exception as exc:
            raise _FlexiqDispatchError(_DISPATCH_FAILED) from exc
        else:
            return
        for item in chunk:
            try:
                _ = self._queue.enqueue(**self._one_options(item))
            except Exception as exc:
                raise _FlexiqDispatchError(_DISPATCH_FAILED) from exc

    @staticmethod
    def _many_options(first: _Prepared, chunk: list[_Prepared]) -> dict[str, object]:
        return {
            "task_name": first.message.task_name,
            "args_list": [item.args for item in chunk],
            "kwargs_list": [item.kwargs for item in chunk],
            "priority": first.priority,
            "queue": first.queue,
            "max_retries": first.max_retries,
            "timeout": first.timeout,
            "delay_list": [item.delay for item in chunk],
            "unique_keys": [item.unique_key for item in chunk],
            "metadata_list": [item.metadata for item in chunk],
            "notes_list": [item.notes for item in chunk],
            "expires_list": [item.expires for item in chunk],
            "result_ttl_list": [item.result_ttl for item in chunk],
            "idempotency_keys": [
                item.idempotency_key
                if item.idempotency_key is not None or item.unique_key is not None
                else f"th:{item.message.id}"
                for item in chunk
            ],
            "idempotent": first.idempotent,
        }

    @staticmethod
    def _one_options(item: _Prepared) -> dict[str, object]:
        return {
            "task_name": item.message.task_name,
            "args": item.args,
            "kwargs": item.kwargs,
            "priority": item.priority,
            "queue": item.queue,
            "max_retries": item.max_retries,
            "timeout": item.timeout,
            "delay": item.delay,
            "unique_key": item.unique_key,
            "metadata": item.metadata,
            "notes": item.notes,
            "expires": item.expires,
            "result_ttl": item.result_ttl,
            "idempotency_key": (
                item.idempotency_key
                if item.idempotency_key is not None or item.unique_key is not None
                else f"th:{item.message.id}"
            ),
            "idempotent": item.idempotent,
        }

    def _on_dead(self, event_type: object, payload: object) -> None:
        _ = event_type
        values = _mapping(payload)
        job_id = values.get("job_id")
        if not isinstance(job_id, str) or self._loop is None:
            return
        error = values.get("error")
        detail = error if isinstance(error, str) else "flexiq moved job to DLQ"
        self._loop.call_soon_threadsafe(self._spawn_dead, job_id, detail)

    def _spawn_dead(self, job_id: str, detail: str) -> None:
        task = asyncio.create_task(self._finish_dead(job_id, detail), name="tallyho-flexiq-dlq")
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _finish_dead(self, job_id: str, detail: str) -> None:
        try:
            await self._finish_dead_inner(job_id, detail)
        except Exception:  # ruff: ignore[blind-except]  # event best-effort; reconcile_dead и sweeper страхуют
            _log.exception("не удалось завершить Item из flexiq JOB_DEAD")

    async def _finish_dead_inner(self, job_id: str, detail: str) -> None:
        identity = await self._job_identity(job_id)
        if identity is None:
            return
        services = self._services
        if services is None:
            raise ConfigurationError(_NOT_INSTALLED)
        await services.finish_dead(
            identity[0],
            identity[1],
            error_type="FlexiqDeadLetter",
            detail=detail,
        )

    async def _job_identity(self, job_id: str) -> tuple[UUID, UUID] | None:
        raw_job = await self._queue.aget_job(job_id)
        if raw_job is None:
            return None
        stored = cast(
            "_StoredJob",
            getattr(raw_job, _PY_JOB),
        )
        _args, kwargs = self.decode(stored.task_name, stored.payload_bytes)
        marker = _mapping(kwargs.get("_th"))
        item_id = _uuid_or_none(marker.get("i"))
        batch_id = _uuid_or_none(marker.get("b"))
        if item_id is None or batch_id is None:
            return None
        return item_id, batch_id

    def _require_runtime(self) -> WorkerRuntime:
        if self._runtime is None:
            raise ConfigurationError(_NOT_INSTALLED)
        return self._runtime

    @staticmethod
    def _marker(message: Message, maximum: int) -> dict[str, object]:
        if message.kind is OutboxKind.CALLBACK:
            return {
                "c": str(message.id),
                "b": str(message.batch_id),
                "r": maximum,
                "s": None,
            }
        return {"i": str(message.id), "b": str(message.batch_id), "r": maximum}

    @staticmethod
    def _reject_decorated_options(options: Mapping[str, object]) -> None:
        if options.get(_BATCH_OPTION) not in {None, False}:
            raise UnsupportedOption(_BATCH_OPTION, hint=_DEBOUNCE_HINT)
        for name in (
            "debounce",
            "debounce_key",
            "debounce_max_wait",
            "debounce_replace_payload",
        ):
            if options.get(name) not in {None, False}:
                raise UnsupportedOption(name, hint=_DEBOUNCE_HINT)

    @staticmethod
    def _validate_call_options(options: Mapping[str, object]) -> None:
        for name in options:
            if name == "depends_on":
                raise UnsupportedOption(name, hint=_FED_BY_HINT)
            if name in _FORBIDDEN_CALL:
                raise UnsupportedOption(name, hint=_DEBOUNCE_HINT)
            if name not in _CALL_OPTIONS:
                raise UnsupportedOption(name)

    @staticmethod
    def _task_config(options: Mapping[str, object]) -> _TaskConfig:
        return _TaskConfig(
            priority=_integer(options.get("priority", 0), "priority", minimum=0),
            queue=_string(options.get("queue", "default"), "queue"),
            max_retries=_integer(options.get("max_retries", 3), "max_retries"),
            timeout=_integer(options.get("timeout", 300), "timeout", minimum=1),
            expires=_number_or_none(options.get("expires"), "expires"),
            idempotent=_boolean(options.get("idempotent", False), "idempotent"),
            retry_on=_exception_types(options.get("retry_on"), "retry_on"),
            dont_retry_on=_exception_types(options.get("dont_retry_on"), "dont_retry_on"),
        )


def _with_infrastructure_retries(config: _TaskConfig) -> _TaskConfig:
    """Дополнить белый список ``retry_on`` ошибками самой библиотеки.

    ``retry_on`` во flexiq — белый список: исключение не из него сразу уводит
    джобу в DLQ. Отказ PostgreSQL на claim, release или finish
    (:class:`CompleterError`) и закрытие установки посреди задачи
    (:class:`ClosedError`) — не ошибки задачи, и терять на них джобу нельзя,
    поэтому они повторяются наравне с ошибками из списка пользователя. Пустой
    список («повторять всё») и список, уже покрывающий эти ошибки, не меняются.
    ``dont_retry_on`` пользователя остаётся сильнее.

    Returns:
        Конфигурация с дополненным ``retry_on``.
    """
    if not config.retry_on:
        return config
    missing = tuple(
        error for error in _INFRASTRUCTURE_ERRORS if not issubclass(error, config.retry_on)
    )
    return replace(config, retry_on=(*config.retry_on, *missing)) if missing else config


def _major_version() -> int:
    try:
        return int(version("flexiq").split(".", maxsplit=1)[0])
    except (ValueError, IndexError):
        return -1


def _call_args(value: object) -> CallArgs:
    if isinstance(value, tuple):
        pair = cast("tuple[object, ...]", value)
        if len(pair) != _PAIR_SIZE:
            raise _FlexiqDispatchError(_DISPATCH_FAILED)
        args, kwargs = pair
        if isinstance(args, tuple) and isinstance(kwargs, dict):
            positional = cast("tuple[object, ...]", args)
            raw = cast("dict[object, object]", kwargs)
            if all(isinstance(key, str) for key in raw):
                return positional, {key: item for key, item in raw.items() if isinstance(key, str)}
    raise _FlexiqDispatchError(_DISPATCH_FAILED)


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        return {}
    raw = cast("Mapping[object, object]", value)
    return {key: item for key, item in raw.items() if isinstance(key, str)}


def _marker_retries(value: object, default: int) -> int:
    retries = _mapping(value).get("r", default)
    return retries if isinstance(retries, int) and not isinstance(retries, bool) else default


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        message = f"{name} должен быть целым >= {minimum}"
        raise ConfigurationError(message)
    return value


def _integer_or_none(value: object, name: str) -> int | None:
    return None if value is None else _integer(value, name)


def _number_or_none(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        message = f"{name} должен быть числом >= 0 или None"
        raise ConfigurationError(message)
    return float(value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{name} должен быть непустой строкой"
        raise ConfigurationError(message)
    return value


def _string_or_none(value: object, name: str) -> str | None:
    return None if value is None else _string(value, name)


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        message = f"{name} должен быть bool"
        raise ConfigurationError(message)
    return value


def _notes(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    values = _mapping(value)
    if not values and value != {}:
        message = "notes должен быть словарём со строковыми ключами"
        raise ConfigurationError(message)
    return dict(values)


def _exception_types(value: object, name: str) -> tuple[type[BaseException], ...]:
    if value is None:
        return ()
    if not isinstance(value, list | tuple):
        raise ConfigurationError(_BAD_OPTIONS)
    result: list[type[BaseException]] = []
    for item in cast("Sequence[object]", value):
        if not isinstance(item, type) or not issubclass(item, BaseException):
            message = f"{name} должен содержать классы исключений"
            raise ConfigurationError(message)
        result.append(item)
    return tuple(result)


def _uuid_or_none(value: object) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except ValueError:
        return None
