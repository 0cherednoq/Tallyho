"""Детерминированный брокер в памяти для сквозных тестов tallyho."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import random
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, cast, final

from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.completer import ItemRef
from tallyho.engine.installation import RuntimeServices
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import OutboxKind
from tallyho.protocols.broker import DeadLetters, Dispatcher, Runtime, Verdict
from tallyho.protocols.serialization import JsonSerializer, PayloadCodec, SerializerCodec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from tallyho.protocols.broker import Message
    from tallyho.protocols.serialization import CallArgs, Serializer
    from tallyho.runtime.tracked import TaskRuntime

__all__ = ["InlineBroker"]

P = ParamSpec("P")
R = TypeVar("R")

_NOT_INSTALLED = "InlineBroker сначала нужно передать в Tallyho.install()"
_UNKNOWN_TASK = "InlineBroker не знает задачу"
_BAD_RATE = "duplicate_delivery_rate должен быть числом от 0 до 1"
_BAD_STEP = "число доставок должно быть целым >= 0"
_BAD_CONCURRENCY = "concurrency должен быть целым >= 1"
_BAD_RETRIES = "max_retries должен быть целым >= 0"


class _Task(Protocol):
    async def __call__(self, *args: object, **kwargs: object) -> object:
        """Выполнить зарегистрированную async-задачу."""


class _Relay(Protocol):
    async def flush_kicked(self) -> int:
        """Отправить fast-path записи."""
        ...

    async def scan_once(self) -> int:
        """Выполнить страховочный scan."""
        ...


@dataclass(frozen=True, slots=True)
class _Delivery:
    message: Message
    retry_count: int = 0


@dataclass(frozen=True, slots=True)
class _Attempt:
    retry_count: int
    max_retries: int


@final
class InlineBroker(Dispatcher, Runtime, PayloadCodec):
    """Очередь в памяти, исполняющая сообщения через настоящий ``TaskRuntime``.

    Брокер последовательный и полностью управляется тестом: :meth:`step`
    выполняет не больше заданного числа доставок, :meth:`drain` — до простоя.
    """

    def __init__(
        self,
        *,
        duplicate_delivery_rate: float = 0.0,
        seed: int | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        """Создать пустой брокер."""
        self.duplicate_delivery_rate = _rate(duplicate_delivery_rate)
        self.seed = seed
        self._random = random.Random(seed)  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # воспроизводимая инъекция дублей, не криптография
        self._codec = SerializerCodec(serializer or JsonSerializer())
        self._tasks: dict[str, _Task] = {}
        self._queue: deque[_Delivery] = deque()
        self._crashed: list[_Delivery] = []
        self._dead: list[Message] = []
        self._runtime: TaskRuntime | None = None
        self._attempt: contextvars.ContextVar[_Attempt | None] = contextvars.ContextVar(
            "tallyho_inline_attempt", default=None
        )
        self._deliveries = 0
        self._kill_at: int | None = None

    @property
    def adapter(self) -> InlineBroker:
        """Вернуть адаптер для ``th.install(broker.adapter)``."""
        return self

    @property
    def relay_autostart(self) -> bool:
        """Relay не отправляет сообщения сам: его проходы вызывают ``step``/``drain``.

        Фоновый цикл relay работает, только пока тест сам запустил
        ``th.maintenance().run()``.
        """
        return False

    @property
    def pending(self) -> int:
        """Число готовых доставок в памяти."""
        return len(self._queue)

    @property
    def deliveries(self) -> int:
        """Число уже взятых воркером доставок, включая дубли и kill."""
        return self._deliveries

    @property
    def dead_letters(self) -> tuple[Message, ...]:
        """Сообщения, исчерпавшие ``max_retries``."""
        return tuple(self._dead)

    @override
    def task_name(self, fn: Callable[P, object]) -> str:
        """Зарегистрировать функцию и вернуть стабильное полное имя.

        Returns:
            Имя ``module.qualname``.

        Raises:
            ConfigurationError: это имя уже занято другой функцией.
        """
        name = f"{fn.__module__}.{fn.__qualname__}"
        task = cast("_Task", fn)
        previous = self._tasks.get(name)
        if previous is not None and inspect.unwrap(previous) is not inspect.unwrap(task):
            message = f"{_UNKNOWN_TASK}: конфликт имени {name!r}"
            raise ConfigurationError(message)
        self._tasks[name] = task
        return name

    @override
    async def dispatch(self, messages: Sequence[Message]) -> None:
        """Добавить сообщения и детерминированно инжектировать дубли.

        Raises:
            ConfigurationError: задача не зарегистрирована или её опции неверны.
        """
        for message in messages:
            if message.task_name not in self._tasks:
                detail = f"{_UNKNOWN_TASK}: {message.task_name!r}"
                raise ConfigurationError(detail)
            _ = self._max_retries(message)
        for message in messages:
            delivery = _Delivery(message)
            self._queue.append(delivery)
            if self._random.random() < self.duplicate_delivery_rate:
                self._queue.append(delivery)

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        """Обернуть задачу установленным tallyho runtime.

        Returns:
            Around-обёртка с исходной сигнатурой.
        """
        runtime = self._require_runtime()
        return cast("Callable[P, Awaitable[R]]", runtime.wrap(fn))

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        """Повторять исключение, пока не израсходован ``max_retries``.

        Returns:
            ``RETRY`` до последней попытки, затем ``FINAL``.
        """
        _ = exc
        attempt = self._attempt.get()
        if attempt is None:
            return Verdict.FINAL
        if attempt.retry_count < attempt.max_retries:
            return Verdict.RETRY
        return Verdict.FINAL

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        """Вернуть Item из локального DLQ после числового курсора.

        Returns:
            Новые Item ids и следующий курсор.

        Raises:
            ConfigurationError: курсор не является целым числом.
        """
        try:
            offset = 0 if since is None else int(since)
        except ValueError as exc:
            message = "курсор InlineBroker должен быть целым числом"
            raise ConfigurationError(message) from exc
        item_ids = tuple(
            message.id for message in self._dead[offset:] if message.kind is OutboxKind.ITEM
        )
        return DeadLetters(item_ids, str(len(self._dead)))

    @override
    def encode(
        self, task_name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> bytes:
        """Закодировать аргументы встроенным сериализатором.

        Returns:
            Сериализованный payload.
        """
        return self._codec.encode(task_name, args, kwargs)

    @override
    def decode(self, task_name: str, data: bytes) -> CallArgs:
        """Раскодировать аргументы встроенным сериализатором.

        Returns:
            Позиционные и именованные аргументы.
        """
        return self._codec.decode(task_name, data)

    def install_runtime(self, services: object) -> None:
        """Принять worker-сервисы от ``Tallyho.install``.

        Raises:
            ConfigurationError: передан объект другого типа.
        """
        if not isinstance(services, RuntimeServices):
            message = "InlineBroker получил несовместимый набор runtime-сервисов"
            raise ConfigurationError(message)
        self._runtime = cast("TaskRuntime", services.runtime)

    def kill_worker_after(self, deliveries: int) -> None:
        """Убить воркер на N-й следующей доставке, оставив взятый lease."""
        self._check_steps(deliveries, positive=True)
        self._kill_at = self._deliveries + deliveries

    async def step(self, deliveries: int = 1) -> int:
        """Выполнить не больше ``deliveries`` готовых попыток.

        Returns:
            Число взятых из очереди доставок.
        """
        self._check_steps(deliveries)
        done = 0
        while done < deliveries:
            await self._pump()
            if not self._queue:
                break
            delivery = self._queue.popleft()
            self._deliveries += 1
            done += 1
            if self._kill_at == self._deliveries:
                self._kill_at = None
                await self._crash(delivery)
                continue
            await self._execute(delivery)
        return done

    async def drain(self, *, concurrency: int = 1) -> int:
        """Выполнять сообщения до полного простоя очереди и relay.

        ``concurrency=1`` сохраняет полностью последовательную семантику.
        Большее значение имитирует ограниченный пул воркеров, не меняя
        детерминированный порядок извлечения сообщений из очереди.

        Returns:
            Число доставок в этом проходе.
        """
        self._check_concurrency(concurrency)
        done = 0
        while True:
            await self._pump()
            work: list[Awaitable[None]] = []
            while self._queue and len(work) < concurrency:
                delivery = self._queue.popleft()
                self._deliveries += 1
                done += 1
                if self._kill_at == self._deliveries:
                    self._kill_at = None
                    work.append(self._crash(delivery))
                else:
                    work.append(self._execute(delivery))
            if work:
                _ = await asyncio.gather(*work)
                continue
            await self._pump(scan=True)
            if not self._queue:
                return done

    async def close(self) -> None:
        """Дождаться внутренних операций Completer и остановить его."""
        runtime = self._runtime
        if runtime is not None:
            await runtime.completer.close()

    async def _execute(self, delivery: _Delivery) -> None:
        message = delivery.message
        task = self._tasks[message.task_name]
        args, kwargs = self.decode(message.task_name, message.payload)
        kwargs["_th"] = self._marker(message)
        maximum = self._max_retries(message)
        token = self._attempt.set(_Attempt(delivery.retry_count, maximum))
        try:
            wrapped = self._require_runtime().wrap(task)
            try:
                _ = await wrapped(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception:  # ruff: ignore[blind-except]  # пользовательская ошибка — штатный сигнал retry/DLQ брокеру
                if delivery.retry_count < maximum:
                    self._queue.append(_Delivery(message, delivery.retry_count + 1))
                else:
                    self._dead.append(message)
        finally:
            self._attempt.reset(token)
        await self._finalize(message)

    async def _crash(self, delivery: _Delivery) -> None:
        message = delivery.message
        if message.kind is OutboxKind.CALLBACK:
            self._crashed.append(delivery)
            return
        claimed = await self._require_runtime().completer.claim(
            ItemRef(id=message.id, batch_id=message.batch_id)
        )
        if claimed.run:
            self._crashed.append(delivery)

    async def _finalize(self, message: Message) -> None:
        finalizer = self._require_runtime().completer.triggers.finalizer
        if finalizer is not None:
            _ = await finalizer.try_finalize(message.batch_id)

    async def _pump(self, *, scan: bool = False) -> None:
        # Финализация и её каскад идут после возврата результата задаче: без
        # ожидания drain мог вернуться раньше, чем колбэк попадёт в outbox.
        await self._require_runtime().completer.settled()
        await self._recover_crashed()
        relay = self._relay()
        _ = await relay.flush_kicked()
        if scan and not self._queue:
            _ = await relay.scan_once()

    async def _recover_crashed(self) -> None:
        if not self._crashed:
            return
        runtime = self._require_runtime()
        item_ids = [
            delivery.message.id
            for delivery in self._crashed
            if delivery.message.kind is OutboxKind.ITEM
        ]
        held: set[object] = set()
        if item_ids:
            async with runtime.completer.engine.connect() as conn:
                held = set(
                    await conn.scalars(
                        select(runtime.completer.tables.lease.c.item_id).where(
                            runtime.completer.tables.lease.c.item_id.in_(item_ids)
                        )
                    )
                )
        remaining: list[_Delivery] = []
        for delivery in self._crashed:
            message = delivery.message
            if message.kind is OutboxKind.ITEM and message.id in held:
                remaining.append(delivery)
            else:
                self._queue.append(delivery)
        self._crashed = remaining

    def _relay(self) -> _Relay:
        relay = self._require_runtime().completer.triggers.relay
        if relay is None:
            raise ConfigurationError(_NOT_INSTALLED)
        return cast("_Relay", cast("object", relay))

    def _require_runtime(self) -> TaskRuntime:
        if self._runtime is None:
            raise ConfigurationError(_NOT_INSTALLED)
        return self._runtime

    @staticmethod
    def _marker(message: Message) -> dict[str, object]:
        if message.kind is OutboxKind.CALLBACK:
            return {"c": str(message.id), "b": str(message.batch_id), "s": None}
        return {"i": str(message.id), "b": str(message.batch_id)}

    @staticmethod
    def _max_retries(message: Message) -> int:
        value = message.options.get("max_retries", 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigurationError(_BAD_RETRIES)
        return value

    @staticmethod
    def _check_steps(deliveries: object, *, positive: bool = False) -> None:
        minimum = 1 if positive else 0
        if isinstance(deliveries, bool) or not isinstance(deliveries, int) or deliveries < minimum:
            raise ConfigurationError(_BAD_STEP)

    @staticmethod
    def _check_concurrency(value: object) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ConfigurationError(_BAD_CONCURRENCY)


def _rate(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not 0 <= value <= 1:
        raise ConfigurationError(_BAD_RATE)
    return float(value)
