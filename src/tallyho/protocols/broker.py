"""Контракт адаптера брокера: :class:`Dispatcher` и :class:`Runtime` (ARCHITECTURE §4.2).

Сторона продюсера — :class:`Dispatcher`: relay читает ``th_outbox`` и отдаёт
брокеру пачку :class:`Message`. Отправка at-least-once: дубль отсекает claim.

Сторона воркера — :class:`Runtime`: обёртка исполнения задачи, вердикт «будет ли
ретрай» после исключения и сверка с DLQ брокера (страховка на случай, когда
брокер убил задачу вопреки вердикту ``RETRY``, ARCHITECTURE §11.3, D-014).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from datetime import timedelta
    from uuid import UUID

    from tallyho.model.states import OutboxKind

__all__ = [
    "CallOptionsValidator",
    "CancellationClassifier",
    "DeadLetter",
    "DeadLetters",
    "Dispatcher",
    "Message",
    "RelayPolicy",
    "RetryLimits",
    "Runtime",
    "RuntimeInstaller",
    "Verdict",
    "WorkerFactory",
    "WorkerRuntime",
    "WorkerServices",
]

P = ParamSpec("P")
R = TypeVar("R")


def _no_options() -> Mapping[str, object]:
    return {}


@dataclass(frozen=True, slots=True, kw_only=True)
class Message:
    """Одна запись outbox к отправке брокеру.

    Attributes:
        id: ``item_id`` для ``kind=ITEM``; стабильный ``callback_id`` (id записи
            outbox) для ``kind=CALLBACK`` — по нему брокер и tallyho отсекают дубли.
        batch_id: батч Item или батч, чей колбэк ставится.
        kind: что отправляется — Item или колбэк финализации.
        task_name: имя задачи у брокера (:meth:`Dispatcher.task_name`).
        payload: аргументы вызова, закодированные
            :class:`~tallyho.protocols.PayloadCodec` адаптера.
        options: опции постановки брокера из ``th.call(...).opts`` (priority,
            queue, delay, metadata, …); JSON-совместимые значения.
        generation: поколение отправки Item (``th_item.generation``): 0 у
            первой отправки, растёт при каждом возврате Item в outbox. Адаптер
            кладёт его в служебный маркер джобы и возвращает из
            :meth:`Runtime.reconcile_dead` (ARCHITECTURE UC-15). Для колбэков — 0.
    """

    id: UUID
    batch_id: UUID
    kind: OutboxKind
    task_name: str
    payload: bytes
    options: Mapping[str, object] = field(default_factory=_no_options)
    generation: int = 0


class Verdict(StrEnum):
    """Что брокер сделает после исключения задачи."""

    RETRY = "retry"
    """Брокер повторит задачу: Item отпускается (``release``), попытка +1."""
    FINAL = "final"
    """Повтора не будет: Item завершается ошибкой (``finish(error)``)."""


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Одна джоба Item, которую брокер окончательно отправил в DLQ.

    Attributes:
        item_id: Item из служебного маркера джобы.
        generation: поколение отправки из маркера — :attr:`Message.generation`
            сообщения, по которому джоба создана. Сверка завершает Item, только
            если это его текущее поколение (ARCHITECTURE UC-15).
        detail: текст ошибки брокера для ``th_item.error``; ``None`` — нет.
    """

    item_id: UUID
    generation: int = 0
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class DeadLetters:
    """Результат одного шага сверки с DLQ брокера.

    Attributes:
        entries: мёртвые джобы этой порции. Одна и та же джоба может прийти
            повторно: движок применяет её идемпотентно.
        cursor: непрозрачный курсор для следующего вызова
            :meth:`Runtime.reconcile_dead`; ``None`` — начать сначала. Движок
            хранит его в БД и сдвигает в транзакции, которая применила
            ``entries``.
        more: порция не последняя: движку стоит сразу вызвать
            :meth:`Runtime.reconcile_dead` с новым курсором.
    """

    entries: tuple[DeadLetter, ...]
    cursor: str | None
    more: bool = False

    @property
    def item_ids(self) -> tuple[UUID, ...]:
        """Items порции в порядке ``entries``."""
        return tuple(entry.item_id for entry in self.entries)


@runtime_checkable
class Dispatcher(Protocol):
    """Сторона продюсера: имена задач и отправка пачки сообщений брокеру."""

    def task_name(self, fn: Callable[P, object]) -> str:
        """Имя, под которым брокер знает функцию задачи.

        Args:
            fn: функция задачи (как её передал пользователь).

        Returns:
            Имя задачи у брокера.
        """
        ...

    async def dispatch(self, messages: Sequence[Message]) -> None:
        """Поставить сообщения в брокер.

        Возврат без исключения означает, что все сообщения приняты брокером и
        записи outbox можно удалить. При исключении relay повторит всю пачку.

        Args:
            messages: пачка сообщений одного прохода relay.
        """
        ...


@runtime_checkable
class CallOptionsValidator(Protocol):
    """Optional producer-side validation of broker-specific call options."""

    def validate_options(self, options: Mapping[str, object]) -> None:
        """Reject invalid options before an Item and its outbox row are written."""
        ...


@runtime_checkable
class RelayPolicy(Protocol):
    """Необязательная часть адаптера: кто вызывает проходы relay (ARCHITECTURE §3.2).

    Обычный адаптер этот протокол не реализует: relay сам запускает фоновый
    цикл при первом ``kick``. Детерминированный тестовый брокер отключает
    автозапуск и вызывает проходы relay сам, чтобы сообщения не уходили в
    обход теста.
    """

    @property
    def relay_autostart(self) -> bool:
        """Запускать ли фоновый цикл relay по ``kick`` после commit."""
        ...


@runtime_checkable
class CancellationClassifier(Protocol):
    """Optional adapter hook for broker-native cooperative cancellation signals."""

    def is_cancelled(self, exc: BaseException) -> bool:
        """Return whether ``exc`` means that the running broker job was cancelled."""
        ...


@runtime_checkable
class RetryLimits(Protocol):
    """Необязательная часть адаптера: умолчание лимита повторов задачи.

    Опция вызова ``max_retries`` хранится в ``th_item.options``, а умолчание
    задачи (декоратор или настройка брокера) знает только адаптер. Sweeper
    спрашивает его, когда решает, вернуть ли Item с истёкшим lease в outbox
    (ARCHITECTURE UC-15, D-012).
    """

    def max_retries(self, task_name: str) -> int:
        """Лимит повторов задачи, когда у вызова нет опции ``max_retries``.

        Значение должно совпадать с тем, что адаптер передаёт брокеру при
        ``dispatch``. Метод не бросает исключений: для незнакомой задачи — 0.

        Args:
            task_name: имя задачи у брокера (:meth:`Dispatcher.task_name`).

        Returns:
            Неотрицательное число повторов.
        """
        ...


@runtime_checkable
class Runtime(Protocol):
    """Сторона воркера: исполнение задачи под учётом tallyho."""

    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        """Обернуть функцию задачи для регистрации в брокере.

        Обёртка вынимает служебный маркер Item из аргументов, захватывает Item
        и после исполнения сообщает итог. Имя задачи у брокера не меняется.

        Args:
            fn: ``async def``-функция задачи.

        Returns:
            Обёртка с той же сигнатурой.
        """
        ...

    def retry_verdict(self, exc: BaseException) -> Verdict:
        """Будет ли брокер повторять текущую задачу после исключения ``exc``.

        Вызывается внутри исполнения задачи, поэтому адаптер знает текущую
        попытку и конфиг ретраев задачи.

        Args:
            exc: исключение, брошенное функцией задачи.

        Returns:
            :attr:`Verdict.RETRY` или :attr:`Verdict.FINAL`.
        """
        ...

    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        """Отдать следующую порцию джоб Items из DLQ брокера.

        Движок вызывает метод периодически из процессов с адаптером и хранит
        курсор между вызовами (ARCHITECTURE UC-15). Порция должна быть
        ограничена: остаток адаптер отдаёт следующими вызовами, выставляя
        :attr:`DeadLetters.more`. Курсор не должен возвращаться назад: джоба,
        попавшая в DLQ до вызова, рано или поздно отдаётся хотя бы один раз.
        Повторная выдача той же джобы допустима.

        Args:
            since: курсор из прошлого :class:`DeadLetters` или ``None``.

        Returns:
            Мёртвые джобы Items и курсор для следующего вызова.
        """
        ...


@runtime_checkable
class WorkerRuntime(Protocol):
    """Собранный engine-owned around-runtime, безопасный для слоя адаптеров."""

    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R | None]]:
        """Обернуть tracked-задачу, не раскрывая engine-типы адаптеру."""
        ...


class WorkerFactory(Protocol):
    """Композиционная фабрика runtime без зависимости engine от верхнего слоя."""

    def __call__(
        self,
        *,
        completer: object,
        broker: Runtime,
        dispatcher: Dispatcher,
        tree_cache: object,
        heartbeat_every: timedelta,
    ) -> WorkerRuntime:
        """Собрать runtime из непрозрачных engine-сервисов."""
        ...


@runtime_checkable
class WorkerServices(Protocol):
    """Узкая граница engine → adapter для сборки runtime и обработки DLQ."""

    @property
    def runtime(self) -> WorkerRuntime:
        """Уже собранный around-runtime этой установки."""
        ...

    async def finish_dead(
        self, item_id: UUID, batch_id: UUID, *, error_type: str, detail: str
    ) -> None:
        """Завершить Item, окончательно убитый внешним брокером."""
        ...


@runtime_checkable
class RuntimeInstaller(Protocol):
    """Необязательная часть адаптера, принимающая собранные worker-сервисы.

    Конкретный объект сервисов принадлежит engine-слою. Протокол оставляет его
    ``object``, чтобы нижний слой не импортировал engine, а адаптер проверил
    ожидаемый тип на своей границе.
    """

    def install_runtime(self, services: object) -> None:
        """Установить tracked around-hook и системные broker hooks."""
        ...
