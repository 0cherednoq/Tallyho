"""Наблюдаемость: протокол :class:`Observer` и пустая реализация :class:`NullObserver`.

Движок сообщает о событиях вне транзакций, по принципу fire-and-forget:
методы синхронные и должны возвращаться быстро (метрики, OpenTelemetry, логи).
Исключение наблюдателя движок не пробрасывает в учёт.

Свой наблюдатель удобно наследовать от :class:`NullObserver` и переопределять
только нужные события: новые события в следующих версиях добавятся в
``NullObserver`` пустыми, и наследник останется совместимым.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from typing_extensions import override

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.model.states import BatchState, ResultClass

__all__ = ["NullObserver", "Observer"]


@runtime_checkable
class Observer(Protocol):
    """Receive synchronous, fire-and-forget engine lifecycle events."""

    def batch_created(self, *, batch_id: UUID, kind: str) -> None:
        """A batch was created and its transaction committed."""
        ...

    def item_claimed(self, *, batch_id: UUID, item_id: UUID, attempt: int) -> None:
        """An Item lease was acquired and its transaction committed."""
        ...

    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        """Итог Item зафиксирован (после commit).

        Args:
            batch_id: батч Item.
            item_id: Item.
            result: класс итога.
            label: метка итога, если задана.
            attempt: номер попытки, на которой получен итог.
        """
        ...

    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        """Батч финализирован (после commit транзакции хука).

        Args:
            batch_id: батч.
            kind: тип батча.
            state: терминальное состояние.
        """
        ...

    def hook_failed(
        self, *, batch_id: UUID, kind: str, hook: str, attempt: int, error: BaseException
    ) -> None:
        """Tx-хук упал, финализация откачена и будет повторена с backoff.

        Args:
            batch_id: батч.
            kind: тип батча.
            hook: имя хука (``on_finalized``, ``on_progress``, ``on_policy_breach``).
            attempt: номер неудачной попытки.
            error: исключение хука.
        """
        ...

    def hook_missing(self, *, batch_id: UUID, kind: str, hook: str) -> None:
        """Нужный батчу tx-хук не зарегистрирован в этом процессе; финализация отложена.

        Args:
            batch_id: батч.
            kind: тип батча.
            hook: имя недостающего хука.
        """
        ...

    def relay_dispatched(self, *, messages: int, duration: float) -> None:
        """Relay отправил пачку сообщений брокеру.

        Args:
            messages: число сообщений.
            duration: длительность отправки, секунды (:meth:`Clock.monotonic`).
        """
        ...

    def completer_flush(self, *, items: int, duration: float) -> None:
        """Completer закоммитил групповую транзакцию.

        Args:
            items: число Items в транзакции.
            duration: длительность транзакции, секунды.
        """
        ...

    def relay_lag(self, *, seconds: float) -> None:
        """Age of the oldest outbox record selected by relay."""
        ...

    def completer_buffer(self, *, items: int) -> None:
        """Current number of operations waiting in Completer."""
        ...

    def oldest_lease(self, *, seconds: float) -> None:
        """Age of the oldest active lease since its latest acquisition/heartbeat."""
        ...

    def transaction_retry(self, *, sqlstate: str) -> None:
        """An internal transaction will be retried for a retryable SQLSTATE."""
        ...


class NullObserver(Observer):
    """Наблюдатель по умолчанию: игнорирует все события."""

    @override
    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        return None

    @override
    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        return None

    @override
    def hook_failed(
        self, *, batch_id: UUID, kind: str, hook: str, attempt: int, error: BaseException
    ) -> None:
        return None

    @override
    def hook_missing(self, *, batch_id: UUID, kind: str, hook: str) -> None:
        return None

    @override
    def relay_dispatched(self, *, messages: int, duration: float) -> None:
        return None

    @override
    def completer_flush(self, *, items: int, duration: float) -> None:
        return None

    @override
    def relay_lag(self, *, seconds: float) -> None:
        return None

    @override
    def completer_buffer(self, *, items: int) -> None:
        return None

    @override
    def oldest_lease(self, *, seconds: float) -> None:
        return None

    @override
    def transaction_retry(self, *, sqlstate: str) -> None:
        return None

    @override
    def batch_created(self, *, batch_id: UUID, kind: str) -> None:
        return None

    @override
    def item_claimed(self, *, batch_id: UUID, item_id: UUID, attempt: int) -> None:
        return None
