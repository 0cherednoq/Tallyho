"""Перечисления технических состояний батча, Item и связанных кодов.

Числовые значения — это ``smallint`` в БД (DECISIONS D-005). Их нельзя менять без
миграции: снимок значений закреплён тестом ``tests/unit/model/test_states.py``.
Терминальные состояния батча и Item имеют код ``>= 10``, поэтому в SQL
«активен» — это ``state < 10``.
"""

from __future__ import annotations

from enum import IntEnum, StrEnum
from typing import Final

__all__ = [
    "TERMINAL_THRESHOLD",
    "BatchState",
    "CancelReason",
    "ItemState",
    "OnFeederFailed",
    "OutboxKind",
    "ResultClass",
]

TERMINAL_THRESHOLD: Final = 10
"""Коды состояний ``>= TERMINAL_THRESHOLD`` терминальные (D-005)."""


class BatchState(IntEnum):
    """Техническое состояние батча (``th_batch.state``, ARCHITECTURE §6.1).

    ``finalizing`` существует только внутри транзакции финализации и в БД не
    сохраняется; код зарезервирован.
    """

    OPEN = 0
    SEALED = 1
    FINALIZING = 2
    SUCCEEDED = 10
    COMPLETED_WITH_ERRORS = 11
    FAILED = 12
    CANCELLED = 13

    @property
    def is_terminal(self) -> bool:
        """Батч финализирован и больше не меняет состояние сам по себе."""
        return self.value >= TERMINAL_THRESHOLD


class ResultClass(IntEnum):
    """Технический класс итога Item: ``ok / skip / error / cancelled``.

    Коды совпадают с терминальными значениями :class:`ItemState`, потому что
    в ``th_item.state`` хранится либо ``active``, либо класс итога (§6.2).
    """

    OK = 10
    SKIP = 11
    ERROR = 12
    CANCELLED = 13

    @property
    def item_state(self) -> ItemState:
        """Терминальное состояние Item, соответствующее классу итога."""
        return ItemState(self.value)


class ItemState(IntEnum):
    """Хранимое состояние Item (``th_item.state``, ARCHITECTURE §6.2)."""

    ACTIVE = 0
    OK = 10
    SKIP = 11
    ERROR = 12
    CANCELLED = 13

    @property
    def is_terminal(self) -> bool:
        """Item завершён; после этого он неизменяем."""
        return self.value >= TERMINAL_THRESHOLD

    @property
    def result_class(self) -> ResultClass | None:
        """Класс итога для терминального Item, ``None`` для ``active``."""
        return ResultClass(self.value) if self.is_terminal else None


class OnFeederFailed(IntEnum):
    """Реакция этапа на упавший источник (``th_batch.on_feeder_failed``).

    ``SEAL`` — закрыть этап и доделать полученное (по умолчанию), ``CANCEL`` —
    поставить этапу запрос отмены (ARCHITECTURE §6.1, §8.1).
    """

    SEAL = 0
    CANCEL = 1


class OutboxKind(IntEnum):
    """Тип записи outbox (``th_outbox.kind``): Item к отправке или колбэк."""

    ITEM = 0
    CALLBACK = 1


class CancelReason(StrEnum):
    """Причина запроса отмены (``th_batch.cancel_reason``, колонка ``text``).

    ``CANCEL`` — явный ``cancel()`` → итог ``cancelled``; ``DEADLINE``,
    ``FAIL_FAST`` и ``POLICY`` (порог политики с ``action="fail"``) → итог
    ``failed`` (ARCHITECTURE §6.1, UC-15).
    """

    CANCEL = "cancel"
    DEADLINE = "deadline"
    FAIL_FAST = "fail_fast"
    POLICY = "policy"

    @property
    def terminal_state(self) -> BatchState:
        """Итоговое состояние батча, финализированного по этой причине."""
        return BatchState.CANCELLED if self is CancelReason.CANCEL else BatchState.FAILED
