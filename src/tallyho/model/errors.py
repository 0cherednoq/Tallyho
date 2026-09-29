"""Иерархия исключений tallyho.

Правило: всё, что библиотека бросает наружу, наследуется от :class:`TallyhoError`.
Проверяется тестом ``tests/architecture/test_conventions.py``.

Превышение ``max_items`` / ``max_depth`` — не ошибка: лишние Items не
вставляются и учитываются в счётчике ``skipped_by_limit`` (ARCHITECTURE §8.1).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID

__all__ = [
    "BatchPurged",
    "ConcurrentModification",
    "ConfigurationError",
    "DownstreamFinalized",
    "HookMissingError",
    "HookTransactionError",
    "InvalidStateError",
    "NotFoundError",
    "SealError",
    "SpawnTargetError",
    "TallyhoError",
    "UnsupportedOption",
]


class TallyhoError(Exception):
    """Базовое исключение библиотеки."""


class ConfigurationError(TallyhoError):
    """Неверная конфигурация клиента, адаптера или батча."""


class NotFoundError(TallyhoError):
    """Батч или Item не найден."""


class InvalidStateError(TallyhoError):
    """Операция недопустима в текущем техническом состоянии батча."""


class ConcurrentModification(TallyhoError):  # ruff: ignore[error-suffix-on-exception-name]  # имя из публичного API
    """Строку батча одновременно изменила другая транзакция, повтор не помог."""


class HookTransactionError(TallyhoError):
    """Tx-хук вызвал ``commit()`` / ``rollback()`` чужой транзакции (A-DB-05).

    Транзакцией хука управляет tallyho; финализация в этом случае откатывается.
    """


class SealError(InvalidStateError):
    """Нарушено правило seal (ARCHITECTURE §6.1, §8.1).

    ``seal()`` этапа с ``fed_by`` — его закрывает библиотека; ``add`` в батч
    после seal или запроса отмены.
    """


class SpawnTargetError(InvalidStateError):
    """Запись в этап, писателем которого вызывающий не является (A-UC-18).

    Писать в этап с ``fed_by`` могут только его собственные задачи и задачи
    его источников (``into=``); продюсер добавлять в такой этап не может.
    """


class DownstreamFinalized(InvalidStateError):  # ruff: ignore[error-suffix-on-exception-name]  # имя из ARCHITECTURE §8 UC-16
    """``retry_failed`` этапа, чьи этапы-получатели уже финализированы (A-UC-13).

    Повтор всего конвейера — ``retry_failed`` на корне.
    """


class BatchPurged(NotFoundError):  # ruff: ignore[error-suffix-on-exception-name]  # имя из ARCHITECTURE §7.6
    """Батч удалён по retention; итог к этому моменту уже в домене (§7.6)."""

    batch_id: UUID

    def __init__(self, batch_id: UUID) -> None:
        """Ошибка для удалённого батча ``batch_id``."""
        super().__init__(f"батч {batch_id} удалён по retention")
        self.batch_id = batch_id


class HookMissingError(ConfigurationError):
    """Батч требует tx-хук, который в процессе не зарегистрирован."""

    kind: str
    hook: str

    def __init__(self, kind: str, hook: str) -> None:
        """Ошибка для хука ``hook`` (``"on_finalized"`` и т. п.) типа батча ``kind``."""
        hint = "проверьте hook_modules в конфигурации Tallyho"
        super().__init__(f"для kind={kind!r} не зарегистрирован tx-хук {hook!r}; {hint}")
        self.kind = kind
        self.hook = hook


class UnsupportedOption(ConfigurationError):  # ruff: ignore[error-suffix-on-exception-name]  # имя из ARCHITECTURE §11.4
    """Опция брокера несовместима с отслеживаемыми задачами (A-FQ-07)."""

    option: str
    hint: str | None

    def __init__(self, option: str, *, hint: str | None = None) -> None:
        """Ошибка для опции ``option``; ``hint`` подсказывает альтернативу."""
        message = f"опция {option!r} не поддерживается для отслеживаемых задач"
        super().__init__(f"{message}: {hint}" if hint else message)
        self.option = option
        self.hint = hint
