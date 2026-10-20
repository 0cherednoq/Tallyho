"""Итог попытки задачи как значение: ``Ok``, ``Skip``, ``Error``, ``Defer`` (UC-18).

Задача сообщает итог возвращаемым значением, а не только императивными вызовами
на фасаде ``item``: итог виден в её сигнатуре и проверяется в тесте сравнением
(``assert await task(...) == Defer(reason="quota")``). Обёртка tracked переводит
эти значения в тот же внутренний итог, что ``item.ok``/``skip``/``error``/``defer``,
поэтому новых путей записи в БД они не создают.

Типы живут в ``model``: их валидация не зависит ни от runtime, ни от engine.
Проверка метки как имени счётчика (``check_counter_name``) остаётся на записи
итога — она принадлежит слою storage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, TypeAlias

from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from datetime import datetime

__all__ = ["Defer", "Error", "Ok", "Outcome", "Skip"]

_DEFER_EMPTY_REASON = "defer требует непустой reason"
_DEFER_DELAY_AND_UNTIL = "defer принимает delay или until, но не оба сразу"
_DEFER_NEGATIVE_DELAY = "delay у defer не может быть отрицательным"


@dataclass(frozen=True, slots=True)
class Ok:
    """Успешный итог попытки: эквивалент ``item.ok(label, result=…, mark=…)``.

    ``label`` без значения даёт метку ``"ok"``, как у императивного вызова.
    """

    label: str | None = None
    result: object = field(default=None, kw_only=True)
    mark: bool | None = field(default=None, kw_only=True)


@dataclass(frozen=True, slots=True)
class Skip:
    """Пропущенный итог попытки: эквивалент ``item.skip(label, mark=…)``."""

    label: str | None = None
    mark: bool | None = field(default=None, kw_only=True)


@dataclass(frozen=True, slots=True)
class Error:
    """Ошибочный итог без исключения: эквивалент ``item.error(label, detail=…)``.

    Терминален: ретраев брокера за ним не следует, в отличие от исключения,
    вышедшего из задачи (UC-04).
    """

    label: str | None = None
    detail: object = field(default=None, kw_only=True)
    mark: bool | None = field(default=None, kw_only=True)


@dataclass(frozen=True, slots=True)
class Defer:
    """Отложение вместо итога: эквивалент ``item.defer(reason=…, delay=…, until=…)``.

    Попытка не тратится: ``attempt`` не растёт, Item возвращается в очередь со
    сроком и причиной (UC-15). Без ``delay`` и ``until`` срок берётся из
    настроек установки (``defer_default_delay``).

    Raises:
        ConfigurationError: пустой ``reason``; ``delay`` и ``until`` вместе;
            отрицательный ``delay``.
    """

    reason: str
    delay: timedelta | None = None
    until: datetime | None = None

    def __post_init__(self) -> None:
        """Проверить причину и срок до того, как значение уйдёт в запись итога.

        Raises:
            ConfigurationError: нарушено одно из правил ``defer``.
        """
        if not self.reason:
            raise ConfigurationError(_DEFER_EMPTY_REASON)
        if self.delay is not None and self.until is not None:
            raise ConfigurationError(_DEFER_DELAY_AND_UNTIL)
        if self.delay is not None and self.delay < timedelta(0):
            raise ConfigurationError(_DEFER_NEGATIVE_DELAY)


Outcome: TypeAlias = Ok | Skip | Error | Defer
"""Закрытое объединение итогов, которые задача может вернуть (UC-18)."""
