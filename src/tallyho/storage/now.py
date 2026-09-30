"""«Сейчас» в SQL (DECISIONS D-002).

Все сроки считаются по времени БД. В рабочем режиме «сейчас» — это ``now()``
PostgreSQL (время начала транзакции). В тестах ``FakeClock`` возвращает aware
``datetime``, и storage биндит его параметром вместо ``now()``.

``storage`` не импортирует :mod:`tallyho.protocols` (import-linter), поэтому
:func:`sql_now` принимает любой объект с методом ``now() -> datetime | None``
(структурный :class:`NowSource`). Протокол ``Clock`` ему соответствует, и
вызывающий код передаёт часы как есть: ``sql_now(clock)``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from sqlalchemy import DateTime, func, literal

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import ColumnElement

__all__ = ["NowSource", "sql_now"]

_NAIVE_MESSAGE = "Clock.now() вернул naive datetime; нужен aware (с tzinfo) или None"


class NowSource(Protocol):
    """Минимальная часть протокола ``Clock``, нужная storage."""

    def now(self) -> datetime | None:
        """Текущее время для SQL.

        Returns:
            ``None`` — использовать ``now()`` БД; иначе aware ``datetime``.
        """
        ...


def sql_now(clock: NowSource) -> ColumnElement[datetime]:
    """SQL-выражение «сейчас» для запросов storage.

    Args:
        clock: Часы движка (``Clock``): ``now()`` возвращает ``None`` или aware
            ``datetime``.

    Returns:
        ``now()`` БД, если часы вернули ``None``; иначе bind-параметр типа
        ``timestamptz`` с этим значением.

    Raises:
        TypeError: Часы вернули naive ``datetime``: PostgreSQL молча отнёс бы
            его к часовому поясу сессии.
    """
    value = clock.now()
    if value is None:
        return func.now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise TypeError(_NAIVE_MESSAGE)
    return literal(value, DateTime(timezone=True))
