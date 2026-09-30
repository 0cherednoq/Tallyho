"""Время: протокол :class:`Clock` и системная реализация (DECISIONS D-002).

Все сроки (``lease_until``, ``available_at``, ``start_at``, ``deadline_at``,
retention, backoff хуков) считаются по времени БД, а не по часам процесса.
``protocols`` не зависит от SQLAlchemy, поэтому «сейчас» передаётся в ``storage``
не SQL-выражением, а значением:

* ``Clock.now()`` возвращает ``None`` — storage подставляет ``now()`` БД
  (время начала транзакции PostgreSQL);
* ``Clock.now()`` возвращает aware ``datetime`` — storage биндит его параметром
  вместо ``now()``. Так ``FakeClock`` из ``tallyho.testing`` двигает время в SQL.

Интервалы внутри процесса (тик Completer, heartbeat, таймауты ожидания) — это
время event loop (``asyncio``), а не :class:`Clock`. ``Clock.monotonic()`` нужен
только для измерения длительностей (события :class:`~tallyho.protocols.Observer`).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from typing_extensions import override

if TYPE_CHECKING:
    from datetime import datetime

__all__ = ["Clock", "SystemClock"]


@runtime_checkable
class Clock(Protocol):
    """Источник времени для движка."""

    def now(self) -> datetime | None:
        """Текущее время для SQL.

        Returns:
            ``None`` — использовать ``now()`` БД; иначе aware ``datetime``,
            который storage передаёт параметром вместо ``now()``.
        """
        ...

    def monotonic(self) -> float:
        """Монотонные секунды для измерения длительностей внутри процесса.

        Returns:
            Секунды от произвольной точки отсчёта; не убывают.
        """
        ...


class SystemClock(Clock):
    """Рабочие часы: «сейчас» — это ``now()`` БД, длительности — ``time.monotonic``."""

    @override
    def now(self) -> datetime | None:
        """Всегда ``None``: storage использует ``now()`` БД.

        Returns:
            ``None``.
        """
        return None

    @override
    def monotonic(self) -> float:
        """Значение ``time.monotonic()``.

        Returns:
            Монотонные секунды процесса.
        """
        return time.monotonic()
