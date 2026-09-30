"""Счётчики батчей: дельты, суммы и запросы к ``th_counter`` (ARCHITECTURE §9).

Счётчики хранятся двумя путями (§9.1, COUNTERS §3.3):

* путь A — Completer прибавляет дельту к строке ``th_counter`` своего слота;
* путь B — транзакция пользователя только вставляет строку в
  ``th_counter_delta``, а Completer потом сворачивает её в слот.

Здесь описаны значения, которыми обмениваются эти пути:
:class:`CounterDelta` (приращение) и :class:`CounterTotals` (точная сумма
слотов и несвёрнутых дельт).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

__all__ = [
    "COUNTER_FIELDS",
    "DELTA_FIELDS",
    "CounterDelta",
    "CounterTotals",
]

COUNTER_FIELDS: Final = (
    "total",
    "ok",
    "skip",
    "error",
    "cancelled",
    "dispatched",
    "w_total",
    "w_done",
    "duplicates",
    "skipped_by_limit",
    "tree_total",
)
"""Счётчики строки ``th_counter`` в порядке колонок (ARCHITECTURE §5.1)."""

DELTA_FIELDS: Final = ("total", "ok", "skip", "error", "cancelled", "w_done")
"""Счётчики, у которых есть колонка ``d_<имя>`` в ``th_counter_delta``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class CounterDelta:
    """Приращение счётчиков одного батча.

    Поля совпадают с колонками ``th_counter``. Путь B (``th_counter_delta``)
    хранит только поля из :data:`DELTA_FIELDS`.
    """

    total: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    dispatched: int = 0
    w_total: int = 0
    w_done: int = 0
    duplicates: int = 0
    skipped_by_limit: int = 0
    tree_total: int = 0

    def as_dict(self) -> dict[str, int]:
        """Значения по именам.

        Returns:
            Словарь в порядке :data:`COUNTER_FIELDS`.
        """
        return {
            "total": self.total,
            "ok": self.ok,
            "skip": self.skip,
            "error": self.error,
            "cancelled": self.cancelled,
            "dispatched": self.dispatched,
            "w_total": self.w_total,
            "w_done": self.w_done,
            "duplicates": self.duplicates,
            "skipped_by_limit": self.skipped_by_limit,
            "tree_total": self.tree_total,
        }

    @property
    def is_zero(self) -> bool:
        """Все поля равны нулю: записывать нечего."""
        return not any(self.as_dict().values())

    @property
    def fits_delta_table(self) -> bool:
        """Дельту можно записать в ``th_counter_delta`` без потерь."""
        values = self.as_dict()
        return not any(values[name] for name in COUNTER_FIELDS if name not in DELTA_FIELDS)

    def __add__(self, other: CounterDelta) -> CounterDelta:
        """Сумма двух приращений.

        Returns:
            Поэлементная сумма.
        """
        return CounterDelta(
            total=self.total + other.total,
            ok=self.ok + other.ok,
            skip=self.skip + other.skip,
            error=self.error + other.error,
            cancelled=self.cancelled + other.cancelled,
            dispatched=self.dispatched + other.dispatched,
            w_total=self.w_total + other.w_total,
            w_done=self.w_done + other.w_done,
            duplicates=self.duplicates + other.duplicates,
            skipped_by_limit=self.skipped_by_limit + other.skipped_by_limit,
            tree_total=self.tree_total + other.tree_total,
        )

    def __neg__(self) -> CounterDelta:
        """Обратное приращение.

        Returns:
            Приращение с противоположными знаками.
        """
        return CounterDelta(**{name: -value for name, value in self.as_dict().items()})

    def __sub__(self, other: CounterDelta) -> CounterDelta:
        """Разность двух приращений.

        Returns:
            Поэлементная разность.
        """
        return self + -other


@dataclass(frozen=True, slots=True, kw_only=True)
class CounterTotals:
    """Точные счётчики батча: сумма слотов ``th_counter`` и несвёрнутых дельт (§9.3).

    Attributes:
        total: Уникальные Items батча (``found``).
        ok: Завершены с классом ``ok``.
        skip: Завершены с классом ``skip``.
        error: Завершены с классом ``error``.
        cancelled: Отменены.
        dispatched: Отправлены брокеру.
        w_total: Сумма весов всех Items.
        w_done: Сумма весов завершённых Items.
        duplicates: Отсечённые дубли spawn/add.
        skipped_by_limit: Не созданы из-за лимитов дерева.
        tree_total: Items всего дерева (только у корня).
    """

    total: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    dispatched: int = 0
    w_total: int = 0
    w_done: int = 0
    duplicates: int = 0
    skipped_by_limit: int = 0
    tree_total: int = 0

    @property
    def done(self) -> int:
        """Завершённые Items: ``ok + skip + error + cancelled``."""
        return self.ok + self.skip + self.error + self.cancelled

    @property
    def pending(self) -> int:
        """Незавершённые Items: ``total - done``."""
        return self.total - self.done

    def as_delta(self) -> CounterDelta:
        """Те же значения как приращение от нуля.

        Returns:
            Приращение с теми же полями.
        """
        return CounterDelta(
            total=self.total,
            ok=self.ok,
            skip=self.skip,
            error=self.error,
            cancelled=self.cancelled,
            dispatched=self.dispatched,
            w_total=self.w_total,
            w_done=self.w_done,
            duplicates=self.duplicates,
            skipped_by_limit=self.skipped_by_limit,
            tree_total=self.tree_total,
        )

    def __add__(self, delta: CounterDelta) -> CounterTotals:
        """Счётчики после приращения ``delta``.

        Returns:
            Новые счётчики.
        """
        return CounterTotals(**(self.as_delta() + delta).as_dict())
