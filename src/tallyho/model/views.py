"""Value-объекты чтения: прогресс, сводка для хуков, представления батча и Item.

Все классы — неизменяемые ``dataclass(frozen=True, slots=True)`` (ARCHITECTURE §3.4).
Словари (``labels``, ``metrics``, ``children``) при создании копируются
в ``MappingProxyType``: сводку, переданную в tx-хук, нельзя поменять на месте.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime, timedelta
    from uuid import UUID

    from tallyho.model.states import BatchState, CancelReason, ItemState

__all__ = [
    "BatchSummary",
    "BatchView",
    "InFlightItem",
    "ItemView",
    "Progress",
]

_V = TypeVar("_V")


def _freeze(mapping: Mapping[str, _V]) -> Mapping[str, _V]:
    """Сделать неизменяемую копию словаря.

    Returns:
        ``MappingProxyType`` над копией ``mapping``.
    """
    return MappingProxyType(dict(mapping))


@dataclass(frozen=True, slots=True, kw_only=True)
class Progress:
    """Прогресс одного батча (ARCHITECTURE §9.3-9.4).

    ``found`` — уникальные Items (ретраи и дубли не входят), ``queued`` —
    ожидают выполнения (``pending - in_flight``), ``ok/skip/error/cancelled`` —
    завершённые по классам итога. ``final`` — итог окончательный: батч
    финализирован. ``expected`` — ожидаемый итог (``None``, если оценки нет),
    ``expected_is_estimate`` — это оценка, а не точное число; ``estimate_basis`` —
    на скольких завершённых родителях построена оценка по ``fed_by``.
    ``ratio`` — доля по весам задач, ``eta`` — время до опустошения.
    """

    found: int = 0
    queued: int = 0
    in_flight: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    duplicates: int = 0
    skipped_by_limit: int = 0
    final: bool = False
    expected: int | None = None
    expected_is_estimate: bool = False
    estimate_basis: int | None = None
    ratio: float | None = None
    eta: timedelta | None = None

    @property
    def done(self) -> int:
        """Завершённые Items: ``ok + skip + error + cancelled``."""
        return self.ok + self.skip + self.error + self.cancelled

    @property
    def pending(self) -> int:
        """Ещё не завершённые Items: ``found - done``."""
        return self.found - self.done


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchSummary:
    """Сводка дерева батчей, которую получают tx-хуки (ARCHITECTURE §7.2, §12.4).

    ``children`` — под-батчи по ключу (``s.children["send"]``), ``labels`` —
    разбивка итогов по меткам, ``metrics`` — пользовательские счётчики
    (``th.item.incr``). ``seq`` монотонно растёт в пределах батча (снимки и
    финализация). ``reason`` — причина запроса отмены, если он был.
    """

    id: UUID
    kind: str
    key: str | None
    state: BatchState
    progress: Progress
    labels: Mapping[str, int]
    metrics: Mapping[str, int]
    children: Mapping[str, BatchSummary]
    seq: int
    reason: CancelReason | None = None
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        """Заморозить словари."""
        object.__setattr__(self, "labels", _freeze(self.labels))
        object.__setattr__(self, "metrics", _freeze(self.metrics))
        object.__setattr__(self, "children", _freeze(self.children))


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchView:
    """Состояние батча и его поддерева для ``handle.view()`` / ``watch()`` (UC-13).

    В отличие от :class:`BatchSummary` содержит технические флаги: пауза,
    запрос отмены, отложенный старт, дедлайн, ошибка tx-хука.
    """

    id: UUID
    kind: str
    key: str | None
    state: BatchState
    progress: Progress
    labels: Mapping[str, int]
    metrics: Mapping[str, int]
    children: Mapping[str, BatchView]
    reason: CancelReason | None = None
    paused_at: datetime | None = None
    cancel_requested_at: datetime | None = None
    start_at: datetime | None = None
    deadline_at: datetime | None = None
    created_at: datetime | None = None
    finished_at: datetime | None = None
    hook_attempts: int = 0
    hook_error: str | None = None

    def __post_init__(self) -> None:
        """Заморозить словари."""
        object.__setattr__(self, "labels", _freeze(self.labels))
        object.__setattr__(self, "metrics", _freeze(self.metrics))
        object.__setattr__(self, "children", _freeze(self.children))

    @property
    def paused(self) -> bool:
        """Батч на паузе."""
        return self.paused_at is not None

    @property
    def cancel_requested(self) -> bool:
        """Запрошена отмена (явная, по дедлайну или политике)."""
        return self.cancel_requested_at is not None


@dataclass(frozen=True, slots=True, kw_only=True)
class ItemView:
    """Item батча для ``handle.items(label=)`` (ARCHITECTURE §5.1, §6.2).

    ``child_batch_id`` задан у виртуального Item под-батча. ``result`` и
    ``error`` — JSON-значения из ``ok(result=)`` / ``error(detail=)``.
    """

    id: UUID
    batch_id: UUID
    state: ItemState
    task_name: str
    label: str | None = None
    attempt: int = 0
    depth: int = 0
    key: str | None = None
    weight: int = 1
    child_batch_id: UUID | None = None
    result: object = None
    error: object = None
    created_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class InFlightItem:
    """Выполняющийся Item для ``handle.in_flight()`` (ARCHITECTURE §9.4).

    ``age`` — сколько задача держит lease; ``progress_done/progress_total`` —
    собственный прогресс задачи из ``th.item.progress``.
    """

    id: UUID
    batch_id: UUID
    worker_id: str
    attempt: int
    lease_until: datetime
    age: timedelta
    progress_done: int | None = None
    progress_total: int | None = None
