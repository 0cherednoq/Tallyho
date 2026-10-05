"""Математика прогресса: чистые функции над «сырыми» счётчиками дерева.

Вход — :class:`NodeCounters` каждого батча дерева (строки ``th_counter`` +
дельты, ``count(th_lease)``, ``expected_total`` и источники ``th_feed``),
выход — :class:`~tallyho.model.views.Progress` на каждый узел
(ARCHITECTURE §9.3-9.4). Запросов к БД здесь нет.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final

from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState
from tallyho.model.views import Progress

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from uuid import UUID

__all__ = [
    "DEFAULT_ESTIMATE_MIN_BASIS",
    "DEFAULT_ESTIMATE_MIN_SHARE",
    "DEFAULT_ETA_WINDOW",
    "NodeCounters",
    "ProgressSettings",
    "RateTracker",
    "compute_progress",
    "ema_rate",
    "estimate_eta",
    "estimate_threshold",
]

DEFAULT_ESTIMATE_MIN_BASIS: Final = 20
"""Сколько завершённых родителей достаточно для оценки итога (§15)."""

DEFAULT_ESTIMATE_MIN_SHARE: Final = 0.05
"""Какая доля источника достаточна для оценки итога (§15)."""

DEFAULT_ETA_WINDOW: Final = timedelta(seconds=60)
"""Окно скользящего среднего скорости для ETA (§15)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ProgressSettings:
    """Параметры расчёта прогресса (ARCHITECTURE §15).

    Оценка итога по ``fed_by`` показывается, когда число завершённых
    родителей достигло ``min(estimate_min_basis, estimate_min_share * expected_F)``.
    ``eta_window`` — окно EMA скорости (:func:`ema_rate`).
    """

    estimate_min_basis: int = DEFAULT_ESTIMATE_MIN_BASIS
    estimate_min_share: float = DEFAULT_ESTIMATE_MIN_SHARE
    eta_window: timedelta = DEFAULT_ETA_WINDOW

    def __post_init__(self) -> None:
        """Проверить параметры (``ConfigurationError`` при выходе за диапазон)."""
        _check_min_basis(self.estimate_min_basis)
        _check_min_share(self.estimate_min_share)
        _check_window(self.eta_window)


def _check_min_basis(basis: object) -> None:
    if isinstance(basis, bool) or not isinstance(basis, int) or basis < 0:
        message = f"estimate_min_basis должен быть целым >= 0, получено {basis!r}"
        raise ConfigurationError(message)


def _check_min_share(share: object) -> None:
    if isinstance(share, bool) or not isinstance(share, int | float) or not 0 <= share <= 1:
        message = f"estimate_min_share должен быть в [0, 1], получено {share!r}"
        raise ConfigurationError(message)


def _check_window(window: object) -> None:
    if not isinstance(window, timedelta) or window <= timedelta(0):
        message = f"eta_window должен быть положительным timedelta, получено {window!r}"
        raise ConfigurationError(message)


def ema_rate(
    previous: float | None,
    *,
    done_delta: int,
    elapsed: timedelta,
    window: timedelta = DEFAULT_ETA_WINDOW,
) -> float | None:
    """Обновить скользящее среднее скорости (завершённых Items в секунду).

    Вес нового замера ``1 - exp(-elapsed / window)``: снимки идут неравномерно,
    поэтому сглаживание зависит от прошедшего времени, а не от числа замеров.

    Returns:
        Новую скорость; ``previous``, если время не прошло.
    """
    seconds = elapsed.total_seconds()
    if seconds <= 0:
        return previous
    instant = max(done_delta, 0) / seconds
    if previous is None:
        return instant
    alpha = 1.0 - math.exp(-seconds / window.total_seconds())
    return previous + alpha * (instant - previous)


def estimate_eta(*, expected: int | None, done: int, rate: float | None) -> timedelta | None:
    """Время до опустошения: ``(expected - done) / rate`` (§9.4).

    Returns:
        ``timedelta(0)``, если работы не осталось; ``None`` без ``expected`` или скорости.
    """
    if expected is None:
        return None
    remaining = expected - done
    if remaining <= 0:
        return timedelta(0)
    if rate is None or rate <= 0:
        return None
    return timedelta(seconds=remaining / rate)


@dataclass(slots=True)
class _RatePoint:
    done: int
    observed_at: float
    value: float | None = None


@dataclass(eq=False, slots=True)
class RateTracker:
    """Состояние EMA скорости по замерам одного потока чтений (§9.4, D-024).

    Замер — ``done`` каждого батча дерева и монотонное время чтения; скорость
    узла — :func:`ema_rate` по разнице с прошлым замером. Батчи, которых нет в
    очередном замере, забываются. Запросов к БД нет: состояние держит вызывающий
    (``watch()``), по одному трекеру на поток.
    """

    _points: dict[UUID, _RatePoint] = field(default_factory=dict, init=False)

    def observe(
        self,
        done: Mapping[UUID, int],
        *,
        now: float,
        window: timedelta = DEFAULT_ETA_WINDOW,
    ) -> Mapping[UUID, float]:
        """Учесть замер ``done`` по батчам на момент ``now`` (секунды монотонных часов).

        Returns:
            Положительные скорости (завершённых Items в секунду) по id батча.
        """
        points: dict[UUID, _RatePoint] = {}
        for batch_id, value in done.items():
            point = self._points.get(batch_id)
            if point is None:
                point = _RatePoint(value, now)
            elif now > point.observed_at:
                # Без прошедшего времени замер копится до следующего чтения.
                point.value = ema_rate(
                    point.value,
                    done_delta=value - point.done,
                    elapsed=timedelta(seconds=now - point.observed_at),
                    window=window,
                )
                point.done = value
                point.observed_at = now
            points[batch_id] = point
        self._points = points
        return {batch_id: point.value for batch_id, point in points.items() if point.value}


@dataclass(frozen=True, slots=True, kw_only=True)
class NodeCounters:
    """«Сырые» счётчики одного батча дерева.

    ``total/ok/skip/error/cancelled/w_total/w_done/duplicates/skipped_by_limit`` —
    суммы ``th_counter`` с дельтами (§9.3), ``in_flight`` — ``count(th_lease)``.
    ``fed_by`` — id батчей-источников (``th_feed``). Виртуальные Items
    под-батчей входят в ``total/ok/...`` родителя; их вес должен быть ``0``,
    иначе он исказит ``ratio`` родителя.
    """

    id: UUID
    parent_id: UUID | None = None
    state: BatchState = BatchState.OPEN
    total: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    w_total: int = 0
    w_done: int = 0
    duplicates: int = 0
    skipped_by_limit: int = 0
    in_flight: int = 0
    expected_total: int | None = None
    fed_by: tuple[UUID, ...] = field(default=())

    @property
    def done(self) -> int:
        """Завершённые Items: ``ok + skip + error + cancelled``."""
        return self.ok + self.skip + self.error + self.cancelled

    @property
    def closed(self) -> bool:
        """Батч закрыт для новых Items: ``sealed``, ``finalizing`` или терминальный."""
        return self.state is not BatchState.OPEN


@dataclass(frozen=True, slots=True)
class _Expected:
    value: int | None
    is_estimate: bool = False
    basis: int | None = None


def estimate_threshold(expected_sources: int, settings: ProgressSettings) -> float:
    """Минимальная выборка родителей для оценки итога по ветвлению.

    Returns:
        ``min(estimate_min_basis, estimate_min_share * expected_sources)``.
    """
    return min(float(settings.estimate_min_basis), settings.estimate_min_share * expected_sources)


class _Tree:
    """Дерево узлов с мемоизацией ``expected`` (рекурсия по ``fed_by``)."""

    def __init__(self, nodes: Iterable[NodeCounters], settings: ProgressSettings) -> None:
        self.settings: Final = settings
        self.nodes: Final[dict[UUID, NodeCounters]] = {}
        self.children: Final[dict[UUID, list[UUID]]] = {}
        for node in nodes:
            if node.id in self.nodes:
                message = f"узел {node.id} передан дважды"
                raise ConfigurationError(message)
            self.nodes[node.id] = node
            self.children[node.id] = []
        for node in self.nodes.values():
            if node.parent_id is not None and node.parent_id in self.children:
                self.children[node.parent_id].append(node.id)
        self._expected: dict[UUID, _Expected] = {}
        self._visiting: set[UUID] = set()
        self._weights: dict[UUID, tuple[float, float] | None] = {}

    def expected(self, node_id: UUID) -> _Expected:
        """Ожидаемый итог узла по правилам §9.4.

        Returns:
            Значение, признак оценки и базу оценки.

        Raises:
            ConfigurationError: цикл в ``fed_by``.
        """
        cached = self._expected.get(node_id)
        if cached is not None:
            return cached
        if node_id in self._visiting:
            message = f"цикл в fed_by через батч {node_id}"
            raise ConfigurationError(message)
        self._visiting.add(node_id)
        try:
            result = self._compute_expected(self.nodes[node_id])
        finally:
            self._visiting.discard(node_id)
        self._expected[node_id] = result
        return result

    def _compute_expected(self, node: NodeCounters) -> _Expected:
        if node.closed:
            return _Expected(node.total)
        if node.expected_total is not None:
            return _Expected(max(node.total, node.expected_total), is_estimate=True)
        if node.fed_by:
            return self._knuth(node)
        return _Expected(None)

    def _knuth(self, node: NodeCounters) -> _Expected:
        """Оценка Кнута: ``found_X / Σ done_F * Σ expected_F``.

        Returns:
            Оценку или ``None``, если выборка мала или итог источника неизвестен.

        Raises:
            ConfigurationError: источник не входит в дерево.
        """
        basis = 0
        expected_sources = 0
        known = True
        for source_id in node.fed_by:
            source = self.nodes.get(source_id)
            if source is None:
                message = f"источник {source_id} батча {node.id} не входит в дерево"
                raise ConfigurationError(message)
            basis += source.done
            source_expected = self.expected(source_id).value
            if source_expected is None:
                known = False
            else:
                expected_sources += source_expected
        if not known or basis < estimate_threshold(expected_sources, self.settings):
            return _Expected(None, basis=basis)
        if basis == 0:
            return _Expected(node.total, is_estimate=True, basis=0)
        # Округление до ближайшего целого без погрешности float.
        estimate = (2 * node.total * expected_sources + basis) // (2 * basis)
        return _Expected(max(node.total, estimate), is_estimate=True, basis=basis)

    def _own_weights(self, node: NodeCounters) -> tuple[float, float] | None:
        """Собственный вклад узла в ``ratio``: ``(w_done, ожидаемый w_total)``.

        Виртуальные Items под-батчей (по одному на ребёнка) из ``found`` и
        ``expected`` вычитаются: их работа считается в поддереве ребёнка.

        Returns:
            Пару весов или ``None``, если ожидаемый объём неизвестен.
        """
        virtual = len(self.children[node.id])
        found = max(node.total - virtual, 0)
        expected = self.expected(node.id).value
        if expected is None:
            # Контейнер без собственных Items (корень конвейера) не мешает доле.
            return (float(node.w_done), float(node.w_total)) if found == 0 else None
        target = max(expected - virtual, found)
        if target == 0:
            return (float(node.w_done), float(node.w_total))
        mean_weight = node.w_total / found if found else 1.0
        return (float(node.w_done), mean_weight * target)

    def weights(self, node_id: UUID) -> tuple[float, float] | None:
        """Веса поддерева: ``(Σ w_done, Σ ожидаемый w_total)`` (§9.4).

        Returns:
            Пару весов или ``None``, если ожидаемый объём какого-то узла неизвестен.
        """
        if node_id in self._weights:
            return self._weights[node_id]
        result = self._own_weights(self.nodes[node_id])
        for child_id in self.children[node_id]:
            child = self.weights(child_id)
            result = (
                None
                if result is None or child is None
                else (result[0] + child[0], result[1] + child[1])
            )
        self._weights[node_id] = result
        return result

    def ratio(self, node_id: UUID) -> float | None:
        """Доля по весам: лист — ``w_done / (w_total / found * expected)``, узел — поддерево.

        Returns:
            Долю в ``[0, 1]`` или ``None``, если объём неизвестен.
        """
        weights = self.weights(node_id)
        if weights is None:
            return None
        done, expected = weights
        if expected <= 0:
            return 1.0 if self.nodes[node_id].closed else None
        return min(max(done / expected, 0.0), 1.0)

    def eta(self, node_id: UUID, rates: Mapping[UUID, float]) -> timedelta | None:
        """ETA узла: у листа — по своей скорости, у узла с детьми — максимум по поддереву.

        Этапы идут параллельно, поэтому дерево опустеет, когда опустеет самый
        медленный из них. Виртуальные Items из собственного объёма вычитаются.

        Returns:
            ETA или ``None``, если чего-то не хватает для расчёта.
        """
        node = self.nodes[node_id]
        children = self.children[node_id]
        expected = self.expected(node_id).value
        if not children:
            return estimate_eta(expected=expected, done=node.done, rate=rates.get(node_id))
        parts = [self.eta(child_id, rates) for child_id in children]
        virtual = len(children)
        finished = sum(self.nodes[child_id].state.is_terminal for child_id in children)
        own_left = node.total > virtual or (expected is not None and expected > virtual)
        if own_left:
            own_expected = None if expected is None else expected - virtual
            own_done = node.done - finished
            parts.append(
                estimate_eta(expected=own_expected, done=own_done, rate=rates.get(node_id))
            )
        known = [part for part in parts if part is not None]
        return max(known) if len(known) == len(parts) else None

    def progress(self, node_id: UUID, rates: Mapping[UUID, float]) -> Progress:
        """Собрать :class:`Progress` узла.

        Returns:
            Прогресс узла.
        """
        node = self.nodes[node_id]
        expected = self.expected(node_id)
        pending = node.total - node.done
        return Progress(
            found=node.total,
            queued=max(pending - node.in_flight, 0),
            in_flight=node.in_flight,
            ok=node.ok,
            skip=node.skip,
            error=node.error,
            cancelled=node.cancelled,
            duplicates=node.duplicates,
            skipped_by_limit=node.skipped_by_limit,
            final=node.state.is_terminal,
            expected=expected.value,
            expected_is_estimate=expected.is_estimate,
            estimate_basis=expected.basis,
            ratio=self.ratio(node_id),
            eta=self.eta(node_id, rates),
        )


def compute_progress(
    nodes: Iterable[NodeCounters],
    *,
    settings: ProgressSettings | None = None,
    rates: Mapping[UUID, float] | None = None,
) -> Mapping[UUID, Progress]:
    """Посчитать :class:`Progress` для каждого узла дерева.

    ``rates`` — скорость (завершённых Items в секунду) по id батча, её ведёт
    Snapshotter / ``watch()`` через :func:`ema_rate`. Без скорости ETA нет.

    Returns:
        ``Progress`` по id батча.
    """
    tree = _Tree(nodes, settings or ProgressSettings())
    known_rates: Mapping[UUID, float] = rates or {}
    return {node_id: tree.progress(node_id, known_rates) for node_id in tree.nodes}
