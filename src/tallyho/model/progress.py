"""Математика прогресса: чистые функции над «сырыми» счётчиками дерева.

Вход — :class:`NodeCounters` каждого батча дерева (строки ``th_counter`` +
дельты, ``count(th_lease)``, ``expected_total`` и источники ``th_feed``),
выход — :class:`~tallyho.model.views.Progress` на каждый узел
(ARCHITECTURE §9.3-9.4). Запросов к БД здесь нет.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
    "NodeCounters",
    "ProgressSettings",
    "compute_progress",
    "estimate_threshold",
]

DEFAULT_ESTIMATE_MIN_BASIS: Final = 20
"""Сколько завершённых родителей достаточно для оценки итога (§15)."""

DEFAULT_ESTIMATE_MIN_SHARE: Final = 0.05
"""Какая доля источника достаточна для оценки итога (§15)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ProgressSettings:
    """Параметры расчёта прогресса (ARCHITECTURE §15).

    Оценка итога по ``fed_by`` показывается, когда число завершённых
    родителей достигло ``min(estimate_min_basis, estimate_min_share * expected_F)``.
    """

    estimate_min_basis: int = DEFAULT_ESTIMATE_MIN_BASIS
    estimate_min_share: float = DEFAULT_ESTIMATE_MIN_SHARE

    def __post_init__(self) -> None:
        """Проверить параметры (``ConfigurationError`` при выходе за диапазон)."""
        _check_min_basis(self.estimate_min_basis)
        _check_min_share(self.estimate_min_share)


def _check_min_basis(basis: object) -> None:
    if isinstance(basis, bool) or not isinstance(basis, int) or basis < 0:
        message = f"estimate_min_basis должен быть целым >= 0, получено {basis!r}"
        raise ConfigurationError(message)


def _check_min_share(share: object) -> None:
    if isinstance(share, bool) or not isinstance(share, int | float) or not 0 <= share <= 1:
        message = f"estimate_min_share должен быть в [0, 1], получено {share!r}"
        raise ConfigurationError(message)


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

    def progress(self, node_id: UUID) -> Progress:
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
        )


def compute_progress(
    nodes: Iterable[NodeCounters],
    *,
    settings: ProgressSettings | None = None,
) -> Mapping[UUID, Progress]:
    """Посчитать :class:`Progress` для каждого узла дерева.

    Returns:
        ``Progress`` по id батча.
    """
    tree = _Tree(nodes, settings or ProgressSettings())
    return {node_id: tree.progress(node_id) for node_id in tree.nodes}
