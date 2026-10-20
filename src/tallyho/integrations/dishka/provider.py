"""Провайдер единицы работы для контейнера Dishka (extra ``tallyho[dishka]``)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dishka import Provider, Scope, from_context

from tallyho.runtime import WorkItem

if TYPE_CHECKING:
    from dishka.dependency_source.composite import CompositeDependencySource

__all__ = ["TallyhoProvider"]


class TallyhoProvider(Provider):
    """Отдаёт текущий :class:`~tallyho.WorkItem` зависимостям REQUEST-scope.

    Объект не создаётся провайдером, а приходит из контекста scope, который
    открывает ``DishkaItemScope``. Поэтому вне задачи его в контейнере нет.
    """

    work_item: CompositeDependencySource = from_context(provides=WorkItem, scope=Scope.REQUEST)
