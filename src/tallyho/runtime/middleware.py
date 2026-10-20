"""Точка расширения вокруг вызова tracked-задачи (ARCHITECTURE §4.2, §11.2).

Обёртка задачи уже делает claim, активирует контекст и записывает итог, но
наружу этот around-слой не выходил. ``ItemMiddleware`` его открывает: цепочка
задаётся один раз в ``Tallyho(..., middleware=[...])``, вызывается после claim и
активации контекста, но вокруг самого вызова задачи, поэтому возвращаемое
значение и исключение проходят через неё и получают обычную семантику UC-18.

Middleware не записывает итог и не владеет ретраями: для итога есть ``Outcome``,
для судьбы исключения — опции ``defer_on``/``error_on``/``skip_on``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from tallyho.runtime.context import WorkItem

__all__ = ["ItemMiddleware"]


class ItemMiddleware(Protocol):
    """Обёртка одной попытки задачи.

    Экземпляр создаётся один раз на установку и видит все попытки процесса,
    поэтому состояние попытки хранить в нём нельзя — для этого есть ``work``.
    """

    async def __call__(self, work: WorkItem, call_next: Callable[[], Awaitable[object]]) -> object:
        """Выполнить попытку, вызвав ``call_next`` ровно один раз.

        Args:
            work: Единица работы этой попытки; тот же объект, что отдаёт
                ``item.require()``.
            call_next: Следующее звено цепочки, в конце — сама задача. Его
                возвращаемое значение нужно вернуть, а исключение — пропустить:
                по ним обёртка определяет итог попытки.

        Returns:
            Значение, которое вернула задача.
        """
        ...
