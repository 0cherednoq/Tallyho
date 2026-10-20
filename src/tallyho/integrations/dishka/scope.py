"""REQUEST-scope на попытку задачи и внедрение зависимостей (extra ``tallyho[dishka]``)."""

from __future__ import annotations

from collections.abc import Awaitable  # ruff: ignore[typing-only-standard-library-import]  # аннотация Protocol
from contextvars import ContextVar
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, cast

from dishka import Scope
from dishka.integrations.base import wrap_injection

from tallyho.model.errors import ConfigurationError
from tallyho.runtime import WorkItem

if TYPE_CHECKING:
    from collections.abc import Callable

    from dishka import AsyncContainer

__all__ = ["DishkaItemScope", "InjectedTask", "current_container", "inject"]

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)

_NO_SCOPE = (
    "scope единицы работы не открыт: зависимости задачи доступны только внутри "
    "отслеживаемой задачи с middleware DishkaItemScope"
)

_request: ContextVar[AsyncContainer | None] = ContextVar("tallyho_dishka_request", default=None)


class DishkaItemScope:
    """``ItemMiddleware``: один REQUEST-scope на попытку задачи.

    Scope закрывается и при успешном выходе, и при исключении, и при отмене —
    ``async with`` гарантирует это для всех трёх путей.
    """

    def __init__(self, container: AsyncContainer) -> None:
        """Запомнить APP-контейнер приложения.

        Args:
            container: Контейнер, собранный с ``TallyhoProvider``.
        """
        self._container: AsyncContainer = container

    async def __call__(self, work: WorkItem, call_next: Callable[[], Awaitable[object]]) -> object:
        """Выполнить попытку в своём REQUEST-scope.

        Args:
            work: Единица работы этой попытки.
            call_next: Следующее звено цепочки middleware.

        Returns:
            Значение, которое вернула задача.
        """
        async with self._container(context={WorkItem: work}, scope=Scope.REQUEST) as request:
            token = _request.set(request)
            try:
                return await call_next()
            finally:
                _request.reset(token)


def current_container() -> AsyncContainer:
    """Вернуть REQUEST-контейнер текущей попытки.

    Returns:
        Контейнер, открытый :class:`DishkaItemScope`.

    Raises:
        ConfigurationError: вызов сделан вне задачи или установка собрана без
            middleware :class:`DishkaItemScope`.
    """
    container = _request.get()
    if container is None:
        raise ConfigurationError(_NO_SCOPE)
    return container


class InjectedTask(Protocol[R_co]):
    """Задача, у которой ``inject`` забрал DI-параметры.

    Аргументы объявлены как ``object``: «сигнатура минус несколько
    параметров» в системе типов не выражается, а требовать внедряемый аргумент
    в ``th.call`` было бы хуже. Поэтому у такой задачи проверяется только тип
    результата, а остальные аргументы — её собственное дело.
    """

    def __call__(self, *args: object, **kwargs: object) -> Awaitable[R_co]:
        """Вызвать задачу без DI-параметров."""
        ...


def inject(func: Callable[P, Awaitable[R_co]]) -> InjectedTask[R_co]:
    """Внедрить ``FromDishka``-параметры задачи из scope её попытки.

    DI-параметры исчезают из видимой сигнатуры, поэтому
    ``th.call(task, run_id, domains)`` типизируется без них.

    Returns:
        Задача без DI-параметров в видимой сигнатуре.
    """
    injected = wrap_injection(
        func=func,
        container_getter=lambda _args, _kwargs: current_container(),
        is_async=True,
    )
    return cast("InjectedTask[R_co]", injected)
