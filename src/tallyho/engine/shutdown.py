"""Закрытие установки: дождаться фоновых задач и остановить Completer и relay (ARCHITECTURE §11.1).

Компоненты привязаны к event loop, в котором начали работать (Completer и
relay воркера — к loop исполнителя брокера), а ``Tallyho.aclose()`` могут
вызвать откуда угодно. Поэтому каждая часть закрытия выполняется в loop своего
владельца (:func:`run_in`), а на всё закрытие отведён общий срок
(:class:`Budget`). Когда срок исчерпан, оставшиеся задачи отменяются, и
закрытие дожидается их: после него в loop нет незавершённых задач библиотеки.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from tallyho.model.errors import CompleterError

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterable
    from datetime import timedelta

    from tallyho.protocols.clock import Clock

__all__ = ["Budget", "close_services", "drain_tasks", "run_in"]

_log = logging.getLogger(__name__)

_HANDOVER_GRACE: Final = 5.0
"""Запас сверх срока закрытия на ответ чужого event loop, секунды."""


class _Completer(Protocol):
    @property
    def loop(self) -> asyncio.AbstractEventLoop | None: ...

    async def close(self, *, requeue_held: bool = False) -> None: ...

    async def abort(self) -> None: ...


class _Relay(Protocol):
    @property
    def loop(self) -> asyncio.AbstractEventLoop | None: ...

    async def close(self, *, grace: float | None = None) -> None: ...


@dataclass(frozen=True, slots=True)
class Budget:
    """Общий срок закрытия по монотонным часам установки.

    Attributes:
        clock: Часы установки.
        until: Значение ``clock.monotonic()``, после которого срок исчерпан.
    """

    clock: Clock
    until: float

    @classmethod
    def start(cls, clock: Clock, timeout: timedelta) -> Budget:
        """Начать отсчёт ``timeout`` от текущего момента.

        Returns:
            Срок, общий для всех шагов закрытия.
        """
        return cls(clock, clock.monotonic() + timeout.total_seconds())

    @property
    def left(self) -> float:
        """Сколько секунд осталось; 0, когда срок исчерпан."""
        return max(0.0, self.until - self.clock.monotonic())


async def run_in(
    owner: asyncio.AbstractEventLoop | None,
    make: Callable[[], Coroutine[object, object, None]],
    *,
    patience: float,
) -> None:
    """Выполнить корутину в event loop владельца и дождаться её.

    Свой loop — напрямую. Чужой работающий — через
    ``run_coroutine_threadsafe``. Чужой остановленный, но не закрытый (flexiq
    останавливает loop исполнителя при выходе из ``run_worker``) — этот loop
    докручивается в служебном потоке, пока корутина не завершится. Закрытый
    loop пропускается: его задачи уже не выполнятся.

    Докрутить можно не всё: запрос SQLAlchemy, прерванный остановкой loop,
    привязан (greenlet) к потоку, который этот loop крутил, и в другом потоке
    завершится ошибкой. Библиотека такие ошибки фоновой работы логирует, а
    недоделанное подбирает sweeper; новые запросы закрытия выполняются штатно.

    Args:
        owner: Loop владельца; ``None`` — компонент ни к чему не привязан.
        make: Фабрика корутины: вызывается ровно один раз, если корутину есть
            где выполнить.
        patience: Сколько секунд ждать ответа чужого работающего loop. Свой
            срок корутина соблюдает сама; это страховка от loop, который
            остановили, пока закрытие ждало.
    """
    current = asyncio.get_running_loop()
    if owner is None or owner is current:
        await make()
        return
    if owner.is_closed():
        _log.warning("закрытие: event loop владельца уже закрыт, его задачи не дожидаются")
        return
    if not owner.is_running():
        await asyncio.to_thread(_drive, owner, make)
        return
    future = asyncio.run_coroutine_threadsafe(make(), owner)
    try:
        async with asyncio.timeout(patience):
            await asyncio.wrap_future(future)
    except TimeoutError:
        _log.warning("закрытие: чужой event loop не ответил за %.1f с", patience)


def _drive(
    owner: asyncio.AbstractEventLoop, make: Callable[[], Coroutine[object, object, None]]
) -> None:
    # Loop остановлен: кроме нас его никто не крутит. Если его всё же успели
    # запустить или закрыть, run_until_complete откажет — корутину закрываем сами.
    coroutine = make()
    try:
        owner.run_until_complete(coroutine)
    except RuntimeError:
        coroutine.close()
        _log.warning("закрытие: остановленный event loop владельца недоступен", exc_info=True)


async def drain_tasks(tasks: Iterable[asyncio.Task[object]], budget: Budget) -> None:
    """Дождаться разовых фоновых задач своего event loop; опоздавшие отменить.

    Отменённая задача получает ``CancelledError``, и функция дожидается её
    завершения. Исключения задач не пробрасываются: они ничьих ожиданий не
    держат, а их работу повторит sweeper.

    Args:
        tasks: Задачи текущего event loop.
        budget: Срок закрытия.
    """
    pending = {task for task in tasks if not task.done()}
    if not pending:
        return
    _, late = await asyncio.wait(pending, timeout=budget.left)
    if not late:
        return
    _log.warning(
        "закрытие: %d фоновых задач не уложились в close_timeout и отменены: %s",
        len(late),
        sorted(task.get_name() for task in late),
    )
    for task in late:
        _ = task.cancel()
    _ = await asyncio.wait(late)


async def _close_completer(completer: _Completer, budget: Budget) -> None:
    try:
        async with asyncio.timeout(budget.left):
            await completer.close(requeue_held=True)
    except TimeoutError:
        _log.warning(
            "закрытие: Completer не уложился в close_timeout; lease вернёт sweeper через lease_ttl"
        )
        await completer.abort()
    except CompleterError:
        _log.exception(
            "закрытие: удержанные Items не возвращены в outbox; lease истекут через lease_ttl"
        )


async def close_services(
    *,
    budget: Budget,
    tasks: Iterable[asyncio.Task[object]],
    completer: _Completer | None,
    relay: _Relay | None,
) -> None:
    """Шаги 2-4 закрытия установки (ARCHITECTURE §11.1).

    Фоновые задачи и Completer закрываются одновременно, каждый в своём event
    loop; relay — после них: финализация ещё может положить колбэк в outbox.

    Args:
        budget: Общий срок закрытия.
        tasks: Незавершённые после-коммитные задачи фасада и операций.
        completer: Completer установки; ``None`` — его нет.
        relay: Relay установки; ``None`` — процесс без адаптера.
    """
    patience = budget.left + _HANDOVER_GRACE
    current = asyncio.get_running_loop()
    by_loop: dict[asyncio.AbstractEventLoop, list[asyncio.Task[object]]] = {}
    for task in tasks:
        by_loop.setdefault(task.get_loop(), []).append(task)
    home = None if completer is None else (completer.loop or current)
    if home is not None:
        _ = by_loop.setdefault(home, [])
    # На каждый loop — одна корутина: остановленный чужой loop докручивает один поток.
    steps = [
        run_in(
            owner,
            _working(group, completer if owner is home else None, budget),
            patience=patience,
        )
        for owner, group in by_loop.items()
    ]
    _ = await asyncio.gather(*steps)
    if relay is not None:
        await run_in(relay.loop, _stopping(relay, budget), patience=patience)


def _working(
    group: list[asyncio.Task[object]], completer: _Completer | None, budget: Budget
) -> Callable[[], Coroutine[object, object, None]]:
    async def work() -> None:
        steps = [drain_tasks(group, budget)]
        if completer is not None:
            steps.append(_close_completer(completer, budget))
        _ = await asyncio.gather(*steps)

    return work


def _stopping(relay: _Relay, budget: Budget) -> Callable[[], Coroutine[object, object, None]]:
    return lambda: relay.close(grace=budget.left)
