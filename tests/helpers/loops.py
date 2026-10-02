"""Event loop в отдельном потоке — модель loop исполнителя flexiq (ARCHITECTURE §11.3)."""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, TypeVar, final

if TYPE_CHECKING:
    from collections.abc import Coroutine

__all__ = ["LoopThread", "library_tasks"]

T = TypeVar("T")


def library_tasks(loop: asyncio.AbstractEventLoop | None = None) -> list[str]:
    """Имена незавершённых задач библиотеки (``tallyho-*``) в loop; по умолчанию — в текущем."""
    return sorted(
        task.get_name()
        for task in asyncio.all_tasks(loop)
        if task.get_name().startswith("tallyho-") and not task.done()
    )


@final
class LoopThread:
    """Свой event loop в daemon-потоке: воркер flexiq исполняет задачи именно так."""

    def __init__(self) -> None:
        """Создать loop и запустить его в потоке."""
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self._thread.start()

    async def run(self, coroutine: Coroutine[object, object, T]) -> T:
        """Выполнить корутину в loop потока и дождаться результата из loop теста."""
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(coroutine, self.loop))

    def stop(self) -> None:
        """Остановить loop, не закрывая его: так делает flexiq при выходе из ``run_worker``."""
        if self._thread.is_alive():
            _ = self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(timeout=5)

    def close(self) -> None:
        """Остановить поток и закрыть loop."""
        self.stop()
        if not self.loop.is_closed():
            self.loop.close()
