"""Готовое окружение для интеграционных тестов пользователя."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.api.client import Tallyho
    from tallyho.testing.broker import InlineBroker
    from tallyho.testing.clock import FakeClock

__all__ = ["TallyhoTestEnv"]


@final
@dataclass(frozen=True, slots=True)
class TallyhoTestEnv:
    """Установленный клиент, брокер и часы одной pytest-фикстуры."""

    th: Tallyho
    broker: InlineBroker
    clock: FakeClock
    engine: AsyncEngine
    schema: str | None

    async def step(self, deliveries: int = 1) -> int:
        """Выполнить ограниченное число доставок.

        Returns:
            Число взятых брокером доставок.
        """
        return await self.broker.step(deliveries)

    async def drain(self) -> int:
        """Выполнить очередь до простоя.

        Returns:
            Число доставок текущего прохода.
        """
        return await self.broker.drain()

    async def run_maintenance_once(self) -> object:
        """Запустить один детерминированный проход maintenance.

        Returns:
            Сводка maintenance.
        """
        return await self.th.run_maintenance_once()

    async def close(self) -> None:
        """Закрыть установку: дождаться фоновых задач библиотеки (``th.aclose()``).

        Вызывается до удаления схемы теста, иначе незавершённая финализация
        столкнётся с ``DROP SCHEMA``. Фикстура ``tallyho_env`` вызывает сама.
        """
        await self.th.aclose()
