"""Модуль хуков, импортирующий уже созданный клиент приложения."""

from __future__ import annotations

from tests.fixtures.circular_hooks.client import th

__all__: list[str] = []


@th.on_finalized("circular")
async def save_result(_session: object, _summary: object) -> None:
    """Сохранить итог тестового батча."""
