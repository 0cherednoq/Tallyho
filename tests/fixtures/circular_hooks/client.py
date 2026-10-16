"""Клиент приложения, который лениво ссылается на соседний модуль хуков."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self, cast

from tallyho import Tallyho

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["th"]


class _FakeEngine:
    def execution_options(self, **_options: object) -> Self:
        return self

    def get_execution_options(self) -> dict[str, object]:
        return {}


_engine = cast("AsyncEngine", cast("object", _FakeEngine()))
th = Tallyho(_engine, hook_modules=["tests.fixtures.circular_hooks.hooks"])
