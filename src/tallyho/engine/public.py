"""Чистая граница engine для API без транзитивной зависимости от storage."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from datetime import timedelta

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.hooks.registry import HookRegistry
    from tallyho.protocols.broker import Dispatcher
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.observer import Observer
    from tallyho.protocols.serialization import Serializer

__all__ = [
    "EngineFacade",
    "EngineSettings",
    "MaintenanceRunner",
    "create_engine_facade",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class EngineSettings:
    """Проверенные API-настройки, нужные композиции engine."""

    counter_slots: int
    completer_tick: timedelta
    completer_max_batch: int
    completer_backpressure: int
    lease_ttl: timedelta
    heartbeat_every: timedelta
    relay_grace: timedelta
    relay_claim_ttl: timedelta
    finalize_grace: timedelta
    hook_timeout: timedelta
    hook_backoff_max: timedelta
    snapshot_tick: timedelta
    estimate_min_basis: int
    estimate_min_share: float
    eta_window: timedelta
    sweep_interval: timedelta
    lock_timeout: timedelta
    watch_throttle: timedelta


class MaintenanceRunner(Protocol):
    """Lifespan-интерфейс leader maintenance."""

    async def run(self) -> None:
        """Работать до остановки или отмены."""
        ...

    def stop(self) -> None:
        """Попросить цикл остановиться."""
        ...

    async def run_once(self) -> object:
        """Выполнить один проход и вернуть сводку."""
        ...


class EngineFacade(Protocol):
    """Операции T6.1, реализованные внутри engine-слоя."""

    def install(self, adapter: Dispatcher) -> None:
        """Собрать producer, worker и maintenance вокруг адаптера."""
        ...

    async def migrate(self) -> int:
        """Применить миграции и вернуть версию."""
        ...

    def maintenance(self) -> MaintenanceRunner | None:
        """Вернуть собранный maintenance или ``None`` до install."""
        ...

    async def run_maintenance_once(self) -> object:
        """Выполнить один проход maintenance."""
        ...


class _AssemblyModule(Protocol):
    def create(  # ruff: ignore[too-many-arguments]  # mirrors dependency boundary
        self,
        engine: AsyncEngine,
        *,
        schema: str | None,
        prefix: str,
        clock: Clock,
        ids: IdFactory,
        observer: Observer,
        serializer: Serializer | None,
        hooks: HookRegistry,
        settings: EngineSettings,
    ) -> EngineFacade: ...


def create_engine_facade(  # ruff: ignore[too-many-arguments]  # dependency boundary
    engine: AsyncEngine,
    *,
    schema: str | None,
    prefix: str,
    clock: Clock,
    ids: IdFactory,
    observer: Observer,
    serializer: Serializer | None,
    hooks: HookRegistry,
    settings: EngineSettings,
) -> EngineFacade:
    """Создать конкретную композицию, не раскрывая storage API-слою.

    Returns:
        Engine-owned реализация фасада.
    """
    loaded = cast("object", importlib.import_module("tallyho.engine.assembly"))
    module = cast("_AssemblyModule", loaded)
    return module.create(
        engine,
        schema=schema,
        prefix=prefix,
        clock=clock,
        ids=ids,
        observer=observer,
        serializer=serializer,
        hooks=hooks,
        settings=settings,
    )
