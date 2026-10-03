"""Чистая граница engine для API без транзитивной зависимости от storage."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

from tallyho.model.states import OnFeederFailed

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Collection, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime, timedelta
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.attributes import AttributeValue
    from tallyho.model.calls import TaskCall
    from tallyho.model.policy import FailurePolicy
    from tallyho.model.states import BatchState, ItemState
    from tallyho.model.views import BatchPage, BatchView, InFlightItem, ItemView
    from tallyho.protocols.broker import Dispatcher, WorkerFactory
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.observer import Observer
    from tallyho.protocols.serialization import Serializer

__all__ = [
    "BatchDefinition",
    "BatchReference",
    "BatchWriter",
    "EngineFacade",
    "EngineSettings",
    "MaintenanceRunner",
    "create_engine_facade",
    "load_worker_factory",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchDefinition:
    """Чистый DTO корня или под-батча для API → engine."""

    kind: str | None = None
    key: str | None = None
    start_at: datetime | None = None
    deadline: datetime | timedelta | None = None
    callbacks: Mapping[str, TaskCall] = field(default_factory=dict[str, "TaskCall"])
    failure_policy: FailurePolicy | None = None
    max_in_flight: int | None = None
    expected_total: int | None = None
    max_items: int | None = None
    retention: timedelta | None = None
    release_required: bool = False
    attributes: Mapping[str, AttributeValue] = field(default_factory=dict[str, "AttributeValue"])
    memo: Mapping[str, object] | None = None
    fed_by: Sequence[UUID] = ()
    on_feeder_failed: OnFeederFailed = OnFeederFailed.SEAL
    max_depth: int | None = None


@dataclass(frozen=True, slots=True)
class BatchReference:
    """Идентификаторы созданного батча."""

    id: UUID
    root_id: UUID
    created: bool


class BatchWriter(Protocol):
    """Транзакционный writer, которым пользуется BatchBuilder."""

    async def create_root(self, spec: BatchDefinition) -> BatchReference:
        """Создать или найти корневой батч."""
        ...

    async def create_child(self, parent_id: UUID, spec: BatchDefinition) -> BatchReference:
        """Создать или найти дочерний батч."""
        ...

    async def add(self, batch_id: UUID, calls: Sequence[TaskCall]) -> None:
        """Добавить подготовленные вызовы."""
        ...

    async def expect(self, batch_id: UUID, total: int) -> None:
        """Повысить ожидаемое число Items."""
        ...

    async def seal(self, batch_id: UUID) -> None:
        """Закрыть батч."""
        ...


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
    items_scan_window: int
    close_timeout: timedelta


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

    def install(self, adapter: Dispatcher | None, worker_factory: WorkerFactory) -> None:
        """Собрать producer, worker и maintenance вокруг адаптера.

        ``None`` — процесс без брокера: relay не создаётся, outbox не захватывается.
        """
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

    async def close(self) -> None:
        """Закрыть установку: дождаться фоновых задач, закрыть Completer и relay (§11.1)."""
        ...

    def writer(
        self, target: AsyncSession | AsyncConnection | None
    ) -> AbstractAsyncContextManager[BatchWriter]:
        """Открыть writer в своей или пользовательской транзакции."""
        ...

    async def view(self, batch_id: UUID) -> BatchView:
        """Прочитать снимок дерева."""
        ...

    async def in_flight(self, batch_id: UUID, limit: int) -> list[InFlightItem]:
        """Прочитать выполняющиеся Items."""
        ...

    def items(
        self,
        batch_id: UUID,
        *,
        states: Collection[ItemState] | None = None,
        labels: Collection[str] | None = None,
    ) -> AsyncIterator[ItemView]:
        """Поток Items батча по состояниям, меткам или их пересечению."""
        ...

    async def list_batches(  # ruff: ignore[too-many-arguments]  # фильтры листинга именованные (ARCHITECTURE §11.2)
        self,
        *,
        kinds: Collection[str] | None = None,
        states: Collection[BatchState] | None = None,
        attributes: Mapping[str, AttributeValue] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> BatchPage:
        """Страница листинга корневых батчей."""
        ...

    async def find(self, kind: str, key: str) -> UUID:
        """Найти корень по ключу."""
        ...

    async def child(self, batch_id: UUID, key: str) -> UUID:
        """Найти прямого потомка."""
        ...

    def watch(self, batch_id: UUID) -> AsyncGenerator[BatchView]:
        """Следить за снимками батча; ``aclose`` дожидается снятия подписки."""
        ...

    async def pause(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        """Поставить дерево на паузу."""
        ...

    async def resume(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        """Снять паузу."""
        ...

    async def cancel(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        """Запросить отмену."""
        ...

    async def reschedule(
        self,
        target: AsyncSession | AsyncConnection | None,
        batch_id: UUID,
        start_at: datetime,
    ) -> int:
        """Перенести ожидающие Items."""
        ...

    async def retry_failed(
        self,
        target: AsyncSession | AsyncConnection | None,
        batch_id: UUID,
        labels: Sequence[str] | None,
    ) -> int:
        """Повторить ошибочные Items."""
        ...

    async def retry_finalize(
        self, target: AsyncSession | AsyncConnection | None, batch_id: UUID
    ) -> None:
        """Повторить финализацию."""
        ...

    async def release(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        """Разрешить очистку дерева."""
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


def load_worker_factory() -> WorkerFactory:
    """Загрузить runtime-композицию, не связывая API/engine статическим импортом.

    Returns:
        Типизированная фабрика worker runtime.
    """
    loaded = importlib.import_module("tallyho.runtime.tracked")
    return cast("WorkerFactory", vars(loaded)["build_runtime"])
