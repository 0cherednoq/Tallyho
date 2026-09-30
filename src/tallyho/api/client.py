"""Публичный клиент ``Tallyho`` и проверяемая конфигурация установки."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar, final

from tallyho.api.batch import BatchBuilder, BatchHandle
from tallyho.api.calls import Call
from tallyho.engine.public import (
    BatchDefinition,
    EngineSettings,
    create_engine_facade,
)
from tallyho.hooks.registry import HookRegistry, import_hook_modules
from tallyho.model.errors import ConfigurationError
from tallyho.model.policy import FailurePolicy as FailurePolicyModel
from tallyho.model.progress import ProgressSettings
from tallyho.protocols.clock import SystemClock
from tallyho.protocols.ids import UuidV7Factory
from tallyho.protocols.observer import NullObserver

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

    from tallyho.engine.public import EngineFacade, MaintenanceRunner
    from tallyho.hooks.registry import FinalizedT, PolicyBreachT, ProgressT
    from tallyho.model.calls import TaskCall
    from tallyho.protocols.broker import Dispatcher
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.observer import Observer
    from tallyho.protocols.serialization import Serializer

__all__ = ["Settings", "Tallyho"]

_ALREADY_INSTALLED = "broker adapter уже установлен"
_NOT_INSTALLED = "сначала вызовите Tallyho.install(adapter)"
P = ParamSpec("P")
R = TypeVar("R")


class _Default:
    pass


_DEFAULT = _Default()


def _positive(name: str, value: object) -> None:
    if not isinstance(value, timedelta) or value <= timedelta(0):
        message = f"{name} должен быть положительным timedelta"
        raise ConfigurationError(message)


def _non_negative(name: str, value: object) -> None:
    if not isinstance(value, timedelta) or value < timedelta(0):
        message = f"{name} должен быть неотрицательным timedelta"
        raise ConfigurationError(message)


def _optional_positive_int(name: str, value: object) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        message = f"{name} должен быть целым >= 1 или None"
        raise ConfigurationError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class Settings:
    """Настройки v1 со значениями по умолчанию из ARCHITECTURE §15."""

    counter_slots: int = 8
    completer_tick: timedelta = timedelta(milliseconds=20)
    completer_max_batch: int = 500
    completer_backpressure: int = 10_000
    lease_ttl: timedelta = timedelta(seconds=60)
    heartbeat_every: timedelta = timedelta(seconds=20)
    relay_grace: timedelta = timedelta(seconds=5)
    relay_claim_ttl: timedelta = timedelta(seconds=30)
    finalize_grace: timedelta = timedelta(seconds=30)
    hook_timeout: timedelta = timedelta(seconds=10)
    hook_backoff_initial: timedelta = timedelta(seconds=1)
    hook_backoff_max: timedelta = timedelta(minutes=5)
    snapshot_tick: timedelta = timedelta(milliseconds=500)
    estimate_min_basis: int = 20
    estimate_min_share: float = 0.05
    eta_window: timedelta = timedelta(seconds=60)
    max_items: int | None = None
    sweep_interval: timedelta = timedelta(seconds=5)
    lock_timeout: timedelta = timedelta(seconds=5)
    retention: timedelta | None = timedelta(days=14)
    watch_throttle: timedelta = timedelta(milliseconds=500)

    def __post_init__(self) -> None:
        """Проверить все диапазоны.

        Raises:
            ConfigurationError: хотя бы одно значение недопустимо.
        """
        integers: dict[str, object] = {
            "counter_slots": self.counter_slots,
            "completer_max_batch": self.completer_max_batch,
            "completer_backpressure": self.completer_backpressure,
        }
        for name, value in integers.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                message = f"{name} должен быть целым >= 1"
                raise ConfigurationError(message)
        if self.completer_backpressure < self.completer_max_batch:
            message = "completer_backpressure должен быть >= completer_max_batch"
            raise ConfigurationError(message)
        for name, value in self._positive_durations().items():
            _positive(name, value)
        _non_negative("relay_grace", self.relay_grace)
        _non_negative("finalize_grace", self.finalize_grace)
        if self.hook_backoff_initial > self.hook_backoff_max:
            message = "hook_backoff_initial должен быть <= hook_backoff_max"
            raise ConfigurationError(message)
        _ = ProgressSettings(
            estimate_min_basis=self.estimate_min_basis,
            estimate_min_share=self.estimate_min_share,
            eta_window=self.eta_window,
        )
        _optional_positive_int("max_items", self.max_items)
        if self.retention is not None:
            _positive("retention", self.retention)

    def _positive_durations(self) -> dict[str, timedelta]:
        return {
            "completer_tick": self.completer_tick,
            "lease_ttl": self.lease_ttl,
            "heartbeat_every": self.heartbeat_every,
            "relay_claim_ttl": self.relay_claim_ttl,
            "hook_timeout": self.hook_timeout,
            "hook_backoff_initial": self.hook_backoff_initial,
            "hook_backoff_max": self.hook_backoff_max,
            "snapshot_tick": self.snapshot_tick,
            "sweep_interval": self.sweep_interval,
            "lock_timeout": self.lock_timeout,
            "watch_throttle": self.watch_throttle,
        }

    @classmethod
    def overridden(cls, values: dict[str, object]) -> Settings:
        """Создать настройки из keyword overrides конструктора ``Tallyho``.

        Returns:
            Проверенный объект настроек.

        Raises:
            ConfigurationError: имя или тип настройки неверны.
        """
        try:
            return replace(cls(), **values)  # type: ignore[arg-type]  # ключи проверяет dataclasses.replace
        except TypeError as exc:
            raise ConfigurationError(str(exc)) from exc

    def engine_settings(self) -> EngineSettings:
        """Передать engine только нужную ему часть конфигурации.

        Returns:
            Неизменяемый DTO engine-слоя.
        """
        return EngineSettings(
            counter_slots=self.counter_slots,
            completer_tick=self.completer_tick,
            completer_max_batch=self.completer_max_batch,
            completer_backpressure=self.completer_backpressure,
            lease_ttl=self.lease_ttl,
            heartbeat_every=self.heartbeat_every,
            relay_grace=self.relay_grace,
            relay_claim_ttl=self.relay_claim_ttl,
            finalize_grace=self.finalize_grace,
            hook_timeout=self.hook_timeout,
            hook_backoff_max=self.hook_backoff_max,
            snapshot_tick=self.snapshot_tick,
            estimate_min_basis=self.estimate_min_basis,
            estimate_min_share=self.estimate_min_share,
            eta_window=self.eta_window,
            sweep_interval=self.sweep_interval,
            lock_timeout=self.lock_timeout,
            watch_throttle=self.watch_throttle,
        )


@final
class Tallyho:
    """Одна установка tallyho поверх пользовательского ``AsyncEngine``."""

    FailurePolicy = FailurePolicyModel

    def __init__(  # ruff: ignore[too-many-arguments]  # публичный конструктор задан PLAN T6.1
        self,
        engine: AsyncEngine,
        *,
        schema: str | None = None,
        prefix: str = "th_",
        hook_modules: Iterable[str] = (),
        clock: Clock | None = None,
        id_factory: IdFactory | None = None,
        observer: Observer | None = None,
        serializer: Serializer | None = None,
        **settings: object,
    ) -> None:
        """Настроить клиент без обращения к БД."""
        self.engine = engine
        self.schema = schema
        self.prefix = prefix
        self.hook_modules = tuple(hook_modules)
        self.settings = Settings.overridden(settings)
        self.clock = clock or SystemClock()
        self.id_factory = id_factory or UuidV7Factory()
        self.observer = observer or NullObserver()
        self.serializer = serializer
        self.hooks = HookRegistry()
        self._adapter: Dispatcher | None = None
        self._engine: EngineFacade = create_engine_facade(
            engine,
            schema=schema,
            prefix=prefix,
            clock=self.clock,
            ids=self.id_factory,
            observer=self.observer,
            serializer=serializer,
            hooks=self.hooks,
            settings=self.settings.engine_settings(),
        )
        import_hook_modules(self.hook_modules)

    def install(self, adapter: Dispatcher) -> None:
        """Связать producer/worker/maintenance с broker adapter.

        Raises:
            ConfigurationError: adapter уже установлен.
        """
        if self._adapter is not None:
            raise ConfigurationError(_ALREADY_INSTALLED)
        self._engine.install(adapter)
        self._adapter = adapter

    async def migrate(self) -> int:
        """Установить или обновить таблицы.

        Returns:
            Версия схемы БД.
        """
        return await self._engine.migrate()

    def maintenance(self) -> MaintenanceRunner:
        """Вернуть lifespan-сервис leader maintenance.

        Returns:
            Собранный lifespan-сервис.

        Raises:
            ConfigurationError: broker adapter ещё не установлен.
        """
        value = self._engine.maintenance()
        if value is None:
            raise ConfigurationError(_NOT_INSTALLED)
        return value

    async def run_maintenance_once(self) -> object:
        """Детерминированно выполнить один полный maintenance-проход.

        Returns:
            Сводка прохода engine.
        """
        _ = self.maintenance()
        return await self._engine.run_maintenance_once()

    def batch(  # ruff: ignore[too-many-arguments]  # публичный API задан ARCHITECTURE §11.2
        self,
        kind: str,
        *,
        key: str | None = None,
        start_at: datetime | None = None,
        on_succeeded: TaskCall | None = None,
        on_completed_with_errors: TaskCall | None = None,
        on_failed: TaskCall | None = None,
        on_cancelled: TaskCall | None = None,
        on_finalized_task: TaskCall | None = None,
        failure_policy: FailurePolicyModel | None = None,
        max_in_flight: int | None = None,
        expected_total: int | None = None,
        max_items: int | _Default | None = _DEFAULT,
        deadline: datetime | timedelta | None = None,
        retention: timedelta | _Default | None = _DEFAULT,
        release_required: bool = False,
        session: AsyncSession | AsyncConnection | None = None,
    ) -> BatchBuilder:
        """Создать транзакционный builder корневого батча.

        Returns:
            Builder, который нужно использовать как ``async with``.

        Raises:
            ConfigurationError: broker adapter ещё не установлен.
        """
        adapter = self._adapter
        if adapter is None:
            raise ConfigurationError(_NOT_INSTALLED)
        callbacks = {
            name: call
            for name, call in {
                "on_succeeded": on_succeeded,
                "on_completed_with_errors": on_completed_with_errors,
                "on_failed": on_failed,
                "on_cancelled": on_cancelled,
                "on_finalized_task": on_finalized_task,
            }.items()
            if call is not None
        }
        effective_max_items = (
            self.settings.max_items if isinstance(max_items, _Default) else max_items
        )
        effective_retention = (
            self.settings.retention if isinstance(retention, _Default) else retention
        )
        return BatchBuilder(
            self._engine,
            adapter,
            BatchDefinition(
                kind=kind,
                key=key,
                start_at=start_at,
                deadline=deadline,
                callbacks=callbacks,
                failure_policy=failure_policy,
                max_in_flight=max_in_flight,
                expected_total=expected_total,
                max_items=effective_max_items,
                retention=effective_retention,
                release_required=release_required,
            ),
            _target=session,
        )

    def handle(self, batch_id: UUID) -> BatchHandle:
        """Создать handle по известному идентификатору.

        Returns:
            Лёгкая ссылка без обращения к БД.
        """
        return BatchHandle(self._engine, batch_id)

    async def find(self, kind: str, key: str) -> BatchHandle:
        """Найти корневой батч по идемпотентному ключу.

        Returns:
            Handle найденного корня.
        """
        return self.handle(await self._engine.find(kind, key))

    def call(
        self,
        fn: Callable[P, Awaitable[R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call[P, R]:
        """Подготовить типизированный вызов задачи.

        Returns:
            Вызов с сохранёнными аргументами и возможностью задать ``opts``.

        Raises:
            ConfigurationError: broker adapter ещё не установлен.
        """
        adapter = self._adapter
        if adapter is None:
            raise ConfigurationError(_NOT_INSTALLED)
        return Call(task_name=adapter.task_name(fn), args=args, kwargs=kwargs)

    def on_finalized(self, kind: str) -> Callable[[FinalizedT], FinalizedT]:
        """Зарегистрировать хук финализации.

        Returns:
            Типосохраняющий декоратор.
        """
        return self.hooks.on_finalized(kind)

    def on_progress(self, kind: str, *, every: timedelta) -> Callable[[ProgressT], ProgressT]:
        """Зарегистрировать хук снимков прогресса.

        Returns:
            Типосохраняющий декоратор.
        """
        return self.hooks.on_progress(kind, every)

    def on_policy_breach(self, kind: str) -> Callable[[PolicyBreachT], PolicyBreachT]:
        """Зарегистрировать хук политики ошибок.

        Returns:
            Типосохраняющий декоратор.
        """
        return self.hooks.on_policy_breach(kind)
