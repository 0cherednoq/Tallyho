"""Конкретная композиция всех engine/storage сервисов одной установки."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tallyho.engine.completer import Completer, CompleterSettings, CompleterTriggers
from tallyho.engine.finalizer import Finalizer, FinalizerSettings
from tallyho.engine.installation import RuntimeServices, create_installation, migrate_installation
from tallyho.engine.maintenance import (
    Maintenance,
    MaintenanceSettings,
    ProgressNotifier,
    ProgressWatcher,
    run_maintenance_once,
)
from tallyho.engine.operations import Operations, OperationTriggers
from tallyho.engine.policy import PolicyEnforcer, PolicyEnforcerSettings
from tallyho.engine.producer import Producer
from tallyho.engine.reads import Reads
from tallyho.engine.relay import Relay, RelaySettings
from tallyho.engine.snapshotter import Snapshotter, SnapshotterSettings
from tallyho.engine.spawn import TreeCache
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.progress import ProgressSettings
from tallyho.protocols.broker import RuntimeInstaller
from tallyho.protocols.serialization import PayloadCodec, SerializerCodec

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.engine.installation import Installation
    from tallyho.engine.public import EngineFacade, EngineSettings
    from tallyho.hooks.registry import HookRegistry
    from tallyho.protocols.broker import Dispatcher
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.observer import Observer
    from tallyho.protocols.serialization import Serializer

__all__ = ["create"]


@dataclass(eq=False, kw_only=True)
class _Facade:
    installation: Installation
    clock: Clock
    ids: IdFactory
    observer: Observer
    serializer: Serializer | None
    hooks: HookRegistry
    settings: EngineSettings
    _maintenance: Maintenance | None = None
    _operations: Operations | None = None
    _reads: Reads | None = None
    _watcher: ProgressWatcher | None = None

    def install(self, adapter: Dispatcher) -> None:
        value = self.installation
        settings = self.settings
        progress_settings = ProgressSettings(
            estimate_min_basis=settings.estimate_min_basis,
            estimate_min_share=settings.estimate_min_share,
            eta_window=settings.eta_window,
        )
        worker_id = self.ids.new_id()
        slot = worker_id.int % settings.counter_slots
        notifier = ProgressNotifier(
            engine=value.engine,
            throttle=settings.watch_throttle,
            clock=self.clock,
        )
        relay = Relay(
            engine=value.engine,
            tables=value.tables,
            clock=self.clock,
            dispatcher=adapter,
            observer=self.observer,
            settings=RelaySettings(
                claim_ttl=settings.relay_claim_ttl,
                grace=settings.relay_grace,
                slot=slot,
            ),
        )
        finalizer = Finalizer(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            ids=self.ids,
            hooks=self.hooks,
            settings=FinalizerSettings(slot=slot, hook_timeout=settings.hook_timeout),
            observer=self.observer,
            relay=relay,
            progress=notifier,
        )
        policy = PolicyEnforcer(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            hooks=self.hooks,
            settings=PolicyEnforcerSettings(slot=slot, hook_timeout=settings.hook_timeout),
            observer=self.observer,
        )
        codec = adapter if isinstance(adapter, PayloadCodec) else SerializerCodec(self.serializer)
        producer = Producer(
            tables=value.tables,
            clock=self.clock,
            ids=self.ids,
            codec=codec,
            hooks=self.hooks,
            slot=slot,
        )
        tree_cache = TreeCache()
        completer = Completer(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            settings=CompleterSettings(
                worker_id=str(worker_id),
                slot=slot,
                tick=settings.completer_tick,
                max_batch=settings.completer_max_batch,
                backpressure=settings.completer_backpressure,
                lease_ttl=settings.lease_ttl,
            ),
            observer=self.observer,
            triggers=CompleterTriggers(
                finalizer=finalizer,
                policy=policy,
                relay=relay,
                producer=producer,
                tree_cache=tree_cache,
                progress=notifier,
            ),
        )
        self._operations = Operations(
            tables=value.tables,
            clock=self.clock,
            triggers=OperationTriggers(relay=relay, finalizer=finalizer, progress=notifier),
            slot=slot,
        )
        self._reads = Reads(
            value.engine,
            value.tables,
            self.clock,
            progress=progress_settings,
            lease_duration=settings.lease_ttl,
        )
        self._watcher = ProgressWatcher(
            engine=value.engine,
            reads=self._reads,
            throttle=settings.watch_throttle,
        )
        snapshotter = Snapshotter(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            hooks=self.hooks,
            settings=SnapshotterSettings(
                hook_timeout=settings.hook_timeout,
                progress=progress_settings,
            ),
            observer=self.observer,
        )
        sweeper = Sweeper(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            finalizer=finalizer,
            relay=relay,
            settings=SweeperSettings(
                slot=slot,
                finalize_grace=settings.finalize_grace,
                hook_backoff_max=settings.hook_backoff_max,
            ),
        )
        self._maintenance = Maintenance(
            engine=value.engine,
            relay=relay,
            sweeper=sweeper,
            snapshotter=snapshotter,
            settings=MaintenanceSettings(
                sweep_interval=settings.sweep_interval,
                snapshot_tick=settings.snapshot_tick,
                watch_throttle=settings.watch_throttle,
            ),
        )
        if isinstance(adapter, RuntimeInstaller):
            adapter.install_runtime(
                RuntimeServices(completer, tree_cache, settings.heartbeat_every)
            )

    async def migrate(self) -> int:
        return await migrate_installation(
            self.installation,
            lock_timeout=self.settings.lock_timeout,
        )

    def maintenance(self) -> Maintenance | None:
        return self._maintenance

    async def run_maintenance_once(self) -> object:
        maintenance = self._maintenance
        if maintenance is None:
            return None
        return await run_maintenance_once(maintenance)


def create(  # ruff: ignore[too-many-arguments]  # called through the pure engine boundary
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
    """Создать engine-фасад одной установки.

    Returns:
        Собранная, но ещё не привязанная к broker установка.
    """
    return _Facade(
        installation=create_installation(engine, schema, prefix),
        clock=clock,
        ids=ids,
        observer=observer,
        serializer=serializer,
        hooks=hooks,
        settings=settings,
    )
