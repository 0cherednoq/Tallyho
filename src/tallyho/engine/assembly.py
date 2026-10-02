"""Конкретная композиция всех engine/storage сервисов одной установки."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

from tallyho.engine.completer import Completer, CompleterSettings, CompleterTriggers
from tallyho.engine.dead_letters import DeadLetterReconciler, DeadLetterSettings
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
from tallyho.engine.producer import CallbackName, Producer, RootSpec, SubBatchSpec
from tallyho.engine.public import BatchReference
from tallyho.engine.reads import Reads
from tallyho.engine.relay import Relay, RelaySettings
from tallyho.engine.shutdown import Budget, close_services
from tallyho.engine.snapshotter import Snapshotter, SnapshotterSettings
from tallyho.engine.spawn import TreeCache
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.errors import ClosedError, ConfigurationError
from tallyho.model.progress import ProgressSettings
from tallyho.protocols.broker import (
    CallOptionsValidator,
    RelayPolicy,
    RetryLimits,
    Runtime,
    RuntimeInstaller,
)
from tallyho.protocols.serialization import PayloadCodec, SerializerCodec
from tallyho.storage.tx import RetryPolicy, after_commit, resolve_connection

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Callable,
        Collection,
        Mapping,
        Sequence,
    )
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

    from tallyho.engine.installation import Installation
    from tallyho.engine.public import (
        BatchDefinition,
        BatchWriter,
        EngineFacade,
        EngineSettings,
    )
    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.attributes import AttributeValue
    from tallyho.model.calls import TaskCall
    from tallyho.model.states import BatchState, ItemState
    from tallyho.model.views import BatchPage, BatchView, InFlightItem, ItemView
    from tallyho.protocols.broker import Dispatcher, WorkerFactory
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.observer import Observer
    from tallyho.protocols.serialization import Serializer

__all__ = ["create"]

_NOT_INSTALLED = "сначала вызовите Tallyho.install(adapter)"
_ROOT_KIND = "kind обязателен для корневого батча"
_CHILD_KEY = "key обязателен для под-батча"
_Service = TypeVar("_Service")
_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _Writer:
    producer: Producer
    conn: AsyncConnection
    target: AsyncSession | AsyncConnection
    notify: Callable[[UUID], None]
    finalize: Callable[[UUID], None]
    observer: Observer

    async def create_root(self, spec: BatchDefinition) -> BatchReference:
        if spec.kind is None:
            raise ConfigurationError(_ROOT_KIND)
        value = await self.producer.create_root(
            self.conn,
            RootSpec(
                kind=spec.kind,
                key=spec.key,
                start_at=spec.start_at,
                deadline=spec.deadline,
                callbacks={CallbackName(name): call for name, call in spec.callbacks.items()},
                failure_policy=spec.failure_policy,
                max_in_flight=spec.max_in_flight,
                expected_total=spec.expected_total,
                max_items=spec.max_items,
                retention=spec.retention,
                release_required=spec.release_required,
                attributes=spec.attributes,
                memo=spec.memo,
            ),
        )
        if value.created:
            await after_commit(
                self.target,
                lambda: self._created(value.id, value.kind),
            )
        return BatchReference(value.id, value.root_id, value.created)

    async def create_child(self, parent_id: UUID, spec: BatchDefinition) -> BatchReference:
        if spec.key is None:
            raise ConfigurationError(_CHILD_KEY)
        value = await self.producer.create_sub_batch(
            self.conn,
            parent_id,
            SubBatchSpec(
                key=spec.key,
                kind=spec.kind,
                start_at=spec.start_at,
                deadline=spec.deadline,
                callbacks={CallbackName(name): call for name, call in spec.callbacks.items()},
                failure_policy=spec.failure_policy,
                max_in_flight=spec.max_in_flight,
                expected_total=spec.expected_total,
                fed_by=spec.fed_by,
                on_feeder_failed=spec.on_feeder_failed,
                max_depth=spec.max_depth,
            ),
        )
        if value.created:
            await after_commit(
                self.target,
                lambda: self._created(value.id, value.kind),
            )
        return BatchReference(value.id, value.root_id, value.created)

    def _created(self, batch_id: UUID, kind: str) -> None:
        try:
            self.observer.batch_created(batch_id=batch_id, kind=kind)
        except Exception:  # ruff: ignore[blind-except]  # observer must not affect committed accounting
            _log.exception("Observer.batch_created failed for batch_id=%s kind=%s", batch_id, kind)

    async def add(self, batch_id: UUID, calls: Sequence[TaskCall]) -> None:
        _ = await self.producer.add_items(self.conn, batch_id, calls)
        await after_commit(self.target, lambda: self.notify(batch_id))

    async def expect(self, batch_id: UUID, total: int) -> None:
        await self.producer.expect(self.conn, batch_id, total)

    async def seal(self, batch_id: UUID) -> None:
        _ = await self.producer.seal(self.conn, batch_id)
        await after_commit(self.target, lambda: self.finalize(batch_id))


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
    _producer: Producer | None = None
    _relay: Relay | None = None
    _finalizer: Finalizer | None = None
    _completer: Completer | None = None
    _closing: bool = False
    _closed: bool = False
    _background: set[asyncio.Task[None]] = field(default_factory=set, init=False)

    def install(self, adapter: Dispatcher | None, worker_factory: WorkerFactory) -> None:
        self._ensure_open()
        value = self.installation
        settings = self.settings
        progress_settings = ProgressSettings(
            estimate_min_basis=settings.estimate_min_basis,
            estimate_min_share=settings.estimate_min_share,
            eta_window=settings.eta_window,
        )
        worker_id = self.ids.new_id()
        slot = worker_id.int % settings.counter_slots
        retry = RetryPolicy(on_retry=self._notify_retry)
        notifier = ProgressNotifier(
            engine=value.engine,
            throttle=settings.watch_throttle,
            clock=self.clock,
        )
        relay = self._build_relay(adapter, slot=slot, retry=retry)
        self._relay = relay
        finalizer = Finalizer(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            ids=self.ids,
            hooks=self.hooks,
            settings=FinalizerSettings(
                slot=slot,
                hook_timeout=settings.hook_timeout,
                retry=retry,
            ),
            observer=self.observer,
            relay=relay,
            progress=notifier,
        )
        self._finalizer = finalizer
        policy = PolicyEnforcer(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            hooks=self.hooks,
            settings=PolicyEnforcerSettings(
                slot=slot,
                hook_timeout=settings.hook_timeout,
                retry=retry,
            ),
            observer=self.observer,
        )
        producer = self._build_producer(adapter, slot=slot)
        self._producer = producer
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
                retry=retry,
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
        self._completer = completer
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
            items_scan_window=settings.items_scan_window,
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
                retry=retry,
            ),
            observer=self.observer,
        )
        sweeper = Sweeper(
            tables=value.tables,
            engine=value.engine,
            clock=self.clock,
            finalizer=finalizer,
            relay=relay,
            limits=self._retry_limits(adapter),
            settings=SweeperSettings(
                slot=slot,
                finalize_grace=settings.finalize_grace,
                hook_backoff_max=settings.hook_backoff_max,
                lease_ttl=settings.lease_ttl,
                retry=retry,
            ),
            observer=self.observer,
        )
        self._maintenance = Maintenance(
            engine=value.engine,
            relay=relay,
            dead_letters=self._build_dead_letters(
                adapter, relay=relay, finalizer=finalizer, slot=slot, retry=retry
            ),
            sweeper=sweeper,
            snapshotter=snapshotter,
            settings=MaintenanceSettings(
                sweep_interval=settings.sweep_interval,
                snapshot_tick=settings.snapshot_tick,
                watch_throttle=settings.watch_throttle,
            ),
            lock_identity=value.maintenance_identity,
        )
        self._install_worker(
            adapter,
            worker_factory=worker_factory,
            completer=completer,
            tree_cache=tree_cache,
        )

    @staticmethod
    def _retry_limits(adapter: object) -> RetryLimits | None:
        # Протокол необязателен: без него sweeper считает умолчание задачи равным 0 (UC-15).
        return adapter if isinstance(adapter, RetryLimits) else None

    def _build_producer(self, adapter: object, *, slot: int) -> Producer:
        return Producer(
            tables=self.installation.tables,
            clock=self.clock,
            ids=self.ids,
            codec=adapter
            if isinstance(adapter, PayloadCodec)
            else SerializerCodec(self.serializer),
            hooks=self.hooks,
            slot=slot,
            option_validator=adapter if isinstance(adapter, CallOptionsValidator) else None,
        )

    def _build_relay(
        self, adapter: Dispatcher | None, *, slot: int, retry: RetryPolicy
    ) -> Relay | None:
        # Без адаптера relay нет: процесс не захватывает outbox (ARCHITECTURE §3.2).
        if adapter is None:
            return None
        settings = self.settings
        return Relay(
            engine=self.installation.engine,
            tables=self.installation.tables,
            clock=self.clock,
            dispatcher=adapter,
            observer=self.observer,
            settings=RelaySettings(
                claim_ttl=settings.relay_claim_ttl,
                grace=settings.relay_grace,
                slot=slot,
                scan_interval=settings.sweep_interval,
            ),
            retry=retry,
            autostart=adapter.relay_autostart if isinstance(adapter, RelayPolicy) else True,
        )

    def _build_dead_letters(
        self,
        adapter: object,
        *,
        relay: Relay | None,
        finalizer: Finalizer,
        slot: int,
        retry: RetryPolicy,
    ) -> DeadLetterReconciler | None:
        # Читать DLQ умеет только адаптер брокера: сверку выполняют процессы,
        # где он установлен, в цикле relay после каждого scan (ARCHITECTURE UC-15).
        if relay is None or not isinstance(adapter, Runtime):
            return None
        reconciler = DeadLetterReconciler(
            tables=self.installation.tables,
            engine=self.installation.engine,
            clock=self.clock,
            source=adapter,
            finalizer=finalizer,
            relay=relay,
            settings=DeadLetterSettings(slot=slot, retry=retry),
        )
        relay.after_scan = reconciler.reconcile_once
        return reconciler

    def _install_worker(
        self,
        adapter: Dispatcher | None,
        *,
        worker_factory: WorkerFactory,
        completer: Completer,
        tree_cache: TreeCache,
    ) -> None:
        if adapter is None or not isinstance(adapter, RuntimeInstaller):
            return
        if not isinstance(adapter, Runtime):
            message = "runtime installer должен реализовывать Runtime"
            raise ConfigurationError(message)
        heartbeat = self.settings.heartbeat_every
        runtime = worker_factory(
            completer=completer,
            broker=adapter,
            dispatcher=adapter,
            tree_cache=tree_cache,
            heartbeat_every=heartbeat,
        )
        adapter.install_runtime(RuntimeServices(completer, tree_cache, heartbeat, runtime))

    async def migrate(self) -> int:
        return await migrate_installation(
            self.installation,
            lock_timeout=self.settings.lock_timeout,
        )

    def maintenance(self) -> Maintenance | None:
        self._ensure_open()
        return self._maintenance

    async def run_maintenance_once(self) -> object:
        maintenance = self.maintenance()
        if maintenance is None:
            return None
        return await run_maintenance_once(maintenance)

    async def close(self) -> None:
        # Шаги и срок закрытия — ARCHITECTURE §11.1. Повторный вызов — no-op.
        if self._closing:
            return
        self._closing = True
        # Транзакция, закоммиченная прямо перед закрытием, создаёт свою задачу
        # финализации через call_soon: даём ей появиться, чтобы дождаться и её.
        await asyncio.sleep(0)
        self._closed = True
        if self._maintenance is not None:
            self._maintenance.stop()
        operations = self._operations
        await close_services(
            budget=Budget.start(self.clock, self.settings.close_timeout),
            tasks=[*self._background, *(operations.shut() if operations is not None else ())],
            completer=self._completer,
            relay=self._relay,
        )

    def _ensure_open(self) -> None:
        if self._closing:
            raise ClosedError

    @asynccontextmanager
    async def writer(
        self, target: AsyncSession | AsyncConnection | None
    ) -> AsyncGenerator[BatchWriter]:
        producer = self._live(self._producer)
        if target is not None:
            yield _Writer(
                producer,
                await resolve_connection(target),
                target,
                self._notify_commit,
                self._finalize_commit,
                self.observer,
            )
            return
        async with self.installation.engine.begin() as conn:
            yield _Writer(
                producer,
                conn,
                conn,
                self._notify_commit,
                self._finalize_commit,
                self.observer,
            )

    def _notify_commit(self, batch_id: UUID) -> None:
        relay = self._relay
        if relay is not None:
            relay.kick([batch_id])

    def _notify_retry(self, sqlstate: str) -> None:
        try:
            self.observer.transaction_retry(sqlstate=sqlstate)
        except Exception:  # ruff: ignore[blind-except]  # observer must not affect retries
            _log.exception("Observer.transaction_retry failed for SQLSTATE %s", sqlstate)

    def _finalize_commit(self, batch_id: UUID) -> None:
        self._notify_commit(batch_id)
        loop = asyncio.get_running_loop()
        loop.call_soon(self._spawn_finalize, batch_id)

    def _spawn_finalize(self, batch_id: UUID) -> None:
        if self._closed:
            return  # установка закрыта: финализацию выполнит sweeper (§11.1)
        task = asyncio.create_task(self._finalize(batch_id), name="tallyho-api-finalize")
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _finalize(self, batch_id: UUID) -> None:
        try:
            _ = await self._require(self._finalizer).try_finalize(batch_id)
        except Exception:  # ruff: ignore[blind-except]  # commit состоялся; sweeper повторит финализацию
            _log.exception("финализация producer batch после commit упала")

    async def view(self, batch_id: UUID) -> BatchView:
        return await self._require(self._reads).view(batch_id)

    async def in_flight(self, batch_id: UUID, limit: int) -> list[InFlightItem]:
        return await self._require(self._reads).in_flight(batch_id, limit=limit)

    def items(
        self,
        batch_id: UUID,
        *,
        states: Collection[ItemState] | None = None,
        labels: Collection[str] | None = None,
    ) -> AsyncIterator[ItemView]:
        return self._require(self._reads).items(batch_id, states=states, labels=labels)

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
        return await self._require(self._reads).list_batches(
            kinds=kinds,
            states=states,
            attributes=attributes,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            cursor=cursor,
        )

    async def find(self, kind: str, key: str) -> UUID:
        return await self._require(self._reads).find(kind, key)

    async def child(self, batch_id: UUID, key: str) -> UUID:
        return await self._require(self._reads).child(batch_id, key)

    def watch(self, batch_id: UUID) -> AsyncIterator[BatchView]:
        return self._require(self._watcher).watch(batch_id)

    async def pause(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        operations = self._live(self._operations)
        if target is not None:
            await operations.pause(target, batch_id)
            return
        async with self.installation.engine.begin() as conn:
            await operations.pause(conn, batch_id)

    async def resume(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        operations = self._live(self._operations)
        if target is not None:
            await operations.resume(target, batch_id)
            return
        async with self.installation.engine.begin() as conn:
            await operations.resume(conn, batch_id)

    async def cancel(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        operations = self._live(self._operations)
        if target is not None:
            _ = await operations.cancel(target, batch_id)
            return
        async with self.installation.engine.begin() as conn:
            _ = await operations.cancel(conn, batch_id)

    async def reschedule(
        self,
        target: AsyncSession | AsyncConnection | None,
        batch_id: UUID,
        start_at: datetime,
    ) -> int:
        operations = self._live(self._operations)
        if target is not None:
            return await operations.reschedule(target, batch_id, start_at)
        async with self.installation.engine.begin() as conn:
            return await operations.reschedule(conn, batch_id, start_at)

    async def retry_failed(
        self,
        target: AsyncSession | AsyncConnection | None,
        batch_id: UUID,
        labels: Sequence[str] | None,
    ) -> int:
        operations = self._live(self._operations)
        if target is not None:
            return await operations.retry_failed(target, batch_id, labels=labels)
        async with self.installation.engine.begin() as conn:
            return await operations.retry_failed(conn, batch_id, labels=labels)

    async def retry_finalize(
        self, target: AsyncSession | AsyncConnection | None, batch_id: UUID
    ) -> None:
        operations = self._live(self._operations)
        if target is not None:
            await operations.retry_finalize(target, batch_id)
            return
        async with self.installation.engine.begin() as conn:
            await operations.retry_finalize(conn, batch_id)

    async def release(self, target: AsyncSession | AsyncConnection | None, batch_id: UUID) -> None:
        operations = self._live(self._operations)
        if target is not None:
            await operations.release(target, batch_id)
            return
        async with self.installation.engine.begin() as conn:
            await operations.release(conn, batch_id)

    def _live(self, value: _Service | None) -> _Service:
        # Сервис, меняющий данные: после закрытия установки недоступен.
        self._ensure_open()
        return self._require(value)

    @staticmethod
    def _require(value: _Service | None) -> _Service:
        if value is None:
            raise ConfigurationError(_NOT_INSTALLED)
        return value


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
