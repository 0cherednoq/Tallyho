"""Атомарные снимки прогресса в доменные таблицы (ARCHITECTURE UC-09)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final

from sqlalchemy import SmallInteger, any_, literal_column, select, update

from tallyho.engine.reads import Reads
from tallyho.model.errors import ConfigurationError, TallyhoError
from tallyho.model.progress import ProgressSettings, ema_rate
from tallyho.model.states import BatchState
from tallyho.protocols.observer import NullObserver
from tallyho.storage.tables import PROGRESS_HOOK
from tallyho.storage.tx import RetryPolicy, TxSettings, hook_session, run_transaction

if TYPE_CHECKING:
    from collections.abc import Hashable, Iterable
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.hooks.registry import HookRegistry, ProgressHook
    from tallyho.model.views import BatchSummary, Progress
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.tables import Tables

__all__ = ["Snapshotter", "SnapshotterSettings"]

_log = logging.getLogger(__name__)

_OPEN = literal_column(str(int(BatchState.OPEN)), SmallInteger())
_SEALED = literal_column(str(int(BatchState.SEALED)), SmallInteger())
_PROGRESS: ColumnElement[str] = literal_column(f"'{PROGRESS_HOOK}'")
_HOOK_NAME: Final = "on_progress"
_POSITIVE_BATCH = "snapshotter batch_size должен быть положительным"
_POSITIVE_TIMEOUT = "snapshotter hook_timeout должен быть положительным"


class _SnapshotRollbackError(TallyhoError):
    """Внутренний сигнал отката транзакции снимка."""


class _CasLostError(_SnapshotRollbackError):
    """Снимок устарел или батч уже финализирован."""


class _HookCallError(_SnapshotRollbackError):
    """Пользовательский ``on_progress`` завершился исключением."""

    error: Exception

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


@dataclass(frozen=True, slots=True, kw_only=True)
class SnapshotterSettings:
    """Ограничения пачки, хука и расчёта прогресса."""

    batch_size: int = 1000
    hook_timeout: timedelta = timedelta(seconds=10)
    progress: ProgressSettings = field(default_factory=ProgressSettings)
    tx: TxSettings = field(default_factory=TxSettings)
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        """Проверить границы настроек.

        Raises:
            ConfigurationError: Размер пачки или таймаут недопустим.
        """
        if self.batch_size <= 0:
            raise ConfigurationError(_POSITIVE_BATCH)
        if self.hook_timeout <= timedelta(0):
            raise ConfigurationError(_POSITIVE_TIMEOUT)


@dataclass(slots=True)
class _Schedule:
    kind: str
    every: timedelta
    next_due: float
    fingerprint: Hashable | None = None
    failures: int = 0


@dataclass(slots=True)
class _Rate:
    done: int
    observed_at: float
    value: float | None = None


@dataclass(eq=False, kw_only=True)
class Snapshotter:
    """Вызывать ``on_progress`` не чаще ``every`` и только при изменениях."""

    tables: Tables
    engine: AsyncEngine
    clock: Clock
    hooks: HookRegistry
    settings: SnapshotterSettings = field(default_factory=SnapshotterSettings)
    observer: Observer = field(default_factory=NullObserver)
    _schedule: dict[UUID, _Schedule] = field(default_factory=dict, init=False)
    _rates: dict[UUID, _Rate] = field(default_factory=dict, init=False)
    _missing: set[UUID] = field(default_factory=set, init=False)

    async def tick(self) -> int:
        """Просканировать активные батчи и записать наступившие снимки.

        Returns:
            Число снимков, чьи hook и CAS закоммитились.
        """
        now = self.clock.monotonic()
        active = await self._active()
        self._refresh_schedule(active, now)
        due = [
            batch_id
            for batch_id, value in sorted(
                self._schedule.items(), key=lambda pair: (pair[1].next_due, pair[0])
            )
            if value.next_due <= now
        ][: self.settings.batch_size]
        if not due:
            return 0

        reader = Reads(
            self.engine,
            self.tables,
            self.clock,
            progress=self.settings.progress,
        )
        previews = await reader.summaries(due)
        self._observe_rates(previews.values(), now)
        changed: list[UUID] = []
        for batch_id in due:
            schedule = self._schedule.get(batch_id)
            if schedule is None:
                continue
            schedule.next_due = now + schedule.every.total_seconds()
            preview = previews.get(batch_id)
            if preview is None:
                self._schedule.pop(batch_id, None)
                continue
            if _fingerprint(preview) != schedule.fingerprint:
                changed.append(batch_id)
        if not changed:
            return 0

        summaries = await reader.summaries(
            changed,
            rates={batch_id: rate.value for batch_id, rate in self._rates.items() if rate.value},
            next_seq=True,
        )
        committed = 0
        for batch_id in changed:
            summary = summaries.get(batch_id)
            schedule = self._schedule.get(batch_id)
            registration = None if schedule is None else self.hooks.progress(schedule.kind)
            if summary is None or schedule is None or registration is None:
                continue
            if await self._commit(summary, registration.hook, schedule):
                schedule.fingerprint = _fingerprint(summary)
                schedule.failures = 0
                committed += 1
        return committed

    async def _active(self) -> dict[UUID, str]:
        batch = self.tables.batch
        statement = (
            select(batch.c.id, batch.c.kind)
            .where(
                batch.c.state.in_((_OPEN, _SEALED)),
                any_(batch.c.hooks) == _PROGRESS,
            )
            .order_by(batch.c.id)
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(statement)
            active: dict[UUID, str] = {}
            for batch_id, kind in result:
                active.setdefault(batch_id, kind)
            return active

    def _refresh_schedule(self, active: dict[UUID, str], now: float) -> None:
        active_ids: set[UUID] = set()
        for batch_id, kind in active.items():
            registration = self.hooks.progress(kind)
            if registration is None:
                self._notify_missing(batch_id=batch_id, kind=kind)
                continue
            active_ids.add(batch_id)
            current = self._schedule.get(batch_id)
            if current is None:
                self._schedule[batch_id] = _Schedule(
                    kind=kind,
                    every=registration.every,
                    next_due=now,
                )
            else:
                current.kind = kind
                current.every = registration.every
        for batch_id in set(self._schedule) - active_ids:
            self._schedule.pop(batch_id, None)
        self._missing.intersection_update(active)

    def _observe_rates(self, summaries: Iterable[BatchSummary], now: float) -> None:
        seen: set[UUID] = set()

        def visit(summary: BatchSummary) -> None:
            if summary.id in seen:
                return
            seen.add(summary.id)
            previous = self._rates.get(summary.id)
            if previous is None:
                self._rates[summary.id] = _Rate(summary.progress.done, now)
            else:
                previous.value = ema_rate(
                    previous.value,
                    done_delta=summary.progress.done - previous.done,
                    elapsed=timedelta(seconds=now - previous.observed_at),
                    window=self.settings.progress.eta_window,
                )
                previous.done = summary.progress.done
                previous.observed_at = now
            for child in summary.children.values():
                visit(child)

        for summary in summaries:
            visit(summary)

    async def _commit(self, summary: BatchSummary, hook: ProgressHook, schedule: _Schedule) -> bool:
        settings = TxSettings(
            lock_timeout=self.settings.tx.lock_timeout,
            statement_timeout=self.settings.hook_timeout,
        )
        try:
            await run_transaction(
                self.engine,
                lambda conn: self._attempt(conn, summary, hook),
                settings=settings,
                policy=self.settings.retry,
            )
        except _CasLostError:
            return False
        except _HookCallError as exc:
            schedule.failures += 1
            self._notify_failed(summary, exc.error, schedule.failures)
            return False
        return True

    async def _attempt(
        self, conn: AsyncConnection, summary: BatchSummary, hook: ProgressHook
    ) -> None:
        try:
            async with asyncio.timeout(self.settings.hook_timeout.total_seconds()):
                async with hook_session(conn) as session:
                    await hook(session, summary)
        except Exception as exc:
            raise _HookCallError(exc) from exc
        seen = summary.seq - 1
        batch = self.tables.batch
        won = await conn.scalar(
            update(batch)
            .where(
                batch.c.id == summary.id,
                batch.c.snap_seq == seen,
                batch.c.state.in_((_OPEN, _SEALED)),
            )
            .values(snap_seq=summary.seq)
            .returning(batch.c.id)
        )
        if won is None:
            raise _CasLostError

    def _notify_missing(self, *, batch_id: UUID, kind: str) -> None:
        if batch_id in self._missing:
            return
        self._missing.add(batch_id)
        try:
            self.observer.hook_missing(batch_id=batch_id, kind=kind, hook=_HOOK_NAME)
        except Exception:  # ruff: ignore[blind-except]  # наблюдаемость не влияет на снимки
            _log.exception("Observer.hook_missing упал")

    def _notify_failed(self, summary: BatchSummary, error: Exception, attempt: int) -> None:
        _log.error("Tx-хук снимка батча %s упал: %s", summary.id, error)
        try:
            self.observer.hook_failed(
                batch_id=summary.id,
                kind=summary.kind,
                hook=_HOOK_NAME,
                attempt=attempt,
                error=error,
            )
        except Exception:  # ruff: ignore[blind-except]  # наблюдаемость не влияет на снимки
            _log.exception("Observer.hook_failed упал")


def _progress_fingerprint(progress: Progress) -> tuple[object, ...]:
    return (
        progress.found,
        progress.queued,
        progress.in_flight,
        progress.ok,
        progress.skip,
        progress.error,
        progress.cancelled,
        progress.duplicates,
        progress.skipped_by_limit,
        progress.final,
        progress.expected,
        progress.expected_is_estimate,
        progress.estimate_basis,
        progress.ratio,
    )


def _fingerprint(summary: BatchSummary) -> Hashable:
    return (
        summary.state,
        summary.reason,
        _progress_fingerprint(summary.progress),
        tuple(sorted(summary.labels.items())),
        tuple((key, _fingerprint(child)) for key, child in sorted(summary.children.items())),
    )
