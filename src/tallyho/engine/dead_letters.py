"""Сверка с DLQ брокера: Items, чья джоба умерла, не записав итог (ARCHITECTURE UC-15).

Джоба уходит в DLQ, не взяв lease, когда claim падает дольше, чем брокер её
повторяет (PostgreSQL недоступен), а событие DLQ потеряно или его обработчик
тоже не смог записать итог. Item остаётся ``active`` без lease, outbox и джобы:
sweeper его не видит, батч не финализируется. Найти такой Item можно только по
DLQ брокера, а читать DLQ умеет только адаптер, поэтому проход выполняют
процессы с адаптером (цикл relay), а не лидер maintenance.

Один проход — до ``max_rounds`` порций. Порция — одна транзакция:

1. строка курсора в ``th_meta`` под ``FOR UPDATE SKIP LOCKED``: процесс, не
   получивший строку, проход пропускает — её держит другой процесс;
2. ``Runtime.reconcile_dead(cursor)``: мёртвые джобы ``(item, generation)``;
3. блокировки ``th_batch FOR SHARE`` → ``th_item FOR UPDATE`` → ``th_lease FOR
   UPDATE`` и правило UC-15: завершается только Item, у которого мёртвая джоба —
   текущее поколение отправки и нет ни lease, ни записи outbox;
4. новый курсор — в той же транзакции.

Правило не зависит от времени: Item, переотправленный после мёртвой джобы,
имеет другое поколение, и запись DLQ прошлой отправки его не касается.

Событие DLQ брокера (``JOB_DEAD`` у flexiq) применяет то же правило через
:meth:`DeadLetterReconciler.settle` — шаг 3 без курсора.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import starmap
from typing import TYPE_CHECKING, Final, Protocol, cast

from sqlalchemy import SmallInteger, Uuid, any_, literal, literal_column, select, update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tallyho.engine.sweeper import FinishRow, finish_active
from tallyho.model.errors import ConfigurationError, InvalidStateError, TallyhoError
from tallyho.model.states import ItemState, OutboxKind
from tallyho.storage.now import sql_now
from tallyho.storage.tx import RetryPolicy, TxSettings, run_transaction

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.protocols.broker import DeadLetter, DeadLetters
    from tallyho.protocols.clock import Clock
    from tallyho.storage.tables import Tables

__all__ = ["CURSOR_KEY", "DeadLetterReconciler", "DeadLetterSettings"]

CURSOR_KEY: Final = "dead_letter_cursor"
"""Ключ курсора сверки в ``th_meta``."""

EXHAUSTED_LABEL: Final = "exhausted"
CANCELLED_LABEL: Final = "cancelled"

_ACTIVE = literal_column(str(int(ItemState.ACTIVE)), SmallInteger())
_ITEM = literal_column(str(int(OutboxKind.ITEM)), SmallInteger())
_ERROR_TYPE: Final = "DeadLetter"
_NO_DETAIL: Final = "брокер перенёс джобу в DLQ, итог Item записан сверкой"
_READ_TIMEOUT: Final = "брокер не отдал порцию DLQ за отведённое время"
_NO_DATABASE_TIME: Final = "БД не вернула текущее время"
_BAD_SETTINGS: Final = "max_rounds и read_timeout сверки с DLQ должны быть положительными"

_log = logging.getLogger(__name__)


class _DeadLetterReadTimeoutError(TallyhoError):
    """Адаптер не отдал порцию DLQ за ``read_timeout``."""


class _Source(Protocol):
    async def reconcile_dead(self, since: str | None) -> DeadLetters: ...


class _Relay(Protocol):
    def kick(self, batch_ids: Iterable[UUID]) -> None: ...


class _Finalizer(Protocol):
    async def try_finalize(self, batch_id: UUID) -> bool: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class DeadLetterSettings:
    """Параметры сверки с DLQ.

    Attributes:
        slot: Слот ``th_counter`` для счётчиков завершённых Items.
        max_rounds: Сколько порций DLQ разбирает один проход; остаток —
            следующий проход.
        read_timeout: Сколько ждать порцию от адаптера. Чтение идёт внутри
            транзакции, которая держит строку курсора.
        tx: Таймауты своих транзакций.
        retry: Повтор транзакции на конфликтах.
    """

    slot: int = 0
    max_rounds: int = 5
    read_timeout: timedelta = timedelta(seconds=30)
    tx: TxSettings = field(default_factory=TxSettings)
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        """Проверить границы настроек.

        Raises:
            ConfigurationError: Значение настройки недопустимо.
        """
        if self.slot < 0:
            message = "slot сверки с DLQ должен быть >= 0"
            raise ConfigurationError(message)
        if self.max_rounds < 1 or self.read_timeout <= timedelta(0):
            raise ConfigurationError(_BAD_SETTINGS)


@dataclass(frozen=True, slots=True)
class _Round:
    """Итог одной порции."""

    finished: int = 0
    more: bool = False
    kick: tuple[UUID, ...] = ()
    finalize: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class _Applied:
    finished: int = 0
    kick: tuple[UUID, ...] = ()
    finalize: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class _ItemRow:
    item_id: UUID
    batch_id: UUID
    generation: int
    weight: int


def _uuids(ids: Iterable[UUID]) -> ColumnElement[Sequence[UUID]]:
    return literal(sorted(ids), ARRAY(Uuid()))


@dataclass(eq=False, kw_only=True)
class DeadLetterReconciler:
    """Завершить Items, чья джоба текущего поколения лежит в DLQ брокера.

    Attributes:
        tables: Таблицы установки.
        engine: Движок БД; схема установки — в ``schema_translate_map``.
        clock: Часы: «сейчас» в SQL (D-002).
        source: Адаптер брокера — сторона :class:`~tallyho.protocols.broker.Runtime`.
        finalizer: Финализация батчей, в которых завершены Items.
        relay: Подсказка relay о местах, освободившихся в окне ``max_in_flight``.
        settings: Параметры сверки.
    """

    tables: Tables
    engine: AsyncEngine
    clock: Clock
    source: _Source
    finalizer: _Finalizer
    relay: _Relay | None = None
    settings: DeadLetterSettings = field(default_factory=DeadLetterSettings)

    async def reconcile_once(self) -> int:
        """Выполнить один проход сверки.

        Returns:
            Сколько Items завершено этим проходом.
        """
        total = 0
        for _ in range(self.settings.max_rounds):
            outcome = await run_transaction(
                self.engine, self._round, settings=self.settings.tx, policy=self.settings.retry
            )
            await self._after_commit(outcome.kick, outcome.finalize)
            total += outcome.finished
            if not outcome.more:
                break
        if total:
            _log.info("сверка с DLQ: завершено %d Items, оставшихся без исполнителя", total)
        return total

    async def settle(self, entries: Sequence[DeadLetter], *, error_type: str = _ERROR_TYPE) -> int:
        """Применить правило сверки к мёртвым джобам из события брокера (``JOB_DEAD``).

        Курсор не читается и не двигается: событие называет джобу само. Правило
        то же, что у прохода сверки, и так же идемпотентно — запись DLQ этой
        джобы сверка позже разберёт ещё раз без последствий.

        Args:
            entries: Мёртвые джобы: Item, поколение отправки и текст ошибки.
            error_type: ``type`` в ``th_item.error`` завершённых Items.

        Returns:
            Сколько Items завершено.
        """
        applied = await run_transaction(
            self.engine,
            lambda conn: self._apply(conn, entries, error_type=error_type),
            settings=self.settings.tx,
            policy=self.settings.retry,
        )
        await self._after_commit(applied.kick, applied.finalize)
        return applied.finished

    async def _after_commit(self, kick: Sequence[UUID], finalize: Sequence[UUID]) -> None:
        if kick and self.relay is not None:
            self.relay.kick(kick)
        for batch_id in finalize:
            _ = await self.finalizer.try_finalize(batch_id)

    async def _round(self, conn: AsyncConnection) -> _Round:
        locked, cursor = await self._lock_cursor(conn)
        if not locked:
            # Курсор держит другой процесс: он и делает эту работу.
            return _Round()
        dead = await self._read(cursor)
        applied = await self._apply(conn, dead.entries)
        if dead.cursor != cursor:
            meta = self.tables.meta
            _ = await conn.execute(
                update(meta).where(meta.c.key == CURSOR_KEY).values(value=dead.cursor or "")
            )
        return _Round(applied.finished, dead.more, applied.kick, applied.finalize)

    async def _lock_cursor(self, conn: AsyncConnection) -> tuple[bool, str | None]:
        meta = self.tables.meta
        row = (
            await conn.execute(
                select(meta.c.value)
                .where(meta.c.key == CURSOR_KEY)
                .with_for_update(skip_locked=True)
            )
        ).first()
        if row is not None:
            return True, row[0] or None
        # Строки нет (первая сверка установки) либо она занята. Вставленная
        # строка принадлежит этой транзакции; конфликт означает «занята».
        created = await conn.execute(
            pg_insert(meta)
            .values(key=CURSOR_KEY, value="")
            .on_conflict_do_nothing(index_elements=[meta.c.key])
            .returning(meta.c.key)
        )
        return created.first() is not None, None

    async def _read(self, cursor: str | None) -> DeadLetters:
        try:
            async with asyncio.timeout(self.settings.read_timeout.total_seconds()):
                return await self.source.reconcile_dead(cursor)
        except TimeoutError as exc:
            raise _DeadLetterReadTimeoutError(_READ_TIMEOUT) from exc

    async def _apply(
        self, conn: AsyncConnection, entries: Sequence[DeadLetter], *, error_type: str = _ERROR_TYPE
    ) -> _Applied:
        details: dict[tuple[UUID, int], str | None] = {}
        for entry in entries:
            _ = details.setdefault((entry.item_id, entry.generation), entry.detail)
        if not details:
            return _Applied()
        candidates = await self._active(conn, {item_id for item_id, _ in details})
        if not candidates:
            return _Applied()
        now = await self._now(conn)
        # Порядок блокировок — как у Completer: th_batch → th_item → th_lease (§9.2).
        cancelling = await self._lock_batches(conn, set(candidates.values()))
        rows = [
            row
            for row in await self._lock_items(conn, candidates)
            if (row.item_id, row.generation) in details and row.batch_id in cancelling
        ]
        orphans = await self._orphans(conn, rows, now)
        errors: dict[UUID, object] = {
            row.item_id: {
                "type": error_type,
                "message": details[row.item_id, row.generation] or _NO_DETAIL,
            }
            for row in orphans
        }
        failed, kick_failed = await finish_active(
            conn,
            self.tables,
            [
                FinishRow(row.item_id, row.batch_id, row.weight)
                for row in orphans
                if not cancelling[row.batch_id]
            ],
            slot=self.settings.slot,
            state=ItemState.ERROR,
            label=EXHAUSTED_LABEL,
            now=now,
            errors=errors,
        )
        # Батч отменяется: Item, которого уже никто не возьмёт, отменяется, как при claim.
        cancelled, kick_cancelled = await finish_active(
            conn,
            self.tables,
            [
                FinishRow(row.item_id, row.batch_id, row.weight)
                for row in orphans
                if cancelling[row.batch_id]
            ],
            slot=self.settings.slot,
            state=ItemState.CANCELLED,
            label=CANCELLED_LABEL,
            now=now,
        )
        return _Applied(
            finished=failed + cancelled,
            kick=tuple(sorted({*kick_failed, *kick_cancelled})),
            finalize=tuple(sorted({row.batch_id for row in orphans})),
        )

    async def _active(self, conn: AsyncConnection, item_ids: set[UUID]) -> dict[UUID, UUID]:
        # item_id → batch_id активных Items. Без блокировок: почти все Items
        # из DLQ уже терминальны, их строки сверка не трогает.
        item = self.tables.item
        found = await conn.execute(
            select(item.c.id, item.c.batch_id).where(
                item.c.id == any_(_uuids(item_ids)), item.c.state == _ACTIVE
            )
        )
        return dict(cast("Iterable[tuple[UUID, UUID]]", found.all()))

    async def _orphans(
        self, conn: AsyncConnection, rows: Sequence[_ItemRow], now: datetime
    ) -> list[_ItemRow]:
        # Items текущего поколения мёртвой джобы, у которых не осталось исполнителя:
        # нет ни lease (живой — выполнение идёт, истёкший — работа sweeper), ни
        # записи outbox (relay отправит снова).
        leases = await self._lock_leases(conn, [row.item_id for row in rows], now)
        await self._mark_redelivered(conn, [item_id for item_id, live in leases.items() if live])
        free = [row for row in rows if row.item_id not in leases]
        queued = await self._queued(conn, [row.item_id for row in free])
        return [row for row in free if row.item_id not in queued]

    async def _lock_batches(self, conn: AsyncConnection, batch_ids: set[UUID]) -> dict[UUID, bool]:
        batch = self.tables.batch
        result = await conn.execute(
            select(batch.c.id, batch.c.cancel_requested_at.is_not(None))
            .where(batch.c.id == any_(_uuids(batch_ids)))
            .order_by(batch.c.id)
            .with_for_update(read=True)
        )
        return {
            batch_id: bool(cancel_requested)
            for batch_id, cancel_requested in cast("Iterable[tuple[UUID, bool]]", result)
        }

    async def _lock_items(
        self, conn: AsyncConnection, candidates: Mapping[UUID, UUID]
    ) -> list[_ItemRow]:
        item = self.tables.item
        result = await conn.execute(
            select(item.c.id, item.c.batch_id, item.c.generation, item.c.weight)
            .where(
                item.c.id == any_(_uuids(candidates)),
                item.c.state == _ACTIVE,
                # Виртуальный Item под-батча брокеру не отправляется.
                item.c.child_batch_id.is_(None),
            )
            .order_by(item.c.id)
            .with_for_update()
        )
        return list(starmap(_ItemRow, cast("Iterable[tuple[UUID, UUID, int, int]]", result)))

    async def _lock_leases(
        self, conn: AsyncConnection, item_ids: Sequence[UUID], now: datetime
    ) -> dict[UUID, bool]:
        # item_id → жив ли lease.
        if not item_ids:
            return {}
        lease = self.tables.lease
        result = await conn.execute(
            select(lease.c.item_id, lease.c.lease_until > now)
            .where(lease.c.item_id == any_(_uuids(item_ids)))
            .order_by(lease.c.item_id)
            .with_for_update()
        )
        return {
            item_id: bool(live) for item_id, live in cast("Iterable[tuple[UUID, bool]]", result)
        }

    async def _queued(self, conn: AsyncConnection, item_ids: Sequence[UUID]) -> set[UUID]:
        if not item_ids:
            return set()
        outbox = self.tables.outbox
        # Запись outbox Item'а имеет id = item_id (D-031): поиск по первичному ключу.
        return set(
            await conn.scalars(
                select(outbox.c.id).where(
                    outbox.c.id == any_(_uuids(item_ids)), outbox.c.kind == _ITEM
                )
            )
        )

    async def _mark_redelivered(self, conn: AsyncConnection, item_ids: Sequence[UUID]) -> None:
        # Джоба закрыта, ретрая от брокера не будет: release вернёт Item в outbox сам (UC-04).
        if not item_ids:
            return
        lease = self.tables.lease
        _ = await conn.execute(
            update(lease)
            .where(lease.c.item_id == any_(_uuids(item_ids)), ~lease.c.redelivered)
            .values(redelivered=True)
        )

    async def _now(self, conn: AsyncConnection) -> datetime:
        value: datetime | None = await conn.scalar(select(sql_now(self.clock)))
        if value is None:
            raise InvalidStateError(_NO_DATABASE_TIME)
        return value
