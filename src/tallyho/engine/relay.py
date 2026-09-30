"""Relay: отправка записей ``th_outbox`` брокеру (ARCHITECTURE §6.3, UC-01, UC-10, §11.2).

Relay не зависит от адаптера: он говорит только с протоколом
:class:`~tallyho.protocols.broker.Dispatcher`. Один проход (раунд):

1. своя транзакция: захват готовых записей ``UPDATE th_outbox SET
   available_at = now + claim_ttl … FOR UPDATE SKIP LOCKED``. Записи
   отменённых или удалённых Items удаляются, записи батча на паузе
   паркуются (``available_at = infinity``);
2. вне транзакции: сборка :class:`~tallyho.protocols.broker.Message`
   (payload и опции Item — из ``th_item``, колбэка — из самой записи) и
   ``dispatch`` группами по ``task_name``;
3. своя транзакция: ``DELETE th_outbox`` отправленного, ``dispatched += n``,
   ``th_expiry`` для Items с опцией ``expires`` (§11.4).

Падение между 1 и 3 оставляет записи захваченными до ``claim_ttl``: потом их
заберёт следующий проход, брокер получит дубль, его отсечёт claim (§6.3).

Два входа: :meth:`Relay.kick` — fast-path после commit продюсера (только
названные батчи, без ``grace``) и :meth:`Relay.scan_once` — страховочный
проход по всем записям старше ``relay_grace``.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast
from uuid import UUID

from sqlalchemy import (
    DateTime,
    Interval,
    Uuid,
    any_,
    delete,
    func,
    literal,
    literal_column,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert

from tallyho.model.errors import ConfigurationError
from tallyho.model.states import TERMINAL_THRESHOLD, OutboxKind
from tallyho.protocols.broker import Message
from tallyho.protocols.observer import NullObserver
from tallyho.storage.counters import CounterDelta, upsert_slots
from tallyho.storage.now import sql_now
from tallyho.storage.tx import run_transaction

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import datetime

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.protocols.broker import Dispatcher
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.tables import Tables
    from tallyho.storage.tx import TxSettings

__all__ = ["Relay", "RelaySettings"]

_log = logging.getLogger(__name__)

_MAX_ATTEMPTS: Final = 32767
"""Предел ``th_outbox.attempts`` (smallint): счётчик захватов не переполняется."""


@dataclass(frozen=True, slots=True, kw_only=True)
class RelaySettings:
    """Параметры relay (ARCHITECTURE §15).

    Attributes:
        claim_ttl: На сколько захват откладывает запись (``relay_claim_ttl``):
            если relay упал до ``DELETE``, запись вернётся через этот срок.
        grace: Возраст записи, после которого её забирает scan
            (``relay_grace``). Свежие записи отправляет fast-path.
        chunk: Сколько записей захватывает один раунд.
        slot: Слот ``th_counter`` для ``dispatched``.
    """

    claim_ttl: timedelta = timedelta(seconds=30)
    grace: timedelta = timedelta(seconds=5)
    chunk: int = 1000
    slot: int = 0

    def __post_init__(self) -> None:
        """Проверить значения.

        Raises:
            ConfigurationError: неположительный ``claim_ttl`` или ``chunk``,
                отрицательные ``grace`` или ``slot``.
        """
        if self.claim_ttl <= timedelta(0):
            message = f"relay_claim_ttl должен быть > 0, получено {self.claim_ttl}"
            raise ConfigurationError(message)
        if self.grace < timedelta(0):
            message = f"relay_grace должен быть >= 0, получено {self.grace}"
            raise ConfigurationError(message)
        if self.chunk < 1:
            message = f"chunk relay должен быть >= 1, получено {self.chunk}"
            raise ConfigurationError(message)
        if self.slot < 0:
            message = f"slot relay должен быть >= 0, получено {self.slot}"
            raise ConfigurationError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class _Row:
    """Готовая запись outbox вместе с тем, что нужно для решения о ней."""

    id: UUID
    kind: OutboxKind
    batch_id: UUID
    task_name: str | None
    payload: bytes | None
    options: object
    paused: bool
    item_state: int | None

    @property
    def broken(self) -> bool:
        # Item удалён или уже завершён (отмена, retention), запись без задачи.
        if self.task_name is None or self.payload is None:
            return True
        if self.kind is OutboxKind.ITEM:
            return self.item_state is None or self.item_state >= TERMINAL_THRESHOLD
        return False

    def message(self) -> Message:
        options = cast("dict[str, object]", self.options) if isinstance(self.options, dict) else {}
        return Message(
            id=self.id,
            batch_id=self.batch_id,
            kind=self.kind,
            task_name=self.task_name or "",
            payload=self.payload or b"",
            options=options,
        )


@dataclass(frozen=True, slots=True)
class _Claim:
    """Итог транзакции захвата."""

    selected: int
    """Сколько готовых записей выбрал запрос."""
    changed: int
    """Сколько из них захвачено, запарковано или удалено."""
    messages: tuple[Message, ...]


def _uuids(ids: Iterable[UUID]) -> ColumnElement[Sequence[UUID]]:
    return literal(list(ids), ARRAY(Uuid()))


def _infinity() -> ColumnElement[datetime]:
    # Запись запаркована: relay её не видит до resume.
    return literal_column("'infinity'::timestamptz", DateTime(timezone=True))


def _expires(options: Mapping[str, object]) -> float | None:
    value = options.get("expires")
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


@dataclass(eq=False, kw_only=True)
class Relay:
    """Отправка ``th_outbox`` брокеру: fast-path :meth:`kick` и страховочный :meth:`scan_once`.

    Attributes:
        engine: Движок БД; схема установки — в ``schema_translate_map``.
        tables: Таблицы установки.
        clock: Часы: «сейчас» в SQL (D-002) и длительности для Observer.
        dispatcher: Адаптер брокера.
        observer: Получатель события ``relay_dispatched``.
        settings: Параметры relay.
        tx_settings: Таймауты своих транзакций; ``None`` — по умолчанию.
    """

    engine: AsyncEngine
    tables: Tables
    clock: Clock
    dispatcher: Dispatcher
    observer: Observer = field(default_factory=NullObserver)
    settings: RelaySettings = field(default_factory=RelaySettings)
    tx_settings: TxSettings | None = None
    _kicked: set[UUID] = field(init=False, default_factory=set[UUID])
    _wakeup: asyncio.Event = field(init=False, default_factory=asyncio.Event)

    # --- входы -------------------------------------------------------------

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        """Попросить отправить записи батчей (fast-path, UC-01).

        Синхронный и быстрый: годится для ``after_commit``. Отправку делает
        :meth:`run` (или :meth:`flush_kicked`). Потерянный kick страхует scan.

        Args:
            batch_ids: Батчи, в outbox которых появились записи.
        """
        self._kicked.update(batch_ids)
        if self._kicked:
            self._wakeup.set()

    async def flush_kicked(self) -> int:
        """Отправить готовые записи батчей из :meth:`kick`, не дожидаясь ``grace``.

        Returns:
            Сколько сообщений принял брокер.
        """
        sent = 0
        while self._kicked:
            batch_ids = frozenset(self._kicked)
            self._kicked.clear()
            sent += await self._drain(batch_ids)
        return sent

    async def run(self) -> None:
        """Цикл fast-path: ждать :meth:`kick` и отправлять; до отмены задачи.

        Ошибка прохода (БД недоступна) не останавливает цикл: записи
        останутся в outbox, их отправит следующий kick или scan.
        """
        while True:
            _ = await self._wakeup.wait()
            self._wakeup.clear()
            try:
                _ = await self.flush_kicked()
            except Exception:  # ruff: ignore[blind-except]  # fast-path — подсказка, пропуск страхует scan
                _log.exception("relay: проход fast-path упал")

    async def scan_once(self) -> int:
        """Страховочный проход: отправить все записи старше ``grace``.

        Раунды по ``chunk`` записей идут, пока готовые записи не кончатся, раунд
        не перестанет что-либо менять или брокер не откажет.

        Returns:
            Сколько сообщений принял брокер.
        """
        return await self._drain(None)

    # --- раунд -------------------------------------------------------------

    async def _drain(self, batch_ids: frozenset[UUID] | None) -> int:
        total = 0
        claim = functools.partial(self._claim, batch_ids=batch_ids)
        while True:
            claimed = await run_transaction(self.engine, claim, settings=self.tx_settings)
            sent, failed = await self._dispatch(claimed.messages)
            if sent:
                confirm = functools.partial(self._confirm, sent=sent)
                await run_transaction(self.engine, confirm, settings=self.tx_settings)
            total += len(sent)
            if failed or claimed.selected < self.settings.chunk or not claimed.changed:
                return total

    async def _claim(self, conn: AsyncConnection, batch_ids: frozenset[UUID] | None) -> _Claim:
        rows = await self._select_due(conn, batch_ids)
        send: list[_Row] = []
        park: list[UUID] = []
        drop: list[UUID] = []
        for row in rows:
            if row.broken:
                drop.append(row.id)
            elif row.kind is not OutboxKind.ITEM:
                send.append(row)
            elif row.paused:
                park.append(row.id)
            else:
                send.append(row)
        if drop:
            _log.warning("relay: удалено %d записей outbox без живого Item или задачи", len(drop))
            _ = await conn.execute(delete(self.tables.outbox).where(self._outbox_in(drop)))
        await self._take(conn, [row.id for row in send], park)
        return _Claim(
            selected=len(rows),
            changed=len(send) + len(park) + len(drop),
            messages=tuple(row.message() for row in send),
        )

    async def _select_due(
        self, conn: AsyncConnection, batch_ids: frozenset[UUID] | None
    ) -> list[_Row]:
        outbox = self.tables.outbox
        item = self.tables.item
        batch = self.tables.batch
        now = sql_now(self.clock)
        cutoff = now if batch_ids is not None else now - literal(self.settings.grace, Interval())
        stmt = (
            select(
                outbox.c.id,
                outbox.c.kind,
                outbox.c.batch_id,
                outbox.c.task_name,
                func.coalesce(outbox.c.payload, item.c.payload),
                func.coalesce(item.c.options, outbox.c.options),
                batch.c.paused_at.is_not(None),
                item.c.state,
            )
            .select_from(
                outbox.outerjoin(item, item.c.id == outbox.c.item_id).outerjoin(
                    batch, batch.c.id == outbox.c.batch_id
                )
            )
            .where(outbox.c.available_at <= cutoff)
            .order_by(outbox.c.available_at)
            .limit(self.settings.chunk)
            .with_for_update(of=outbox, skip_locked=True)
        )
        if batch_ids is not None:
            stmt = stmt.where(outbox.c.batch_id == any_(_uuids(sorted(batch_ids))))
        result = await conn.execute(stmt)
        return [
            _Row(
                id=row_id,
                kind=OutboxKind(kind),
                batch_id=batch_id,
                task_name=task_name,
                payload=payload,
                options=options,
                paused=bool(paused),
                item_state=item_state,
            )
            for (
                row_id,
                kind,
                batch_id,
                task_name,
                payload,
                options,
                paused,
                item_state,
            ) in result
        ]

    async def _take(self, conn: AsyncConnection, send: list[UUID], park: list[UUID]) -> None:
        outbox = self.tables.outbox
        now = sql_now(self.clock)
        if send:
            _ = await conn.execute(
                update(outbox)
                .where(self._outbox_in(send))
                .values(
                    available_at=now + literal(self.settings.claim_ttl, Interval()),
                    attempts=func.least(outbox.c.attempts + 1, _MAX_ATTEMPTS),
                )
            )
        if park:
            _ = await conn.execute(
                update(outbox).where(self._outbox_in(park)).values(available_at=_infinity())
            )

    def _outbox_in(self, ids: Iterable[UUID]) -> ColumnElement[bool]:
        return self.tables.outbox.c.id == any_(_uuids(ids))

    # --- отправка ----------------------------------------------------------

    async def _dispatch(self, messages: Sequence[Message]) -> tuple[list[Message], bool]:
        groups: dict[str, list[Message]] = {}
        for message in messages:
            groups.setdefault(message.task_name, []).append(message)
        sent: list[Message] = []
        failed = False
        for task_name in sorted(groups):
            group = groups[task_name]
            started = self.clock.monotonic()
            try:
                await self.dispatcher.dispatch(group)
            except Exception:  # ruff: ignore[blind-except]  # брокер отказал: записи вернутся через claim_ttl
                _log.exception("relay: dispatch %d сообщений %r не удался", len(group), task_name)
                failed = True
                continue
            sent.extend(group)
            self._notify(len(group), self.clock.monotonic() - started)
        return sent, failed

    def _notify(self, messages: int, duration: float) -> None:
        try:
            self.observer.relay_dispatched(messages=messages, duration=duration)
        except Exception:  # ruff: ignore[blind-except]  # наблюдатель не влияет на учёт
            _log.exception("relay: Observer.relay_dispatched упал")

    async def _confirm(self, conn: AsyncConnection, sent: Sequence[Message]) -> None:
        outbox = self.tables.outbox
        by_id = {message.id: message for message in sent}
        deleted = await conn.execute(
            delete(outbox)
            .where(self._outbox_in(sorted(by_id)))
            .returning(outbox.c.id, outbox.c.batch_id, outbox.c.kind)
        )
        dispatched: Counter[UUID] = Counter()
        expiring: list[tuple[UUID, float]] = []
        for row_id, batch_id, kind in deleted:
            if kind != OutboxKind.ITEM:
                continue
            dispatched[batch_id] += 1
            expires = _expires(by_id[row_id].options)
            if expires is not None:
                expiring.append((row_id, expires))
        if expiring:
            await self._write_expiry(conn, expiring)
        if dispatched:
            slot = self.settings.slot
            await upsert_slots(
                conn,
                self.tables,
                {
                    (batch_id, slot): CounterDelta(dispatched=n)
                    for batch_id, n in dispatched.items()
                },
            )

    async def _write_expiry(
        self, conn: AsyncConnection, expiring: list[tuple[UUID, float]]
    ) -> None:
        # Срок считается от отправки: flexiq не выполнит джобу позже, sweeper
        # завершит не захваченный вовремя Item как error("expired") (§11.4).
        expiry = self.tables.expiry
        now = sql_now(self.clock)
        stmt = insert(expiry).values(
            [
                {
                    "item_id": item_id,
                    "expires_at": now + literal(timedelta(seconds=secs), Interval()),
                }
                for item_id, secs in sorted(expiring)
            ]
        )
        _ = await conn.execute(
            stmt.on_conflict_do_update(
                index_elements=[expiry.c.item_id],
                set_={"expires_at": stmt.excluded.expires_at},
            )
        )
