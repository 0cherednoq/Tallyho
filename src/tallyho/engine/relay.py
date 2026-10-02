"""Relay: отправка записей ``th_outbox`` брокеру (ARCHITECTURE §6.3, UC-01, UC-10, §11.2).

Relay не зависит от адаптера: он говорит только с протоколом
:class:`~tallyho.protocols.broker.Dispatcher`. Один проход (раунд):

1. своя транзакция: захват готовых записей ``UPDATE th_outbox SET
   available_at = now + claim_ttl … FOR UPDATE SKIP LOCKED``. Записи
   отменённых или удалённых Items удаляются, записи батча на паузе и сверх
   окна ``max_in_flight`` паркуются (``available_at = infinity``);
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

Оба входа обслуживает один фоновый цикл на процесс (ARCHITECTURE §3.2). Он
стартует лениво в event loop первого ``kick`` либо явно — :meth:`Relay.start`
(его зовёт ``Maintenance.run``); останавливают его :meth:`Relay.stop` и
:meth:`Relay.close`. Scan не привязан к лидерству maintenance: параллельные
проходы разных процессов расходятся по ``FOR UPDATE SKIP LOCKED``.

Окно ``max_in_flight`` считается по ``th_window``: строка на отправленный и
не завершённый Item. Захват записей батча с окном сериализован
``pg_try_advisory_xact_lock``; занятый батч relay пропускает до следующего
прохода. Места освобождает завершение Item — :func:`release_window`
(Completer, T4.3b); scan дополнительно возвращает места функцией
:func:`refill_window`.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import logging
import threading
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Interval,
    SmallInteger,
    Uuid,
    any_,
    case,
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
from tallyho.storage.tx import RetryPolicy, run_transaction

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
    from datetime import datetime

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from tallyho.protocols.broker import Dispatcher
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.observer import Observer
    from tallyho.storage.tables import Tables
    from tallyho.storage.tx import TxSettings

__all__ = ["Relay", "RelaySettings", "refill_window", "release_window", "window_lock_key"]

_log = logging.getLogger(__name__)

_MAX_ATTEMPTS: Final = 32767
"""Предел ``th_outbox.attempts`` (smallint): счётчик захватов не переполняется."""

_LOCK_PERSON: Final = b"tallyho.window"
_SelectedRow = tuple[
    UUID,
    int,
    UUID,
    str | None,
    bytes | None,
    object,
    bool,
    int | None,
    int | None,
    float,
]


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
        scan_interval: Период страховочного scan в фоновом цикле
            (``sweep_interval``).
    """

    claim_ttl: timedelta = timedelta(seconds=30)
    grace: timedelta = timedelta(seconds=5)
    chunk: int = 1000
    slot: int = 0
    scan_interval: timedelta = timedelta(seconds=5)

    def __post_init__(self) -> None:
        """Проверить значения.

        Raises:
            ConfigurationError: неположительный ``claim_ttl``, ``chunk`` или
                ``scan_interval``, отрицательные ``grace`` или ``slot``.
        """
        if self.scan_interval <= timedelta(0):
            message = f"период scan relay должен быть > 0, получено {self.scan_interval}"
            raise ConfigurationError(message)
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
    max_in_flight: int | None
    item_state: int | None
    lag: float

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
    lag: float


@dataclass(eq=False, slots=True)
class _Pump:
    """Состояние одного запуска фонового цикла."""

    wakeup: asyncio.Event
    scan_due: bool
    """Scan нужен в ближайшем проходе, не дожидаясь ``scan_interval``."""
    stopping: bool = False
    task: asyncio.Task[None] = field(init=False)
    """Задача цикла; ``Relay._spawn`` задаёт её сразу после создания состояния."""

    @property
    def alive(self) -> bool:
        # Задача работает, а её event loop ещё не закрыт.
        return not self.task.done() and not self.task.get_loop().is_closed()

    def wake(self) -> None:
        # Разбудить цикл из любого потока; закрытый loop будить уже некому.
        owner = self.task.get_loop()
        if owner is _running_loop():
            self.wakeup.set()
            return
        with contextlib.suppress(RuntimeError):
            _ = owner.call_soon_threadsafe(self.wakeup.set)


def _running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def _uuids(ids: Iterable[UUID]) -> ColumnElement[Sequence[UUID]]:
    return literal(list(ids), ARRAY(Uuid()))


def _infinity(*, negative: bool = False) -> ColumnElement[datetime]:
    # infinity — запаркована; -infinity — возвращена из парковки в начало очереди.
    value = "'-infinity'::timestamptz" if negative else "'infinity'::timestamptz"
    return literal_column(value, DateTime(timezone=True))


def _small(value: int) -> ColumnElement[int]:
    # Коды — литералами, а не bind-параметрами (D-020).
    return literal_column(str(int(value)), SmallInteger())


def window_lock_key(batch_id: UUID) -> int:
    """Ключ ``pg_advisory_xact_lock``, под которым relay захватывает записи батча с окном.

    Args:
        batch_id: Батч с ``max_in_flight``.

    Returns:
        Знаковое 64-битное число из blake2b от id батча.
    """
    digest = hashlib.blake2b(batch_id.bytes, digest_size=8, person=_LOCK_PERSON).digest()
    return int.from_bytes(digest, "big", signed=True)


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
        autostart: Запускать ли фоновый цикл лениво при :meth:`kick`.
            ``False`` — проходы вызывает владелец (``InlineBroker``): цикл
            работает, только пока запущен явно через :meth:`start`.
    """

    engine: AsyncEngine
    tables: Tables
    clock: Clock
    dispatcher: Dispatcher
    observer: Observer = field(default_factory=NullObserver)
    settings: RelaySettings = field(default_factory=RelaySettings)
    tx_settings: TxSettings | None = None
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    autostart: bool = True
    _kicked: set[UUID] = field(init=False, default_factory=set[UUID])
    _pump: _Pump | None = field(init=False, default=None)
    _closed: bool = field(init=False, default=False)
    _guard: threading.Lock = field(init=False, default_factory=threading.Lock)

    # --- входы -------------------------------------------------------------

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        """Попросить отправить записи батчей (fast-path, UC-01).

        Синхронный и быстрый: годится для ``after_commit``. Отправку делает
        фоновый цикл: работающий — будится (в том числе из другого потока),
        а при ``autostart`` и без цикла — стартует в текущем event loop. Без
        цикла id копятся до :meth:`flush_kicked`. Потерянный kick страхует scan.

        Args:
            batch_ids: Батчи, в outbox которых появились записи.
        """
        self._kicked.update(batch_ids)
        if not self._kicked or self._closed:
            return
        with self._guard:
            pump = self._pump
            if pump is not None and pump.alive:
                pump.wake()
                return
            loop = _running_loop()
            if self.autostart and loop is not None:
                self._spawn(loop, scan_now=False)

    async def flush_kicked(self) -> int:
        """Отправить готовые записи батчей из :meth:`kick`, не дожидаясь ``grace``.

        Returns:
            Сколько сообщений принял брокер.
        """
        sent = 0
        while self._kicked:
            # Множество подменяется целиком: kick, пришедший во время прохода,
            # попадает в следующий раунд.
            kicked, self._kicked = self._kicked, set()
            sent += await self._drain(frozenset(kicked))
        return sent

    # --- фоновый цикл ------------------------------------------------------

    @property
    def running(self) -> bool:
        """Работает ли фоновый цикл."""
        pump = self._pump
        return pump is not None and pump.alive

    def start(self, *, scan_now: bool = False) -> None:
        """Запустить фоновый цикл в текущем event loop; повторный вызов безвреден.

        Вызывается из корутины. После :meth:`close` ничего не делает.

        Args:
            scan_now: Выполнить страховочный scan сразу, а не через
                ``scan_interval`` (запуск maintenance подбирает потерянное).
        """
        if self._closed:
            return
        with self._guard:
            pump = self._pump
            if pump is not None and pump.alive:
                if scan_now:
                    pump.scan_due = True
                    pump.wake()
                return
            self._spawn(asyncio.get_running_loop(), scan_now=scan_now)

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        """Event loop работающего фонового цикла; ``None``, если цикла нет."""
        pump = self._pump
        return pump.task.get_loop() if pump is not None and pump.alive else None

    async def stop(self, *, grace: float | None = None) -> None:
        """Остановить фоновый цикл и дождаться его; цикл можно запустить снова.

        Остановка мягкая: текущий проход завершается, уже полученные kick
        отправляются. Задача цикла из другого event loop только получает
        просьбу остановиться — дождаться её можно лишь в её loop
        (:attr:`loop`).

        Args:
            grace: Сколько секунд ждать мягкой остановки; ``None`` — без
                ограничения. По истечении задача цикла отменяется: захваченные
                записи вернутся через ``relay_claim_ttl``.
        """
        with self._guard:
            pump = self._pump
            self._pump = None
        if pump is None or not pump.alive:
            return
        pump.stopping = True
        pump.wake()
        if pump.task.get_loop() is not asyncio.get_running_loop():
            return
        # wait, а не await task: отмена самой задачи цикла не должна
        # выглядеть отменой вызывающего.
        _, late = await asyncio.wait({pump.task}, timeout=grace)
        if late:
            _log.warning("relay: цикл не остановился за %.1f с и отменён", grace)
            _ = pump.task.cancel()
            _ = await asyncio.wait(late)

    async def close(self, *, grace: float | None = None) -> None:
        """Остановить цикл насовсем: после закрытия kick только копит id.

        Args:
            grace: Срок мягкой остановки, как у :meth:`stop`.
        """
        self._closed = True
        await self.stop(grace=grace)

    def _spawn(self, loop: asyncio.AbstractEventLoop, *, scan_now: bool) -> None:
        pump = _Pump(wakeup=asyncio.Event(), scan_due=scan_now)
        if self._kicked or scan_now:
            pump.wakeup.set()
        pump.task = loop.create_task(self._run(pump), name="tallyho-relay")
        self._pump = pump

    async def _run(self, pump: _Pump) -> None:
        # Ошибка прохода (БД недоступна) не останавливает цикл: записи
        # останутся в outbox, их отправит следующий kick или scan.
        loop = asyncio.get_running_loop()
        every = self.settings.scan_interval.total_seconds()
        next_scan = loop.time() + every
        while True:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(max(0.0, next_scan - loop.time())):
                    _ = await pump.wakeup.wait()
            pump.wakeup.clear()
            await self._pass(self.flush_kicked, "fast-path")
            if pump.stopping:
                return
            if pump.scan_due or loop.time() >= next_scan:
                pump.scan_due = False
                await self._pass(self.scan_once, "scan")
                next_scan = loop.time() + every

    @staticmethod
    async def _pass(step: Callable[[], Awaitable[int]], name: str) -> None:
        try:
            _ = await step()
        except Exception:  # ruff: ignore[blind-except]  # проход — страховка: записи остаются в outbox до следующего
            _log.exception("relay: проход %s упал", name)

    async def scan_once(self) -> int:
        """Страховочный проход: вернуть места окна и отправить все записи старше ``grace``.

        Раунды по ``chunk`` записей идут, пока готовые записи не кончатся, раунд
        не перестанет что-либо менять или брокер не откажет.

        Returns:
            Сколько сообщений принял брокер.
        """

        async def refill(conn: AsyncConnection) -> None:
            _ = await refill_window(conn, self.tables)

        await run_transaction(self.engine, refill, settings=self.tx_settings, policy=self.retry)
        return await self._drain(None)

    # --- раунд -------------------------------------------------------------

    async def _drain(self, batch_ids: frozenset[UUID] | None) -> int:
        total = 0
        claim = functools.partial(self._claim, batch_ids=batch_ids)
        while True:
            claimed = await run_transaction(
                self.engine, claim, settings=self.tx_settings, policy=self.retry
            )
            self._notify_lag(claimed.lag)
            sent, failed = await self._dispatch(claimed.messages)
            if sent:
                confirm = functools.partial(self._confirm, sent=sent)
                await run_transaction(
                    self.engine, confirm, settings=self.tx_settings, policy=self.retry
                )
            total += len(sent)
            if failed or claimed.selected < self.settings.chunk or not claimed.changed:
                return total

    async def _claim(self, conn: AsyncConnection, batch_ids: frozenset[UUID] | None) -> _Claim:
        rows = await self._select_due(conn, batch_ids)
        send: list[_Row] = []
        park: list[UUID] = []
        drop: list[UUID] = []
        windowed: dict[UUID, list[_Row]] = {}
        for row in rows:
            if row.broken:
                drop.append(row.id)
            elif row.kind is not OutboxKind.ITEM:
                send.append(row)
            elif row.paused:
                park.append(row.id)
            elif row.max_in_flight is not None:
                windowed.setdefault(row.batch_id, []).append(row)
            else:
                send.append(row)
        new_window: list[_Row] = []
        for batch_id in sorted(windowed):
            passed = await self._fit_window(conn, batch_id, windowed[batch_id])
            if passed is None:
                continue  # батч захватывает другой relay: записи остаются готовыми
            allowed, fresh = passed
            send.extend(allowed)
            new_window.extend(fresh)
            taken = {row.id for row in allowed}
            park.extend(row.id for row in windowed[batch_id] if row.id not in taken)
        if drop:
            _log.warning("relay: удалено %d записей outbox без живого Item или задачи", len(drop))
            _ = await conn.execute(delete(self.tables.outbox).where(self._outbox_in(drop)))
        await self._take(conn, [row.id for row in send], park)
        if new_window:
            window = self.tables.window
            _ = await conn.execute(
                insert(window)
                .values([{"item_id": row.id, "batch_id": row.batch_id} for row in new_window])
                .on_conflict_do_nothing()
            )
        return _Claim(
            selected=len(rows),
            changed=len(send) + len(park) + len(drop),
            messages=tuple(row.message() for row in send),
            lag=max(
                (row.lag for row in rows),
                default=0.0,
            ),
        )

    async def _select_due(
        self, conn: AsyncConnection, batch_ids: frozenset[UUID] | None
    ) -> list[_Row]:
        outbox = self.tables.outbox
        item = self.tables.item
        batch = self.tables.batch
        now = sql_now(self.clock)
        lag = cast(
            "ColumnElement[float]",
            case(
                (
                    outbox.c.available_at > _infinity(negative=True),
                    func.extract("epoch", now - outbox.c.available_at),
                ),
                else_=literal(0.0),
            ),
        )
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
                batch.c.max_in_flight,
                item.c.state,
                lag,
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
        typed_rows = cast("Iterable[_SelectedRow]", result)
        return [
            _Row(
                id=row_id,
                kind=OutboxKind(kind),
                batch_id=batch_id,
                task_name=task_name,
                payload=payload,
                options=options,
                paused=bool(paused),
                max_in_flight=max_in_flight,
                item_state=item_state,
                lag=max(0.0, float(lag_seconds)),
            )
            for (
                row_id,
                kind,
                batch_id,
                task_name,
                payload,
                options,
                paused,
                max_in_flight,
                item_state,
                lag_seconds,
            ) in typed_rows
        ]

    async def _fit_window(
        self, conn: AsyncConnection, batch_id: UUID, rows: list[_Row]
    ) -> tuple[list[_Row], list[_Row]] | None:
        # Сколько записей батча с окном можно отправить. None — батч занят другим relay.
        lock = func.pg_try_advisory_xact_lock(
            literal(window_lock_key(batch_id), BigInteger()), type_=Boolean()
        )
        if not await conn.scalar(select(lock)):
            return None
        window = self.tables.window
        used = int(
            await conn.scalar(
                select(func.count()).select_from(window).where(window.c.batch_id == batch_id)
            )
            or 0
        )
        # Уже в окне: захвачены прошлым проходом, который не дошёл до DELETE.
        held = set(
            await conn.scalars(
                select(window.c.item_id).where(
                    window.c.item_id == any_(_uuids(row.id for row in rows))
                )
            )
        )
        room = (rows[0].max_in_flight or 0) - used
        allowed: list[_Row] = []
        fresh: list[_Row] = []
        for row in rows:
            if row.id in held:
                allowed.append(row)
            elif room > 0:
                allowed.append(row)
                fresh.append(row)
                room -= 1
        return allowed, fresh

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

    def _notify_lag(self, seconds: float) -> None:
        try:
            self.observer.relay_lag(seconds=seconds)
        except Exception:  # ruff: ignore[blind-except]  # observer must not affect delivery
            _log.exception("relay: Observer.relay_lag failed")

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


# --- окно max_in_flight --------------------------------------------------------


async def release_window(
    conn: AsyncConnection, tables: Tables, item_ids: Iterable[UUID]
) -> list[UUID]:
    """Освободить места окна ``max_in_flight`` завершённых Items.

    Вызывает Completer в транзакции finish (T4.3b) после CAS Items: удаляет
    их строки ``th_window`` и возвращает в очередь столько запаркованных
    записей каждого батча (не на паузе), сколько мест освободилось. Для Items
    без окна — ничего не делает. После commit вызывающий зовёт
    :meth:`Relay.kick` для вернувшихся батчей.

    Возвращённая запись получает ``available_at = -infinity``: она ждала
    дольше всех, поэтому встаёт в начало очереди и сразу видна scan, без
    ``relay_grace``.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        item_ids: Завершённые Items.

    Returns:
        Батчи, в которых освободились места, по возрастанию id.
    """
    ids = sorted(set(item_ids))
    if not ids:
        return []
    window = tables.window
    freed: Counter[UUID] = Counter(
        await conn.scalars(
            delete(window).where(window.c.item_id == any_(_uuids(ids))).returning(window.c.batch_id)
        )
    )
    _ = await _unpark(conn, tables, freed)
    return sorted(freed)


async def refill_window(conn: AsyncConnection, tables: Tables) -> int:
    """Вернуть в очередь запаркованные записи батчей, у которых в окне есть место.

    Страховка scan: места могли освободиться без :func:`release_window`
    (resume, sweeper, ручная правка). Рассматриваются активные батчи с
    ``max_in_flight`` не на паузе, у которых есть запаркованные записи.
    Свободно = окно - строки ``th_window`` - готовые (не запаркованные)
    записи батча.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.

    Returns:
        Сколько записей возвращено в очередь.
    """
    batch = tables.batch
    outbox = tables.outbox
    parked = (
        select(outbox.c.id)
        .where(outbox.c.batch_id == batch.c.id, outbox.c.available_at == _infinity())
        .exists()
    )
    candidates = await conn.execute(
        select(batch.c.id, batch.c.max_in_flight)
        .where(
            batch.c.state < _small(TERMINAL_THRESHOLD),
            batch.c.max_in_flight.is_not(None),
            batch.c.paused_at.is_(None),
            parked,
        )
        .order_by(batch.c.id)
    )
    room: Counter[UUID] = Counter()
    for batch_id, max_in_flight in candidates:
        limit = max_in_flight or 0
        in_window = await _bounded_count(
            conn,
            tables.window.c.batch_id == batch_id,
            column=tables.window.c.item_id,
            limit=limit,
        )
        ready = await _bounded_count(
            conn,
            (outbox.c.batch_id == batch_id)
            & (outbox.c.kind == _small(OutboxKind.ITEM))
            & (outbox.c.available_at < _infinity()),
            column=outbox.c.id,
            limit=limit,
        )
        busy = in_window + ready
        if busy < limit:
            room[batch_id] = limit - busy
    return await _unpark(conn, tables, room)


async def _bounded_count(
    conn: AsyncConnection, where: ColumnElement[bool], *, column: ColumnElement[UUID], limit: int
) -> int:
    # count(*) не дальше limit строк: окно маленькое, а готовых записей может быть много.
    sample = select(column).where(where).limit(limit).subquery()
    return int(await conn.scalar(select(func.count()).select_from(sample)) or 0)


async def _unpark(conn: AsyncConnection, tables: Tables, room: Mapping[UUID, int]) -> int:
    # Не больше room[batch] запаркованных записей Items батча, не стоящего на паузе.
    if not room:
        return 0
    batch = tables.batch
    outbox = tables.outbox
    active = set(
        await conn.scalars(
            select(batch.c.id).where(
                batch.c.id == any_(_uuids(sorted(room))), batch.c.paused_at.is_(None)
            )
        )
    )
    moved = 0
    for batch_id in sorted(active):
        chosen = (
            select(outbox.c.id)
            .where(
                outbox.c.batch_id == batch_id,
                outbox.c.available_at == _infinity(),
                outbox.c.kind == _small(OutboxKind.ITEM),
            )
            .limit(room[batch_id])
            .with_for_update(skip_locked=True)
        )
        result = await conn.execute(
            update(outbox)
            .where(outbox.c.id.in_(chosen))
            .values(available_at=_infinity(negative=True))
        )
        moved += result.rowcount
    return moved
