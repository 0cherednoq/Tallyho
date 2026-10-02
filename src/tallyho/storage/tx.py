"""Транзакции storage: чужие сессии, свои транзакции и их повтор.

Правила (DECISIONS D-004, ARCHITECTURE §7.3, §10, COUNTERS §3.6, P10):

* функции storage и engine принимают ``AsyncConnection``; ``AsyncSession``
  пользователя разворачивается на границе через :func:`resolve_connection`;
* commit/rollback чужой транзакции мы никогда не делаем;
* каждая своя транзакция ставит ``SET LOCAL lock_timeout`` и
  ``statement_timeout`` (:func:`own_transaction`), а :func:`run_transaction`
  повторяет её целиком на ``40001`` (serialization failure), ``40P01``
  (deadlock) и ``55P03`` (lock not available) с экспоненциальным backoff и
  джиттером;
* :func:`after_commit` вызывает колбэк только после commit внешней транзакции
  пользователя (откат транзакции или savepoint'а его отбрасывает);
* tx-хук получает :class:`HookSession` (:func:`hook_session`): commit, rollback
  и close в ней бросают ``HookTransactionError``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Final, TypeVar, cast
from weakref import WeakKeyDictionary

from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho.model.errors import ConcurrentModification, HookTransactionError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.orm import Session, SessionTransaction

__all__ = [
    "RETRYABLE_SQLSTATES",
    "AfterCommit",
    "HookSession",
    "RetryPolicy",
    "TxSettings",
    "after_commit",
    "after_commit_pending",
    "hook_session",
    "is_retryable",
    "own_transaction",
    "resolve_connection",
    "run_transaction",
    "set_local_timeouts",
    "sqlstate_of",
]

T = TypeVar("T")

_log = logging.getLogger(__name__)

RETRYABLE_SQLSTATES: Final = frozenset({"40001", "40P01", "55P03"})
"""SQLSTATE, на которых своя транзакция повторяется: serialization failure,
deadlock detected, lock not available (``lock_timeout``)."""

_ONE_MS: Final = timedelta(milliseconds=1)
_LOCK_CONFIG: Final = "set_config('lock_timeout', :lock, true)"
_STATEMENT_CONFIG: Final = "set_config('statement_timeout', :statement, true)"
_SET_LOCK_TIMEOUT: Final = text(f"SELECT {_LOCK_CONFIG}")
_SET_BOTH_TIMEOUTS: Final = text(f"SELECT {_LOCK_CONFIG}, {_STATEMENT_CONFIG}")
_RETRIES_EXHAUSTED = "транзакция не прошла: дедлок, конфликт сериализации или lock_timeout"


@dataclass(frozen=True, slots=True, kw_only=True)
class TxSettings:
    """Таймауты своей транзакции (``SET LOCAL``).

    Attributes:
        lock_timeout: Сколько ждать чужую блокировку (ARCHITECTURE §15: 5 с).
        statement_timeout: Предел одного запроса; ``None`` — не менять
            настройку сервера.
    """

    lock_timeout: timedelta = timedelta(seconds=5)
    statement_timeout: timedelta | None = timedelta(seconds=30)


@dataclass(frozen=True, slots=True, kw_only=True)
class RetryPolicy:
    """Повтор своей транзакции: экспоненциальный backoff с джиттером.

    Attributes:
        attempts: Сколько всего попыток, включая первую.
        base_delay: Пауза перед первым повтором без джиттера, секунды.
        max_delay: Верхняя граница паузы, секунды.
    """

    attempts: int = 5
    base_delay: float = 0.05
    max_delay: float = 2.0
    on_retry: Callable[[str], None] | None = None

    def delay(self, retry: int, rng: random.Random) -> float:
        """Пауза перед повтором номер ``retry`` (с нуля).

        Верхняя граница растёт как ``base_delay * 2**retry`` до ``max_delay``;
        пауза берётся случайно из её второй половины («equal jitter»), чтобы
        столкнувшиеся транзакции разошлись, а пауза всё равно росла.

        Args:
            retry: Номер повтора, начиная с 0.
            rng: Источник случайности; в тестах — с фиксированным seed.

        Returns:
            Пауза в секундах.
        """
        cap = min(self.max_delay, self.base_delay * 2.0**retry)
        half = cap / 2
        return half + rng.uniform(0.0, half)


async def resolve_connection(target: AsyncSession | AsyncConnection) -> AsyncConnection:
    """Соединение, в транзакции которого работает storage.

    Для ``AsyncSession`` — ``await session.connection()``: сессия начинает
    транзакцию, если её ещё нет, а внутри ``begin_nested()`` наши запросы
    попадают в текущий savepoint и откатываются вместе с ним. Сессия не
    сбрасывается (``flush``): настройки ``autoflush`` пользователя не меняются.
    ``AsyncConnection`` возвращается как есть.

    Args:
        target: Сессия или соединение пользователя.

    Returns:
        ``AsyncConnection`` той же транзакции.

    """
    if isinstance(target, AsyncSession):
        return await target.connection()
    return target


def _pg_duration(value: timedelta) -> str:
    return f"{int(value / _ONE_MS)}ms"


async def set_local_timeouts(conn: AsyncConnection, settings: TxSettings) -> None:
    """``SET LOCAL lock_timeout`` / ``statement_timeout`` текущей транзакции.

    Через ``set_config(..., is_local => true)``: значения идут параметрами.

    Args:
        conn: Соединение в открытой транзакции.
        settings: Значения таймаутов.
    """
    if settings.statement_timeout is None:
        _ = await conn.execute(_SET_LOCK_TIMEOUT, {"lock": _pg_duration(settings.lock_timeout)})
        return
    params = {
        "lock": _pg_duration(settings.lock_timeout),
        "statement": _pg_duration(settings.statement_timeout),
    }
    _ = await conn.execute(_SET_BOTH_TIMEOUTS, params)


@contextlib.asynccontextmanager
async def own_transaction(
    engine: AsyncEngine, settings: TxSettings | None = None
) -> AsyncGenerator[AsyncConnection]:
    """Своя транзакция: ``engine.begin()`` + ``SET LOCAL`` таймаутов.

    Commit — при выходе без исключения, иначе rollback. Одна попытка, без
    повтора: повторяет :func:`run_transaction`.

    Args:
        engine: Движок БД.
        settings: Таймауты; по умолчанию :class:`TxSettings`.

    Yields:
        Соединение в открытой транзакции.
    """
    async with engine.begin() as conn:
        await set_local_timeouts(conn, settings or TxSettings())
        yield conn


def sqlstate_of(exc: BaseException) -> str | None:
    """SQLSTATE ошибки драйвера, завёрнутой SQLAlchemy.

    Args:
        exc: Любое исключение.

    Returns:
        Код SQLSTATE или ``None``, если это не ошибка БД или код неизвестен.
    """
    if not isinstance(exc, DBAPIError):
        return None
    # asyncpg (через адаптер SQLAlchemy) и psycopg кладут код в ``orig.sqlstate``.
    state = cast("object", getattr(exc.orig, "sqlstate", None))
    return state if isinstance(state, str) else None


def is_retryable(exc: BaseException) -> bool:
    """Можно ли повторить свою транзакцию после этой ошибки.

    Args:
        exc: Исключение из транзакции.

    Returns:
        ``True`` для SQLSTATE из :data:`RETRYABLE_SQLSTATES`.
    """
    return sqlstate_of(exc) in RETRYABLE_SQLSTATES


async def run_transaction(
    engine: AsyncEngine,
    work: Callable[[AsyncConnection], Awaitable[T]],
    *,
    settings: TxSettings | None = None,
    policy: RetryPolicy | None = None,
    rng: random.Random | None = None,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
) -> T:
    """Выполнить ``work`` в своей транзакции с повтором на конфликтах.

    Каждая попытка — новая транзакция :func:`own_transaction`; ``work``
    вызывается заново и не должен иметь побочных эффектов вне БД.

    Args:
        engine: Движок БД.
        work: Работа внутри транзакции.
        settings: Таймауты транзакции.
        policy: Сколько раз и с какими паузами повторять.
        rng: Источник джиттера; в тестах — ``random.Random(seed)``.
        sleep: Ожидание между попытками; в тестах — запись пауз.

    Returns:
        Результат ``work`` из успешной попытки.

    Raises:
        ConcurrentModification: Все попытки упали на
            :data:`RETRYABLE_SQLSTATES`.
        DBAPIError: Прочие ошибки БД пробрасываются сразу, без повтора.
    """
    policy = policy or RetryPolicy()
    rng = rng or random.Random()  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # джиттер, не криптография
    retry = 0
    while True:
        try:
            async with own_transaction(engine, settings) as conn:
                return await work(conn)
        except DBAPIError as exc:
            if not is_retryable(exc):
                raise
            if retry + 1 >= policy.attempts:
                raise ConcurrentModification(_RETRIES_EXHAUSTED) from exc
            state = sqlstate_of(exc)
            if state is not None and policy.on_retry is not None:
                try:
                    policy.on_retry(state)
                except Exception:  # ruff: ignore[blind-except]  # telemetry cannot change retry semantics
                    _log.exception("transaction retry observer failed for SQLSTATE %s", state)
        _ = await sleep(policy.delay(retry, rng))
        retry += 1


# --- after_commit ---------------------------------------------------------------------

AfterCommit = Callable[[], object]
"""Колбэк после commit: синхронный, без аргументов; результат игнорируется."""

_NOT_STARTED = "AsyncConnection ещё не открыт: after_commit нужен в транзакции"


@dataclass(eq=False)
class _Pending:
    """Колбэки одной сессии или соединения и стек их savepoint'ов.

    Для каждого открытого savepoint'а стек хранит число колбэков на момент его
    начала: откат savepoint'а отбрасывает всё, что зарегистрировано после, а
    release оставляет колбэки родителю. Savepoint'ы вложены строго (LIFO), поэтому
    закрывается всегда вершина стека. Если стек пуст, закрывается savepoint,
    открытый до первой регистрации, — он старше всех колбэков, и его откат
    отбрасывает всё.
    """

    callbacks: list[AfterCommit] = field(default_factory=list)
    stack: list[int] = field(default_factory=list)
    released: set[object] = field(default_factory=set)

    def savepoint_started(self) -> None:
        self.stack.append(len(self.callbacks))

    def savepoint_ended(self, *, rolled_back: bool) -> None:
        start = self.stack.pop() if self.stack else 0
        if rolled_back:
            del self.callbacks[start:]

    def committed(self) -> None:
        callbacks = self.callbacks
        self.reset()
        for callback in callbacks:
            _run_callback(callback)

    def reset(self) -> None:
        self.callbacks = []
        self.stack.clear()
        self.released.clear()


def _run_callback(callback: AfterCommit) -> None:
    # Commit уже состоялся: ошибка колбэка не должна выглядеть как ошибка commit
    # и не должна мешать остальным колбэкам. Колбэки — подсказки (kick relay,
    # fold), пропуск страхуют relay scan и sweeper.
    try:
        _ = callback()
    except Exception:  # ruff: ignore[blind-except]  # commit уже прошёл, ошибку только логируем
        _log.exception("after_commit: колбэк %r упал", callback)


_sessions: WeakKeyDictionary[Session, _Pending] = WeakKeyDictionary()
_connections: WeakKeyDictionary[Connection, _Pending] = WeakKeyDictionary()


def _session_pending(session: Session) -> _Pending:
    pending = _sessions.get(session)
    if pending is not None:
        return pending
    pending = _sessions[session] = _Pending()

    def on_create(_session: Session, transaction: SessionTransaction) -> None:
        if transaction.nested:
            pending.savepoint_started()

    def on_commit(sess: Session) -> None:
        # after_commit срабатывает и на release savepoint'а, пока он ещё текущий.
        nested = sess.get_nested_transaction()
        if nested is None:
            pending.committed()
        else:
            pending.released.add(nested)

    def on_end(_session: Session, transaction: SessionTransaction) -> None:
        if transaction.nested:
            released = transaction in pending.released
            pending.released.discard(transaction)
            pending.savepoint_ended(rolled_back=not released)
        elif transaction.parent is None:
            pending.reset()

    event.listen(session, "after_transaction_create", on_create)
    event.listen(session, "after_commit", on_commit)
    event.listen(session, "after_transaction_end", on_end)
    return pending


def _connection_pending(conn: Connection) -> _Pending:
    pending = _connections.get(conn)
    if pending is not None:
        return pending
    pending = _connections[conn] = _Pending()

    # Имя в событии savepoint бывает None (его генерирует SQLAlchemy позже),
    # поэтому savepoint'ы соединения отслеживаются стеком, а не по имени.
    def on_savepoint(_conn: Connection, _name: str | None) -> None:
        pending.savepoint_started()

    def on_release(_conn: Connection, _name: str, _context: object) -> None:
        pending.savepoint_ended(rolled_back=False)

    def on_rollback_savepoint(_conn: Connection, _name: str, _context: object) -> None:
        pending.savepoint_ended(rolled_back=True)

    def on_commit(_conn: Connection) -> None:
        pending.committed()

    def on_rollback(_conn: Connection) -> None:
        pending.reset()

    event.listen(conn, "savepoint", on_savepoint)
    event.listen(conn, "release_savepoint", on_release)
    event.listen(conn, "rollback_savepoint", on_rollback_savepoint)
    event.listen(conn, "commit", on_commit)
    event.listen(conn, "rollback", on_rollback)
    return pending


async def after_commit(target: AsyncSession | AsyncConnection, callback: AfterCommit) -> None:
    """Вызвать ``callback`` после commit внешней транзакции ``target``.

    Колбэк привязан к текущему уровню транзакции: откат savepoint'а (или всей
    транзакции) отбрасывает его, release savepoint'а передаёт родителю, а
    commit корневой транзакции вызывает — ровно один раз. Колбэки вызываются в
    порядке регистрации; исключение колбэка логируется и не мешает остальным.

    * ``AsyncSession`` — события ``sync_session``; колбэк вызывается после
      успешного COMMIT. Сессия начинает транзакцию, если её ещё нет.
    * ``AsyncConnection`` — события ``Connection``. В SQLAlchemy нет события
      после COMMIT соединения, поэтому колбэк вызывается событием ``commit``
      непосредственно **перед** отправкой COMMIT. Если COMMIT упадёт, колбэк
      уже вызван, а данные транзакции могут быть ещё не видны другим
      соединениям. Колбэк должен только подталкивать фоновую работу, которая
      перечитывает БД сама и страхуется опросом (relay scan, sweeper).

    Args:
        target: Сессия или соединение пользователя.
        callback: Синхронная функция без аргументов; должна быть быстрой.

    Raises:
        TypeError: ``AsyncConnection`` ещё не открыт.
    """
    if isinstance(target, HookSession):
        # Сессия хука не коммитится сама: колбэк ждёт commit нашей транзакции.
        target = await target.connection()
    if isinstance(target, AsyncSession):
        _ = await target.connection()
        _session_pending(target.sync_session).callbacks.append(callback)
        return
    sync = target.sync_connection
    if sync is None:
        raise TypeError(_NOT_STARTED)
    _connection_pending(sync).callbacks.append(callback)


async def after_commit_pending(
    target: AsyncSession | AsyncConnection, callback: AfterCommit
) -> bool:
    """Ждёт ли ``callback`` commit транзакции ``target``.

    ``True`` — колбэк зарегистрирован :func:`after_commit`, а записи его уровня
    транзакции ещё в силе: commit не было, savepoint и транзакция не откатаны.
    После commit (колбэк вызван) и после отката — ``False``. По ответу
    вызывающий отличает «моя запись в этой транзакции ещё действует» от «её
    откатили, пишем заново».

    Args:
        target: Сессия или соединение, переданные в :func:`after_commit`.
        callback: Тот же объект колбэка.

    Returns:
        ``True``, пока колбэк ждёт commit.
    """
    if isinstance(target, HookSession):
        target = await target.connection()
    if isinstance(target, AsyncSession):
        pending = _sessions.get(target.sync_session)
    else:
        sync = target.sync_connection
        pending = None if sync is None else _connections.get(sync)
    return pending is not None and callback in pending.callbacks


# --- HookSession ----------------------------------------------------------------------

_HOOK_COMMIT = "commit() внутри tx-хука запрещён: транзакцией хука управляет tallyho"
_HOOK_ROLLBACK = "rollback() внутри tx-хука запрещён: бросьте исключение, tallyho откатит всё"
_HOOK_CLOSE = "close() внутри tx-хука запрещён: сессию закрывает tallyho"


class HookSession(AsyncSession):
    """Сессия tx-хука (ARCHITECTURE §7.3, A-DB-05).

    Обычная ``AsyncSession``, привязанная к нашему соединению и транзакции:
    ORM и Core пользователя работают в той же транзакции, что и CAS
    финализации. ``commit()``, ``rollback()`` и ``close()`` бросают
    :class:`~tallyho.model.errors.HookTransactionError`: транзакцией владеет
    tallyho. Создаётся только через :func:`hook_session`. Вложенные
    ``begin_nested()`` разрешены. :func:`after_commit` для неё срабатывает при
    commit нашей транзакции.
    """

    @override
    async def commit(self) -> None:
        """Запрещено в хуке.

        Raises:
            HookTransactionError: Всегда.
        """
        raise HookTransactionError(_HOOK_COMMIT)

    @override
    async def rollback(self) -> None:
        """Запрещено в хуке.

        Raises:
            HookTransactionError: Всегда.
        """
        raise HookTransactionError(_HOOK_ROLLBACK)

    @override
    async def close(self) -> None:
        """Запрещено в хуке.

        Raises:
            HookTransactionError: Всегда.
        """
        raise HookTransactionError(_HOOK_CLOSE)


@contextlib.asynccontextmanager
async def hook_session(conn: AsyncConnection) -> AsyncGenerator[HookSession]:
    """:class:`HookSession` в транзакции ``conn`` на время вызова хука.

    На выходе без исключения сбрасывает (``flush``) изменения ORM хука, чтобы
    они попали в БД до следующих шагов транзакции (CAS финализации). Сессия
    закрывается в любом случае; commit/rollback транзакции ``conn`` остаются
    за вызывающим (``own_transaction``).

    Args:
        conn: Соединение нашей транзакции.

    Yields:
        Сессия для хука пользователя.
    """
    session = HookSession(bind=conn, expire_on_commit=False)
    try:
        yield session
        await session.flush()
    finally:
        await AsyncSession.close(session)
