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
* :func:`after_commit` вызывает колбэк только после подтверждённого COMMIT
  внешней транзакции (откат транзакции или savepoint'а и ошибка COMMIT его
  отбрасывают); своя транзакция — :func:`begin_transaction` /
  :func:`own_transaction` — доставляет колбэки сразу по выходе;
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
from weakref import WeakKeyDictionary, WeakSet

from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho.model.errors import ConcurrentModification, HookTransactionError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from sqlalchemy.engine import (
        Connection,
        Dialect,
        Engine,
        ExceptionContext,
        RootTransaction,
    )
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
    "begin_transaction",
    "deliver_committed",
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
    """Своя транзакция: :func:`begin_transaction` + ``SET LOCAL`` таймаутов.

    Commit — при выходе без исключения, иначе rollback; колбэки
    :func:`after_commit` вызываются после подтверждённого COMMIT, до возврата
    управления. Одна попытка, без повтора: повторяет :func:`run_transaction`.

    Args:
        engine: Движок БД.
        settings: Таймауты; по умолчанию :class:`TxSettings`.

    Yields:
        Соединение в открытой транзакции.
    """
    async with begin_transaction(engine) as conn:
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
_SPIN_SECONDS: Final = 0.005
_MAX_POLL_SECONDS: Final = 0.05


@dataclass(eq=False)
class _Commit:
    """Колбэки транзакции соединения, чей COMMIT отправлен, но ещё не подтверждён.

    Событие ``commit`` соединения SQLAlchemy срабатывает до отправки COMMIT.
    Исход виден позже: корневая транзакция становится неактивной при любом
    исходе, а ошибка COMMIT до этого проходит через ``handle_error`` движка
    (``failed``).
    """

    transaction: RootTransaction
    callbacks: list[AfterCommit]
    failed: bool = False

    @property
    def in_flight(self) -> bool:
        return self.transaction.is_active and not self.failed


@dataclass(eq=False)
class _Pending:
    """Колбэки одной сессии или соединения и стек их savepoint'ов.

    Для каждого открытого savepoint'а стек хранит число колбэков на момент его
    начала: откат savepoint'а отбрасывает всё, что зарегистрировано после, а
    release оставляет колбэки родителю. Savepoint'ы вложены строго (LIFO), поэтому
    закрывается всегда вершина стека. Если стек пуст, закрывается savepoint,
    открытый до первой регистрации, — он старше всех колбэков, и его откат
    отбрасывает всё.

    ``commits`` бывают только у соединения: транзакции, чей COMMIT отправлен, а
    исход ещё не разобран (:func:`_settle`).
    """

    callbacks: list[AfterCommit] = field(default_factory=list)
    stack: list[int] = field(default_factory=list)
    released: set[object] = field(default_factory=set)
    commits: list[_Commit] = field(default_factory=list)

    def savepoint_started(self) -> None:
        self.stack.append(len(self.callbacks))

    def savepoint_ended(self, *, rolled_back: bool) -> None:
        start = self.stack.pop() if self.stack else 0
        if rolled_back:
            del self.callbacks[start:]

    def take(self) -> list[AfterCommit]:
        callbacks = self.callbacks
        self.reset()
        return callbacks

    def committed(self) -> None:
        _run_callbacks(self.take())

    def reset(self) -> None:
        self.callbacks = []
        self.stack.clear()
        self.released.clear()

    def waiting(self, callback: AfterCommit) -> bool:
        if callback in self.callbacks:
            return True
        return any(commit.in_flight and callback in commit.callbacks for commit in self.commits)


def _run_callbacks(callbacks: list[AfterCommit]) -> None:
    for callback in callbacks:
        _run_callback(callback)


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
_owned: WeakSet[Connection] = WeakSet()
_watched: WeakSet[Dialect] = WeakSet()
# Соединения пользователя, чьи колбэки ждут опроса в этом event loop (_deliver_later).
_polling: WeakKeyDictionary[asyncio.AbstractEventLoop, set[_Pending]] = WeakKeyDictionary()


def _session_pending(session: Session) -> _Pending:
    pending = _sessions.get(session)
    if pending is not None:
        return pending
    pending = _sessions[session] = _Pending()

    def on_create(_session: Session, transaction: SessionTransaction) -> None:
        if transaction.nested:
            pending.savepoint_started()

    def on_commit(sess: Session) -> None:
        # after_commit сессии срабатывает после COMMIT драйвера, а также на
        # release savepoint'а, пока он ещё текущий.
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


def _settle(pending: _Pending) -> None:
    """Разобрать транзакции, чей COMMIT завершился: при успехе вызвать колбэки.

    Порядок commit'ов сохраняется: пока самый старый COMMIT в пути, более
    новые не разбираются.
    """
    while pending.commits and not pending.commits[0].in_flight:
        commit = pending.commits.pop(0)
        if not commit.failed:
            _run_callbacks(commit.callbacks)


def _on_engine_error(context: ExceptionContext) -> None:
    # До этого события корневая транзакция с упавшим COMMIT ещё активна, после —
    # неактивна при любом исходе: отличить ошибку COMMIT можно только здесь.
    conn = context.connection
    pending = None if conn is None else _connections.get(conn)
    if pending is None:
        return
    for commit in pending.commits:
        if commit.transaction.is_active:
            commit.failed = True


def _watch_errors(engine: Engine) -> None:
    # handle_error слушается на диалекте движка; движки из execution_options()
    # делят диалект с родителем, поэтому слушатель ставится один раз на диалект.
    dialect = engine.dialect
    if dialect in _watched:
        return
    event.listen(engine, "handle_error", _on_engine_error)
    _watched.add(dialect)


def _deliver_later(pending: _Pending) -> None:
    """Опрашивать соединение пользователя, пока его COMMIT не завершится.

    Первые миллисекунды — на каждом проходе event loop (COMMIT обычно короче),
    затем с растущей паузой до :data:`_MAX_POLL_SECONDS`. Раньше опроса
    колбэки доставят начало следующей транзакции соединения и вызовы
    :func:`after_commit` / :func:`after_commit_pending`.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # вне event loop колбэки доставят следующие обращения к соединению
    started = loop.time()
    polled = _polling.setdefault(loop, set())
    polled.add(pending)

    def check() -> None:
        _settle(pending)
        if not pending.commits:
            polled.discard(pending)
            return
        elapsed = loop.time() - started
        if elapsed < _SPIN_SECONDS:
            _ = loop.call_soon(check)
        else:
            _ = loop.call_later(min(elapsed / 2, _MAX_POLL_SECONDS), check)

    _ = loop.call_soon(check)


def deliver_committed() -> None:
    """Сразу доставить колбэки соединений этого event loop, чей COMMIT завершился.

    Опрос :func:`_deliver_later` после первых миллисекунд идёт с паузами; тот,
    кто закрывается (Completer), вызывает функцию, чтобы после-коммитная работа
    уже закоммиченных транзакций была запланирована до флага закрытия. Колбэки
    COMMIT, ещё находящихся в пути, остаются опросу. Вне event loop — no-op.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    polled = _polling.get(loop)
    if not polled:
        return
    for pending in list(polled):
        _settle(pending)
        if not pending.commits:
            polled.discard(pending)


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

    def on_begin(_conn: Connection) -> None:
        # Новая транзакция: COMMIT предыдущей точно завершился.
        _settle(pending)

    def on_commit(sync: Connection) -> None:
        # Событие срабатывает ДО отправки COMMIT: колбэки ждут его исхода.
        callbacks = pending.take()
        transaction = sync.get_transaction()
        if not callbacks or transaction is None:
            return
        pending.commits.append(_Commit(transaction, callbacks))
        _watch_errors(sync.engine)
        if sync not in _owned:
            _deliver_later(pending)

    def on_rollback(_conn: Connection) -> None:
        pending.reset()

    event.listen(conn, "savepoint", on_savepoint)
    event.listen(conn, "release_savepoint", on_release)
    event.listen(conn, "rollback_savepoint", on_rollback_savepoint)
    event.listen(conn, "begin", on_begin)
    event.listen(conn, "commit", on_commit)
    event.listen(conn, "rollback", on_rollback)
    return pending


@contextlib.asynccontextmanager
async def begin_transaction(engine: AsyncEngine) -> AsyncGenerator[AsyncConnection]:
    """Своя транзакция ``engine.begin()`` с доставкой :func:`after_commit` по выходе.

    Колбэки, зарегистрированные на соединении, вызываются сразу после
    успешного выхода из ``engine.begin()``, когда COMMIT уже подтверждён, и до
    возврата управления вызывающему. Исключение (в том числе ошибка COMMIT)
    их отбрасывает.

    Args:
        engine: Движок БД.

    Yields:
        Соединение в открытой транзакции.
    """
    sync: Connection | None = None
    try:
        async with engine.begin() as conn:
            sync = conn.sync_connection
            if sync is not None:
                _owned.add(sync)
            yield conn
    except BaseException:
        if sync is not None:
            _ = _connections.pop(sync, None)
        raise
    finally:
        if sync is not None:
            _owned.discard(sync)
    pending = None if sync is None else _connections.pop(sync, None)
    if pending is not None:
        _settle(pending)


async def after_commit(target: AsyncSession | AsyncConnection, callback: AfterCommit) -> None:
    """Вызвать ``callback`` после commit внешней транзакции ``target``.

    Колбэк привязан к текущему уровню транзакции: откат savepoint'а (или всей
    транзакции) отбрасывает его, release savepoint'а передаёт родителю, а
    успешный COMMIT корневой транзакции вызывает — ровно один раз и только
    после того, как COMMIT подтверждён (ARCHITECTURE §11.1). Ошибка COMMIT
    колбэк отбрасывает. Колбэки вызываются в порядке регистрации; исключение
    колбэка логируется и не мешает остальным.

    * ``AsyncSession`` — событие ``after_commit`` сессии, после COMMIT
      драйвера. Сессия начинает транзакцию, если её ещё нет.
    * Соединение своей транзакции (:func:`begin_transaction`,
      :func:`own_transaction`) — сразу по выходе из неё.
    * ``AsyncConnection`` пользователя — событие ``commit`` соединения
      срабатывает до отправки COMMIT, поэтому колбэк ждёт его исхода и
      вызывается в ближайшем проходе event loop после COMMIT или при
      следующем обращении к соединению, но не внутри ``await conn.commit()``.
      Ошибку COMMIT библиотека узнаёт событием ``handle_error`` движка.

    Args:
        target: Сессия или соединение.
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
    pending = _connection_pending(sync)
    _settle(pending)
    pending.callbacks.append(callback)


async def after_commit_pending(
    target: AsyncSession | AsyncConnection, callback: AfterCommit
) -> bool:
    """Ждёт ли ``callback`` commit транзакции ``target``.

    ``True`` — колбэк зарегистрирован :func:`after_commit`, а записи его уровня
    транзакции ещё в силе: commit не было (или COMMIT ещё в пути), savepoint и
    транзакция не откатаны. После commit (колбэк вызван) и после отката —
    ``False``. По ответу вызывающий отличает «моя запись в этой транзакции ещё
    действует» от «её откатили, пишем заново».

    Для ``AsyncConnection`` сначала доставляет колбэки завершившихся COMMIT:
    после ``await conn.commit()`` колбэк вызван к моменту ответа.

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
        if pending is not None:
            _settle(pending)
    return pending is not None and pending.waiting(callback)


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
