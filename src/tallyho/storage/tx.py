"""Транзакции storage: чужие сессии, свои транзакции и их повтор.

Правила (DECISIONS D-004, ARCHITECTURE §7.3, §10, COUNTERS §3.6, P10):

* функции storage и engine принимают ``AsyncConnection``; ``AsyncSession``
  пользователя разворачивается на границе через :func:`resolve_connection`;
* commit/rollback чужой транзакции мы никогда не делаем;
* каждая своя транзакция ставит ``SET LOCAL lock_timeout`` и
  ``statement_timeout`` (:func:`own_transaction`), а :func:`run_transaction`
  повторяет её целиком на ``40001`` (serialization failure), ``40P01``
  (deadlock) и ``55P03`` (lock not available) с экспоненциальным backoff и
  джиттером.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Final, TypeVar, cast

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho.model.errors import ConcurrentModification

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = [
    "RETRYABLE_SQLSTATES",
    "RetryPolicy",
    "TxSettings",
    "is_retryable",
    "own_transaction",
    "resolve_connection",
    "run_transaction",
    "set_local_timeouts",
    "sqlstate_of",
]

T = TypeVar("T")

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
        _ = await sleep(policy.delay(retry, rng))
        retry += 1
