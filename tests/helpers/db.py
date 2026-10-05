"""Хелперы интеграционных тестов для работы с PostgreSQL.

Каждый тест работает в своей схеме (``temporary_schema``), поэтому тесты не мешают
друг другу ни при случайном порядке (``pytest-randomly``), ни при параллельном запуске
(``pytest -n``), ни на общей БД из ``TALLYHO_TEST_DSN``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import event, text

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from sqlalchemy.engine import ExceptionContext
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = [
    "DEADLOCK_SQLSTATE",
    "SCHEMA_PREFIX",
    "DbError",
    "LockRow",
    "backend_pid",
    "blocked_by",
    "deadlock_count",
    "deadlocks",
    "held_locks",
    "record_db_errors",
    "schema_connection",
    "schema_exists",
    "schema_transaction",
    "temporary_schema",
    "unique_schema_name",
    "wait_blocked_by",
]

SCHEMA_PREFIX = "t_"
DEADLOCK_SQLSTATE = "40P01"


def unique_schema_name() -> str:
    """Имя схемы, уникальное для теста: префикс, xdist-воркер и случайный суффикс."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    return f"{SCHEMA_PREFIX}{worker}_{uuid.uuid4().hex[:12]}"


def _quoted(schema: str) -> str:
    # Имя генерируем сами, но всё равно не подставляем его в SQL без кавычек.
    return '"' + schema.replace('"', '""') + '"'


@contextlib.asynccontextmanager
async def temporary_schema(engine: AsyncEngine) -> AsyncGenerator[str]:
    """Создаёт пустую схему и удаляет её со всем содержимым на выходе."""
    schema = unique_schema_name()
    async with engine.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA {_quoted(schema)}"))
    try:
        yield schema
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {_quoted(schema)} CASCADE"))


@contextlib.asynccontextmanager
async def schema_transaction(engine: AsyncEngine, schema: str) -> AsyncGenerator[AsyncConnection]:
    """Транзакция, где таблицы без схемы попадают в ``schema``; commit на выходе."""
    async with engine.begin() as raw:
        yield await raw.execution_options(schema_translate_map={None: schema})


@contextlib.asynccontextmanager
async def schema_connection(engine: AsyncEngine, schema: str) -> AsyncGenerator[AsyncConnection]:
    """Соединение в ``schema`` без commit: незакоммиченное откатится при закрытии."""
    async with engine.connect() as raw:
        yield await raw.execution_options(schema_translate_map={None: schema})


async def schema_exists(conn: AsyncConnection, schema: str) -> bool:
    """Есть ли схема с таким именем в текущей БД."""
    found = await conn.scalar(
        text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :schema)"),
        {"schema": schema},
    )
    return bool(found)


async def deadlock_count(conn: AsyncConnection) -> int:
    """Счётчик дедлоков текущей БД из ``pg_stat_database.deadlocks``.

    Счётчик общий на БД, а backend, поймавший дедлок, публикует статистику с
    задержкой (до ~1 с; сразу — после ``pg_stat_force_next_flush()`` или при
    отключении). Поэтому им нельзя доказывать «в моём сценарии дедлоков не было»:
    в окно «до/после» попадает дедлок соседнего теста, в том числе намеренный и
    уже завершившийся. Для такой проверки есть :func:`record_db_errors`. Снимок
    статистики текущей транзакции сбрасывается здесь же.
    """
    await conn.execute(text("SELECT pg_stat_clear_snapshot()"))
    value = await conn.scalar(
        text("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
    )
    return int(value or 0)


@dataclass(frozen=True, slots=True)
class DbError:
    """Ошибка БД на соединении движка: SQLSTATE и запрос, на котором она возникла."""

    sqlstate: str | None
    statement: str | None


@contextlib.contextmanager
def record_db_errors(engine: AsyncEngine) -> Generator[list[DbError]]:
    """Записывает ошибки БД на всех соединениях ``engine``, пока открыт контекст.

    Событие ``handle_error`` срабатывает на каждой ошибке драйвера — и на той,
    которую библиотека потом повторила (``40P01`` в ``run_transaction``), и на
    той, что ушла пользователю. Чужие соединения той же БД сюда не попадают,
    поэтому проверка не зависит от соседних тестов. Движки, полученные через
    ``engine.execution_options(...)``, наследуют слушателя.
    """
    errors: list[DbError] = []

    def on_error(context: ExceptionContext) -> None:
        state = cast("object", getattr(context.original_exception, "sqlstate", None))
        errors.append(DbError(state if isinstance(state, str) else None, context.statement))

    event.listen(engine.sync_engine, "handle_error", on_error)
    try:
        yield errors
    finally:
        event.remove(engine.sync_engine, "handle_error", on_error)


def deadlocks(errors: list[DbError]) -> list[DbError]:
    """Только дедлоки (``40P01``) из записанных ошибок."""
    return [error for error in errors if error.sqlstate == DEADLOCK_SQLSTATE]


async def backend_pid(conn: AsyncConnection) -> int:
    """PID backend'а PostgreSQL, который обслуживает соединение."""
    return int(await conn.scalar(text("SELECT pg_backend_pid()")) or 0)


_BLOCKED_SQL = text("""
    SELECT count(*) FROM pg_locks l
    WHERE NOT l.granted AND :pid = ANY(pg_blocking_pids(l.pid))
""")


async def blocked_by(conn: AsyncConnection, pid: int) -> int:
    """Сколько ожидающих блокировок стоит в очереди за backend'ом ``pid`` прямо сейчас."""
    return int(await conn.scalar(_BLOCKED_SQL, {"pid": pid}) or 0)


async def wait_blocked_by(conn: AsyncConnection, pid: int, *, attempts: int = 1000) -> None:
    """Дождаться, пока какой-нибудь backend встанет в очередь за блокировкой ``pid``.

    ``conn`` — отдельное соединение наблюдателя: ни держатель блокировки, ни
    ожидающий. ``pg_locks`` читается заново при каждом запросе; опрос идёт раз
    в 10 мс, не дольше ``attempts`` раз.
    """
    for _ in range(attempts):
        if await blocked_by(conn, pid):
            return
        await asyncio.sleep(0.01)
    message = f"никто не ждёт блокировку backend'а {pid}"
    raise AssertionError(message)


@dataclass(frozen=True, slots=True)
class LockRow:
    """Строка ``pg_locks`` в удобном для assert'ов виде."""

    pid: int
    locktype: str
    relation: str | None
    mode: str
    granted: bool


_LOCKS_SQL = text("""
    SELECT l.pid, l.locktype, n.nspname || '.' || c.relname AS relation, l.mode, l.granted
    FROM pg_locks l
    LEFT JOIN pg_class c ON c.oid = l.relation
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE (l.database IS NULL
           OR l.database = (SELECT oid FROM pg_database WHERE datname = current_database()))
      AND l.pid IS NOT NULL
      AND (:include_own OR l.pid <> pg_backend_pid())
    ORDER BY l.pid, l.locktype, relation, l.mode
""")


async def held_locks(conn: AsyncConnection, *, include_own: bool = False) -> list[LockRow]:
    """Блокировки из ``pg_locks`` в текущей БД; по умолчанию — кроме своего backend'а.

    ``relation`` — имя отношения со схемой (``schema.table``), для блокировок
    не на отношениях (``transactionid``, ``virtualxid``, ``advisory``) — ``None``.
    Ожидающие блокировки тоже возвращаются, у них ``granted=False``.
    """
    rows = await conn.execute(_LOCKS_SQL, {"include_own": include_own})
    return [
        LockRow(
            pid=int(row.pid),
            locktype=str(row.locktype),
            relation=None if row.relation is None else str(row.relation),
            mode=str(row.mode),
            granted=bool(row.granted),
        )
        for row in rows
    ]
