"""``after_commit`` срабатывает строго после COMMIT (Fix-16).

Колбэк сам проверяет отдельным синхронным соединением (psycopg), видна ли
строка его транзакции. До исправления колбэк ``AsyncConnection`` вызывался
событием ``commit``, то есть до отправки COMMIT, и строку не видел.
Ошибка COMMIT (отложенное ограничение уникальности) колбэк отбрасывает.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Literal, cast, final

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
    UniqueConstraint,
    create_engine,
    func,
    insert,
    select,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from tallyho.storage.tx import (
    after_commit,
    after_commit_pending,
    begin_transaction,
    deliver_committed,
    own_transaction,
)
from tests.helpers.after_commit import pause_commit_polling
from tests.helpers.probe import ProbeColumns, committed_ids, create_probe, insert_id

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

Probe = Table[ProbeColumns]
Target = AsyncSession | AsyncConnection
Write = Callable[[AsyncConnection, Target], Awaitable[None]]
Entry = Literal["session", "connection", "engine-begin", "begin", "own"]
ENTRIES: tuple[Entry, ...] = ("session", "connection", "engine-begin", "begin", "own")
_DELIVERY_TIMEOUT = 5.0


@pytest.fixture
async def probe(engine: AsyncEngine, schema: str) -> Probe:
    return await create_probe(engine, schema)


@pytest.fixture
async def deferred(engine: AsyncEngine, schema: str) -> Table[ProbeColumns]:
    """Таблица, уникальность которой проверяется только на COMMIT."""
    table = Table(
        "deferred",
        MetaData(schema=schema),
        Column("id", Integer()),
        UniqueConstraint("id", deferrable=True, initially="DEFERRED"),
    )
    async with engine.begin() as conn:
        await conn.run_sync(table.create)
    return cast("Table[ProbeColumns]", table)


async def _insert_duplicates(conn: AsyncConnection, table: Table[ProbeColumns]) -> None:
    _ = await conn.execute(insert(table), [{"id": 1}, {"id": 1}])


@pytest.fixture
def observer(postgres_dsn: str) -> Iterator[Engine]:
    """Синхронное соединение со стороны: видит только закоммиченное."""
    url = make_url(postgres_dsn).set(drivername="postgresql+psycopg")
    value = create_engine(url)
    try:
        yield value
    finally:
        value.dispose()


@final
class Seen:
    """Что колбэк увидел из другого соединения в момент вызова."""

    def __init__(self, observer: Engine, probe: Probe, value: int) -> None:
        self.observer = observer
        self.probe = probe
        self.value = value
        self.visible: list[bool] = []
        self.called = asyncio.Event()

    def __call__(self) -> None:
        query = select(func.count()).where(self.probe.c.id == self.value)
        with self.observer.connect() as conn:
            self.visible.append(conn.scalar(query) == 1)
        self.called.set()

    async def delivered(self) -> list[bool]:
        async with asyncio.timeout(_DELIVERY_TIMEOUT):
            _ = await self.called.wait()
        return self.visible


async def _commit_with(entry: Entry, engine: AsyncEngine, write: Write) -> None:
    if entry == "session":
        async with AsyncSession(engine) as session:
            await write(await session.connection(), session)
            await session.commit()
    elif entry == "connection":
        async with engine.connect() as conn:
            await write(conn, conn)
            await conn.commit()
    elif entry == "engine-begin":
        async with engine.begin() as conn:
            await write(conn, conn)
    elif entry == "begin":
        async with begin_transaction(engine) as conn:
            await write(conn, conn)
    else:
        async with own_transaction(engine) as conn:
            await write(conn, conn)


def _writer(probe: Probe, seen: Seen) -> Write:
    async def write(conn: AsyncConnection, target: Target) -> None:
        await insert_id(conn, probe, seen.value)
        await after_commit(target, seen)

    return write


@pytest.mark.parametrize("entry", ENTRIES)
async def test_callback_sees_committed_row(
    engine: AsyncEngine, probe: Probe, *, observer: Engine, entry: Entry
) -> None:
    seen = Seen(observer, probe, 1)

    await _commit_with(entry, engine, _writer(probe, seen))

    assert await seen.delivered() == [True]


@pytest.mark.parametrize("entry", ENTRIES)
async def test_callback_sees_committed_row_on_warm_pool(
    engine: AsyncEngine, probe: Probe, *, observer: Engine, entry: Entry
) -> None:
    # Прогретый пул: COMMIT уходит по уже открытому соединению без задержек.
    for value in range(1, 21):
        seen = Seen(observer, probe, value)
        await _commit_with(entry, engine, _writer(probe, seen))
        assert await seen.delivered() == [True], value


@pytest.mark.parametrize("entry", ["begin", "own"])
async def test_own_transaction_delivers_before_returning(
    engine: AsyncEngine, probe: Probe, *, observer: Engine, entry: Entry
) -> None:
    seen = Seen(observer, probe, 1)

    await _commit_with(entry, engine, _writer(probe, seen))

    assert seen.visible == [True]


@pytest.mark.parametrize("entry", ENTRIES)
async def test_failed_commit_drops_callbacks(
    engine: AsyncEngine, deferred: Probe, entry: Entry
) -> None:
    called: list[str] = []

    async def write(conn: AsyncConnection, target: Target) -> None:
        await _insert_duplicates(conn, deferred)
        await after_commit(target, lambda: called.append("failed"))

    with pytest.raises(IntegrityError):
        await _commit_with(entry, engine, write)
    await asyncio.sleep(0.05)  # дать опросу event loop шанс ошибочно вызвать колбэк

    assert called == []
    assert await committed_ids(engine, deferred) == []


async def test_failed_commit_then_next_commit_on_same_connection(
    engine: AsyncEngine, probe: Probe, deferred: Probe
) -> None:
    called: list[str] = []

    def failed() -> None:
        called.append("failed")

    async with engine.connect() as conn:
        await _insert_duplicates(conn, deferred)
        await after_commit(conn, failed)
        with pytest.raises(IntegrityError):
            await conn.commit()
        # Ошибку COMMIT видно до отката: колбэк больше не ждёт commit.
        assert not await after_commit_pending(conn, failed)
        await conn.rollback()

        await insert_id(conn, probe, 1)
        await after_commit(conn, lambda: called.append("next"))
        await conn.commit()
        assert not await after_commit_pending(conn, failed)

    assert called == ["next"]


async def test_next_transaction_delivers_previous_commit(engine: AsyncEngine, probe: Probe) -> None:
    called: list[str] = []
    async with engine.connect() as conn:
        await insert_id(conn, probe, 1)
        await after_commit(conn, lambda: called.append("a"))
        await conn.commit()
        # Начало новой транзакции на том же соединении: COMMIT предыдущей завершён.
        _ = await conn.execute(insert(probe).values(id=2))
        assert called == ["a"]
        await conn.rollback()


async def test_commit_in_flight_is_still_pending(engine: AsyncEngine, probe: Probe) -> None:
    states: list[bool] = []
    checked = 0
    async with engine.connect() as conn:
        await insert_id(conn, probe, 1)

        def callback() -> None:
            states.append(True)

        await after_commit(conn, callback)
        commit = asyncio.ensure_future(conn.commit())
        # Пока COMMIT в пути, колбэк ещё ждёт его исхода. Первая проверка идёт
        # всегда: шаг commit, отправивший COMMIT, ждёт ответа сервера.
        while not commit.done():
            await asyncio.sleep(0)
            if not commit.done() and not states:
                assert await after_commit_pending(conn, callback)
                checked += 1
        await commit
        # Опрос event loop доставляет колбэк с паузами (D-057), и под нагрузкой
        # он мог ещё не сработать. Обращение к соединению доставляет сразу: после
        # ``await conn.commit()`` колбэк вызван к моменту ответа, ровно один раз.
        assert not await after_commit_pending(conn, callback)
        assert states == [True]

    assert checked >= 1
    assert states == [True]


async def test_deliver_committed_runs_callbacks_of_paused_poll(
    engine: AsyncEngine, probe: Probe, monkeypatch: pytest.MonkeyPatch
) -> None:
    pause_commit_polling(monkeypatch)
    called: list[str] = []
    async with engine.connect() as conn:
        await insert_id(conn, probe, 1)
        await after_commit(conn, lambda: called.append("a"))
        await conn.commit()
        for _ in range(3):
            await asyncio.sleep(0)
        assert called == []  # опрос стоит, колбэк ещё не доставлен
        deliver_committed()
        assert called == ["a"]
        deliver_committed()
        assert called == ["a"]


def test_deliver_committed_outside_loop_is_noop() -> None:
    deliver_committed()
