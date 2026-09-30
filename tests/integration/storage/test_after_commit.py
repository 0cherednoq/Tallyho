"""``after_commit``: только после commit внешней транзакции, ровно один раз.

Одинаковые сценарии прогоняются для ``AsyncSession`` и ``AsyncConnection``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pytest
from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import registry

from tallyho.storage.tx import after_commit, resolve_connection
from tests.helpers.probe import ProbeColumns, committed_ids, create_probe, insert_id

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from sqlalchemy.ext.asyncio import AsyncEngine

Probe = Table[ProbeColumns]
Target = AsyncSession | AsyncConnection


class Calls:
    """Счётчик вызовов колбэков по именам."""

    def __init__(self) -> None:
        self.names: list[str] = []

    def callback(self, name: str) -> Callable[[], None]:
        def call() -> None:
            self.names.append(name)

        return call


@pytest.fixture
async def probe(engine: AsyncEngine, schema: str) -> Probe:
    return await create_probe(engine, schema)


@pytest.fixture(params=["session", "connection"])
async def target(engine: AsyncEngine, request: pytest.FixtureRequest) -> AsyncIterator[Target]:
    kind: Literal["session", "connection"] = request.param
    if kind == "session":
        async with AsyncSession(engine) as session:
            yield session
    else:
        async with engine.connect() as conn:
            yield conn


async def _write(target: Target, probe: Probe, value: int) -> None:
    await insert_id(await resolve_connection(target), probe, value)


async def test_called_once_after_commit(engine: AsyncEngine, target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    await after_commit(target, calls.callback("a"))

    assert calls.names == []
    await target.commit()

    assert calls.names == ["a"]
    assert await committed_ids(engine, probe) == [1]

    # Следующая транзакция той же сессии/соединения старые колбэки не вызывает.
    await _write(target, probe, 2)
    await target.commit()
    assert calls.names == ["a"]


async def test_not_called_on_rollback(target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    await after_commit(target, calls.callback("a"))

    await target.rollback()
    await _write(target, probe, 2)
    await target.commit()

    assert calls.names == []


async def test_callbacks_run_in_registration_order(target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    for name in ("a", "b", "c"):
        await after_commit(target, calls.callback(name))

    await target.commit()

    assert calls.names == ["a", "b", "c"]


async def test_savepoint_rollback_discards_its_callbacks(target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    await after_commit(target, calls.callback("outer"))
    nested = await target.begin_nested()
    await _write(target, probe, 2)
    await after_commit(target, calls.callback("inner"))

    await nested.rollback()
    assert calls.names == []
    await after_commit(target, calls.callback("after"))
    await target.commit()

    assert calls.names == ["outer", "after"]


async def test_released_savepoint_passes_callbacks_to_parent(target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    nested = await target.begin_nested()
    await after_commit(target, calls.callback("inner"))

    await nested.commit()
    assert calls.names == []
    await target.commit()

    assert calls.names == ["inner"]


async def test_outer_savepoint_rollback_discards_released_inner(
    target: Target, probe: Probe
) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    await after_commit(target, calls.callback("root"))
    outer = await target.begin_nested()
    inner = await target.begin_nested()
    await after_commit(target, calls.callback("inner"))
    await inner.commit()

    await outer.rollback()
    await target.commit()

    assert calls.names == ["root"]


async def test_savepoint_opened_before_first_registration(target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    nested = await target.begin_nested()
    await after_commit(target, calls.callback("inner"))

    await nested.rollback()
    await target.commit()

    assert calls.names == []


async def test_root_commit_with_open_savepoint_fires(target: Target, probe: Probe) -> None:
    calls = Calls()
    await _write(target, probe, 1)
    _ = await target.begin_nested()
    await after_commit(target, calls.callback("inner"))

    await target.commit()

    assert calls.names == ["inner"]


async def test_close_without_commit_discards(engine: AsyncEngine, probe: Probe) -> None:
    calls = Calls()
    async with AsyncSession(engine) as session:
        await _write(session, probe, 1)
        await after_commit(session, calls.callback("session"))
    async with engine.connect() as conn:
        await _write(conn, probe, 2)
        await after_commit(conn, calls.callback("connection"))

    assert calls.names == []
    assert await committed_ids(engine, probe) == []


async def test_session_registration_begins_transaction(engine: AsyncEngine) -> None:
    calls = Calls()
    async with AsyncSession(engine) as session:
        await after_commit(session, calls.callback("a"))
        assert session.in_transaction()
        await session.commit()

    assert calls.names == ["a"]


async def test_failing_callback_is_logged_and_others_run(
    engine: AsyncEngine, target: Target, probe: Probe, *, caplog: pytest.LogCaptureFixture
) -> None:
    calls = Calls()

    def broken() -> None:
        message = "boom"
        raise RuntimeError(message)

    await _write(target, probe, 1)
    await after_commit(target, broken)
    await after_commit(target, calls.callback("next"))

    with caplog.at_level(logging.ERROR, logger="tallyho.storage.tx"):
        await target.commit()

    assert calls.names == ["next"]
    assert await committed_ids(engine, probe) == [1]
    assert any(record.exc_info for record in caplog.records)


@dataclass
class ProbeRow:
    """ORM-объект пользователя поверх таблицы-зонда."""

    id: int


async def test_orm_flush_keeps_callbacks(engine: AsyncEngine, probe: Probe) -> None:
    calls = Calls()
    orm = registry()
    _ = orm.map_imperatively(ProbeRow, probe)
    try:
        async with AsyncSession(engine, autoflush=False) as session:
            await after_commit(session, calls.callback("a"))
            session.add(ProbeRow(1))
            # flush открывает и закрывает внутреннюю под-транзакцию сессии.
            await session.flush()
            await session.commit()
    finally:
        orm.dispose()

    assert calls.names == ["a"]
    assert await committed_ids(engine, probe) == [1]
