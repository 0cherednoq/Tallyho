"""Продюсер: seal (§6.1) и полный путь UC-01 в одной транзакции."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import update
from typing_extensions import override

from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.model.calls import TaskCall
from tallyho.model.errors import NotFoundError, SealError
from tallyho.model.states import BatchState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterTotals

if TYPE_CHECKING:
    from tests.integration.engine.conftest import Env

KIND = "invoices"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
LATER = datetime(2026, 9, 30, 13, 0, tzinfo=UTC)


class FixedClock(SystemClock):
    """Часы с заданным «сейчас» в SQL."""

    def __init__(self, at: datetime) -> None:
        self.at: datetime = at

    @override
    def now(self) -> datetime | None:
        return self.at


async def set_batch(env: Env, batch_id: UUID, **values: object) -> None:
    async with env.transaction() as conn:
        batch = env.tables.batch
        _ = await conn.execute(update(batch).where(batch.c.id == batch_id).values(values))


async def test_seal_open_batch(env: Env) -> None:
    async with env.transaction() as conn:
        root = await replace(env.producer, clock=FixedClock(NOW)).create_root(
            conn, RootSpec(kind=KIND)
        )
    producer = replace(env.producer, clock=FixedClock(LATER))
    async with env.transaction() as conn:
        assert await producer.seal(conn, root.id)
        # Повторный seal в той же транзакции — без изменений.
        assert not await producer.seal(conn, root.id)
    row = await env.batch(root.id)
    assert row["state"] == BatchState.SEALED
    assert row["updated_at"] == LATER
    assert row["created_at"] == NOW
    async with env.transaction() as conn:
        assert not await producer.seal(conn, root.id)
        with pytest.raises(SealError):
            _ = await producer.add_items(conn, root.id, [TaskCall(task_name="t")])


async def test_seal_of_finalized_batch_is_noop(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
    await set_batch(env, root.id, state=BatchState.SUCCEEDED)
    async with env.transaction() as conn:
        assert not await env.producer.seal(conn, root.id)
    assert (await env.batch(root.id))["state"] == BatchState.SUCCEEDED


async def test_seal_of_stage_is_error(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        # Источник закрывает продюсер, этап — нет.
        assert await env.producer.seal(conn, pages.id)
        with pytest.raises(SealError, match="fed_by"):
            _ = await env.producer.seal(conn, cards.id)
    assert (await env.batch(cards.id))["state"] == BatchState.OPEN


@pytest.mark.parametrize("state", [BatchState.OPEN, BatchState.CANCELLED])
async def test_seal_of_cancelled_batch_is_error(env: Env, state: BatchState) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND))
    await set_batch(env, root.id, state=state, cancel_requested_at=NOW)
    async with env.transaction() as conn:
        with pytest.raises(SealError, match="отменяемого"):
            _ = await env.producer.seal(conn, root.id)
    assert (await env.batch(root.id))["state"] == state


async def test_seal_missing_batch(env: Env) -> None:
    async with env.transaction() as conn:
        with pytest.raises(NotFoundError):
            _ = await env.producer.seal(conn, UUID(int=1))


async def test_uc01_in_one_transaction(env: Env) -> None:
    calls = [TaskCall(task_name="render", args=(n,)) for n in range(2_500)]
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=KIND, key="run:1"))
        result = await env.producer.add_items(conn, root.id, calls)
        assert await env.producer.seal(conn, root.id)
    assert result.found == 2_500
    assert (await env.batch(root.id))["state"] == BatchState.SEALED
    assert await env.counters(root.id) == CounterTotals(
        total=2_500, w_total=2_500, tree_total=2_500
    )
    assert await env.count(env.tables.outbox) == 2_500
