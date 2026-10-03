"""Отказ чтения дерева батча после claim (ARCHITECTURE §10)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, cast, final

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from tallyho.model.errors import CompleterError, NotFoundError
from tallyho.model.states import ItemState
from tallyho.runtime import TaskRuntime
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import open_completer, seed
from tests.integration.runtime.test_tracked import FakeRuntime

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.engine.spawn import TreeCache, TreeSnapshot
    from tallyho.storage.tables import Tables
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


@final
class FailingTreeCache:
    """Кэш дерева без снимков, чтение которого падает так, как падает драйвер БД."""

    def __init__(self, error: BaseException) -> None:
        self.error = error

    def get(self, batch_id: UUID) -> TreeSnapshot | None:
        del batch_id
        return None

    async def load(self, conn: AsyncConnection, tables: Tables, batch_id: UUID) -> TreeSnapshot:
        del conn, tables, batch_id
        await asyncio.sleep(0)
        raise self.error


async def _item(env: Env, item_id: UUID) -> tuple[ItemState, int, int]:
    """``(state, attempt, строк th_lease)`` Item."""
    item = env.tables.item
    lease = env.tables.lease
    async with env.connection() as conn:
        row = (
            await conn.execute(select(item.c.state, item.c.attempt).where(item.c.id == item_id))
        ).one()
        leases = await conn.scalar(
            select(func.count()).select_from(lease).where(lease.c.item_id == item_id)
        )
    return ItemState(row[0]), int(row[1]), int(leases or 0)


@pytest.mark.parametrize(
    ("error", "raised"),
    [
        (OperationalError("SELECT", {}, ConnectionResetError("connection lost")), CompleterError),
        (NotFoundError("batch"), NotFoundError),
        # Отмена во время чтения: lease тоже отпускается, CancelledError не подменяется.
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_tree_load_failure_releases_lease_and_raises_tallyho_error(
    env: Env, error: BaseException, raised: type[BaseException]
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    called: list[int] = []
    async with open_completer(env) as completer:
        runtime = TaskRuntime(
            completer=completer,
            broker=FakeRuntime(),
            dispatcher=RecordingDispatcher(),
            tree_cache=cast("TreeCache", cast("object", FailingTreeCache(error))),
            heartbeat_every=timedelta(seconds=20),
        )

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            called.append(1)

        with pytest.raises(raised) as caught:
            await runtime.wrap(task)(_th={"i": str(ref.id), "b": str(ref.batch_id)})

    assert called == []
    if raised is CompleterError:
        # Исходная ошибка драйвера сохранена; адаптер повторит джобу по retry_on.
        assert caught.value.__cause__ is error
    # Lease отпущен как перед ретраем брокера: повтор не ждёт lease_ttl.
    assert await _item(env, ref.id) == (ItemState.ACTIVE, 1, 0)
