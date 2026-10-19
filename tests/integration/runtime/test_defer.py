"""item.defer в обёртке задачи: успешный выход, complete_in, порядок (RM2.2)."""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import delete, func, select
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import ItemState
from tallyho.protocols.broker import CancellationClassifier, DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import NOW, lease_row, open_completer, seed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from sqlalchemy import RowMapping

    from tallyho.engine.completer import Completer, ItemRef
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

DELAY = timedelta(minutes=5)


@dataclass
class Broker(Runtime, CancellationClassifier):
    """Брокер теста: считает запросы вердикта и отмены."""

    verdict: Verdict = Verdict.FINAL
    cancels: bool = False
    asked: int = 0

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        self.asked += 1
        return self.verdict

    @override
    def is_cancelled(self, exc: BaseException) -> bool:
        return self.cancels

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)


def _runtime(completer: Completer, broker: Broker | None = None) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=broker or Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=timedelta(seconds=20),
        defer_default_delay=DELAY,
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


async def _item(env: Env, item_id: UUID) -> RowMapping:
    table = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(table).where(table.c.id == item_id))).mappings().one()


async def _outbox(env: Env, item_id: UUID) -> RowMapping | None:
    outbox = env.tables.outbox
    async with env.connection() as conn:
        result = await conn.execute(select(outbox).where(outbox.c.item_id == item_id))
        return result.mappings().one_or_none()


async def test_defer_is_a_successful_outcome(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker()
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> str:
            await asyncio.sleep(0)
            item.defer(reason="нет кредитов")
            return "deferred"

        # Брокеру — успех: вердикт о ретрае не спрашивают, исключения нет.
        assert await _runtime(completer, broker).wrap(task)(_th=_marker(ref)) == "deferred"

    assert broker.asked == 0
    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.ACTIVE
    assert row["attempt"] == 0
    assert row["defers"] == 1
    assert row["generation"] == 1
    # Попытка не ушла в DLQ и не записала ошибку.
    assert await env.count(env.tables.item_mark) == 0
    assert await lease_row(env, ref.id) is None
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == "нет кредитов"
    assert entry["available_at"] == NOW + DELAY
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.error, counters.pending) == (0, 0, 1)


async def test_defer_in_user_transaction_is_atomic(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            # env.transaction коммитит на выходе: домен и defer — один commit.
            async with env.transaction() as conn:
                item.defer(reason="нет кредитов", delay=timedelta(minutes=1))
                await item.complete_in(conn)

        await _runtime(completer).wrap(task)(_th=_marker(ref))
    # Выход open_completer дождался после-коммитной транзакции (lease, дельты).

    row = await _item(env, ref.id)
    assert row["defers"] == 1
    assert ItemState(row["state"]) is ItemState.ACTIVE
    assert await lease_row(env, ref.id) is None
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == "нет кредитов"


async def test_defer_in_user_transaction_rolls_back_together(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with env.connection() as conn:
                trans = await conn.begin()
                await insert_id(conn, probe, 1)
                item.defer(reason="нет кредитов", delay=timedelta(minutes=1))
                await item.complete_in(conn)
                await trans.rollback()

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    # Домен и defer откатились вместе: доменной строки нет.
    assert await committed_ids(env.engine, probe) == []


async def test_ok_after_defer_is_configuration_error(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            item.defer(reason="нет кредитов")
            item.ok("поздно")

        with pytest.raises(ConfigurationError, match="после defer"):
            await _runtime(completer).wrap(task)(_th=_marker(ref))


async def test_deferred_item_can_finish_later(env: Env) -> None:
    # Отложенный Item позже доходит и завершается: батч не завис.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def defer_once(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            item.defer(reason="нет кредитов", delay=timedelta(minutes=1))

        await _runtime(completer).wrap(defer_once)(_th=_marker(ref))
        # Имитация новой отправки relay: убрать запись outbox, затем выполнить снова.
        outbox = env.tables.outbox
        async with env.transaction() as conn:
            _ = await conn.execute(delete(outbox).where(outbox.c.item_id == ref.id))

        async def ok_now(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            item.ok("готово")

        await _runtime(completer).wrap(ok_now)(_th=_marker(ref))

    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.OK
    assert row["defers"] == 1
    assert row["attempt"] == 0
    async with env.connection() as conn:
        pending = await conn.scalar(
            select(func.count())
            .select_from(env.tables.outbox)
            .where(env.tables.outbox.c.batch_id == seeded.batch_id)
        )
    assert pending == 0
