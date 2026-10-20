"""Итог попытки возвращаемым значением и ``complete_in(..., outcome)`` (RM8.2, UC-18)."""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.model.errors import ConfigurationError
from tallyho.model.outcomes import Defer, Error, Ok, Skip
from tallyho.model.states import ItemState
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
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
class Broker(Runtime):
    """Брокер теста: вердикт о ретрае задаёт тест."""

    verdict: Verdict = Verdict.FINAL
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


async def test_returned_ok_writes_label_and_result(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            return Ok("chunk", result={"checked": 50})

        # Возвращённый объект достаётся вызывающему без изменений.
        assert await _runtime(completer).wrap(task)(_th=_marker(ref)) == Ok(
            "chunk", result={"checked": 50}
        )

    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.OK
    assert row["label"] == "chunk"
    assert row["result"] == {"checked": 50}
    assert (await env.counters(ref.batch_id)).ok == 1


async def test_returned_skip_and_error_are_terminal_without_retry(env: Env) -> None:
    seeded = await seed(env, 2)
    skipped, failed = seeded.refs
    broker = Broker()
    async with open_completer(env) as completer:

        async def skip_task(**_kwargs: object) -> Skip:
            await asyncio.sleep(0)
            return Skip("duplicate")

        async def error_task(**_kwargs: object) -> Error:
            await asyncio.sleep(0)
            return Error("invalid_domain", detail={"code": 7})

        runtime = _runtime(completer, broker)
        _ = await runtime.wrap(skip_task)(_th=_marker(skipped))
        _ = await runtime.wrap(error_task)(_th=_marker(failed))

    # Error значением — итог, а не исключение: вердикт о ретрае не спрашивают.
    assert broker.asked == 0
    assert ItemState((await _item(env, skipped.id))["state"]) is ItemState.SKIP
    row = await _item(env, failed.id)
    assert ItemState(row["state"]) is ItemState.ERROR
    assert row["label"] == "invalid_domain"
    assert row["error"] == {"code": 7}
    counters = await env.counters(skipped.batch_id)
    assert (counters.skip, counters.error) == (1, 1)


async def test_returned_defer_does_not_spend_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker()
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Defer:
            await asyncio.sleep(0)
            return Defer(reason="no_accounts", delay=timedelta(minutes=1))

        _ = await _runtime(completer, broker).wrap(task)(_th=_marker(ref))

    assert broker.asked == 0
    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.ACTIVE
    assert (row["attempt"], row["defers"]) == (0, 1)
    assert await lease_row(env, ref.id) is None
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == "no_accounts"
    assert entry["available_at"] == NOW + timedelta(minutes=1)


async def test_returned_defer_without_delay_uses_default(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Defer:
            await asyncio.sleep(0)
            return Defer(reason="quota")

        _ = await _runtime(completer).wrap(task)(_th=_marker(ref))

    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["available_at"] == NOW + DELAY


async def test_returned_defer_until_sets_absolute_deadline(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    moment = NOW + timedelta(hours=2)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Defer:
            await asyncio.sleep(0)
            return Defer(reason="quota", until=moment)

        _ = await _runtime(completer).wrap(task)(_th=_marker(ref))

    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["available_at"] == moment


async def test_arbitrary_value_is_still_plain_ok(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> dict[str, int]:
            await asyncio.sleep(0)
            return {"checked": 50}

        assert await _runtime(completer).wrap(task)(_th=_marker(ref)) == {"checked": 50}

    row = await _item(env, ref.id)
    # Старая семантика: не-Outcome значение — успех с меткой по умолчанию.
    assert ItemState(row["state"]) is ItemState.OK
    assert row["label"] == "ok"
    assert row["result"] is None


async def test_complete_in_takes_outcome_in_user_transaction(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with env.transaction() as conn:
                await insert_id(conn, probe, 1)
                await item.complete_in(conn, Ok("chunk", result={"checked": 1}))

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert await committed_ids(env.engine, probe) == [1]
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.OK, "chunk")


async def test_complete_in_with_defer_rolls_back_together(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with env.connection() as conn:
                trans = await conn.begin()
                await insert_id(conn, probe, 1)
                await item.complete_in(conn, Defer(reason="no_accounts"))
                await trans.rollback()

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert await committed_ids(env.engine, probe) == []
    # Доменная запись откатилась вместе с записью итога. Сам итог остался
    # заданным, и отложение выполнила успешная ветка обёртки — ровно как
    # после отката транзакции с императивным item.defer (UC-08).
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["defers"]) == (ItemState.ACTIVE, 1)
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == "no_accounts"


@pytest.mark.parametrize(
    "outcome",
    [Ok("поздно"), Skip("поздно"), Error("поздно"), Defer(reason="поздно")],
)
async def test_imperative_outcome_and_returned_outcome_conflict(
    env: Env, outcome: Ok | Skip | Error | Defer
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok | Skip | Error | Defer:
            await asyncio.sleep(0)
            item.ok("первый")
            return outcome

        with pytest.raises(ConfigurationError, match="дважды"):
            _ = await _runtime(completer).wrap(task)(_th=_marker(ref))


async def test_returned_outcome_after_defer_conflicts(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            item.defer(reason="ждём")
            return Ok("поздно")

        with pytest.raises(ConfigurationError, match="дважды"):
            _ = await _runtime(completer).wrap(task)(_th=_marker(ref))


async def test_complete_in_outcome_conflicts_with_imperative_outcome(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with env.transaction() as conn:
                item.skip("первый")
                await item.complete_in(conn, Ok("второй"))

        with pytest.raises(ConfigurationError, match="дважды"):
            await _runtime(completer).wrap(task)(_th=_marker(ref))

    # Ни один из двух итогов не записан: конфликт — ошибка в коде, и попытка
    # завершается по вердикту брокера, как любое исключение задачи (UC-18).
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.ERROR, "exhausted")


async def test_committed_complete_in_wins_over_returned_outcome(env: Env) -> None:
    # Итог, уже записанный complete_in, возвращённым значением не переписывается:
    # Item терминален и этой попытке больше не принадлежит (UC-03). Возврат
    # обёртки здесь контрактом не является — он зависит от того, доставлен ли к
    # этому моменту после-коммитный колбэк соединения, — поэтому проверяем БД.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> Skip:
            async with env.transaction() as conn:
                await item.complete_in(conn)
            return Skip("второй")

        _ = await _runtime(completer).wrap(task)(_th=_marker(ref))

    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.OK, "ok")
    assert (await env.counters(ref.batch_id)).ok == 1
