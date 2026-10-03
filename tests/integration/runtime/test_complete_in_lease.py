"""``item.complete_in`` и попытка, потерявшая Item (ARCHITECTURE UC-08, I-04).

Пока задача работала, её Item завершил sweeper, отменили или перехватил другой
исполнитель. Доменная строка такой попытки не должна закоммититься, а обёртка
``th.tracked`` не должна ни писать итог, ни отдавать задачу в ретрай брокера.
"""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho.engine.completer import CompleterSettings, FinishResult
from tallyho.engine.spawn import TreeCache
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.errors import ConfigurationError, LeaseLostError
from tallyho.model.states import ItemState, ResultClass
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tallyho.storage.tx import resolve_connection
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    SETTINGS,
    Finalized,
    MovableClock,
    lease_row,
    open_completer,
    schema_engine,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from uuid import UUID

    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.engine.completer import Completer, ItemRef
    from tests.helpers.probe import ProbeColumns
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

LATER = NOW + SETTINGS.lease_ttl + timedelta(seconds=1)
"""Момент, когда lease, взятый в ``NOW``, уже истёк."""
OTHER = CompleterSettings(worker_id="worker-2", slot=COMPLETER_SLOT + 1)


class TaskFailedError(RuntimeError):
    """Ошибка пользовательской задачи в тесте."""


@dataclass
class Broker(Runtime):
    """Брокер теста: считает, сколько раз обёртка спросила вердикт."""

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
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


@pytest.fixture
async def probe(env: Env) -> Table[ProbeColumns]:
    """Доменная таблица пользователя: одна строка на выполненный эффект задачи."""
    return await create_probe(env.engine, env.schema)


@pytest.fixture
async def thief(env: Env) -> AsyncGenerator[Completer]:
    """Completer другого воркера, для которого lease из ``NOW`` уже истёк."""
    async with open_completer(env, clock=MovableClock(LATER), settings=OTHER) as completer:
        yield completer


async def _deliver(env: Env, probe: Table[ProbeColumns], value: int) -> None:
    """Тело задачи: доменная строка и итог Item — один commit пользователя."""
    async with env.transaction() as conn:
        await insert_id(conn, probe, value)
        item.ok("sent")
        await item.complete_in(conn)


async def _sweep(env: Env) -> None:
    """Проход sweeper после истечения lease: попытки исчерпаны, Item завершается."""
    sweeper = Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=MovableClock(LATER),
        finalizer=Finalized(),
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )
    assert await sweeper.expire_leases() == 1


async def _item(env: Env, item_id: UUID) -> tuple[ItemState, str | None, int]:
    """``(state, label, attempt)`` Item."""
    table = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(table.c.state, table.c.label, table.c.attempt).where(table.c.id == item_id)
            )
        ).one()
    return ItemState(row[0]), row[1], int(row[2])


async def _owner(env: Env, item_id: UUID) -> tuple[str, int] | None:
    """``(worker_id, attempt)`` lease Item."""
    lease = await lease_row(env, item_id)
    return None if lease is None else (lease["worker_id"], lease["attempt"])


@pytest.mark.parametrize(
    ("cancel", "expected"),
    [
        (False, (ItemState.ERROR, "lease_expired", 0)),
        (True, (ItemState.CANCELLED, "cancelled", 0)),
    ],
    ids=["lease_expired", "cancelled"],
)
async def test_complete_in_raises_when_item_was_finished_without_the_task(
    env: Env, probe: Table[ProbeColumns], *, cancel: bool, expected: tuple[ItemState, str, int]
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> str:
            if cancel:
                await set_batch(env, ref.batch_id, cancel_requested_at=NOW)
            await _sweep(env)
            before = await env.counters(ref.batch_id)
            with pytest.raises(LeaseLostError) as caught:
                await _deliver(env, probe, 1)
            assert caught.value.item_id == ref.id
            assert await env.counters(ref.batch_id) == before
            return "returned"

        assert await _runtime(completer).wrap(task)(_th=_marker(ref)) == "returned"

    assert await committed_ids(env.engine, probe) == []
    assert await _item(env, ref.id) == expected
    counters = await env.counters(ref.batch_id)
    assert (counters.ok, counters.error + counters.cancelled, counters.pending) == (0, 1, 0)
    assert await env.count(env.tables.counter_delta) == 0


async def test_stolen_lease_first_executor_cannot_finish_item(
    env: Env, probe: Table[ProbeColumns], thief: Completer
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            stolen = await thief.claim(ref)
            assert (stolen.run, stolen.attempt) == (True, 1)
            with pytest.raises(LeaseLostError):
                await _deliver(env, probe, 1)

        await _runtime(completer).wrap(task)(_th=_marker(ref))

        # Попытка, поймавшая LeaseLostError и вернувшаяся обычным образом, итог не пишет.
        assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)
        assert await _owner(env, ref.id) == (OTHER.worker_id, 1)
        assert await committed_ids(env.engine, probe) == []

        async with env.transaction() as conn:
            await insert_id(conn, probe, 2)
            value = FinishResult(result_class=ResultClass.OK, label="second")
            assert await thief.complete_in(conn, ref, value, attempt=1)

    assert await committed_ids(env.engine, probe) == [2]
    assert await _item(env, ref.id) == (ItemState.OK, "second", 1)
    assert (await env.counters(ref.batch_id)).ok == 1


@pytest.mark.parametrize("verdict", [Verdict.RETRY, Verdict.FINAL])
@pytest.mark.parametrize("stolen", [False, True], ids=["swept", "stolen"])
async def test_uncaught_lease_lost_ends_attempt_without_retry_or_error(
    env: Env,
    probe: Table[ProbeColumns],
    thief: Completer,
    *,
    caplog: pytest.LogCaptureFixture,
    verdict: Verdict,
    stolen: bool,
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker(verdict)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> str:
            if stolen:
                assert (await thief.claim(ref)).run
            else:
                await _sweep(env)
            await _deliver(env, probe, 1)
            return "unreachable"

        with caplog.at_level("INFO", logger="tallyho.runtime.tracked"):
            # Брокер получает успех без результата, как при DUPLICATE и TERMINAL.
            assert await _runtime(completer, broker).wrap(task)(_th=_marker(ref)) is None

    assert broker.asked == 0
    assert "потеряла lease" in caplog.text
    assert await committed_ids(env.engine, probe) == []
    counters = await env.counters(ref.batch_id)
    if stolen:
        # Ни error("exhausted"), ни release: Item и lease остались у второго исполнителя.
        assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)
        assert await _owner(env, ref.id) == (OTHER.worker_id, 1)
        assert (counters.error, counters.pending) == (0, 1)
    else:
        assert await _item(env, ref.id) == (ItemState.ERROR, "lease_expired", 0)
        assert (counters.error, counters.pending) == (1, 0)
        assert await env.count(env.tables.item_mark) == 1


@pytest.mark.parametrize("verdict", [Verdict.RETRY, Verdict.FINAL])
async def test_other_exception_after_lost_lease_is_reraised_without_writes(
    env: Env, probe: Table[ProbeColumns], thief: Completer, *, verdict: Verdict
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker(verdict)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            assert (await thief.claim(ref)).run
            try:
                await _deliver(env, probe, 1)
            except LeaseLostError as exc:
                raise TaskFailedError from exc

        with pytest.raises(TaskFailedError):
            await _runtime(completer, broker).wrap(task)(_th=_marker(ref))

    assert broker.asked == 0
    assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)
    assert await _owner(env, ref.id) == (OTHER.worker_id, 1)


async def test_cancelling_wrapper_after_lost_lease_keeps_foreign_lease(
    env: Env, probe: Table[ProbeColumns], thief: Completer
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    waiting = asyncio.Event()
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            assert (await thief.claim(ref)).run
            with pytest.raises(LeaseLostError):
                await _deliver(env, probe, 1)
            waiting.set()
            await asyncio.Event().wait()

        async def invoke() -> None:
            await _runtime(completer).wrap(task)(_th=_marker(ref))

        running: asyncio.Task[None] = asyncio.create_task(invoke())
        await waiting.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

    assert await _item(env, ref.id) == (ItemState.ACTIVE, None, 1)
    assert await _owner(env, ref.id) == (OTHER.worker_id, 1)


async def test_lease_lost_raised_by_user_code_is_an_ordinary_failure(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker(Verdict.FINAL)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise LeaseLostError(ref.id)

        with pytest.raises(LeaseLostError):
            await _runtime(completer, broker).wrap(task)(_th=_marker(ref))

    assert broker.asked == 1
    assert await _item(env, ref.id) == (ItemState.ERROR, "exhausted", 0)


# --- повторный вызов в той же попытке ---------------------------------------------------


async def test_repeated_call_in_same_transaction_is_noop(
    env: Env, probe: Table[ProbeColumns]
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with AsyncSession(schema_engine(env)) as session:
                await insert_id(await resolve_connection(session), probe, 1)
                item.ok("first")
                await item.complete_in(session)
                item.error("second")
                # Та же транзакция, переданная соединением: первая запись в силе.
                await item.complete_in(await session.connection())
                await session.commit()
            # После commit — тоже ничего: Item завершён этой попыткой.
            async with env.transaction() as conn:
                await item.complete_in(conn)

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert await committed_ids(env.engine, probe) == [1]
    assert await _item(env, ref.id) == (ItemState.OK, "first", 0)
    counters = await env.counters(ref.batch_id)
    assert (counters.ok, counters.error, counters.pending) == (1, 0, 0)


async def test_repeated_call_right_after_connection_commit_is_noop(
    env: Env, probe: Table[ProbeColumns]
) -> None:
    """Fix-16: колбэк AsyncConnection вызывается после COMMIT, а не внутри commit()."""
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    outcomes: list[str] = []
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with env.connection() as conn:
                await insert_id(conn, probe, 1)
                item.ok("first")
                await item.complete_in(conn)
                await conn.commit()
                # Без прохода event loop: колбэк COMMIT ещё не доставлен опросом.
                item.error("second")
                try:
                    await item.complete_in(conn)
                except LeaseLostError:
                    outcomes.append("lease lost")
                else:
                    outcomes.append("noop")
                await conn.rollback()

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert outcomes == ["noop"]
    assert await committed_ids(env.engine, probe) == [1]
    assert await _item(env, ref.id) == (ItemState.OK, "first", 0)


async def test_repeated_call_in_another_open_transaction_is_rejected(
    env: Env, probe: Table[ProbeColumns]
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with env.transaction() as first:
                await insert_id(first, probe, 1)
                await item.complete_in(first)
                async with env.connection() as second:
                    await insert_id(second, probe, 2)
                    with pytest.raises(ConfigurationError, match="одной транзакции"):
                        await item.complete_in(second)

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert await committed_ids(env.engine, probe) == [1]
    assert await _item(env, ref.id) == (ItemState.OK, "ok", 0)


@pytest.mark.parametrize("kind", ["session", "connection"])
@pytest.mark.parametrize("scope", ["savepoint", "transaction"])
async def test_call_after_rollback_writes_again(
    env: Env, probe: Table[ProbeColumns], *, kind: str, scope: str
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def attempt_in(target: AsyncSession | AsyncConnection) -> None:
            undo = await target.begin_nested() if scope == "savepoint" else target
            await insert_id(await resolve_connection(target), probe, 1)
            item.ok("discarded")
            await item.complete_in(target)
            await undo.rollback()
            # Запись отменена вместе с удалением lease: проверка и CAS — заново.
            await insert_id(await resolve_connection(target), probe, 2)
            item.ok("kept")
            await item.complete_in(target)
            await target.commit()

        async def task(**_kwargs: object) -> None:
            if kind == "session":
                async with AsyncSession(schema_engine(env)) as session:
                    await attempt_in(session)
            else:
                async with env.connection() as conn:
                    await attempt_in(conn)

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert await committed_ids(env.engine, probe) == [2]
    assert await _item(env, ref.id) == (ItemState.OK, "kept", 0)
    assert (await env.counters(ref.batch_id)).ok == 1


async def test_released_savepoint_keeps_first_completion(
    env: Env, probe: Table[ProbeColumns]
) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with AsyncSession(schema_engine(env)) as session:
                nested = await session.begin_nested()
                await insert_id(await resolve_connection(session), probe, 1)
                item.ok("first")
                await item.complete_in(session)
                await nested.commit()
                item.ok("second")
                await item.complete_in(session)
                await session.commit()

        await _runtime(completer).wrap(task)(_th=_marker(ref))

    assert await committed_ids(env.engine, probe) == [1]
    assert await _item(env, ref.id) == (ItemState.OK, "first", 0)
    assert (await env.counters(ref.batch_id)).ok == 1
