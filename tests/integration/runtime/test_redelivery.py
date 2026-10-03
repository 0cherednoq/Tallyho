"""Повторная доставка при живом lease не оставляет Item без исполнителя (Fix-7).

Сценарий хаоса A-CH-10: брокер повторно доставляет ту же джобу (``requeue_job``
flexiq, реап «мёртвого» воркера), пока исходное выполнение держит lease. Дубль
получает ``DUPLICATE`` и возвращает успех — брокер закрывает джобу. Исходное
выполнение потом падает с повторяемой ошибкой, но повторять её брокеру уже
нечем: вернуть Item в работу должен сам tallyho.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select, update

from tallyho.engine.spawn import TreeCache
from tallyho.model.states import ItemState
from tallyho.protocols.broker import Verdict
from tallyho.runtime import TaskRuntime
from tests.helpers.relay import RecordingDispatcher, relay_env
from tests.integration.engine.completer_env import (
    NOW,
    SETTINGS,
    RecordingRelay,
    TaskLimits,
    lease_row,
    open_completer,
    seed,
)
from tests.integration.runtime.test_tracked import FakeRuntime, TaskFailedError

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.engine.completer import Completer
    from tallyho.protocols.broker import RetryLimits
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

OTHER = replace(SETTINGS, worker_id="worker-2", slot=SETTINGS.slot + 2)


def _runtime(completer: Completer, verdict: Verdict) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=FakeRuntime(verdict),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=timedelta(seconds=20),
    )


async def _item(env: Env, item_id: UUID) -> tuple[ItemState, int]:
    item = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(select(item.c.state, item.c.attempt).where(item.c.id == item_id))
        ).one()
    return ItemState(row.state), row.attempt


async def _outbox_ids(env: Env) -> list[UUID]:
    outbox = env.tables.outbox
    async with env.connection() as conn:
        return list(await conn.scalars(select(outbox.c.item_id)))


@pytest.mark.parametrize("outcome", ["ok", "exhausted"])
async def test_retry_after_acknowledged_duplicate_completes_item(env: Env, outcome: str) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    # Возврат в outbox тратит попытку: у Item должна остаться хотя бы одна (UC-04).
    await _set_max_retries(env, ref.id, 1)
    marker = {"i": str(ref.id), "b": str(ref.batch_id)}
    entered = asyncio.Event()
    proceed = asyncio.Event()
    kicks = RecordingRelay()
    relay = relay_env(env, NOW)
    runs = 0

    async def task(**_kwargs: object) -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            entered.set()
            _ = await proceed.wait()
            raise TaskFailedError
        await asyncio.sleep(0)
        if outcome == "exhausted":
            raise TaskFailedError
        return "done"

    async with (
        open_completer(env, relay=kicks) as first,
        open_completer(env, settings=OTHER) as second,
    ):
        running = _runtime(first, Verdict.RETRY).wrap(task)

        async def invoke() -> None:
            _ = await running(_th=marker)

        original: asyncio.Task[None] = asyncio.create_task(invoke())
        _ = await entered.wait()

        # Брокер доставил ту же джобу ещё раз другому воркеру: живой lease → успех,
        # задача не вызывается, джоба у брокера закрыта.
        redelivered = _runtime(second, Verdict.FINAL).wrap(task)
        assert await redelivered(_th=marker) is None
        assert runs == 1

        # Исходное выполнение падает с повторяемой ошибкой; брокер её не повторит.
        proceed.set()
        with pytest.raises(TaskFailedError):
            await original

        assert await _item(env, ref.id) == (ItemState.ACTIVE, 1)
        assert await lease_row(env, ref.id) is None
        # Item не потерян: он снова в outbox, relay разбужен и отправляет его.
        assert await _outbox_ids(env) == [ref.id]
        assert kicks.calls == [[ref.batch_id]]
        relay.relay.kick(kicks.calls[0])
        assert await relay.relay.flush_kicked() == 1
        assert relay.dispatcher.ids == [ref.id]

        # Новая джоба выполняет Item до терминального итога.
        if outcome == "ok":
            assert await redelivered(_th=marker) == "done"
        else:
            with pytest.raises(TaskFailedError):
                await redelivered(_th=marker)

    expected = ItemState.OK if outcome == "ok" else ItemState.ERROR
    assert await _item(env, ref.id) == (expected, 1)
    assert runs == 2
    assert await _outbox_ids(env) == []
    counters = await env.counters(ref.batch_id)
    assert (counters.ok, counters.error) == ((1, 0) if outcome == "ok" else (0, 1))
    # Отправлен дважды, один раз возвращён в outbox: у брокера одна живая джоба.
    assert counters.dispatched == 1


async def _set_max_retries(env: Env, item_id: UUID, value: int) -> None:
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id == item_id).values(options={"max_retries": value})
        )


async def _item_outcome(env: Env, item_id: UUID) -> tuple[ItemState, int, str | None, int]:
    item = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(item.c.state, item.c.attempt, item.c.label, item.c.generation).where(
                    item.c.id == item_id
                )
            )
        ).one()
    return ItemState(row.state), row.attempt, row.label, row.generation


@pytest.mark.parametrize("source", ["options", "adapter"])
async def test_redelivery_loop_stops_when_item_attempts_are_exhausted(
    env: Env, source: str
) -> None:
    """Fix-19: каждая новая джоба доставляется дважды, а попытка падает с ``RETRY``.

    Брокер (flexiq после отказа PostgreSQL) дублирует каждую доставку: дубль
    закрывает джобу, ``release`` возвращает Item в outbox, новая джоба снова
    дублируется. Ретраи брокера при этом не копятся — у новой джобы счёт с нуля, —
    поэтому круг обрывает только лимит попыток Item (UC-04, D-012).
    """
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    limits: RetryLimits | None = None
    if source == "options":
        await _set_max_retries(env, ref.id, 1)
    else:
        limits = TaskLimits(1)
    kicks = RecordingRelay()
    relay = relay_env(env, NOW)
    entered = asyncio.Event()
    proceed = asyncio.Event()
    runs = 0

    async def task(**_kwargs: object) -> str:
        nonlocal runs
        runs += 1
        entered.set()
        _ = await proceed.wait()
        raise TaskFailedError

    async with (
        open_completer(env, relay=kicks, limits=limits) as first,
        open_completer(env, settings=OTHER, limits=limits) as second,
    ):
        original = _runtime(first, Verdict.RETRY).wrap(task)
        duplicate = _runtime(second, Verdict.RETRY).wrap(task)

        async def cycle(generation: int) -> None:
            entered.clear()
            proceed.clear()
            marker = {"i": str(ref.id), "b": str(ref.batch_id), "g": generation}

            async def invoke() -> None:
                _ = await original(_th=marker)

            running: asyncio.Task[None] = asyncio.create_task(invoke())
            _ = await entered.wait()
            # Дубль той же джобы упирается в живой lease: брокеру — успех.
            assert await duplicate(_th=marker) is None
            proceed.set()
            with pytest.raises(TaskFailedError):
                await running

        # Первый круг: попытка 0 меньше лимита 1 — Item снова в outbox, attempt 1.
        await cycle(0)
        assert await _item_outcome(env, ref.id) == (ItemState.ACTIVE, 1, None, 1)
        assert await _outbox_ids(env) == [ref.id]
        relay.relay.kick([ref.batch_id])
        assert await relay.relay.flush_kicked() == 1

        # Второй круг: попыток не осталось — Item завершён, в outbox не вернулся.
        await cycle(1)

    assert runs == 2
    assert await _item_outcome(env, ref.id) == (ItemState.ERROR, 1, "exhausted", 1)
    assert await _outbox_ids(env) == []
    assert await lease_row(env, ref.id) is None
    assert kicks.calls == [[ref.batch_id]]
    counters = await env.counters(ref.batch_id)
    assert (counters.ok, counters.error) == (0, 1)
    assert counters.dispatched == 1
