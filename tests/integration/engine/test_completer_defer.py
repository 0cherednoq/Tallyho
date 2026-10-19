"""Completer defer: отложение Item без траты попытки (RM2.2, UC-15)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select, text, update

from tallyho.engine.operations import Operations
from tallyho.model.states import ItemState, OutboxKind
from tallyho.protocols.clock import SystemClock
from tests.integration.engine.completer_env import (
    NOW,
    RecordingRelay,
    lease_row,
    open_completer,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy import RowMapping

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

REASON = "нет кредитов"


async def _item(env: Env, item_id: UUID) -> RowMapping:
    item = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(item).where(item.c.id == item_id))).mappings().one()


async def _outbox(env: Env, item_id: UUID) -> RowMapping | None:
    outbox = env.tables.outbox
    async with env.connection() as conn:
        result = await conn.execute(select(outbox).where(outbox.c.item_id == item_id))
        return result.mappings().one_or_none()


async def _window_rows(env: Env, batch_id: UUID) -> int:
    window = env.tables.window
    async with env.connection() as conn:
        return int(
            await conn.scalar(
                select(func.count()).select_from(window).where(window.c.batch_id == batch_id)
            )
            or 0
        )


async def test_defer_returns_item_without_spending_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    before = await _item(env, ref.id)
    relay = RecordingRelay()
    async with open_completer(env, relay=relay) as completer:
        claim = await completer.claim(ref)
        assert claim.run
        assert await completer.defer(
            ref, reason=REASON, delay=timedelta(minutes=5), attempt=claim.attempt
        )
        assert completer.held == frozenset()

    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.ACTIVE
    assert row["attempt"] == before["attempt"]
    assert row["defers"] == 1
    assert row["first_deferred_at"] == NOW
    assert row["generation"] == before["generation"] + 1
    assert row["finished_at"] is None
    # lease отдан, Item снова в outbox со сроком и причиной.
    assert await lease_row(env, ref.id) is None
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == REASON
    assert entry["available_at"] == NOW + timedelta(minutes=5)
    assert OutboxKind(entry["kind"]) is OutboxKind.ITEM
    # dispatched уменьшился: relay увеличит его снова при следующей отправке.
    counters = await env.counters(seeded.batch_id)
    assert counters.dispatched == 0


async def test_defer_until_sets_absolute_deadline(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    moment = NOW + timedelta(hours=2)
    async with open_completer(env) as completer:
        claim = await completer.claim(ref)
        assert await completer.defer(ref, reason=REASON, until=moment, attempt=claim.attempt)
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["available_at"] == moment


async def test_second_defer_bumps_counters_again(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    outbox = env.tables.outbox
    delay = timedelta(minutes=1)
    async with open_completer(env) as completer:
        first = await completer.claim(ref)
        assert await completer.defer(ref, reason=REASON, delay=delay, attempt=first.attempt)
        # Имитация новой отправки relay: запись outbox убрана, lease взят снова.
        async with env.transaction() as conn:
            _ = await conn.execute(delete(outbox).where(outbox.c.item_id == ref.id))
        second = await completer.claim(ref)
        assert await completer.defer(ref, reason="снова", delay=delay, attempt=second.attempt)
    row = await _item(env, ref.id)
    assert row["defers"] == 2
    assert row["generation"] == 2
    # first_deferred_at ставится один раз — остаётся от первого отложения.
    assert row["first_deferred_at"] == NOW


async def test_pause_after_defer_keeps_deadline(env: Env) -> None:
    # defer, затем pause: срок записи переживает парковку паузой (RM1.2 + RM2.2).
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    moment = NOW + timedelta(minutes=30)
    async with open_completer(env) as completer:
        claim = await completer.claim(ref)
        assert await completer.defer(ref, reason=REASON, until=moment, attempt=claim.attempt)
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        # pause поддерева не трогает отложенные записи (defer_reason IS NOT NULL).
        _ = await conn.execute(
            update(outbox)
            .where(outbox.c.batch_id == seeded.batch_id, outbox.c.defer_reason.is_(None))
            .values(available_at=text("'infinity'"))
        )
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["available_at"] == moment
    assert entry["defer_reason"] == REASON


async def test_defer_frees_window_and_unparks_waiting(env: Env) -> None:
    seeded = await seed(env, 2, kind="limited")
    await set_batch(env, seeded.batch_id, max_in_flight=1)
    running, waiting = seeded.refs
    window = env.tables.window
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        # running занимает единственное место окна; waiting ждёт в outbox (∞).
        _ = await conn.execute(insert(window).values(item_id=running.id, batch_id=seeded.batch_id))
        _ = await conn.execute(
            insert(outbox).values(
                id=waiting.id,
                kind=int(OutboxKind.ITEM),
                batch_id=seeded.batch_id,
                item_id=waiting.id,
                task_name="send",
                available_at=text("'infinity'"),
            )
        )
    relay = RecordingRelay()
    async with open_completer(env, relay=relay) as completer:
        claim = await completer.claim(running)
        assert await completer.defer(
            running, reason=REASON, delay=timedelta(minutes=1), attempt=claim.attempt
        )

    # Место окна освободилось, ждавшая запись распаркована в начало очереди.
    assert await _window_rows(env, seeded.batch_id) == 0
    outbox = env.tables.outbox
    async with env.connection() as conn:
        ready = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.item_id == waiting.id, outbox.c.available_at <= NOW)
        )
    assert ready == 1
    assert seeded.batch_id in [b for call in relay.calls for b in call]


async def test_cancel_after_defer_cancels_item(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        claim = await completer.claim(ref)
        assert await completer.defer(
            ref, reason=REASON, delay=timedelta(minutes=1), attempt=claim.attempt
        )
    operations = Operations(tables=env.tables, clock=SystemClock())
    async with env.transaction() as conn:
        _ = await operations.cancel(conn, seeded.batch_id)
    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.CANCELLED
    # Отложенная запись outbox убрана отменой.
    assert await _outbox(env, ref.id) is None
