"""Метка ``cancelled`` и счётчики батча согласованы на всех путях отмены (Fix-22).

Отмена Item идёт тремя путями: ``Operations.cancel`` сразу завершает
неотправленные Items (и запаркованные окном ``max_in_flight``), Completer
лениво отменяет отправленные при claim (UC-03, §6.1), Sweeper — Items с
истёкшим lease у батча с запросом отмены. Каждый путь обязан прибавить метку
итога в ``th_metric``, иначе ``view.labels`` и сводка хука расходятся
со счётчиками ``th_counter``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select, update

from tallyho.engine.completer import (
    CANCELLED_LABEL,
    ClaimOutcome,
    FinishResult,
    ItemRef,
)
from tallyho.engine.operations import Operations
from tallyho.engine.producer import RootSpec
from tallyho.engine.reads import Reads
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.calls import TaskCall
from tallyho.model.states import ItemState, ResultClass
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, upsert_slots
from tests.integration.engine.completer_env import (
    RELAY_SLOT,
    Finalized,
    MovableClock,
    open_completer,
    schema_engine,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.engine.completer import Completer
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

_INFINITY = datetime.max.replace(tzinfo=UTC)
_COUNTER_BY_STATE = {
    ItemState.OK: "ok",
    ItemState.SKIP: "skip",
    ItemState.ERROR: "error",
    ItemState.CANCELLED: "cancelled",
}


async def _batch(env: Env, n: int, *, dispatched: int) -> tuple[UUID, list[ItemRef]]:
    """Корень с ``n`` Items; первые ``dispatched`` уже у брокера (outbox пуст)."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail"))
        calls = [TaskCall(task_name="send", args=(i,), kwargs={}) for i in range(n)]
        _ = await env.producer.add_items(conn, root.id, calls)
        item = env.tables.item
        ids = list(
            await conn.scalars(
                select(item.c.id).where(item.c.batch_id == root.id).order_by(item.c.id)
            )
        )
        sent = ids[:dispatched]
        outbox = env.tables.outbox
        _ = await conn.execute(delete(outbox).where(outbox.c.item_id.in_(sent)))
        await upsert_slots(
            conn, env.tables, {(root.id, RELAY_SLOT): CounterDelta(dispatched=dispatched)}
        )
    return root.id, [ItemRef(item_id, root.id) for item_id in ids]


async def _cancel(env: Env, batch_id: UUID) -> int:
    async with env.transaction() as conn:
        return await Operations(tables=env.tables, clock=SystemClock()).cancel(conn, batch_id)


async def _labels(env: Env, batch_id: UUID) -> dict[str, int]:
    view = await Reads(schema_engine(env), env.tables, SystemClock()).view(batch_id)
    return {name: value for name, value in view.labels.items() if value}


async def _run(completer: Completer, ref: ItemRef, value: FinishResult) -> None:
    claimed = await completer.claim(ref)
    assert claimed.outcome is ClaimOutcome.CLAIMED
    assert await completer.finish(ref, value, attempt=claimed.attempt)


async def test_lazy_cancel_on_claim_counts_cancelled_label(env: Env) -> None:
    batch_id, refs = await _batch(env, 5, dispatched=3)
    sent = refs[:3]
    assert await _cancel(env, batch_id) == 2
    async with open_completer(env, clock=MovableClock(datetime.now(UTC))) as completer:
        for ref in sent:
            assert (await completer.claim(ref)).outcome is ClaimOutcome.CANCELLED
    counters = await env.counters(batch_id)
    assert counters.cancelled == 5
    assert await _labels(env, batch_id) == {CANCELLED_LABEL: 5}


async def _park_and_orphan(env: Env, batch_id: UUID, *, parked: ItemRef, dead: ItemRef) -> None:
    """Запарковать запись outbox окном и оставить Item с истёкшим lease погибшего воркера."""
    outbox = env.tables.outbox
    async with env.transaction() as conn:
        # Запись сверх окна max_in_flight relay паркует (§11.2).
        _ = await conn.execute(
            update(outbox).where(outbox.c.item_id == parked.id).values(available_at=_INFINITY)
        )
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=dead.id,
                batch_id=batch_id,
                lease_until=datetime.now(UTC) - timedelta(seconds=1),
                worker_id="dead",
                attempt=0,
            )
        )


async def _expire_leases(env: Env) -> int:
    sweeper = Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        finalizer=Finalized(),
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )
    return await sweeper.expire_leases()


async def _assert_labels_match(env: Env, batch_id: UUID, labels: dict[str, int]) -> None:
    """Метки равны итогам Items, а сумма меток класса — счётчику класса."""
    item = env.tables.item
    async with env.connection() as conn:
        rows = await conn.execute(
            select(item.c.state, item.c.label, func.count())
            .where(item.c.batch_id == batch_id)
            .group_by(item.c.state, item.c.label)
        )
        outcomes = [(ItemState(state), str(label), int(count)) for state, label, count in rows]
    assert labels == {label: count for _, label, count in outcomes}
    by_class = dict.fromkeys(_COUNTER_BY_STATE.values(), 0)
    for state, label, _ in outcomes:
        by_class[_COUNTER_BY_STATE[state]] += labels[label]
    counters = await env.counters(batch_id)
    assert by_class == {
        "ok": counters.ok,
        "skip": counters.skip,
        "error": counters.error,
        "cancelled": counters.cancelled,
    }


async def test_labels_match_counters_after_mixed_terminal_paths(env: Env) -> None:
    # ok, error — до отмены; два последних Item в outbox — отмена сразу; lazy — при
    # claim; dead — sweeper по истёкшему lease; running — доделан после отмены.
    batch_id, refs = await _batch(env, 9, dispatched=7)
    ok, error, lazy_a, lazy_b, lazy_c, dead, running, _queued, parked = refs
    await _park_and_orphan(env, batch_id, parked=parked, dead=dead)
    async with open_completer(env, clock=MovableClock(datetime.now(UTC))) as completer:
        await _run(completer, ok, FinishResult(result_class=ResultClass.OK))
        await _run(completer, error, FinishResult(result_class=ResultClass.ERROR, label="boom"))
        claimed = await completer.claim(running)
        assert claimed.outcome is ClaimOutcome.CLAIMED

        assert await _cancel(env, batch_id) == 2

        for ref in (lazy_a, lazy_b, lazy_c):
            assert (await completer.claim(ref)).outcome is ClaimOutcome.CANCELLED
        # Выполняющийся Item доделывается (§6.1).
        assert await completer.finish(
            running,
            FinishResult(result_class=ResultClass.OK, label="late"),
            attempt=claimed.attempt,
        )
    assert await _expire_leases(env) == 1

    counters = await env.counters(batch_id)
    assert (counters.ok, counters.error, counters.cancelled, counters.total) == (2, 1, 6, 9)
    labels = await _labels(env, batch_id)
    assert labels == {"ok": 1, "late": 1, "boom": 1, CANCELLED_LABEL: 6}
    await _assert_labels_match(env, batch_id, labels)
