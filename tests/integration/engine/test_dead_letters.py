"""Сверка с DLQ брокера на PostgreSQL: правило поколений, курсор и конкуренция (UC-15, Fix-6)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import insert, select, update

from tallyho.engine.dead_letters import CURSOR_KEY, DeadLetterReconciler, DeadLetterSettings
from tallyho.model.errors import ConcurrentModification, ConfigurationError, TallyhoError
from tallyho.model.states import ItemState, OutboxKind
from tallyho.protocols.broker import DeadLetter, DeadLetters
from tallyho.storage.tx import RetryPolicy, TxSettings
from tests.integration.engine.completer_env import (
    NOW,
    Finalized,
    MovableClock,
    RecordingRelay,
    lease_row,
    schema_engine,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

SLOT = 9


@dataclass
class Dlq:
    """DLQ брокера в памяти: порции по курсору-номеру, как у ``InlineBroker``."""

    pages: list[list[DeadLetter]] = field(default_factory=list[list[DeadLetter]])
    calls: list[str | None] = field(default_factory=list[str | None])
    delay: float = 0.0
    started: asyncio.Event = field(default_factory=asyncio.Event)

    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        self.calls.append(since)
        self.started.set()
        await asyncio.sleep(self.delay)
        index = 0 if since is None else int(since)
        if index >= len(self.pages):
            return DeadLetters((), since)
        return DeadLetters(
            tuple(self.pages[index]), str(index + 1), more=index + 1 < len(self.pages)
        )


@dataclass
class Rig:
    reconciler: DeadLetterReconciler
    dlq: Dlq
    finalizer: Finalized
    relay: RecordingRelay
    clock: MovableClock


def rig(env: Env, *pages: list[DeadLetter], settings: DeadLetterSettings | None = None) -> Rig:
    dlq = Dlq(pages=list(pages))
    finalizer = Finalized()
    relay = RecordingRelay()
    clock = MovableClock()
    reconciler = DeadLetterReconciler(
        tables=env.tables,
        engine=schema_engine(env),
        clock=clock,
        source=dlq,
        finalizer=finalizer,
        relay=relay,
        settings=settings or DeadLetterSettings(slot=SLOT),
    )
    return Rig(reconciler, dlq, finalizer, relay, clock)


async def cursor(env: Env) -> str | None:
    meta = env.tables.meta
    async with env.connection() as conn:
        return await conn.scalar(select(meta.c.value).where(meta.c.key == CURSOR_KEY))


async def item_row(env: Env, item_id: UUID) -> tuple[int, str | None, object]:
    item = env.tables.item
    async with env.connection() as conn:
        row = (
            await conn.execute(
                select(item.c.state, item.c.label, item.c.error).where(item.c.id == item_id)
            )
        ).one()
    return int(row[0]), row[1], row[2]


async def add_lease(env: Env, item_id: UUID, batch_id: UUID, *, live: bool) -> None:
    shift = timedelta(seconds=30 if live else -30)
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=item_id,
                batch_id=batch_id,
                lease_until=NOW + shift,
                worker_id="worker",
                attempt=0,
            )
        )


async def test_orphan_of_current_generation_becomes_exhausted(env: Env) -> None:
    seeded = await seed(env, 2)
    dead, alive = seeded.refs
    env_rig = rig(env, [DeadLetter(dead.id, 0, "boom")])

    assert await env_rig.reconciler.reconcile_once() == 1

    assert await item_row(env, dead.id) == (
        int(ItemState.ERROR),
        "exhausted",
        {"type": "DeadLetter", "message": "boom"},
    )
    assert (await item_row(env, alive.id))[0] == int(ItemState.ACTIVE)
    totals = await env.counters(seeded.batch_id)
    assert (totals.error, totals.w_done, totals.dispatched) == (1, 1, 2)
    async with env.connection() as conn:
        mark = env.tables.item_mark
        marks = [tuple(row) for row in await conn.execute(select(mark.c.label, mark.c.item_id))]
        metric = env.tables.metric
        metrics = [
            tuple(row)
            for row in await conn.execute(select(metric.c.name, metric.c.slot, metric.c.value))
        ]
        finished_at = await conn.scalar(
            select(env.tables.item.c.finished_at).where(env.tables.item.c.id == dead.id)
        )
    assert marks == [("exhausted", dead.id)]
    assert metrics == [("exhausted", SLOT, 1)]
    assert finished_at == NOW
    assert env_rig.finalizer.calls == [seeded.batch_id]
    assert await cursor(env) == "1"


async def test_repeated_pass_is_noop_and_cursor_does_not_move_back(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    env_rig = rig(env, [DeadLetter(ref.id)])
    assert await env_rig.reconciler.reconcile_once() == 1
    before = await env.counters(seeded.batch_id)

    assert await env_rig.reconciler.reconcile_once() == 0
    assert await env_rig.reconciler.reconcile_once() == 0

    # Курсор читается из БД: каждый следующий проход продолжает с сохранённого места.
    assert env_rig.dlq.calls == [None, "1", "1"]
    assert await cursor(env) == "1"
    assert await env.counters(seeded.batch_id) == before
    assert env_rig.finalizer.calls == [seeded.batch_id]
    assert (await item_row(env, ref.id))[2] == {
        "type": "DeadLetter",
        "message": "брокер перенёс джобу в DLQ, итог Item записан сверкой",
    }


async def test_same_dead_letter_again_does_not_change_terminal_item(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    # Адаптер вправе отдать запись повторно (перекрытие обхода flexiq).
    env_rig = rig(env, [DeadLetter(ref.id, 0, "first")], [DeadLetter(ref.id, 0, "second")])
    assert await env_rig.reconciler.reconcile_once() == 1

    totals = await env.counters(seeded.batch_id)
    assert (totals.error, totals.w_done) == (1, 1)
    assert (await item_row(env, ref.id))[2] == {"type": "DeadLetter", "message": "first"}
    assert await cursor(env) == "2"
    assert env_rig.finalizer.calls == [seeded.batch_id]


async def test_redispatched_item_is_not_touched_by_older_dead_job(env: Env) -> None:
    seeded = await seed(env, 2)
    resent, current = seeded.refs
    item = env.tables.item
    async with env.transaction() as conn:
        # Оба Item возвращались в outbox: теперь их отправка — поколение 1.
        _ = await conn.execute(
            update(item).where(item.c.batch_id == seeded.batch_id).values(generation=1)
        )
    # Мёртвая джоба первой отправки и мёртвая джоба текущей.
    env_rig = rig(env, [DeadLetter(resent.id, 0), DeadLetter(current.id, 1)])

    assert await env_rig.reconciler.reconcile_once() == 1

    # Новая джоба `resent` ждёт в очереди брокера: по данным tallyho он выглядит
    # так же, как осиротевший, и отличает его только поколение.
    assert (await item_row(env, resent.id))[:2] == (int(ItemState.ACTIVE), None)
    assert (await item_row(env, current.id))[:2] == (int(ItemState.ERROR), "exhausted")
    assert await cursor(env) == "1"


async def test_item_with_live_lease_is_left_running_and_marked(env: Env) -> None:
    seeded = await seed(env, 2)
    running, expired = seeded.refs
    await add_lease(env, running.id, seeded.batch_id, live=True)
    await add_lease(env, expired.id, seeded.batch_id, live=False)
    env_rig = rig(env, [DeadLetter(running.id), DeadLetter(expired.id)])

    assert await env_rig.reconciler.reconcile_once() == 0

    assert (await item_row(env, running.id))[0] == int(ItemState.ACTIVE)
    assert (await item_row(env, expired.id))[0] == int(ItemState.ACTIVE)
    live = await lease_row(env, running.id)
    stale = await lease_row(env, expired.id)
    assert live is not None
    assert stale is not None
    # Джоба закрыта: если выполнение упадёт с вердиктом RETRY, release вернёт Item в outbox.
    assert live["redelivered"] is True
    # Истёкший lease — работа sweeper: сверка его не трогает.
    assert stale["redelivered"] is False
    assert env_rig.finalizer.calls == []
    assert await cursor(env) == "1"


async def test_item_waiting_in_outbox_is_not_finished(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with env.transaction() as conn:
        # Relay отправил джобу, но не успел удалить запись: он отправит её снова.
        _ = await conn.execute(
            insert(env.tables.outbox).values(
                id=ref.id,
                kind=int(OutboxKind.ITEM),
                batch_id=seeded.batch_id,
                item_id=ref.id,
                task_name="send",
                available_at=NOW + timedelta(seconds=30),
            )
        )
    env_rig = rig(env, [DeadLetter(ref.id)])

    assert await env_rig.reconciler.reconcile_once() == 0

    assert (await item_row(env, ref.id))[0] == int(ItemState.ACTIVE)
    assert await env.count(env.tables.outbox) == 1


async def test_orphan_of_cancelling_batch_is_cancelled(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    await set_batch(env, seeded.batch_id, cancel_requested_at=NOW, cancel_reason="cancel")
    env_rig = rig(env, [DeadLetter(ref.id, 0, "boom")])

    assert await env_rig.reconciler.reconcile_once() == 1

    assert await item_row(env, ref.id) == (int(ItemState.CANCELLED), "cancelled", None)
    totals = await env.counters(seeded.batch_id)
    assert (totals.cancelled, totals.error, totals.w_done) == (1, 0, 1)
    assert await env.count(env.tables.item_mark) == 0
    assert env_rig.finalizer.calls == [seeded.batch_id]


async def test_unknown_terminal_and_virtual_items_are_skipped(env: Env) -> None:
    seeded = await seed(env, 2)
    done, virtual = seeded.refs
    other = await seed(env, 1, kind="other")
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id == done.id).values(state=int(ItemState.OK), label="ok")
        )
        _ = await conn.execute(
            update(item).where(item.c.id == virtual.id).values(child_batch_id=other.batch_id)
        )
    missing = other.batch_id  # не id Item
    env_rig = rig(env, [DeadLetter(done.id), DeadLetter(virtual.id), DeadLetter(missing)])

    assert await env_rig.reconciler.reconcile_once() == 0

    assert (await item_row(env, done.id))[:2] == (int(ItemState.OK), "ok")
    assert (await item_row(env, virtual.id))[0] == int(ItemState.ACTIVE)
    assert env_rig.finalizer.calls == []
    assert await cursor(env) == "1"


async def test_pass_reads_several_portions_but_not_more_than_limit(env: Env) -> None:
    seeded = await seed(env, 3)
    first, second, third = seeded.refs
    env_rig = rig(
        env,
        [DeadLetter(first.id)],
        [DeadLetter(second.id)],
        [DeadLetter(third.id)],
        settings=DeadLetterSettings(slot=SLOT, max_rounds=2),
    )

    assert await env_rig.reconciler.reconcile_once() == 2
    assert await cursor(env) == "2"
    assert (await item_row(env, third.id))[0] == int(ItemState.ACTIVE)

    # Остаток разбирает следующий проход.
    assert await env_rig.reconciler.reconcile_once() == 1
    assert env_rig.dlq.calls == [None, "1", "2"]
    assert await cursor(env) == "3"
    assert (await item_row(env, third.id))[1] == "exhausted"


async def test_window_slot_is_released_and_relay_is_kicked(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.window).values(item_id=ref.id, batch_id=seeded.batch_id)
        )
    env_rig = rig(env, [DeadLetter(ref.id)])

    assert await env_rig.reconciler.reconcile_once() == 1

    assert await env.count(env.tables.window) == 0
    assert env_rig.relay.calls == [[seeded.batch_id]]


async def test_reconciler_without_relay_still_releases_window(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.window).values(item_id=ref.id, batch_id=seeded.batch_id)
        )
    env_rig = rig(env, [DeadLetter(ref.id)])
    env_rig.reconciler.relay = None

    assert await env_rig.reconciler.reconcile_once() == 1

    # Подсказать некому, но место освобождено: его подберёт scan relay.
    assert await env.count(env.tables.window) == 0
    assert env_rig.relay.calls == []


async def test_failed_portion_keeps_cursor_and_is_applied_by_next_pass(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    settings = DeadLetterSettings(
        slot=SLOT,
        tx=TxSettings(lock_timeout=timedelta(milliseconds=100)),
        retry=RetryPolicy(attempts=1),
    )
    env_rig = rig(env, [DeadLetter(ref.id)], settings=settings)
    item = env.tables.item
    async with env.transaction() as holder:
        # Строку Item держит чужая транзакция: завершение не проходит.
        _ = await holder.execute(select(item.c.id).where(item.c.id == ref.id).with_for_update())
        with pytest.raises(ConcurrentModification):
            _ = await env_rig.reconciler.reconcile_once()
        # Курсор двигается только вместе с завершениями: порция не потеряна.
        assert await cursor(env) is None

    assert await env_rig.reconciler.reconcile_once() == 1
    assert env_rig.dlq.calls == [None, None]
    assert await cursor(env) == "1"
    assert (await item_row(env, ref.id))[1] == "exhausted"


async def test_two_processes_do_not_do_the_same_pass(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    # Строка курсора уже есть: её создал первый проход установки.
    assert await rig(env).reconciler.reconcile_once() == 0
    slow = rig(env, [DeadLetter(ref.id)])
    slow.dlq.delay = 0.5
    other = rig(env, [DeadLetter(ref.id)])

    first = asyncio.create_task(slow.reconciler.reconcile_once())
    # Первый процесс взял строку курсора и читает DLQ.
    _ = await asyncio.wait_for(slow.dlq.started.wait(), timeout=10)
    assert await other.reconciler.reconcile_once() == 0
    # Второй процесс не ждал первого и не читал DLQ: строку курсора держал первый.
    assert not first.done()
    assert other.dlq.calls == []
    assert await first == 1

    assert await cursor(env) == "1"
    # После освобождения строки он продолжает с курсора первого.
    assert await other.reconciler.reconcile_once() == 0
    assert other.dlq.calls == ["1"]


async def test_first_pass_of_installation_is_not_done_twice(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    slow = rig(env, [DeadLetter(ref.id)])
    slow.dlq.delay = 0.3
    other = rig(env, [DeadLetter(ref.id)])

    # Строки курсора ещё нет: оба процесса пытаются её создать, выигрывает один.
    first = asyncio.create_task(slow.reconciler.reconcile_once())
    _ = await asyncio.wait_for(slow.dlq.started.wait(), timeout=10)
    assert await other.reconciler.reconcile_once() == 0
    assert await first == 1

    assert other.dlq.calls == []
    assert await cursor(env) == "1"


async def test_slow_broker_read_fails_the_pass_without_moving_cursor(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    settings = DeadLetterSettings(slot=SLOT, read_timeout=timedelta(milliseconds=50))
    env_rig = rig(env, [DeadLetter(ref.id)], settings=settings)
    env_rig.dlq.delay = 5

    with pytest.raises(TallyhoError, match="DLQ"):
        _ = await env_rig.reconciler.reconcile_once()

    assert await cursor(env) is None
    assert (await item_row(env, ref.id))[0] == int(ItemState.ACTIVE)


async def test_empty_cursor_from_adapter_is_stored_as_start(env: Env) -> None:
    env_rig = rig(env)

    assert await env_rig.reconciler.reconcile_once() == 0
    assert await env_rig.reconciler.reconcile_once() == 0

    # Строка курсора создаётся первым проходом; пустое значение — «с начала».
    assert (await cursor(env), env_rig.dlq.calls) == ("", [None, None])


@pytest.mark.parametrize(
    "build",
    [
        lambda: DeadLetterSettings(slot=-1),
        lambda: DeadLetterSettings(max_rounds=0),
        lambda: DeadLetterSettings(read_timeout=timedelta(0)),
    ],
)
def test_settings_are_validated(build: Callable[[], DeadLetterSettings]) -> None:
    with pytest.raises(ConfigurationError):
        _ = build()


# --- событие DLQ брокера (JOB_DEAD): то же правило без курсора (Fix-18) ---------------


async def test_event_applies_rule_without_touching_cursor(env: Env) -> None:
    seeded = await seed(env, 4)
    orphan, resent, running, queued = seeded.refs
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(update(item).where(item.c.id == resent.id).values(generation=2))
        _ = await conn.execute(
            insert(env.tables.outbox).values(
                id=queued.id,
                kind=int(OutboxKind.ITEM),
                batch_id=seeded.batch_id,
                item_id=queued.id,
                task_name="send",
                available_at=NOW + timedelta(seconds=30),
            )
        )
    await add_lease(env, running.id, seeded.batch_id, live=True)
    env_rig = rig(env)
    entries = [
        DeadLetter(orphan.id, 0, "boom"),
        DeadLetter(resent.id, 1, "old"),
        DeadLetter(running.id, 0, "closed"),
        DeadLetter(queued.id, 0, "again"),
    ]

    assert await env_rig.reconciler.settle(entries, error_type="FlexiqDeadLetter") == 1
    assert await env_rig.reconciler.settle(entries, error_type="FlexiqDeadLetter") == 0

    assert await item_row(env, orphan.id) == (
        int(ItemState.ERROR),
        "exhausted",
        {"type": "FlexiqDeadLetter", "message": "boom"},
    )
    for ref in (resent, running, queued):
        assert (await item_row(env, ref.id))[:2] == (int(ItemState.ACTIVE), None)
    live = await lease_row(env, running.id)
    assert live is not None
    assert live["redelivered"] is True
    assert env_rig.finalizer.calls == [seeded.batch_id]
    # Курсор — только у прохода сверки; DLQ брокера событие не читает.
    assert await cursor(env) is None
    assert env_rig.dlq.calls == []


async def test_event_releases_window_slot_and_kicks_relay(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.window).values(item_id=ref.id, batch_id=seeded.batch_id)
        )
    env_rig = rig(env)

    assert await env_rig.reconciler.settle([DeadLetter(ref.id)]) == 1

    assert (await item_row(env, ref.id))[2] == {
        "type": "DeadLetter",
        "message": "брокер перенёс джобу в DLQ, итог Item записан сверкой",
    }
    assert await env.count(env.tables.window) == 0
    assert env_rig.relay.calls == [[seeded.batch_id]]
