"""Completer finish: CAS, counters, metrics, marks and max_in_flight window."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import DateTime, insert, literal_column, select, update
from typing_extensions import override

from tallyho.engine.completer import FinishResult, ItemRef
from tallyho.model.states import ItemState, OutboxKind, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.storage.metric_names import METRIC_PREFIX
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    SETTINGS,
    CommitCounter,
    Finalized,
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


@dataclass
class RecordingFinishObserver(NullObserver):
    """Record committed finishes, including the attempt read by the CAS transaction."""

    finished: list[tuple[UUID, ResultClass, str | None, int]] = field(default_factory=list)

    @override
    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        del batch_id
        self.finished.append((item_id, result, label, attempt))


async def _item(env: Env, item_id: UUID) -> RowMapping:
    item = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(item).where(item.c.id == item_id))).mappings().one()


async def test_finish_records_result_counters_metrics_and_finalizer(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    finalizer = Finalized()
    value = FinishResult(
        result_class=ResultClass.OK,
        result={"message_id": "m-1"},
        metrics={"bytes": 42},
    )
    async with open_completer(env, finalizer=finalizer) as completer:
        assert (await completer.claim(ref)).run
        assert await completer.finish(ref, value)
        assert completer.held == frozenset()

    row = await _item(env, ref.id)
    assert row["state"] == ItemState.OK
    assert row["label"] == "ok"
    assert row["result"] == {"message_id": "m-1"}
    assert row["error"] is None
    assert row["finished_at"] == NOW
    assert await lease_row(env, ref.id) is None
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.w_done, counters.pending) == (1, 1, 0)
    metric = env.tables.metric
    async with env.connection() as conn:
        rows = (
            await conn.execute(
                select(metric.c.name, metric.c.slot, metric.c.value).order_by(metric.c.name)
            )
        ).all()
    # Метрика — строка с зарезервированным префиксом, метка итога — под своим именем.
    assert rows == [(METRIC_PREFIX + "bytes", COMPLETER_SLOT, 42), ("ok", COMPLETER_SLOT, 1)]
    assert await env.count(env.tables.item_mark) == 0
    assert finalizer.calls == [seeded.batch_id]


async def test_concurrent_duplicate_finish_counts_once(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    ok = FinishResult(result_class=ResultClass.SKIP, label="suppressed")
    losing = FinishResult(result_class=ResultClass.ERROR, label="late")
    async with open_completer(env) as completer:
        results = await asyncio.gather(completer.finish(ref, ok), completer.finish(ref, losing))
    assert sorted(results) == [False, True]
    row = await _item(env, ref.id)
    assert (row["state"], row["label"]) == (ItemState.SKIP, "suppressed")
    counters = await env.counters(seeded.batch_id)
    assert (counters.skip, counters.error, counters.w_done) == (1, 0, 1)


async def test_finish_rejects_mismatched_batch_reference(env: Env) -> None:
    seeded = await seed(env, 1)
    actual = seeded.refs[0]
    wrong = ItemRef(actual.id, uuid4())
    async with open_completer(env) as completer:
        changed = await completer.finish(wrong, FinishResult(result_class=ResultClass.OK))
    assert changed is False
    assert (await _item(env, actual.id))["state"] == ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 1


async def test_thousand_finishes_are_grouped_into_few_transactions(env: Env) -> None:
    n = 1000
    seeded = await seed(env, n)
    counter = CommitCounter()
    value = FinishResult(result_class=ResultClass.OK)
    async with open_completer(env, counter=counter) as completer:
        results = await asyncio.gather(*(completer.finish(ref, value) for ref in seeded.refs))
    assert all(results)
    assert counter.commits <= math.ceil(n / SETTINGS.max_batch) + 1
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.w_done, counters.pending) == (n, n, 0)


async def test_batched_finish_preserves_fields_accumulates_metrics_and_cas_scope(env: Env) -> None:
    seeded = await seed(env, 3)
    error_ref, ok_ref, terminal_ref = seeded.refs
    item = env.tables.item
    expiry = env.tables.expiry
    async with env.transaction() as conn:
        _ = await conn.execute(update(item).where(item.c.id == error_ref.id).values(attempt=2))
        _ = await conn.execute(update(item).where(item.c.id == ok_ref.id).values(attempt=3))
        _ = await conn.execute(
            update(item)
            .where(item.c.id == terminal_ref.id)
            .values(
                state=int(ItemState.SKIP),
                label="already-done",
                finished_at=NOW - timedelta(seconds=1),
            )
        )
        _ = await conn.execute(
            insert(expiry).values(
                [
                    {"item_id": ref.id, "expires_at": NOW + timedelta(minutes=1)}
                    for ref in seeded.refs
                ]
            )
        )

    observer = RecordingFinishObserver()
    error_value = FinishResult(
        result_class=ResultClass.ERROR,
        label="shared",
        error={"code": 550},
        metrics={"payloads": 2},
    )
    ok_value = FinishResult(
        result_class=ResultClass.OK,
        label="shared",
        result={"message_id": "m-2"},
        metrics={"payloads": 3},
    )
    async with open_completer(env, observer=observer) as completer:
        results = await asyncio.gather(
            completer.finish(error_ref, error_value),
            completer.finish(ok_ref, ok_value),
            completer.finish(
                terminal_ref,
                FinishResult(result_class=ResultClass.ERROR, label="late"),
            ),
        )

    assert list(results) == [True, True, False]
    assert (await _item(env, error_ref.id))["error"] == {"code": 550}
    assert (await _item(env, ok_ref.id))["result"] == {"message_id": "m-2"}
    terminal = await _item(env, terminal_ref.id)
    assert (terminal["state"], terminal["label"]) == (ItemState.SKIP, "already-done")
    metric = env.tables.metric
    async with env.connection() as conn:
        metrics = dict(
            (
                await conn.execute(
                    select(metric.c.name, metric.c.value).where(
                        metric.c.batch_id == seeded.batch_id
                    )
                )
            ).all()
        )
        expiry_ids = set(await conn.scalars(select(expiry.c.item_id)))
    assert metrics == {METRIC_PREFIX + "payloads": 5, "shared": 2}
    assert expiry_ids == {terminal_ref.id}
    assert sorted(observer.finished) == sorted(
        [
            (error_ref.id, ResultClass.ERROR, "shared", 2),
            (ok_ref.id, ResultClass.OK, "shared", 3),
        ]
    )


async def test_error_is_marked_by_default_and_mark_can_be_disabled(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    async with open_completer(env) as completer:
        assert await completer.finish(
            first,
            FinishResult(
                result_class=ResultClass.ERROR,
                label="hard_bounce",
                error={"code": 550},
            ),
        )
        assert await completer.finish(
            second,
            FinishResult(result_class=ResultClass.ERROR, label="rejected", mark=False),
        )
    mark = env.tables.item_mark
    async with env.connection() as conn:
        rows = (await conn.execute(select(mark).order_by(mark.c.item_id))).mappings().all()
    assert len(rows) == 1
    assert rows[0]["batch_id"] == seeded.batch_id
    assert rows[0]["item_id"] == first.id
    assert rows[0]["label"] == "hard_bounce"


async def test_finish_releases_window_unparks_next_and_kicks_relay(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    await set_batch(env, seeded.batch_id, max_in_flight=1)
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.window).values(item_id=first.id, batch_id=seeded.batch_id)
        )
        _ = await conn.execute(
            insert(env.tables.outbox).values(
                id=second.id,
                kind=int(OutboxKind.ITEM),
                batch_id=seeded.batch_id,
                item_id=second.id,
                task_name="send",
                available_at=literal_column("'infinity'::timestamptz", DateTime(timezone=True)),
            )
        )
    relay = RecordingRelay()
    async with open_completer(env, relay=relay) as completer:
        assert await completer.finish(first, FinishResult(result_class=ResultClass.OK))
    assert await env.count(env.tables.window) == 0
    outbox = env.tables.outbox
    async with env.connection() as conn:
        unparked = await conn.scalar(
            select(
                outbox.c.available_at
                == literal_column("'-infinity'::timestamptz", DateTime(timezone=True))
            ).where(outbox.c.id == second.id)
        )
    assert unparked is True
    assert relay.calls == [[seeded.batch_id]]
