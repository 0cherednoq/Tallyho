"""Восстановительные проходы Sweeper на PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, insert, select, update

from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.reads import Reads
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.calls import TaskCall
from tallyho.model.errors import BatchPurged, ConfigurationError
from tallyho.model.states import BatchState, CancelReason, ItemState, OnFeederFailed
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, insert_delta
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from collections.abc import Iterable
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class _Finalizer:
    tried: list[UUID]

    def __init__(self) -> None:
        self.tried = []

    async def try_finalize(self, batch_id: UUID) -> bool:
        self.tried.append(batch_id)
        return True


class _Relay:
    kicked: list[UUID]

    def __init__(self) -> None:
        self.kicked = []

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        self.kicked.extend(batch_ids)


def sweeper(env: Env, finalizer: _Finalizer | None = None, relay: _Relay | None = None) -> Sweeper:
    """Sweeper над схемой теста без grace-периода."""
    return Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        finalizer=finalizer or _Finalizer(),
        relay=relay,
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )


async def make_items(
    env: Env, count: int = 1, *, deadline: datetime | None = None
) -> tuple[UUID, list[UUID]]:
    """Создать корень с Items и удалить outbox, имитируя dispatch."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="work", deadline=deadline))
        _ = await env.producer.add_items(
            conn,
            root.id,
            [TaskCall(task_name="task", args=(index,), kwargs={}) for index in range(count)],
        )
        ids = list(
            await conn.scalars(
                select(env.tables.item.c.id)
                .where(env.tables.item.c.batch_id == root.id)
                .order_by(env.tables.item.c.id)
            )
        )
        _ = await conn.execute(
            env.tables.outbox.delete().where(env.tables.outbox.c.item_id.in_(ids))
        )
    return root.id, ids


async def test_expire_leases_requeues_exhausts_and_cleans_terminal(env: Env) -> None:
    batch_id, ids = await make_items(env, 3)
    now = datetime.now(UTC)
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == ids[0])
            .values(options={"max_retries": 2}, attempt=0)
        )
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == ids[1])
            .values(options={"max_retries": 1}, attempt=1)
        )
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == ids[2])
            .values(state=int(ItemState.OK), label="ok", finished_at=now)
        )
        _ = await conn.execute(
            insert(env.tables.lease),
            [
                {
                    "item_id": item_id,
                    "batch_id": batch_id,
                    "lease_until": now - timedelta(seconds=1),
                    "worker_id": "dead",
                    "attempt": index,
                }
                for index, item_id in enumerate(ids)
            ],
        )
        _ = await conn.execute(
            update(env.tables.batch).where(env.tables.batch.c.id == batch_id).values(paused_at=now)
        )
    cancelled_batch, cancelled_ids = await make_items(env)
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == cancelled_batch)
            .values(cancel_requested_at=now, cancel_reason=CancelReason.CANCEL.value)
        )
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=cancelled_ids[0],
                batch_id=cancelled_batch,
                lease_until=now - timedelta(seconds=1),
                worker_id="dead",
                attempt=0,
            )
        )
    finalizer = _Finalizer()
    relay = _Relay()
    assert await sweeper(env, finalizer, relay).expire_leases() == 4
    assert await env.count(env.tables.lease) == 0
    async with env.connection() as conn:
        result = await conn.execute(
            select(env.tables.item.c.id, env.tables.item.c.state).where(
                env.tables.item.c.id.in_(ids)
            )
        )
        rows = dict(result.all())
        queued = await conn.scalar(
            select(func.count())
            .select_from(env.tables.outbox)
            .where(env.tables.outbox.c.item_id == ids[0])
        )
        available_at = await conn.scalar(
            select(env.tables.outbox.c.available_at).where(env.tables.outbox.c.item_id == ids[0])
        )
        cancelled_state = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == cancelled_ids[0])
        )
    assert rows[ids[0]] == int(ItemState.ACTIVE)
    assert rows[ids[1]] == int(ItemState.ERROR)
    assert rows[ids[2]] == int(ItemState.OK)
    assert queued == 1
    assert available_at is not None
    assert available_at.year == 9999
    assert cancelled_state == int(ItemState.CANCELLED)
    assert set(finalizer.tried) == {batch_id, cancelled_batch}
    assert batch_id in relay.kicked


async def test_finalize_stuck_and_deadline(env: Env) -> None:
    finalizer = _Finalizer()
    async with env.transaction() as conn:
        empty = await env.producer.create_root(conn, RootSpec(kind="empty"))
        _ = await env.producer.seal(conn, empty.id)
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == empty.id)
            .values(updated_at=func.now() - timedelta(minutes=1))
        )
        retry = await env.producer.create_root(conn, RootSpec(kind="retry"))
        _ = await env.producer.seal(conn, retry.id)
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == retry.id)
            .values(
                hook_attempts=2,
                hook_error="boom",
                updated_at=func.now() - timedelta(seconds=5),
            )
        )
        backing_off = await env.producer.create_root(conn, RootSpec(kind="backing-off"))
        _ = await env.producer.seal(conn, backing_off.id)
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == backing_off.id)
            .values(hook_attempts=1, hook_error="boom", updated_at=func.now())
        )
        pending = await env.producer.create_root(conn, RootSpec(kind="pending"))
        _ = await env.producer.add_items(
            conn, pending.id, [TaskCall(task_name="task", args=(1,), kwargs={})]
        )
        _ = await env.producer.seal(conn, pending.id)
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == pending.id)
            .values(updated_at=func.now() - timedelta(minutes=1))
        )
    assert await sweeper(env, finalizer).finalize_stuck() == 2
    assert empty.id in finalizer.tried
    assert retry.id in finalizer.tried
    assert backing_off.id not in finalizer.tried
    assert pending.id not in finalizer.tried

    root_id, _ = await make_items(env, deadline=datetime.now(UTC) - timedelta(seconds=1))
    assert await sweeper(env, finalizer).enforce_deadlines() == 0
    row = await env.batch(root_id)
    assert row["cancel_reason"] == CancelReason.DEADLINE.value
    assert row["cancel_requested_at"] is not None


async def test_seal_orphan_stage_and_reconcile_drift(env: Env) -> None:
    finalizer = _Finalizer()
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="root"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        stage = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="stage", fed_by=(source.id,))
        )
        failed_source = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="failed-source")
        )
        cancel_stage = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(
                key="cancel-stage",
                fed_by=(failed_source.id,),
                on_feeder_failed=OnFeederFailed.CANCEL,
            ),
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == source.id)
            .values(state=int(BatchState.SUCCEEDED), finished_at=func.now())
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == failed_source.id)
            .values(state=int(BatchState.FAILED), finished_at=func.now())
        )
    subject = sweeper(env, finalizer)
    assert await subject.seal_orphan_stages() == 2
    assert (await env.batch(stage.id))["state"] == int(BatchState.SEALED)
    assert (await env.batch(cancel_stage.id))["cancel_requested_at"] is not None

    batch_id, ids = await make_items(env)
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == ids[0])
            .values(state=int(ItemState.OK), label="ok", finished_at=func.now())
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == batch_id)
            .values(state=int(BatchState.SEALED))
        )
    assert await subject.reconcile_drift() >= 1
    assert (await env.counters(batch_id)).ok == 1


async def test_fold_deltas_and_expire_unclaimed(env: Env) -> None:
    finalizer = _Finalizer()
    relay = _Relay()
    batch_id, ids = await make_items(env)
    async with env.transaction() as conn:
        _ = await insert_delta(
            conn,
            env.tables,
            {batch_id: CounterDelta(duplicates=2)},
            created_at=func.now() - timedelta(minutes=1),
        )
        _ = await insert_delta(
            conn,
            env.tables,
            {batch_id: CounterDelta(duplicates=3)},
            created_at=func.now(),
        )
    subject = Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        finalizer=finalizer,
        relay=relay,
        settings=SweeperSettings(finalize_grace=timedelta(seconds=30)),
    )
    assert await subject.fold_stale_deltas() == 1
    assert await env.count(env.tables.counter_delta) == 1
    async with env.connection() as conn:
        folded_duplicates = await conn.scalar(
            select(func.sum(env.tables.counter.c.duplicates)).where(
                env.tables.counter.c.batch_id == batch_id
            )
        )
    assert folded_duplicates == 2
    assert (await env.counters(batch_id)).duplicates == 5
    subject = sweeper(env, finalizer, relay)
    assert await subject.fold_stale_deltas() == 1
    assert (await env.counters(batch_id)).duplicates == 5

    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.expiry).values(
                item_id=ids[0], expires_at=datetime.now(UTC) - timedelta(seconds=1)
            )
        )
    assert await subject.expire_unclaimed() == 1
    async with env.connection() as conn:
        state, label = (
            await conn.execute(
                select(env.tables.item.c.state, env.tables.item.c.label).where(
                    env.tables.item.c.id == ids[0]
                )
            )
        ).one()
    assert state == int(ItemState.ERROR)
    assert label == "expired"
    assert batch_id in finalizer.tried


async def test_retention_requires_release_and_purges_tree(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn,
            RootSpec(
                kind="root",
                retention=timedelta(microseconds=1),
                release_required=True,
            ),
        )
        child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="child"))
        _ = await env.producer.add_items(
            conn, child.id, [TaskCall(task_name="task", args=(1,), kwargs={})]
        )
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id.in_([root.id, child.id]))
            .values(state=int(BatchState.SUCCEEDED), finished_at=func.now() - timedelta(days=1))
        )
    subject = Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        finalizer=_Finalizer(),
        settings=SweeperSettings(batch_size=1, finalize_grace=timedelta(0)),
    )
    assert await subject.retention() == 0
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(released_at=func.now())
        )
    assert await subject.retention() == 1
    assert await env.count(env.tables.batch) == 0
    assert await env.count(env.tables.item) == 0
    with pytest.raises(BatchPurged):
        _ = await Reads(schema_engine(env), env.tables, SystemClock()).view(root.id)


async def test_settings_and_empty_full_sweep(env: Env) -> None:
    with pytest.raises(ConfigurationError):
        _ = SweeperSettings(batch_size=0)
    with pytest.raises(ConfigurationError):
        _ = SweeperSettings(slot=-1)
    with pytest.raises(ConfigurationError):
        _ = SweeperSettings(finalize_grace=timedelta(seconds=-1))
    result = await sweeper(env).sweep()
    assert result.leases == 0
    assert result.retained == 0
