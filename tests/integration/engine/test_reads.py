"""Read-side integration tests (T4.11, UC-13 and UC-14)."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import delete, event, insert, update

from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.reads import Reads
from tallyho.model.calls import TaskCall
from tallyho.model.errors import BatchPurged, ConfigurationError, NotFoundError
from tallyho.model.states import CancelReason, ItemState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, upsert_metrics, upsert_slots
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.engine import Connection

    from tests.integration.engine.conftest import Env

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)


@contextlib.contextmanager
def statements_of(env: Env) -> Generator[list[str]]:
    """Collect SQL statements sent through the test engine."""
    statements: list[str] = []

    def record(_conn: Connection, _cursor: object, statement: str, *args: object) -> None:
        del args
        statements.append(statement)

    event.listen(env.engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(env.engine.sync_engine, "before_cursor_execute", record)


def reader(env: Env) -> Reads:
    """Create the read side over the test schema."""
    return Reads(schema_engine(env), env.tables, SystemClock())


async def tree(env: Env, *, children: int) -> tuple[UUID, list[UUID]]:
    """Create a root with direct children and return their ids."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail", key=f"job:{children}"))
        child_ids = [
            (
                await env.producer.create_sub_batch(
                    conn, root.id, SubBatchSpec(key=f"part-{index}")
                )
            ).id
            for index in range(children)
        ]
    return root.id, child_ids


@pytest.mark.parametrize("children", [0, 8])
async def test_view_uses_one_statement_independent_of_tree_size(env: Env, children: int) -> None:
    root_id, _ = await tree(env, children=children)

    with statements_of(env) as statements:
        view = await reader(env).view(root_id)

    assert view.id == root_id
    assert len(view.children) == children
    assert len(statements) == 1


async def test_summaries_read_multiple_trees_in_one_statement(env: Env) -> None:
    first, _ = await tree(env, children=1)
    second, _ = await tree(env, children=2)

    with statements_of(env) as statements:
        summaries = await reader(env).summaries([first, second], next_seq=True)

    assert set(summaries) == {first, second}
    assert {summary.seq for summary in summaries.values()} == {1}
    assert len(statements) == 1


async def test_view_aggregates_counters_metrics_feeds_and_leases(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="pipeline", key="p:1"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        target = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="target", expected_total=10)
        )
        _ = await env.producer.add_items(
            conn,
            target.id,
            [TaskCall(task_name="deliver", key="a"), TaskCall(task_name="deliver", key="b")],
        )
        await env.producer.add_feed(conn, target.id, [source.id])
        item = env.tables.item
        item_id = await conn.scalar(
            item.select().with_only_columns(item.c.id).where(item.c.batch_id == target.id)
        )
        assert item_id is not None
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=item_id,
                batch_id=target.id,
                lease_until=NOW + timedelta(seconds=60),
                worker_id="w1",
                attempt=1,
                progress_done=3,
                progress_total=7,
            )
        )
        await upsert_slots(
            conn,
            env.tables,
            {(target.id, 11): CounterDelta(ok=1, w_done=1, duplicates=2)},
        )
        await upsert_metrics(conn, env.tables, {(target.id, "sent", 11): 4})
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == target.id)
            .values(cancel_requested_at=NOW, cancel_reason=CancelReason.CANCEL.value)
        )

    reads = reader(env)
    view = await reads.view(target.id)
    summary = await reads.summary(target.id)

    assert view.progress.found == 2
    assert view.progress.ok == 1
    assert view.progress.in_flight == 1
    assert view.progress.queued == 0
    assert view.progress.duplicates == 2
    assert view.progress.expected == 10
    assert view.metrics == {"sent": 4}
    assert view.labels == {"sent": 4}
    assert view.cancel_requested
    assert view.reason is CancelReason.CANCEL
    assert summary.progress == view.progress
    assert summary.metrics == view.metrics


async def test_batch_purged_is_one_statement_and_stable(env: Env) -> None:
    missing = UUID(int=42)

    with statements_of(env) as first, pytest.raises(BatchPurged) as caught:
        _ = await reader(env).view(missing)
    with statements_of(env) as second, pytest.raises(BatchPurged):
        _ = await reader(env).view(missing)

    assert caught.value.batch_id == missing
    assert len(first) == len(second) == 1


async def test_in_flight_marked_items_and_lookups(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail", key="campaign:1"))
        child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="send"))
        _ = await env.producer.add_items(
            conn,
            child.id,
            [TaskCall(task_name="deliver", key="a"), TaskCall(task_name="deliver", key="b")],
        )
        item = env.tables.item
        ids = list(
            await conn.scalars(
                item.select()
                .with_only_columns(item.c.id)
                .where(item.c.batch_id == child.id)
                .order_by(item.c.id)
            )
        )
        _ = await conn.execute(
            update(item)
            .where(item.c.id.in_(ids))
            .values(
                state=int(ItemState.ERROR), label="bounce", error={"code": 550}, finished_at=NOW
            )
        )
        _ = await conn.execute(
            insert(env.tables.item_mark),
            [{"batch_id": child.id, "label": "bounce", "item_id": item_id} for item_id in ids],
        )
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=ids[0],
                batch_id=child.id,
                lease_until=NOW + timedelta(seconds=60),
                worker_id="worker",
                attempt=2,
            )
        )

    reads = Reads(
        schema_engine(env),
        env.tables,
        SystemClock(),
        item_page_size=1,
    )
    assert await reads.find("mail", "campaign:1") == root.id
    assert await reads.child(root.id, "send") == child.id
    leases = await reads.in_flight(child.id, limit=1)
    marked = [entry async for entry in reads.items(child.id, label="bounce")]

    assert [entry.id for entry in marked] == ids
    assert all(entry.state is ItemState.ERROR for entry in marked)
    assert leases[0].id == ids[0]
    assert leases[0].worker_id == "worker"
    with pytest.raises(NotFoundError):
        _ = await reads.find("mail", "absent")
    with pytest.raises(NotFoundError):
        _ = await reads.child(root.id, "absent")

    async with env.transaction() as conn:
        _ = await conn.execute(
            delete(env.tables.batch).where(env.tables.batch.c.root_id == root.id)
        )
    with pytest.raises(BatchPurged):
        _ = [entry async for entry in reads.items(child.id, label="bounce")]
    with pytest.raises(BatchPurged):
        _ = await reads.in_flight(child.id)


async def test_read_limits_are_validated(env: Env) -> None:
    with pytest.raises(ConfigurationError):
        _ = Reads(schema_engine(env), env.tables, SystemClock(), item_page_size=0)
    with pytest.raises(ConfigurationError):
        _ = Reads(
            schema_engine(env),
            env.tables,
            SystemClock(),
            lease_duration=timedelta(0),
        )

    root_id, _ = await tree(env, children=0)
    with pytest.raises(ConfigurationError):
        _ = await reader(env).in_flight(root_id, limit=0)
