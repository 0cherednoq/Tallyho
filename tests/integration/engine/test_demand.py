"""th.demand: спрос активных батчей одним запросом (RM4.1, UC-15)."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import event, insert, update

from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.reads import Reads
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, insert_delta
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from collections.abc import Generator
    from uuid import UUID

    from sqlalchemy.engine import Connection

    from tests.integration.engine.conftest import Env

__all__: list[str] = []

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)


def reader(env: Env) -> Reads:
    return Reads(schema_engine(env), env.tables, SystemClock())


@contextlib.contextmanager
def selects_of(env: Env) -> Generator[list[str]]:
    """Собрать SELECT-запросы, прошедшие через тестовый движок."""
    statements: list[str] = []

    def record(_c: Connection, _cur: object, statement: str, *args: object) -> None:
        del args
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(env.engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(env.engine.sync_engine, "before_cursor_execute", record)


async def make_root(
    env: Env,
    kind: str,
    key: str,
    *,
    items: int = 0,
    attributes: dict[str, str | int | bool] | None = None,
) -> UUID:
    async with env.transaction() as conn:
        root = await env.producer.create_root(
            conn, RootSpec(kind=kind, key=key, attributes=attributes or {})
        )
        if items:
            _ = await env.producer.add_items(
                conn, root.id, [TaskCall(task_name="t", args=(i,)) for i in range(items)]
            )
    return root.id


async def test_demand_matches_view_counters(env: Env) -> None:
    root_id = await make_root(env, "mail", "m:1", items=5)
    # Один Item выполняется (lease), один отложен (outbox defer_reason).
    item = env.tables.item
    async with env.transaction() as conn:
        ids = list(await conn.scalars(item.select().with_only_columns(item.c.id)))
        _ = await conn.execute(
            insert(env.tables.lease).values(
                item_id=ids[0],
                batch_id=root_id,
                lease_until=NOW + timedelta(seconds=60),
                worker_id="w1",
                attempt=1,
            )
        )
        _ = await conn.execute(
            update(env.tables.outbox)
            .where(env.tables.outbox.c.item_id == ids[1])
            .values(defer_reason="нет кредитов")
        )

    reads = reader(env)
    view = await reads.view(root_id)
    (row,) = await reads.demand(["mail"])
    assert row.id == root_id
    assert row.pending == view.progress.found - view.progress.ok - view.progress.error
    assert row.in_flight == view.progress.in_flight == 1
    assert row.deferred == 1
    assert row.max_in_flight is None
    assert row.paused is False
    assert row.truncated is False


async def test_demand_counts_unfolded_deltas(env: Env) -> None:
    root_id = await make_root(env, "mail", "m:2", items=4)
    async with env.transaction() as conn:
        # Две задачи завершены дельтой пути B, ещё не свёрнутой в th_counter.
        _ = await insert_delta(
            conn, env.tables, {root_id: CounterDelta(ok=2, w_done=2)}, created_at=NOW
        )
    (row,) = await reader(env).demand(["mail"])
    # found=4, done=2 (из несвёрнутой дельты) → pending=2.
    assert row.pending == 2


async def test_demand_reflects_window_and_pause(env: Env) -> None:
    closed = await make_root(env, "limited", "l:0", items=1)
    unbounded = await make_root(env, "limited", "l:n", items=1)
    batch = env.tables.batch
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(batch).where(batch.c.id == closed).values(max_in_flight=0, paused_at=NOW)
        )
    rows = {r.id: r for r in await reader(env).demand(["limited"])}
    assert rows[closed].max_in_flight == 0
    assert rows[closed].paused is True
    assert rows[unbounded].max_in_flight is None
    assert rows[unbounded].paused is False


async def test_demand_filters_by_root_attributes_for_sub_batches(env: Env) -> None:
    async with env.transaction() as conn:
        de = await env.producer.create_root(
            conn, RootSpec(kind="pool", key="de", attributes={"region": "DE"})
        )
        fr = await env.producer.create_root(
            conn, RootSpec(kind="pool", key="fr", attributes={"region": "FR"})
        )
        de_child = await env.producer.create_sub_batch(
            conn, de.id, SubBatchSpec(key="stage", kind="pool")
        )
        _ = await env.producer.create_sub_batch(conn, fr.id, SubBatchSpec(key="stage", kind="pool"))

    rows = await reader(env).demand(["pool"], attributes={"region": "DE"})
    ids = {r.id for r in rows}
    # И корень DE, и его под-батч; батчи FR отфильтрованы.
    assert de.id in ids
    assert de_child.id in ids
    assert fr.id not in ids
    # Атрибуты корня видны и у под-батча.
    child_row = next(r for r in rows if r.id == de_child.id)
    assert child_row.attributes == {"region": "DE"}


async def test_demand_query_count_is_independent_of_batch_count(env: Env) -> None:
    for index in range(6):
        _ = await make_root(env, "many", f"k:{index}", items=2)
    with selects_of(env) as statements:
        rows = await reader(env).demand(["many"])
    assert len(rows) == 6
    assert len(statements) == 1


async def test_demand_sets_truncated_at_limit(env: Env) -> None:
    for index in range(3):
        _ = await make_root(env, "cap", f"c:{index}")
    rows = await reader(env).demand(["cap"], limit=2)
    assert len(rows) == 2
    assert all(r.truncated for r in rows)
    full = await reader(env).demand(["cap"], limit=10)
    assert len(full) == 3
    assert not any(r.truncated for r in full)


async def test_demand_excludes_terminal_batches(env: Env) -> None:
    active = await make_root(env, "mix", "a")
    done = await make_root(env, "mix", "d")
    batch = env.tables.batch
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(batch).where(batch.c.id == done).values(state=int(BatchState.SUCCEEDED))
        )
    ids = {r.id for r in await reader(env).demand(["mix"])}
    assert ids == {active}


async def test_demand_requires_kinds(env: Env) -> None:
    with pytest.raises(ConfigurationError, match="kind"):
        _ = await reader(env).demand([])
    with pytest.raises(ConfigurationError, match="kind"):
        _ = await reader(env).demand("mail")  # строка вместо коллекции
    with pytest.raises(ConfigurationError, match="limit"):
        _ = await reader(env).demand(["mail"], limit=0)
