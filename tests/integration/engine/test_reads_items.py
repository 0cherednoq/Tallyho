"""``Reads.items(states=, labels=)``: окна, пересечение фильтров, ошибки (A-AT-08)."""

from __future__ import annotations

import contextlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from sqlalchemy import delete, event, insert, text, update

from tallyho.engine.producer import RootSpec, SubBatchSpec
from tallyho.engine.reads import Reads
from tallyho.model.calls import TaskCall
from tallyho.model.errors import BatchPurged, ConfigurationError
from tallyho.model.states import ItemState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.item_scan import item_window_statement
from tests.integration.engine.completer_env import schema_engine

if TYPE_CHECKING:
    from collections.abc import Collection, Generator, Iterator, Mapping

    from sqlalchemy.engine import Connection

    from tests.integration.engine.conftest import Env

NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)


@contextlib.contextmanager
def statements_of(env: Env) -> Generator[list[str]]:
    """Собрать SQL, отправленный через движок теста."""
    statements: list[str] = []

    def record(_conn: Connection, _cursor: object, statement: str, *args: object) -> None:
        del args
        statements.append(statement)

    event.listen(env.engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(env.engine.sync_engine, "before_cursor_execute", record)


def reader(env: Env, *, window: int = 3, page: int = 2) -> Reads:
    return Reads(
        schema_engine(env),
        env.tables,
        SystemClock(),
        item_page_size=page,
        items_scan_window=window,
    )


async def batch_with_items(env: Env, count: int) -> tuple[UUID, list[UUID]]:
    """Корень с ``count`` активными Items; id возвращаются по возрастанию."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail", key=f"items:{count}"))
        _ = await env.producer.add_items(
            conn,
            root.id,
            [TaskCall(task_name="deliver", key=f"k{index}") for index in range(count)],
        )
        item = env.tables.item
        ids = list(
            await conn.scalars(
                item.select()
                .with_only_columns(item.c.id)
                .where(item.c.batch_id == root.id)
                .order_by(item.c.id)
            )
        )
    return root.id, ids


async def finish(  # ruff: ignore[too-many-positional-arguments]  # тестовый хелпер: батч, Items, состояние
    env: Env,
    batch_id: UUID,
    ids: Collection[UUID],
    state: ItemState,
    *,
    label: str | None = None,
    mark: bool = False,
) -> None:
    """Перевести Items в терминальное состояние напрямую, как это делает finish."""
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(item)
            .where(item.c.id.in_(ids))
            .values(state=int(state), label=label, finished_at=NOW)
        )
        if mark:
            _ = await conn.execute(
                insert(env.tables.item_mark),
                [{"batch_id": batch_id, "label": label, "item_id": item_id} for item_id in ids],
            )


async def collect(
    reads: Reads,
    batch_id: UUID,
    *,
    states: Collection[ItemState] | None = None,
    labels: Collection[str] | None = None,
) -> list[UUID]:
    return [entry.id async for entry in reads.items(batch_id, states=states, labels=labels)]


async def test_cancelled_items_are_listed_across_windows(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 10)
    cancelled = [ids[0], ids[4], ids[9]]
    await finish(env, batch_id, cancelled, ItemState.CANCELLED, label="cancelled")

    with statements_of(env) as statements:
        found = await collect(reader(env), batch_id, states={ItemState.CANCELLED})

    assert found == cancelled
    # 1 проверка существования + окна по 3 строки: 3, 3, 3, 1.
    assert len(statements) == 5


async def test_window_edge_moves_without_matches(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 7)
    await finish(env, batch_id, [ids[-1]], ItemState.ERROR, label="bad")

    assert await collect(reader(env), batch_id, states=[ItemState.ERROR]) == [ids[-1]]
    assert await collect(reader(env), batch_id, states=[ItemState.SKIP]) == []


async def test_exact_multiple_of_window_ends_with_empty_window(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 6)
    await finish(env, batch_id, ids, ItemState.CANCELLED)

    with statements_of(env) as statements:
        found = await collect(reader(env), batch_id, states={ItemState.CANCELLED})

    assert found == ids
    # Существование + два полных окна + пустое окно, которое и сообщает о конце.
    assert len(statements) == 4


async def test_several_states_and_duplicates_in_filter(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 5)
    await finish(env, batch_id, ids[:2], ItemState.ERROR, label="bad")
    await finish(env, batch_id, ids[2:3], ItemState.CANCELLED)
    await finish(env, batch_id, ids[3:4], ItemState.OK, label="ok")

    found = await collect(
        reader(env),
        batch_id,
        states=[ItemState.ERROR, ItemState.CANCELLED, ItemState.ERROR],
    )

    assert found == ids[:3]


async def test_labels_walk_marks_page_by_page(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 7)
    await finish(env, batch_id, ids[:3], ItemState.ERROR, label="bounce", mark=True)
    await finish(env, batch_id, ids[3:5], ItemState.ERROR, label="rejected", mark=True)
    await finish(env, batch_id, ids[5:6], ItemState.ERROR, label="unmarked")

    reads = reader(env)
    assert await collect(reads, batch_id, labels=["bounce"]) == ids[:3]
    both = await collect(reads, batch_id, labels=["rejected", "bounce", "rejected"])
    assert both == [*ids[3:5], *ids[:3]]
    assert await collect(reads, batch_id, labels=("absent",)) == []
    # Непомеченный Item по метке не находится, но находится по состоянию.
    assert await collect(reads, batch_id, labels=["unmarked"]) == []
    assert ids[5] in await collect(reads, batch_id, states={ItemState.ERROR})


async def test_states_and_labels_intersect(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 4)
    await finish(env, batch_id, ids[:3], ItemState.ERROR, label="bounce", mark=True)
    # Повтор вернул один помеченный Item в работу: метка в th_item_mark осталась.
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id == ids[1]).values(state=int(ItemState.ACTIVE))
        )

    reads = reader(env)
    assert await collect(reads, batch_id, labels=["bounce"]) == ids[:3]
    errors = await collect(reads, batch_id, states={ItemState.ERROR}, labels=["bounce"])
    assert errors == [ids[0], ids[2]]
    assert await collect(reads, batch_id, states={ItemState.OK}, labels=["bounce"]) == []


async def test_virtual_item_is_returned_as_is(env: Env) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="mail", key="virtual"))
        child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="send"))

    found = [entry async for entry in reader(env).items(root.id, states={ItemState.ACTIVE})]

    assert [entry.child_batch_id for entry in found] == [child.id]
    assert found[0].key is None


async def test_item_view_fields(env: Env) -> None:
    batch_id, ids = await batch_with_items(env, 1)
    item = env.tables.item
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(item)
            .where(item.c.id == ids[0])
            .values(
                state=int(ItemState.ERROR),
                label="bounce",
                attempt=3,
                error={"code": 550},
                finished_at=NOW,
            )
        )

    (view,) = [entry async for entry in reader(env).items(batch_id, states={ItemState.ERROR})]

    assert (view.id, view.batch_id, view.state) == (ids[0], batch_id, ItemState.ERROR)
    assert (view.task_name, view.label, view.attempt, view.key) == ("deliver", "bounce", 3, "k0")
    assert view.error == {"code": 550}
    assert view.result is None
    assert view.finished_at == NOW
    assert view.created_at is not None


@pytest.mark.parametrize(
    ("states", "labels"),
    [
        (None, None),
        ((), None),
        (None, []),
        (None, "bounce"),
        (None, b"bounce"),
        (None, [1]),
        ("error", None),
        ([12], None),
        (ItemState.ERROR, None),
        ({ItemState.ERROR}, 5),
    ],
    ids=repr,
)
def test_bad_filters_fail_before_any_query(env: Env, states: object, labels: object) -> None:
    reads = reader(env)
    with statements_of(env) as statements, pytest.raises(ConfigurationError):
        _ = reads.items(
            UUID(int=1),  # до проверки фильтров id не используется
            states=cast("Collection[ItemState] | None", states),
            labels=cast("Collection[str] | None", labels),
        )
    assert statements == []


async def test_purged_batch(env: Env) -> None:
    batch_id, _ = await batch_with_items(env, 1)
    async with env.transaction() as conn:
        _ = await conn.execute(delete(env.tables.batch).where(env.tables.batch.c.id == batch_id))

    reads = reader(env)
    with pytest.raises(BatchPurged):
        _ = await collect(reads, batch_id, states={ItemState.ERROR})
    with pytest.raises(BatchPurged):
        _ = await collect(reads, batch_id, labels=["bounce"])


def test_scan_window_must_be_positive(env: Env) -> None:
    with pytest.raises(ConfigurationError):
        _ = Reads(schema_engine(env), env.tables, SystemClock(), items_scan_window=0)


def _nodes(plan: Mapping[str, object]) -> Iterator[Mapping[str, object]]:
    yield plan
    for child in cast("list[Mapping[str, object]]", plan.get("Plans", [])):
        yield from _nodes(child)


async def test_one_statement_reads_at_most_one_window(env: Env) -> None:
    batch_id, _ = await batch_with_items(env, 40)
    window = 7
    statement = item_window_statement(
        env.tables, batch_id=batch_id, states=[ItemState.CANCELLED], after=None, window=window
    )
    async with env.transaction() as conn:
        # На 40 строках планировщик выбрал бы Seq Scan; план боевого размера — по индексу.
        _ = await conn.execute(text("SET LOCAL enable_seqscan = off"))
        quoted = '"' + env.schema.replace('"', '""') + '"'
        _ = await conn.execute(text(f"SET LOCAL search_path TO {quoted}"))
        sql = str(statement.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True}))
        raw: object = (
            await conn.execute(text(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"))
        ).scalar_one()
    document = cast(
        "list[Mapping[str, Mapping[str, object]]]",
        json.loads(raw) if isinstance(raw, str) else raw,
    )
    scans = [
        node
        for node in _nodes(document[0]["Plan"])
        if node.get("Relation Name") == env.tables.item.name
    ]

    assert scans
    assert all(node["Node Type"] != "Seq Scan" for node in scans)
    assert all(cast("int", node["Actual Rows"]) <= window for node in scans)
