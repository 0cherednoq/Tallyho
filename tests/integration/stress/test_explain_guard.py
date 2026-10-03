"""EXPLAIN guard for the million-row hot path (COUNTERS section 4.2)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast, final
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import text

from tallyho import Tallyho
from tallyho.storage.hot_queries import HOT_QUERIES, HotQueryProbe
from tallyho.storage.tables import build_metadata
from tests.helpers.db import schema_connection

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping

    from sqlalchemy.engine import Dialect
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql.elements import ClauseElement

    from tallyho.storage.hot_queries import HotQuery
    from tallyho.storage.tables import Tables

__all__: list[str] = []

pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.slow,
    pytest.mark.timeout(300),
]

ITEM_COUNT = 1_000_000
BATCH_COUNT = 10_000
CHILD_COUNT = 30_000
DELTA_COUNT = 100_000
FORBIDDEN_SEQ_SCAN = frozenset(
    {"th_item", "th_batch", "th_counter", "th_batch_attr", "th_metric", "th_counter_delta"}
)


@dataclass(frozen=True, slots=True)
class _PopulatedDatabase:
    engine: AsyncEngine
    schema: str
    tables: Tables
    probe: HotQueryProbe


@final
class _PlanRegressionError(AssertionError):
    pass


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def populated_database(
    postgres_dsn: str,
) -> AsyncIterator[_PopulatedDatabase]:
    from sqlalchemy.ext.asyncio import create_async_engine  # ruff: ignore[import-outside-top-level]  # module-scoped engine belongs to this expensive fixture

    from tests.helpers.db import temporary_schema  # ruff: ignore[import-outside-top-level]  # module-scoped schema cannot use function fixture

    engine = create_async_engine(postgres_dsn)
    try:
        async with temporary_schema(engine) as schema:
            th = Tallyho(engine, schema=schema)
            await th.migrate()
            async with schema_connection(engine, schema) as connection:
                await _set_search_path(connection, schema)
                await _populate(connection)
            yield _PopulatedDatabase(engine, schema, build_metadata(), _probe())
    finally:
        await engine.dispose()


def _probe() -> HotQueryProbe:
    return HotQueryProbe(
        batch_id=UUID("00000000-0000-0000-0000-000000000001"),
        root_id=UUID("00000000-0000-0000-0000-000000000001"),
        parent_id=UUID("00000000-0000-0000-0000-000000000001"),
        item_id=UUID("10000000-0000-0000-0000-000000000001"),
        kind="explain-root",
        key="1",
        cutoff=datetime(2040, 1, 1, tzinfo=UTC),
    )


async def _populate(connection: AsyncConnection) -> None:
    await connection.execute(
        text(
            """
            INSERT INTO th_batch (
                id, root_id, parent_id, kind, key, state, options, hooks,
                on_feeder_failed, snap_seq, hook_attempts, retention,
                release_required, created_at, updated_at, finished_at, deadline_at
            )
            SELECT
                ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                NULL,
                'explain-root', g::text,
                CASE WHEN g <= 100 THEN 0 ELSE 10 END,
                '{}'::jsonb,
                CASE WHEN g <= 100 THEN ARRAY['progress']::text[] ELSE '{}'::text[] END,
                0, 0, 0, interval '14 days', false,
                timestamptz '2030-01-01', timestamptz '2030-01-01',
                CASE WHEN g BETWEEN 101 AND 200 THEN timestamptz '2030-01-02' ELSE NULL END,
                CASE WHEN g <= 100 THEN timestamptz '2030-01-03' ELSE NULL END
            FROM generate_series(1, :batches) AS g
            """
        ),
        {"batches": BATCH_COUNT},
    )
    await connection.execute(
        text(
            """
            INSERT INTO th_batch (
                id, root_id, parent_id, kind, key, state, options, hooks,
                on_feeder_failed, snap_seq, hook_attempts, retention,
                release_required, created_at, updated_at
            )
            SELECT
                ('01000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                concat(
                    '00000000-0000-0000-0000-',
                    lpad((((g - 1) % :batches) + 1)::text, 12, '0')
                )::uuid,
                concat(
                    '00000000-0000-0000-0000-',
                    lpad((((g - 1) % :batches) + 1)::text, 12, '0')
                )::uuid,
                'explain-child', 'child-' || g::text, 10, '{}'::jsonb, '{}'::text[],
                0, 0, 0, interval '14 days', false,
                timestamptz '2030-01-01', timestamptz '2030-01-02'
            FROM generate_series(1, :children) AS g
            """
        ),
        {"batches": BATCH_COUNT, "children": CHILD_COUNT},
    )
    await connection.execute(
        text(
            """
            INSERT INTO th_item (
                id, batch_id, state, attempt, depth, task_name, payload, key,
                weight, created_at
            )
            SELECT
                concat(
                    '10000000-0000-0000-', lpad((g / 100000000)::text, 4, '0'),
                    '-', lpad(g::text, 12, '0')
                )::uuid,
                concat(
                    '00000000-0000-0000-0000-',
                    lpad((((g - 1) % :batches) + 1)::text, 12, '0')
                )::uuid,
                0, 0, 0, 'explain.task', '\\x'::bytea, g::text, 1,
                timestamptz '2030-01-01'
            FROM generate_series(1, :items) AS g
            """
        ),
        {"batches": BATCH_COUNT, "items": ITEM_COUNT},
    )
    await connection.execute(
        text(
            """
            INSERT INTO th_counter (batch_id, slot)
            SELECT
                ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                slot
            FROM generate_series(1, :batches) AS g
            CROSS JOIN generate_series(0, 7) AS slot
            """
        ),
        {"batches": BATCH_COUNT},
    )
    await connection.execute(
        text(
            """
            INSERT INTO th_metric (batch_id, name, slot, value)
            SELECT
                ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                name,
                slot,
                1
            FROM generate_series(1, :batches) AS g
            CROSS JOIN generate_series(-2, 7) AS slot
            CROSS JOIN unnest(ARRAY['ok', 'error', 'skip']) AS name
            """
        ),
        {"batches": BATCH_COUNT},
    )
    # Несвёрнутые дельты пути B после долгой недоступности Completer.
    await connection.execute(
        text(
            """
            INSERT INTO th_counter_delta (batch_id, d_ok, created_at)
            SELECT
                concat(
                    '00000000-0000-0000-0000-',
                    lpad((((g - 1) % :batches) + 1)::text, 12, '0')
                )::uuid,
                1,
                timestamptz '2030-01-01'
            FROM generate_series(1, :deltas) AS g
            """
        ),
        {"batches": BATCH_COUNT, "deltas": DELTA_COUNT},
    )
    await connection.execute(
        text(
            """
            INSERT INTO th_batch_attr (batch_id, attributes)
            SELECT
                ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                jsonb_build_object('key', g::text, 'tenant', 'tenant-' || (g % 50)::text)
            FROM generate_series(1, :batches) AS g
            """
        ),
        {"batches": BATCH_COUNT},
    )
    await connection.execute(text("ANALYZE th_batch_attr"))
    await connection.execute(text("ANALYZE th_batch"))
    await connection.execute(text("ANALYZE th_item"))
    await connection.execute(text("ANALYZE th_counter"))
    await connection.execute(text("ANALYZE th_metric"))
    await connection.execute(text("ANALYZE th_counter_delta"))
    await connection.commit()


async def _set_search_path(connection: AsyncConnection, schema: str) -> None:
    quoted = '"' + schema.replace('"', '""') + '"'
    await connection.execute(text(f"SET LOCAL search_path TO {quoted}"))


def _sql(statement: ClauseElement, dialect: Dialect) -> str:
    compiled = statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    return str(compiled)


async def _plan(connection: AsyncConnection, query: HotQuery) -> Mapping[str, object]:
    sql = _sql(query.statement, connection.dialect)
    result = await connection.execute(text(f"EXPLAIN (FORMAT JSON) {sql}"))
    raw: object = result.scalar_one()
    decoded = cast("object", json.loads(raw)) if isinstance(raw, str) else raw
    assert isinstance(decoded, list)
    documents = cast("list[object]", decoded)
    assert documents
    top = documents[0]
    assert isinstance(top, dict)
    document = cast("Mapping[str, object]", top)
    plan = document.get("Plan")
    assert isinstance(plan, dict)
    return cast("Mapping[str, object]", plan)


def _nodes(plan: Mapping[str, object]) -> Iterator[Mapping[str, object]]:
    yield plan
    children = plan.get("Plans", [])
    assert isinstance(children, list)
    for child in cast("list[object]", children):
        assert isinstance(child, dict)
        yield from _nodes(cast("Mapping[str, object]", child))


def _guard(query: HotQuery, plan: Mapping[str, object]) -> None:
    for node in _nodes(plan):
        relation = node.get("Relation Name")
        node_type = node.get("Node Type")
        rows = node.get("Plan Rows")
        if node_type == "Seq Scan" and relation in FORBIDDEN_SEQ_SCAN:
            message = f"{query.name}: sequential scan on {relation}"
            raise _PlanRegressionError(message)
        if isinstance(rows, int) and rows > query.max_plan_rows:
            message = f"{query.name}: planner estimates {rows} rows in {node_type}"
            raise _PlanRegressionError(message)


async def test_hot_path_plans_have_no_large_or_sequential_scans(
    populated_database: _PopulatedDatabase,
) -> None:
    database = populated_database
    queries = HOT_QUERIES.build(database.tables, database.probe)
    assert len(queries) == 16
    assert len({query.name for query in queries}) == len(queries)
    async with schema_connection(database.engine, database.schema) as connection:
        await _set_search_path(connection, database.schema)
        for query in queries:
            _guard(query, await _plan(connection, query))


async def test_guard_rejects_a_removed_hot_path_index(
    populated_database: _PopulatedDatabase,
) -> None:
    database = populated_database
    query = next(
        query
        for query in HOT_QUERIES.build(database.tables, database.probe)
        if query.name == "item.by_batch"
    )
    async with schema_connection(database.engine, database.schema) as connection:
        await _set_search_path(connection, database.schema)
        await connection.execute(text("DROP INDEX th_item_batch_idx"))
        with pytest.raises(_PlanRegressionError, match="sequential scan on th_item"):
            _guard(query, await _plan(connection, query))
