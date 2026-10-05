"""Накопленная история: терминальные деревья, вставленные SQL-генератором (P-04, P-11).

Выполнять миллионы Items через воркеры ради истории незачем: по COUNTERS §4.1 история —
это уже лежащие в таблицах терминальные Items, поверх которых идёт горячая нагрузка. Строки
повторяют то, что оставляет финализированный батч ``bench.history``: корень ``succeeded`` с
``finished_at`` в прошлом, Items ``ok`` и слот счётчиков. Идентификаторы — UUIDv7 с моментами
в прошлом, поэтому история лежит левее горячего края индексов, как настоящая.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final

from benchmarks.app import KIND_HISTORY
from benchmarks.stand import ident, sql
from tallyho.model.states import BatchState, ItemState

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["load_history"]

_CHUNK: Final = 500_000

_UUID7: Final = """
CREATE OR REPLACE FUNCTION {uuid7}(ms bigint, n bigint, variant text) RETURNS uuid
LANGUAGE sql IMMUTABLE AS $$
    SELECT (lpad(to_hex(ms), 12, '0') || '7' || lpad(to_hex(n % 4096), 3, '0')
            || variant || lpad(to_hex(n), 15, '0'))::uuid
$$
"""
_BATCHES: Final = """
INSERT INTO {batch} (id, root_id, parent_id, kind, key, state, options, hooks,
                     on_feeder_failed, snap_seq, hook_attempts, retention, release_required,
                     expected_total, created_at, updated_at, finished_at)
SELECT {uuid7}(CAST(:base_ms AS bigint) + g, g, 'b'),
       {uuid7}(CAST(:base_ms AS bigint) + g, g, 'b'),
       NULL, :kind, 'h:' || g, :state, '{{}}'::jsonb, '{{}}'::text[], 0, 0, 0,
       CAST(:retention AS interval), false, :items,
       CAST(:base AS timestamptz) + make_interval(secs => g / 1000.0),
       CAST(:finished AS timestamptz), CAST(:finished AS timestamptz)
FROM generate_series(1, :trees) AS g
"""
_COUNTERS: Final = """
INSERT INTO {counter} (batch_id, slot, total, ok, w_total, w_done, tree_total)
SELECT {uuid7}(CAST(:base_ms AS bigint) + g, g, 'b'), 0, :items, :items, :items, :items, :items
FROM generate_series(1, :trees) AS g
"""
_ITEMS: Final = """
INSERT INTO {item} (id, batch_id, state, label, attempt, depth, task_name, payload, weight,
                    created_at, finished_at)
SELECT {uuid7}(CAST(:base_ms AS bigint) + g / 100, g, 'a'),
       {uuid7}(CAST(:base_ms AS bigint) + ((g - 1) / :items + 1), (g - 1) / :items + 1, 'b'),
       :state, 'done', 0, 0, 'bench.history', '\\x'::bytea, 1,
       CAST(:base AS timestamptz) + make_interval(secs => g / 100000.0),
       CAST(:finished AS timestamptz)
FROM generate_series(CAST(:first AS bigint), CAST(:last AS bigint)) AS g
"""


async def load_history(
    engine: AsyncEngine,
    schema: str,
    *,
    trees: int,
    items_per_tree: int,
    retention: timedelta | None,
    finished_ago: timedelta,
) -> float:
    """Вставить ``trees`` терминальных корней по ``items_per_tree`` Items.

    Returns:
        Секунды на вставку и ``VACUUM ANALYZE``.
    """
    started = time.monotonic()
    names = {
        "uuid7": ident(schema, "bench_uuid7"),
        "batch": ident(schema, "th_batch"),
        "counter": ident(schema, "th_counter"),
        "item": ident(schema, "th_item"),
    }
    finished = datetime.now(UTC) - finished_ago
    base = finished - timedelta(hours=1)
    common = {"base_ms": int(base.timestamp() * 1000), "items": items_per_tree, "trees": trees}
    async with engine.begin() as connection:
        _ = await connection.execute(sql(_UUID7, **names))
        _ = await connection.execute(
            sql(_BATCHES, **names),
            {
                **common,
                "kind": KIND_HISTORY,
                "state": int(BatchState.SUCCEEDED),
                "retention": retention,
                "base": base,
                "finished": finished,
            },
        )
        _ = await connection.execute(sql(_COUNTERS, **names), common)
    total = trees * items_per_tree
    for first in range(1, total + 1, _CHUNK):
        async with engine.begin() as connection:
            _ = await connection.execute(
                sql(_ITEMS, **names),
                {
                    **common,
                    "state": int(ItemState.OK),
                    "base": base,
                    "finished": finished,
                    "first": first,
                    "last": min(first + _CHUNK - 1, total),
                },
            )
    async with engine.connect() as raw:
        connection = await raw.execution_options(isolation_level="AUTOCOMMIT")
        for table in ("th_batch", "th_item", "th_counter"):
            _ = await connection.execute(sql("VACUUM (ANALYZE) {t}", t=ident(schema, table)))
    return time.monotonic() - started
