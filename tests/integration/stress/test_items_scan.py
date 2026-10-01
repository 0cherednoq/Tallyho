"""A-AT-09: ``items(states=)`` на батче в миллион Items с редкими совпадениями."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho
from tallyho.engine.reads import DEFAULT_ITEMS_SCAN_WINDOW, Reads
from tallyho.model.states import ItemState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.tables import build_metadata
from tests.helpers.db import schema_connection, temporary_schema

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

__all__: list[str] = []

pytestmark = [pytest.mark.slow, pytest.mark.timeout(300)]

ITEM_COUNT = 1_000_000
MATCH_EVERY = 1_000  # 0,1% совпадений
BATCH_ID = UUID("00000000-0000-0000-0000-000000000001")
# Запас к statement_timeout: один statement читает одно окно, а не остаток батча.
MAX_STATEMENT_SECONDS = 2.0


async def test_rare_states_are_found_in_bounded_statements(postgres_dsn: str) -> None:
    engine = create_async_engine(postgres_dsn)
    durations: list[float] = []
    started: dict[int, float] = {}

    def before(_conn: Connection, cursor: object, *args: object) -> None:
        del args
        started[id(cursor)] = time.perf_counter()

    def after(_conn: Connection, cursor: object, *args: object) -> None:
        del args
        durations.append(time.perf_counter() - started.pop(id(cursor)))

    try:
        async with temporary_schema(engine) as schema:
            _ = await Tallyho(engine, schema=schema).migrate()
            async with schema_connection(engine, schema) as connection:
                quoted = '"' + schema.replace('"', '""') + '"'
                _ = await connection.execute(text(f"SET LOCAL search_path TO {quoted}"))
                _ = await connection.execute(
                    text(
                        """
                        INSERT INTO th_batch (
                            id, root_id, kind, state, options, hooks, on_feeder_failed,
                            snap_seq, hook_attempts, release_required, created_at, updated_at
                        ) VALUES (
                            :batch, :batch, 'scan', 12, '{}'::jsonb, '{}'::text[], 0,
                            0, 0, false, now(), now()
                        )
                        """
                    ),
                    {"batch": BATCH_ID},
                )
                _ = await connection.execute(
                    text(
                        """
                        INSERT INTO th_item (
                            id, batch_id, state, attempt, depth, task_name, payload, key,
                            weight, created_at
                        )
                        SELECT
                            ('10000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
                            :batch,
                            CASE WHEN g % :every = 0
                                THEN CAST(:cancelled AS smallint) ELSE CAST(:ok AS smallint) END,
                            0, 0, 'scan.task', '\\x'::bytea, g::text, 1, now()
                        FROM generate_series(1, :items) AS g
                        """
                    ),
                    {
                        "batch": BATCH_ID,
                        "every": MATCH_EVERY,
                        "cancelled": int(ItemState.CANCELLED),
                        "ok": int(ItemState.OK),
                        "items": ITEM_COUNT,
                    },
                )
                _ = await connection.execute(text("ANALYZE th_item"))
                await connection.commit()

            reads = Reads(
                engine.execution_options(schema_translate_map={None: schema}),
                build_metadata(),
                SystemClock(),
            )
            event.listen(engine.sync_engine, "before_cursor_execute", before)
            event.listen(engine.sync_engine, "after_cursor_execute", after)
            total_started = time.perf_counter()
            try:
                found = [
                    entry.key async for entry in reads.items(BATCH_ID, states={ItemState.CANCELLED})
                ]
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", before)
                event.remove(engine.sync_engine, "after_cursor_execute", after)
            total = time.perf_counter() - total_started
    finally:
        await engine.dispose()

    expected = [str(number) for number in range(MATCH_EVERY, ITEM_COUNT + 1, MATCH_EVERY)]
    assert found == expected
    # Проверка существования + по одному statement на окно + пустое завершающее окно.
    assert len(durations) == ITEM_COUNT // DEFAULT_ITEMS_SCAN_WINDOW + 2
    slowest = max(durations)
    scanned = f"items(states=): {len(found)} of {ITEM_COUNT}, {len(durations)} statements"
    timing = f"total {total:.2f}s, slowest statement {slowest * 1000:.1f}ms"
    print(f"{scanned}, {timing}")  # ruff: ignore[print]  # замер для журнала PROGRESS, виден с pytest -s
    assert slowest < MAX_STATEMENT_SECONDS
