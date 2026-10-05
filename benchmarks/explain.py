"""EXPLAIN-гард на заполненной БД бенчмарка (COUNTERS §4.2, ACCEPTANCE P-04).

Те же запросы горячего пути, что у ``tests/integration/stress/test_explain_guard.py``
(реестр ``tallyho.storage.hot_queries.HOT_QUERIES``), но на данных, которые оставил прогон:
настоящие идентификаторы батча, корня и Item.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, cast

from benchmarks.stand import ident, sql
from tallyho.storage.hot_queries import HOT_QUERIES, HotQueryProbe
from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["FORBIDDEN_SEQ_SCAN", "PlanFinding", "explain_guard"]

FORBIDDEN_SEQ_SCAN: Final = frozenset(
    {"th_item", "th_batch", "th_counter", "th_batch_attr", "th_metric", "th_counter_delta"}
)

# Пробные идентификаторы: Item, его батч и корень дерева вида ``kind``.
_PROBE: Final = """
SELECT b.id, b.root_id, coalesce(b.parent_id, b.id), r.kind, coalesce(r.key, ''), i.id
FROM {item} i
JOIN {batch} b ON b.id = i.batch_id
JOIN {batch} r ON r.id = b.root_id
WHERE r.kind = :kind
LIMIT 1
"""


@dataclass(frozen=True, slots=True)
class PlanFinding:
    """Запрос горячего пути и что с его планом не так (``None`` — всё в порядке)."""

    query: str
    problem: str | None


def _nodes(plan: Mapping[str, object]) -> Iterator[Mapping[str, object]]:
    yield plan
    children = plan.get("Plans", [])
    if isinstance(children, list):
        for child in cast("list[object]", children):
            if isinstance(child, dict):
                yield from _nodes(cast("Mapping[str, object]", child))


def _problem(plan: Mapping[str, object], max_rows: int) -> str | None:
    for node in _nodes(plan):
        relation = node.get("Relation Name")
        node_type = node.get("Node Type")
        rows = node.get("Plan Rows")
        if node_type == "Seq Scan" and relation in FORBIDDEN_SEQ_SCAN:
            return f"Seq Scan по {relation}"
        if isinstance(rows, int) and rows > max_rows:
            return f"оценка {rows} строк в {node_type} (порог {max_rows})"
    return None


async def explain_guard(engine: AsyncEngine, schema: str, *, kind: str) -> list[PlanFinding]:
    """Проверить планы всех запросов горячего пути на данных схемы ``schema``.

    Returns:
        По находке на каждый запрос реестра.
    """
    tables = build_metadata()
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                sql(
                    _PROBE,
                    item=ident(schema, "th_item"),
                    batch=ident(schema, "th_batch"),
                ),
                {"kind": kind},
            )
        ).one()
        probe = HotQueryProbe(
            batch_id=cast("UUID", row[0]),
            root_id=cast("UUID", row[1]),
            parent_id=cast("UUID", row[2]),
            item_id=cast("UUID", row[5]),
            kind=cast("str", row[3]),
            key=cast("str", row[4]),
            cutoff=datetime.now(UTC),
        )
        _ = await connection.execute(sql("SET search_path TO {schema}", schema=ident(schema)))
        findings: list[PlanFinding] = []
        for query in HOT_QUERIES.build(tables, probe):
            compiled = query.statement.compile(
                dialect=connection.dialect, compile_kwargs={"literal_binds": True}
            )
            raw = cast(
                "object",
                (
                    # Без text(): литералы плана (время «12:30») не должны стать bind-параметрами.
                    await connection.exec_driver_sql(f"EXPLAIN (FORMAT JSON) {compiled}")
                ).scalar_one(),
            )
            decoded = cast("object", json.loads(raw)) if isinstance(raw, str) else raw
            documents = cast("list[dict[str, object]]", decoded)
            plan = cast("Mapping[str, object]", documents[0]["Plan"])
            findings.append(PlanFinding(query.name, _problem(plan, query.max_plan_rows)))
        await connection.rollback()
    return findings
