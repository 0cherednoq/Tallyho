"""Прогон одной ячейки матрицы масштаба P-04: нагрузка, сбор задержек, оракул, EXPLAIN."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast

from benchmarks.app import KIND_NOOP, KIND_PIPELINE
from benchmarks.explain import explain_guard
from benchmarks.harness import create_pipeline, harness
from benchmarks.history import load_history
from benchmarks.metrics import Sample
from benchmarks.stand import ident, sql
from tallyho.model.states import BatchState, ItemState

if TYPE_CHECKING:
    from uuid import UUID

    from benchmarks.app import AppConfig
    from benchmarks.context import RunContext
    from benchmarks.harness import Harness
    from benchmarks.metrics import LatencyLog

__all__ = ["Cell", "CellRun", "run_cell"]

_HISTORY_TREE: Final = 1_000
_SAMPLE_EVERY: Final = 0.5
_FAILED_EVERY: Final = 2.0
_SIZE_EVERY: Final = 2.0
_SIZE_QUERIES: Final = {
    "th_lease": "SELECT count(*) FROM {lease}",
    "th_outbox": "SELECT count(*) FROM {outbox}",
    "th_counter_delta": "SELECT count(*) FROM {counter_delta}",
}
_RELATIONS: Final = ("th_item", "th_batch", "th_counter", "th_lease", "th_outbox")
_MB: Final = 1_048_576

_FINALIZATION: Final = """
SELECT extract(epoch FROM r.finished_at), extract(epoch FROM r.finished_at - max(i.finished_at))
FROM {batch} r
JOIN {batch} b ON b.root_id = r.id
JOIN {item} i ON i.batch_id = b.id
WHERE r.kind = :kind AND r.parent_id IS NULL AND r.finished_at IS NOT NULL
GROUP BY r.id, r.finished_at
"""
_ROOTS_SUCCEEDED: Final = (
    "SELECT count(*) FROM {batch} WHERE kind = :kind AND parent_id IS NULL AND state = :state"
)
_FINALIZED: Final = "SELECT count(*) FROM {finalized} WHERE kind = :kind"
_ITEMS_OK: Final = """
SELECT count(*) FROM {item} i
JOIN {batch} b ON b.id = i.batch_id
JOIN {batch} r ON r.id = b.root_id
WHERE r.kind = :kind AND i.state = :ok
"""
_INDEX_BLOCKS: Final = """
SELECT coalesce(sum(idx_blks_hit), 0)::bigint, coalesce(sum(idx_blks_read), 0)::bigint
FROM pg_statio_user_indexes WHERE schemaname = :schema
"""


@dataclass(frozen=True, slots=True)
class Cell:
    """Ячейка матрицы: батчи x Items (или конвейер) поверх ``history`` терминальных Items.

    Attributes:
        name: подпись в отчёте.
        batches: батчей (у конвейера — корней).
        items: Items в батче (у конвейера не используется).
        history: терминальных Items истории до начала нагрузки.
        fanout: конвейер ``(карточек на страницу, PDF на карточку)``.
    """

    name: str
    batches: int
    items: int
    history: int = 0
    fanout: tuple[int, int] | None = None

    @property
    def total(self) -> int:
        """Items, которые выполнят воркеры."""
        if self.fanout is not None:
            cards, pdfs = self.fanout
            return self.batches * (1 + cards + cards * pdfs)
        return self.batches * self.items

    @property
    def kind(self) -> str:
        """Вид корней ячейки."""
        return KIND_PIPELINE if self.fanout is not None else KIND_NOOP


@dataclass(slots=True)
class CellRun:
    """Итог ячейки: задержки по операциям и состояние БД после прогона."""

    cell: Cell
    log: LatencyLog
    sizes: dict[str, list[tuple[float, int]]]
    duration: float
    history_s: float
    plans: list[tuple[str, str | None]]
    index_hit: float | None
    relation_mb: dict[str, float]
    violations: list[str]
    item_rows: int


async def _create_all(stand: Harness, cell: Cell, created: list[UUID]) -> None:
    th, tasks = stand.th, stand.tasks
    for index in range(cell.batches):
        started = time.monotonic()
        if cell.fanout is not None:
            created.append(await create_pipeline(th, tasks, index, pages=1))
        else:
            first = index * cell.items
            async with th.batch(KIND_NOOP, key=f"b:{index}", expected_total=cell.items) as batch:
                await batch.add_calls(
                    th.call(tasks.noop, value) for value in range(first, first + cell.items)
                )
            created.append(batch.handle.id)
        finished = time.monotonic()
        stand.latencies.add("create", finished - stand.origin, finished - started)


async def _timed(
    stand: Harness, operation: str, *, created: list[UUID], rng: random.Random
) -> None:
    handle = stand.th.handle(rng.choice(created))
    started = time.monotonic()
    if operation == "read_progress":
        _ = await handle.view()
    else:
        async for _ in handle.items(states=[ItemState.ERROR]):
            break
    finished = time.monotonic()
    stand.latencies.add(operation, finished - stand.origin, finished - started)


async def _sample(
    stand: Harness, created: list[UUID], sizes: dict[str, list[tuple[float, int]]]
) -> None:
    rng = random.Random(7)  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # выбор батча для чтения, не криптография
    last_failed = 0.0
    last_size = 0.0
    while True:
        await asyncio.sleep(_SAMPLE_EVERY)
        if not created:
            continue
        await _timed(stand, "read_progress", created=created, rng=rng)
        now = time.monotonic()
        if now - last_failed >= _FAILED_EVERY:
            last_failed = now
            await _timed(stand, "failed_items", created=created, rng=rng)
        if now - last_size >= _SIZE_EVERY:
            last_size = now
            for table, query in _SIZE_QUERIES.items():
                count = await stand.scalar(query)
                sizes.setdefault(table, []).append((stand.now(), count))


async def _finalization(stand: Harness, kind: str) -> list[Sample]:
    async with stand.observer.connect() as connection:
        rows = (await connection.execute(sql(_FINALIZATION, **stand.idents), {"kind": kind})).all()
    return [
        Sample(
            float(cast("float", row[0])) - stand.wall_origin,
            max(0.0, float(cast("float", row[1]))),
        )
        for row in rows
    ]


async def _relation_stats(stand: Harness) -> tuple[float | None, dict[str, float]]:
    schema = stand.names.tallyho
    async with stand.observer.connect() as connection:
        blocks = (await connection.execute(sql(_INDEX_BLOCKS), {"schema": schema})).one()
        hit, read = cast("int", blocks[0]), cast("int", blocks[1])
        sizes: dict[str, float] = {}
        for table in _RELATIONS:
            size = cast(
                "int",
                await connection.scalar(
                    sql("SELECT pg_total_relation_size(CAST(:relation AS regclass))"),
                    {"relation": ident(schema, table)},
                ),
            )
            sizes[table] = size / _MB
    total = hit + read
    return (hit / total if total else None), sizes


async def _oracle(stand: Harness, cell: Cell) -> list[str]:
    violations: list[str] = []
    kind = cell.kind
    succeeded = await stand.scalar(
        _ROOTS_SUCCEEDED, {"kind": kind, "state": int(BatchState.SUCCEEDED)}
    )
    if succeeded != cell.batches:
        violations.append(f"succeeded {succeeded} из {cell.batches} корней")
    finals = await stand.scalar(_FINALIZED, {"kind": kind})
    if kind == KIND_NOOP and finals != cell.batches:
        violations.append(f"финализаций {finals} на {cell.batches} батчей")
    done = await stand.scalar(_ITEMS_OK, {"kind": kind, "ok": int(ItemState.OK)})
    if done != cell.total:
        violations.append(f"Items ok {done} из {cell.total}")
    return violations


def _collect(stand: Harness) -> LatencyLog:
    log = stand.latencies
    events = stand.pool.events()
    for at, name, seconds in events.ops:
        log.add(name, at - stand.wall_origin, seconds)
    for at, _, seconds in events.flushes:
        log.add("completer_flush", at - stand.wall_origin, seconds)
    return log


async def _load(
    stand: Harness, cell: Cell, within: float
) -> tuple[float, dict[str, list[tuple[float, int]]]]:
    stand.origin = time.monotonic()
    stand.wall_origin = time.time()
    created: list[UUID] = []
    sizes: dict[str, list[tuple[float, int]]] = {}
    sampler = asyncio.create_task(_sample(stand, created, sizes), name="bench-p04-sampler")
    try:
        await _create_all(stand, cell, created)
        duration = await stand.wait_roots(cell.kind, within=within)
    finally:
        _ = sampler.cancel()
        _ = await asyncio.wait([sampler])
    return duration, sizes


async def run_cell(
    ctx: RunContext, cell: Cell, *, index: int, processes: int, concurrency: int, within: float
) -> CellRun:
    """Свежие схемы, история (если есть), все батчи сразу, затем замеры и проверки.

    Returns:
        Итог ячейки.
    """

    def configure(config: AppConfig) -> AppConfig:
        if cell.fanout is None:
            return config
        cards, pdfs = cell.fanout
        return replace(config, cards_per_page=cards, pdfs_per_card=pdfs)

    async with harness(
        ctx,
        name=f"p04_{index}",
        processes=processes,
        concurrency=concurrency,
        configure=configure,
    ) as stand:
        history_s = 0.0
        if cell.history:
            ctx.log(f"P-04 {cell.name}: история {cell.history} Items")
            history_s = await load_history(
                stand.observer,
                stand.names.tallyho,
                trees=max(1, cell.history // _HISTORY_TREE),
                items_per_tree=min(cell.history, _HISTORY_TREE),
                retention=None,
                finished_ago=timedelta(days=1),
            )
        ctx.log(f"P-04 {cell.name}: {cell.total} Items через воркеры")
        duration, sizes = await _load(stand, cell, within)
        log = _collect(stand)
        log.extend("finalization", await _finalization(stand, cell.kind))
        violations = await _oracle(stand, cell)
        findings = await explain_guard(stand.observer, stand.names.tallyho, kind=cell.kind)
        index_hit, relation_mb = await _relation_stats(stand)
        item_rows = await stand.scalar("SELECT count(*) FROM {item}")
    return CellRun(
        cell=cell,
        log=log,
        sizes=sizes,
        duration=duration,
        history_s=history_s,
        plans=[(finding.query, finding.problem) for finding in findings],
        index_hit=index_hit,
        relation_mb=relation_mb,
        violations=violations,
        item_rows=item_rows,
    )
