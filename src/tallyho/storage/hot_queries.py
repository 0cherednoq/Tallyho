"""Registry of hot-path statements guarded by PostgreSQL ``EXPLAIN``.

The registry is deliberately made of SQLAlchemy statements rather than copied SQL
strings.  Partial-index predicates therefore use the same model constants and table
metadata as the runtime queries they represent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeVar, final

from sqlalchemy import any_, literal, select

from tallyho.model.states import TERMINAL_THRESHOLD, BatchState, ItemState
from tallyho.storage.batch_listing import DEFAULT_LIST_LIMIT, list_batches_statement
from tallyho.storage.item_scan import DEFAULT_ITEMS_SCAN_WINDOW, item_window_statement
from tallyho.storage.tables import PROGRESS_HOOK

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import Select
    from sqlalchemy.sql.elements import ClauseElement

    from tallyho.storage.tables import Tables

__all__ = ["HOT_QUERIES", "HotQuery", "HotQueryProbe", "HotQueryRegistry"]

_OPEN_OR_SEALED = (int(BatchState.OPEN), int(BatchState.SEALED))
_PAGE = 1_000
# Листинг идёт по индексу до LIMIT: узел скана оценивается всеми корнями одного kind.
_LISTED_ROOTS = 10_000


@dataclass(frozen=True, slots=True)
class HotQueryProbe:
    """Concrete values used to build reproducible EXPLAIN statements."""

    batch_id: UUID
    root_id: UUID
    parent_id: UUID
    item_id: UUID
    kind: str
    key: str
    cutoff: datetime


@dataclass(frozen=True, slots=True)
class HotQuery:
    """One named statement and the largest acceptable planner estimate."""

    name: str
    statement: ClauseElement
    max_plan_rows: int


class _Builder(Protocol):
    def __call__(self, tables: Tables, probe: HotQueryProbe) -> ClauseElement: ...


_BuilderT = TypeVar("_BuilderT", bound=_Builder)


class _Registrar(Protocol):
    def __call__(self, builder: _BuilderT) -> _BuilderT: ...


@final
class HotQueryRegistry:
    """Collect statement builders next to their storage query definitions."""

    def __init__(self) -> None:
        """Create an empty ordered registry."""
        self._builders: list[tuple[str, _Builder, int]] = []

    def register(
        self,
        name: str,
        *,
        max_plan_rows: int = _PAGE,
    ) -> _Registrar:
        """Register a statement builder while preserving its concrete type.

        Returns:
            A decorator that returns the original builder unchanged.
        """

        def decorator(builder: _BuilderT) -> _BuilderT:
            self._builders.append((name, builder, max_plan_rows))
            return builder

        return decorator

    def build(self, tables: Tables, probe: HotQueryProbe) -> tuple[HotQuery, ...]:
        """Build every registered query for one populated-database probe.

        Returns:
            Ordered immutable query descriptions.
        """
        return tuple(
            HotQuery(name, builder(tables, probe), max_plan_rows)
            for name, builder, max_plan_rows in self._builders
        )


HOT_QUERIES = HotQueryRegistry()


@HOT_QUERIES.register("batch.by_id", max_plan_rows=1)
def _batch_by_id(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    return select(tables.batch.c.id).where(tables.batch.c.id == probe.batch_id)


@HOT_QUERIES.register("batch.root_by_kind_key", max_plan_rows=1)
def _root_by_kind_key(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    batch = tables.batch
    return select(batch.c.id).where(
        batch.c.parent_id.is_(None),
        batch.c.kind == probe.kind,
        batch.c.key == probe.key,
    )


@HOT_QUERIES.register("batch.child_by_root_key", max_plan_rows=1)
def _child_by_root_key(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    batch = tables.batch
    return select(batch.c.id).where(
        batch.c.parent_id.is_not(None),
        batch.c.root_id == probe.root_id,
        batch.c.key == probe.key,
    )


@HOT_QUERIES.register("batch.children_by_parent")
def _children_by_parent(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    batch = tables.batch
    return (
        select(batch.c.id)
        .where(batch.c.parent_id == probe.parent_id)
        .order_by(batch.c.id)
        .limit(_PAGE)
    )


@HOT_QUERIES.register("batch.active_for_sweeper")
def _active_for_sweeper(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    batch = tables.batch
    return (
        select(batch.c.id)
        .where(batch.c.state < TERMINAL_THRESHOLD, batch.c.updated_at <= probe.cutoff)
        .order_by(batch.c.updated_at)
        .limit(_PAGE)
    )


@HOT_QUERIES.register("batch.deadlines")
def _deadlines(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    batch = tables.batch
    return (
        select(batch.c.id)
        .where(
            batch.c.deadline_at.is_not(None),
            batch.c.state.in_(_OPEN_OR_SEALED),
            batch.c.deadline_at <= probe.cutoff,
        )
        .order_by(batch.c.deadline_at)
        .limit(_PAGE)
    )


@HOT_QUERIES.register("batch.progress_snapshots")
def _progress_snapshots(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    del probe
    batch = tables.batch
    return (
        select(batch.c.id)
        .where(
            batch.c.state.in_(_OPEN_OR_SEALED),
            literal(PROGRESS_HOOK) == any_(batch.c.hooks),
        )
        .order_by(batch.c.id)
        .limit(_PAGE)
    )


@HOT_QUERIES.register("batch.retention")
def _retention(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    batch = tables.batch
    return (
        select(batch.c.id)
        .where(
            batch.c.id == batch.c.root_id,
            batch.c.finished_at.is_not(None),
            batch.c.retention.is_not(None),
            (~batch.c.release_required) | batch.c.released_at.is_not(None),
            batch.c.finished_at <= probe.cutoff,
        )
        .order_by(batch.c.finished_at)
        .limit(_PAGE)
    )


@HOT_QUERIES.register("batch.list_by_kind", max_plan_rows=_LISTED_ROOTS)
def _list_by_kind(tables: Tables, probe: HotQueryProbe) -> Select[*tuple[object, ...]]:
    return list_batches_statement(
        tables, kinds=(probe.kind,), before_id=probe.root_id, limit=DEFAULT_LIST_LIMIT + 1
    )


@HOT_QUERIES.register("batch.list_by_attributes", max_plan_rows=_LISTED_ROOTS)
def _list_by_attributes(tables: Tables, probe: HotQueryProbe) -> Select[*tuple[object, ...]]:
    return list_batches_statement(
        tables, attributes={"key": probe.key}, limit=DEFAULT_LIST_LIMIT + 1
    )


@HOT_QUERIES.register("item.by_id", max_plan_rows=1)
def _item_by_id(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    return select(tables.item.c.id).where(tables.item.c.id == probe.item_id)


@HOT_QUERIES.register("item.by_batch")
def _items_by_batch(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    item = tables.item
    return (
        select(item.c.id).where(item.c.batch_id == probe.batch_id).order_by(item.c.id).limit(_PAGE)
    )


@HOT_QUERIES.register("item.scan_window", max_plan_rows=DEFAULT_ITEMS_SCAN_WINDOW)
def _item_scan_window(tables: Tables, probe: HotQueryProbe) -> Select[*tuple[object, ...]]:
    return item_window_statement(
        tables,
        batch_id=probe.batch_id,
        states=(ItemState.ERROR, ItemState.CANCELLED),
        after=probe.item_id,
        window=DEFAULT_ITEMS_SCAN_WINDOW,
    )


@HOT_QUERIES.register("item.by_batch_key", max_plan_rows=1)
def _item_by_batch_key(tables: Tables, probe: HotQueryProbe) -> Select[UUID]:
    item = tables.item
    return select(item.c.id).where(
        item.c.batch_id == probe.batch_id,
        item.c.key == probe.key,
    )


@HOT_QUERIES.register("counter.by_batch")
def _counter_by_batch(tables: Tables, probe: HotQueryProbe) -> Select[UUID, int]:
    counter = tables.counter
    return (
        select(counter.c.batch_id, counter.c.slot)
        .where(counter.c.batch_id == probe.batch_id)
        .order_by(counter.c.slot)
    )
