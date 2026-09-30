"""Read-side batch operations: tree views, marked items, leases, and lookup."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final, TypeAlias, cast, final
from uuid import UUID

from sqlalchemy import BigInteger, func, select
from sqlalchemy import cast as sql_cast

from tallyho.model.errors import BatchPurged, ConfigurationError, NotFoundError
from tallyho.model.progress import NodeCounters, ProgressSettings, compute_progress
from tallyho.model.states import BatchState, CancelReason, ItemState
from tallyho.model.views import BatchSummary, BatchView, InFlightItem, ItemView
from tallyho.storage.now import sql_now

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from sqlalchemy import RowMapping
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql import ColumnElement, Select

    from tallyho.protocols.clock import Clock
    from tallyho.storage.tables import Tables

__all__ = ["DEFAULT_ITEM_PAGE_SIZE", "DEFAULT_LEASE_DURATION", "Reads"]

DEFAULT_ITEM_PAGE_SIZE: Final = 1000
DEFAULT_LEASE_DURATION: Final = timedelta(seconds=60)

_LeaseRow: TypeAlias = tuple[
    UUID,
    UUID,
    str,
    int,
    datetime,
    timedelta,
    int | None,
    int | None,
]
_ItemRow: TypeAlias = tuple[
    UUID,
    UUID,
    int,
    str,
    str | None,
    int,
    int,
    str | None,
    int,
    UUID | None,
    object,
    object,
    datetime,
    datetime | None,
]

_NON_POSITIVE_LIMIT = "limit должен быть положительным целым числом"
_ROOT_NOT_FOUND = "корневой батч не найден"
_CHILD_NOT_FOUND = "под-батч не найден"


@dataclass(frozen=True, slots=True)
class _Node:
    id: UUID
    parent_id: UUID | None
    kind: str
    key: str | None
    state: BatchState
    expected_total: int | None
    snap_seq: int
    reason: CancelReason | None
    paused_at: datetime | None
    cancel_requested_at: datetime | None
    start_at: datetime | None
    deadline_at: datetime | None
    created_at: datetime
    finished_at: datetime | None
    hook_attempts: int
    hook_error: str | None
    counters: NodeCounters
    values: Mapping[str, int]


@final
class Reads:
    """Read model over one tallyho installation."""

    def __init__(
        self,
        engine: AsyncEngine,
        tables: Tables,
        clock: Clock,
        *,
        progress: ProgressSettings | None = None,
        item_page_size: int = DEFAULT_ITEM_PAGE_SIZE,
        lease_duration: timedelta = DEFAULT_LEASE_DURATION,
    ) -> None:
        """Configure reads without owning or mutating application transactions.

        Raises:
            ConfigurationError: A page size or lease duration is not positive.
        """
        _positive_limit(item_page_size)
        if lease_duration <= timedelta(0):
            message = "lease_duration должен быть положительным timedelta"
            raise ConfigurationError(message)
        self.engine = engine
        self.tables = tables
        self.clock = clock
        self.progress = progress or ProgressSettings()
        self.item_page_size = item_page_size
        self.lease_duration = lease_duration

    async def view(self, batch_id: UUID) -> BatchView:
        """Read a batch subtree with one SQL statement, independent of tree size.

        Returns:
            The requested batch and its descendants.
        """
        async with self.engine.connect() as conn:
            nodes = await self._tree(conn, batch_id)
        progress = compute_progress((node.counters for node in nodes), settings=self.progress)
        children = _children(nodes)

        def build(node: _Node) -> BatchView:
            return BatchView(
                id=node.id,
                kind=node.kind,
                key=node.key,
                state=node.state,
                progress=progress[node.id],
                labels=node.values,
                metrics=node.values,
                children={
                    child.key or str(child.id): build(child) for child in children.get(node.id, ())
                },
                reason=node.reason,
                paused_at=node.paused_at,
                cancel_requested_at=node.cancel_requested_at,
                start_at=node.start_at,
                deadline_at=node.deadline_at,
                created_at=node.created_at,
                finished_at=node.finished_at,
                hook_attempts=node.hook_attempts,
                hook_error=node.hook_error,
            )

        return build(_target(nodes, batch_id))

    async def summary(self, batch_id: UUID) -> BatchSummary:
        """Read the hook-shaped summary for a batch subtree.

        Returns:
            The requested summary and its descendants.
        """
        async with self.engine.connect() as conn:
            nodes = await self._tree(conn, batch_id)
        progress = compute_progress((node.counters for node in nodes), settings=self.progress)
        children = _children(nodes)

        def build(node: _Node) -> BatchSummary:
            return BatchSummary(
                id=node.id,
                kind=node.kind,
                key=node.key,
                state=node.state,
                progress=progress[node.id],
                labels=node.values,
                metrics=node.values,
                children={
                    child.key or str(child.id): build(child) for child in children.get(node.id, ())
                },
                seq=node.snap_seq,
                reason=node.reason,
                finished_at=node.finished_at,
            )

        return build(_target(nodes, batch_id))

    async def in_flight(self, batch_id: UUID, *, limit: int = 100) -> list[InFlightItem]:
        """Return at most ``limit`` current leases ordered by Item id.

        Returns:
            Current leases in stable Item id order.

        Raises:
            BatchPurged: The batch is no longer present.
        """
        _positive_limit(limit)
        lease = self.tables.lease
        now = sql_now(self.clock)
        acquired_at = lease.c.lease_until - self.lease_duration
        statement = (
            select(
                lease.c.item_id,
                lease.c.batch_id,
                lease.c.worker_id,
                lease.c.attempt,
                lease.c.lease_until,
                func.greatest(now - acquired_at, timedelta(0)).label("age"),
                lease.c.progress_done,
                lease.c.progress_total,
            )
            .where(lease.c.batch_id == batch_id)
            .order_by(lease.c.item_id)
            .limit(limit)
        )
        async with self.engine.connect() as conn:
            if not await self._exists(conn, batch_id):
                raise BatchPurged(batch_id)
            rows = cast(
                "list[_LeaseRow]",
                (await conn.execute(statement)).all(),
            )
        return [
            InFlightItem(
                id=item_id,
                batch_id=row_batch_id,
                worker_id=worker_id,
                attempt=attempt,
                lease_until=lease_until,
                age=age,
                progress_done=progress_done,
                progress_total=progress_total,
            )
            for (
                item_id,
                row_batch_id,
                worker_id,
                attempt,
                lease_until,
                age,
                progress_done,
                progress_total,
            ) in rows
        ]

    async def items(self, batch_id: UUID, *, label: str) -> AsyncIterator[ItemView]:
        """Stream marked Items using ``(batch_id, label, item_id)`` keyset pages.

        Yields:
            Marked Items in stable id order.

        Raises:
            BatchPurged: The batch is no longer present.
        """
        mark = self.tables.item_mark
        item = self.tables.item
        cursor: UUID | None = None
        async with self.engine.connect() as conn:
            if not await self._exists(conn, batch_id):
                raise BatchPurged(batch_id)
        while True:
            where: list[ColumnElement[bool]] = [
                mark.c.batch_id == batch_id,
                mark.c.label == label,
            ]
            if cursor is not None:
                where.append(mark.c.item_id > cursor)
            statement = (
                select(
                    item.c.id,
                    item.c.batch_id,
                    item.c.state,
                    item.c.task_name,
                    item.c.label,
                    item.c.attempt,
                    item.c.depth,
                    item.c.key,
                    item.c.weight,
                    item.c.child_batch_id,
                    item.c.result,
                    item.c.error,
                    item.c.created_at,
                    item.c.finished_at,
                )
                .select_from(mark.join(item, item.c.id == mark.c.item_id))
                .where(*where)
                .order_by(mark.c.item_id)
                .limit(self.item_page_size)
            )
            async with self.engine.connect() as conn:
                rows = cast(
                    "list[_ItemRow]",
                    (await conn.execute(statement)).all(),
                )
            for (
                item_id,
                row_batch_id,
                state,
                task_name,
                row_label,
                attempt,
                depth,
                key,
                weight,
                child_batch_id,
                result,
                error,
                created_at,
                finished_at,
            ) in rows:
                cursor = item_id
                yield ItemView(
                    id=item_id,
                    batch_id=row_batch_id,
                    state=ItemState(state),
                    task_name=task_name,
                    label=row_label,
                    attempt=attempt,
                    depth=depth,
                    key=key,
                    weight=weight,
                    child_batch_id=child_batch_id,
                    result=result,
                    error=error,
                    created_at=created_at,
                    finished_at=finished_at,
                )
            if len(rows) < self.item_page_size:
                return

    async def find(self, kind: str, key: str) -> UUID:
        """Find a root batch by its idempotency key.

        Returns:
            The root batch id.

        Raises:
            NotFoundError: There is no matching root.
        """
        batch = self.tables.batch
        statement = select(batch.c.id).where(
            batch.c.kind == kind, batch.c.key == key, batch.c.parent_id.is_(None)
        )
        async with self.engine.connect() as conn:
            batch_id = await conn.scalar(statement)
        if batch_id is None:
            raise NotFoundError(_ROOT_NOT_FOUND)
        return batch_id

    async def child(self, batch_id: UUID, key: str) -> UUID:
        """Find a direct child of ``batch_id`` by key.

        Returns:
            The child batch id.

        Raises:
            NotFoundError: There is no matching direct child.
        """
        batch = self.tables.batch
        statement = select(batch.c.id).where(batch.c.parent_id == batch_id, batch.c.key == key)
        async with self.engine.connect() as conn:
            child_id = await conn.scalar(statement)
        if child_id is None:
            raise NotFoundError(_CHILD_NOT_FOUND)
        return child_id

    async def _tree(self, conn: AsyncConnection, batch_id: UUID) -> list[_Node]:
        rows = (await conn.execute(self._tree_statement(batch_id))).mappings().all()
        if not rows:
            raise BatchPurged(batch_id)
        return [_node(row) for row in rows]

    def _tree_statement(self, batch_id: UUID) -> Select[tuple[object]]:
        batch = self.tables.batch
        counter = self.tables.counter
        delta = self.tables.counter_delta
        metric = self.tables.metric
        feed = self.tables.feed
        lease = self.tables.lease
        root_id = select(batch.c.root_id).where(batch.c.id == batch_id).scalar_subquery()

        def counter_sum(column: ColumnElement[int]) -> ColumnElement[int]:
            return (
                select(sql_cast(func.coalesce(func.sum(column), 0), BigInteger))
                .where(counter.c.batch_id == batch.c.id)
                .scalar_subquery()
            )

        def delta_sum(column: ColumnElement[int]) -> ColumnElement[int]:
            return (
                select(sql_cast(func.coalesce(func.sum(column), 0), BigInteger))
                .where(delta.c.batch_id == batch.c.id)
                .scalar_subquery()
            )

        metric_totals = (
            select(metric.c.name, func.sum(metric.c.value).label("total"))
            .where(metric.c.batch_id == batch.c.id)
            .group_by(metric.c.name)
            .correlate(batch)
            .subquery()
        )
        values = select(
            func.jsonb_object_agg(metric_totals.c.name, metric_totals.c.total)
        ).scalar_subquery()
        feeds = (
            select(func.array_agg(feed.c.feeder_id))
            .where(feed.c.fed_id == batch.c.id)
            .scalar_subquery()
        )
        in_flight = select(func.count()).where(lease.c.batch_id == batch.c.id).scalar_subquery()
        statement = (
            select(
                batch,
                (counter_sum(counter.c.total) + delta_sum(delta.c.d_total)).label("total"),
                (counter_sum(counter.c.ok) + delta_sum(delta.c.d_ok)).label("ok"),
                (counter_sum(counter.c.skip) + delta_sum(delta.c.d_skip)).label("skip"),
                (counter_sum(counter.c.error) + delta_sum(delta.c.d_error)).label("error"),
                (counter_sum(counter.c.cancelled) + delta_sum(delta.c.d_cancelled)).label(
                    "cancelled"
                ),
                (counter_sum(counter.c.w_total) + delta_sum(delta.c.d_w_total)).label("w_total"),
                (counter_sum(counter.c.w_done) + delta_sum(delta.c.d_w_done)).label("w_done"),
                (counter_sum(counter.c.duplicates) + delta_sum(delta.c.d_duplicates)).label(
                    "duplicates"
                ),
                (
                    counter_sum(counter.c.skipped_by_limit) + delta_sum(delta.c.d_skipped_by_limit)
                ).label("skipped_by_limit"),
                in_flight.label("in_flight"),
                values.label("values"),
                feeds.label("fed_by"),
            )
            .where(batch.c.root_id == root_id)
            .order_by(batch.c.id)
        )
        return cast("Select[tuple[object]]", statement)

    async def _exists(self, conn: AsyncConnection, batch_id: UUID) -> bool:
        return (
            await conn.scalar(
                select(self.tables.batch.c.id).where(self.tables.batch.c.id == batch_id)
            )
            is not None
        )


def _node(row: RowMapping) -> _Node:
    batch_id = cast("UUID", row["id"])
    state = BatchState(cast("int", row["state"]))
    fed_by_value = cast("object", row["fed_by"])
    fed_by = tuple(cast("list[UUID]", fed_by_value)) if isinstance(fed_by_value, list) else ()
    values_value = cast("object", row["values"])
    raw_values = (
        cast("dict[object, object]", values_value) if isinstance(values_value, dict) else {}
    )
    values = {
        str(key): value
        for key, value in raw_values.items()
        if isinstance(value, int) and not isinstance(value, bool)
    }
    return _Node(
        id=batch_id,
        parent_id=cast("UUID | None", row["parent_id"]),
        kind=cast("str", row["kind"]),
        key=cast("str | None", row["key"]),
        state=state,
        expected_total=cast("int | None", row["expected_total"]),
        snap_seq=cast("int", row["snap_seq"]),
        reason=_reason(cast("object", row["cancel_reason"])),
        paused_at=cast("datetime | None", row["paused_at"]),
        cancel_requested_at=cast("datetime | None", row["cancel_requested_at"]),
        start_at=cast("datetime | None", row["start_at"]),
        deadline_at=cast("datetime | None", row["deadline_at"]),
        created_at=cast("datetime", row["created_at"]),
        finished_at=cast("datetime | None", row["finished_at"]),
        hook_attempts=cast("int", row["hook_attempts"]),
        hook_error=cast("str | None", row["hook_error"]),
        counters=NodeCounters(
            id=batch_id,
            parent_id=cast("UUID | None", row["parent_id"]),
            state=state,
            total=cast("int", row["total"]),
            ok=cast("int", row["ok"]),
            skip=cast("int", row["skip"]),
            error=cast("int", row["error"]),
            cancelled=cast("int", row["cancelled"]),
            w_total=cast("int", row["w_total"]),
            w_done=cast("int", row["w_done"]),
            duplicates=cast("int", row["duplicates"]),
            skipped_by_limit=cast("int", row["skipped_by_limit"]),
            in_flight=cast("int", row["in_flight"]),
            expected_total=cast("int | None", row["expected_total"]),
            fed_by=fed_by,
        ),
        values=values,
    )


def _children(nodes: list[_Node]) -> Mapping[UUID, tuple[_Node, ...]]:
    grouped: defaultdict[UUID, list[_Node]] = defaultdict(list)
    for node in nodes:
        if node.parent_id is not None:
            grouped[node.parent_id].append(node)
    return {parent_id: tuple(children) for parent_id, children in grouped.items()}


def _target(nodes: list[_Node], batch_id: UUID) -> _Node:
    for node in nodes:
        if node.id == batch_id:
            return node
    raise BatchPurged(batch_id)


def _reason(value: object) -> CancelReason | None:
    return CancelReason(value) if isinstance(value, str) else None


def _positive_limit(value: int) -> None:
    if isinstance(value, bool) or value <= 0:
        raise ConfigurationError(_NON_POSITIVE_LIMIT)
