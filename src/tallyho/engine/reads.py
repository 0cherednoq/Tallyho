"""Read-side batch operations: tree views, marked items, leases, and lookup."""

from __future__ import annotations

import base64
from collections import defaultdict
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final, TypeAlias, TypeVar, cast, final
from uuid import UUID

from sqlalchemy import BigInteger, func, select
from sqlalchemy import cast as sql_cast

from tallyho.model.errors import BatchPurged, ConfigurationError, NotFoundError
from tallyho.model.progress import NodeCounters, ProgressSettings, compute_progress
from tallyho.model.states import BatchState, CancelReason, ItemState
from tallyho.model.views import (
    BatchInfo,
    BatchPage,
    BatchSummary,
    BatchView,
    InFlightItem,
    ItemView,
)
from tallyho.storage.attributes import attributes_from_json, memo_from_json
from tallyho.storage.batch_listing import (
    DEFAULT_LIST_LIMIT,
    MAX_LIST_LIMIT,
    list_batches_statement,
)
from tallyho.storage.item_scan import (
    DEFAULT_ITEMS_SCAN_WINDOW,
    item_window_statement,
    marked_window_statement,
)
from tallyho.storage.metric_names import split_metric_rows
from tallyho.storage.now import sql_now

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping

    from sqlalchemy import RowMapping
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql import ColumnElement, Select

    from tallyho.model.attributes import AttributeValue
    from tallyho.protocols.clock import Clock
    from tallyho.storage.tables import Tables

__all__ = [
    "DEFAULT_ITEMS_SCAN_WINDOW",
    "DEFAULT_ITEM_PAGE_SIZE",
    "DEFAULT_LEASE_DURATION",
    "Reads",
]

DEFAULT_ITEM_PAGE_SIZE: Final = 1000
DEFAULT_LEASE_DURATION: Final = timedelta(seconds=60)

_MemberT = TypeVar("_MemberT")

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
_WindowRow: TypeAlias = tuple[
    UUID | None,
    int,
    UUID | None,
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
"""Граница окна (``last_id``, ``scanned``) и колонки ``ITEM_VIEW_FIELDS``."""
_ListRow: TypeAlias = tuple[UUID, str, str | None, int, datetime, datetime | None, object]
"""Колонки ``BATCH_INFO_FIELDS`` и jsonb ``attributes``."""

_NON_POSITIVE_LIMIT = "limit должен быть положительным целым числом"
_ROOT_NOT_FOUND = "корневой батч не найден"
_CHILD_NOT_FOUND = "под-батч не найден"
_NO_ITEM_FILTER = "handle.items() требует хотя бы один фильтр: states= или labels="
_EMPTY_ITEM_FILTER = "фильтр handle.items() не может быть пустой коллекцией"
_BAD_STATES = "states должен быть коллекцией ItemState"
_BAD_LABELS = "labels должен быть коллекцией строк, а не одной строкой"
_BAD_KINDS = "kinds должен быть коллекцией строк, а не одной строкой"
_BAD_BATCH_STATES = "states должен быть коллекцией BatchState"
_BAD_CURSOR = "cursor листинга не распознан: передавайте BatchPage.next_cursor без изменений"
_BAD_LIST_LIMIT = f"limit листинга должен быть целым от 1 до {MAX_LIST_LIMIT}"
_BAD_BOUND = "created_after и created_before должны быть datetime с часовым поясом"
_CURSOR_VERSION = b"\x01"


@dataclass(frozen=True, slots=True)
class _ItemFilter:
    """Проверенные фильтры ``items``: пустой кортеж — фильтр не задан."""

    states: tuple[ItemState, ...]
    labels: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Node:
    id: UUID
    root_id: UUID
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
    labels: Mapping[str, int]
    metrics: Mapping[str, int]
    attributes: Mapping[str, AttributeValue]
    memo: Mapping[str, object] | None


@final
class Reads:
    """Read model over one tallyho installation."""

    def __init__(  # ruff: ignore[too-many-arguments]  # настройки чтения именованные, со значениями по умолчанию
        self,
        engine: AsyncEngine,
        tables: Tables,
        clock: Clock,
        *,
        progress: ProgressSettings | None = None,
        item_page_size: int = DEFAULT_ITEM_PAGE_SIZE,
        items_scan_window: int = DEFAULT_ITEMS_SCAN_WINDOW,
        lease_duration: timedelta = DEFAULT_LEASE_DURATION,
    ) -> None:
        """Configure reads without owning or mutating application transactions.

        Raises:
            ConfigurationError: A page size, scan window or lease duration is not positive.
        """
        _positive_limit(item_page_size)
        _positive_limit(items_scan_window)
        if lease_duration <= timedelta(0):
            message = "lease_duration должен быть положительным timedelta"
            raise ConfigurationError(message)
        self.engine = engine
        self.tables = tables
        self.clock = clock
        self.progress = progress or ProgressSettings()
        self.item_page_size = item_page_size
        self.items_scan_window = items_scan_window
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
        roots = _roots(nodes)

        def build(node: _Node) -> BatchView:
            root = roots.get(node.root_id, node)
            return BatchView(
                id=node.id,
                kind=node.kind,
                key=node.key,
                state=node.state,
                progress=progress[node.id],
                labels=node.labels,
                metrics=node.metrics,
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
                attributes=root.attributes,
                memo=root.memo,
            )

        return build(_target(nodes, batch_id))

    async def summary(self, batch_id: UUID) -> BatchSummary:
        """Read the hook-shaped summary for a batch subtree.

        Returns:
            The requested summary and its descendants.

        Raises:
            BatchPurged: The batch is no longer present.
        """
        summaries = await self.summaries([batch_id])
        try:
            return summaries[batch_id]
        except KeyError as exc:
            raise BatchPurged(batch_id) from exc

    async def summaries(
        self,
        batch_ids: Iterable[UUID],
        *,
        rates: Mapping[UUID, float] | None = None,
        next_seq: bool = False,
    ) -> dict[UUID, BatchSummary]:
        """Read hook-shaped summaries for several batch subtrees in one statement.

        Missing or concurrently purged ids are omitted. ``next_seq`` prepares
        the target summaries for a Snapshotter CAS; descendant seq values stay
        at their committed values.

        Returns:
            Summaries keyed by requested batch id.
        """
        ids = sorted(set(batch_ids))
        if not ids:
            return {}
        async with self.engine.connect() as conn:
            nodes = await self._forest(conn, ids)
        progress = compute_progress(
            (node.counters for node in nodes), settings=self.progress, rates=rates
        )
        children = _children(nodes)
        roots = _roots(nodes)

        def build(node: _Node, *, target_id: UUID) -> BatchSummary:
            return BatchSummary(
                id=node.id,
                kind=node.kind,
                key=node.key,
                state=node.state,
                progress=progress[node.id],
                labels=node.labels,
                metrics=node.metrics,
                children={
                    child.key or str(child.id): build(child, target_id=target_id)
                    for child in children.get(node.id, ())
                },
                seq=node.snap_seq + int(next_seq and node.id == target_id),
                reason=node.reason,
                finished_at=node.finished_at,
                attributes=roots.get(node.root_id, node).attributes,
            )

        by_id = {node.id: node for node in nodes}
        return {
            batch_id: build(by_id[batch_id], target_id=batch_id)
            for batch_id in ids
            if batch_id in by_id
        }

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

    def items(
        self,
        batch_id: UUID,
        *,
        states: Collection[ItemState] | None = None,
        labels: Collection[str] | None = None,
    ) -> AsyncIterator[ItemView]:
        """Stream Items of one batch filtered by state, mark label, or both.

        ``labels`` walks ``th_item_mark`` in pages of ``item_page_size``;
        ``states`` alone walks ``th_item`` by ``(batch_id, id)`` in windows of
        ``items_scan_window`` rows. No statement reads more rows than its window,
        however rare the matches are. Arguments are validated here, before any
        database round trip; ``BatchPurged`` is raised on the first step of the
        returned iterator.

        A call without filters, with an empty filter, or with a filter that is
        not a collection of the expected values fails with ``ConfigurationError``.

        Returns:
            Matching Items; the order is not part of the contract.
        """
        return self._items(batch_id, _item_filter(states, labels))

    async def _items(self, batch_id: UUID, selection: _ItemFilter) -> AsyncIterator[ItemView]:
        async with self.engine.connect() as conn:
            if not await self._exists(conn, batch_id):
                raise BatchPurged(batch_id)
        if not selection.labels:
            async for view in self._windows(batch_id, selection.states, None):
                yield view
            return
        for label in selection.labels:
            async for view in self._windows(batch_id, selection.states, label):
                yield view

    async def _windows(
        self, batch_id: UUID, states: tuple[ItemState, ...], label: str | None
    ) -> AsyncIterator[ItemView]:
        window = self.items_scan_window if label is None else self.item_page_size
        after: UUID | None = None
        while True:
            statement = (
                item_window_statement(
                    self.tables, batch_id=batch_id, states=states, after=after, window=window
                )
                if label is None
                else marked_window_statement(
                    self.tables,
                    batch_id=batch_id,
                    label=label,
                    states=states,
                    after=after,
                    window=window,
                )
            )
            async with self.engine.connect() as conn:
                rows = cast("list[_WindowRow]", (await conn.execute(statement)).all())
            # Первая строка есть всегда: граница окна возвращается и без совпадений.
            after, scanned = rows[0][0], rows[0][1]
            for row in rows:
                if row[2] is not None:
                    yield _item_view(row)
            if scanned < window:
                return

    async def list_batches(  # ruff: ignore[too-many-arguments]  # фильтры листинга именованные (ARCHITECTURE §11.2)
        self,
        *,
        kinds: Collection[str] | None = None,
        states: Collection[BatchState] | None = None,
        attributes: Mapping[str, AttributeValue] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = DEFAULT_LIST_LIMIT,
        cursor: str | None = None,
    ) -> BatchPage:
        """List root batches from newest to oldest with keyset pagination.

        ``attributes`` must already be normalized. Counters are not read.

        Returns:
            One page and an opaque cursor for the next one.

        Raises:
            ConfigurationError: A filter, the limit or the cursor is malformed.
        """
        if isinstance(limit, bool) or not 1 <= limit <= MAX_LIST_LIMIT:
            raise ConfigurationError(_BAD_LIST_LIMIT)
        for bound in (created_after, created_before):
            if bound is not None and bound.tzinfo is None:
                raise ConfigurationError(_BAD_BOUND)
        statement = list_batches_statement(
            self.tables,
            kinds=_collection(kinds, str, _BAD_KINDS),
            states=_collection(states, BatchState, _BAD_BATCH_STATES),
            attributes=attributes,
            created_after=created_after,
            created_before=created_before,
            before_id=_decode_cursor(cursor),
            # Лишняя строка говорит, есть ли следующая страница.
            limit=limit + 1,
        )
        async with self.engine.connect() as conn:
            rows = cast("list[_ListRow]", (await conn.execute(statement)).all())
        items = tuple(
            BatchInfo(
                id=batch_id,
                kind=kind,
                key=key,
                state=BatchState(state),
                attributes=attributes_from_json(raw_attributes),
                created_at=created_at,
                finished_at=finished_at,
            )
            for batch_id, kind, key, state, created_at, finished_at, raw_attributes in rows[:limit]
        )
        more = len(rows) > limit
        return BatchPage(items=items, next_cursor=_encode_cursor(items[-1].id) if more else None)

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
        nodes = await self._forest(conn, [batch_id])
        if not nodes:
            raise BatchPurged(batch_id)
        return nodes

    async def _forest(self, conn: AsyncConnection, batch_ids: Iterable[UUID]) -> list[_Node]:
        ids = sorted(set(batch_ids))
        if not ids:
            return []
        rows = (await conn.execute(self._forest_statement(ids))).mappings().all()
        if not rows:
            return []
        return [_node(row) for row in rows]

    def _forest_statement(self, batch_ids: list[UUID]) -> Select[tuple[object]]:
        batch = self.tables.batch
        counter = self.tables.counter
        delta = self.tables.counter_delta
        metric = self.tables.metric
        feed = self.tables.feed
        lease = self.tables.lease
        attr = self.tables.batch_attr
        root_ids = select(batch.c.root_id).where(batch.c.id.in_(batch_ids))

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
                # Строка есть только у корня с атрибутами: остальные узлы берут их у него.
                attr.c.attributes.label("attributes"),
                attr.c.memo.label("memo"),
            )
            .select_from(batch.outerjoin(attr, attr.c.batch_id == batch.c.id))
            .where(batch.c.root_id.in_(root_ids))
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
    labels, metrics = split_metric_rows(
        {
            str(key): value
            for key, value in raw_values.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }
    )
    return _Node(
        id=batch_id,
        root_id=cast("UUID", row["root_id"]),
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
        labels=labels,
        metrics=metrics,
        attributes=attributes_from_json(cast("object", row["attributes"])),
        memo=memo_from_json(cast("object", row["memo"])),
    )


def _roots(nodes: list[_Node]) -> Mapping[UUID, _Node]:
    """Корни прочитанных деревьев: у них лежат атрибуты и ``memo`` всего дерева.

    Returns:
        Корень по ``root_id``.
    """
    return {node.id: node for node in nodes if node.parent_id is None}


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


def _item_filter(states: object, labels: object) -> _ItemFilter:
    if states is None and labels is None:
        raise ConfigurationError(_NO_ITEM_FILTER)
    state_values = _collection(states, ItemState, _BAD_STATES)
    label_values = _collection(labels, str, _BAD_LABELS)
    return _ItemFilter(
        states=tuple(sorted(set(state_values))),
        labels=tuple(dict.fromkeys(label_values)),
    )


def _collection(value: object, member: type[_MemberT], message: str) -> tuple[_MemberT, ...]:
    """Проверить один фильтр ``items``.

    Строка — тоже ``Collection``, но как фильтр она молча разобралась бы на
    символы, поэтому отклоняется явно.

    Returns:
        Значения фильтра; пустой кортеж, если фильтр не задан (``None``).

    Raises:
        ConfigurationError: Значение не коллекция, пусто или содержит чужой тип.
    """
    if value is None:
        return ()
    if isinstance(value, str | bytes) or not isinstance(value, Collection):
        raise ConfigurationError(message)
    members: tuple[object, ...] = tuple(value)
    if not members:
        raise ConfigurationError(_EMPTY_ITEM_FILTER)
    checked = tuple(entry for entry in members if isinstance(entry, member))
    if len(checked) != len(members):
        raise ConfigurationError(message)
    return checked


def _encode_cursor(batch_id: UUID) -> str:
    return base64.urlsafe_b64encode(_CURSOR_VERSION + batch_id.bytes).decode("ascii").rstrip("=")


def _decode_cursor(cursor: object) -> UUID | None:
    """Разобрать курсор листинга.

    Returns:
        Последний id предыдущей страницы или ``None`` для первой.

    Raises:
        ConfigurationError: Курсор не строка или не создан ``list_batches``.
    """
    if cursor is None:
        return None
    if not isinstance(cursor, str):
        raise ConfigurationError(_BAD_CURSOR)
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
    except ValueError as exc:
        raise ConfigurationError(_BAD_CURSOR) from exc
    if len(raw) != 1 + 16 or raw[:1] != _CURSOR_VERSION:
        raise ConfigurationError(_BAD_CURSOR)
    return UUID(bytes=raw[1:])


def _item_view(row: _WindowRow) -> ItemView:
    (
        item_id,
        batch_id,
        state,
        task_name,
        label,
        attempt,
        depth,
        key,
        weight,
        child_batch_id,
        result,
        error,
        created_at,
        finished_at,
    ) = row[2:]
    return ItemView(
        id=cast("UUID", item_id),
        batch_id=batch_id,
        state=ItemState(state),
        task_name=task_name,
        label=label,
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


def _positive_limit(value: int) -> None:
    if isinstance(value, bool) or value <= 0:
        raise ConfigurationError(_NON_POSITIVE_LIMIT)
