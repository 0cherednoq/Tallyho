"""Запросы ``handle.items(states=, labels=)``: обход окнами фиксированного размера.

На ``th_item`` нет индекса по ``state`` (ARCHITECTURE §5.2, D-041), поэтому запрос
``WHERE state IN (…) LIMIT n`` при редких совпадениях читал бы весь остаток батча
одним statement. Здесь каждый statement читает по индексу не больше ``window``
строк, а фильтр применяется снаружи материализованного окна. Вместе с
совпадениями всегда возвращается «граница» окна — последний прочитанный id и
число прочитанных строк, — поэтому курсор сдвигается и тогда, когда совпадений нет.

Форма результата у обоих запросов одна: ``last_id``, ``scanned`` и колонки
``ITEM_VIEW_FIELDS``. Строк не меньше одной; если совпадений в окне нет, колонки
Item в единственной строке равны ``NULL``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, TypeVarTuple

from sqlalchemy import SmallInteger, func, literal_column, select, true

if TYPE_CHECKING:
    from collections.abc import Collection
    from uuid import UUID

    from sqlalchemy import Select
    from sqlalchemy.sql import ColumnElement

    from tallyho.model.states import ItemState
    from tallyho.storage.tables import Tables

__all__ = [
    "DEFAULT_ITEMS_SCAN_WINDOW",
    "ITEM_VIEW_FIELDS",
    "item_window_statement",
    "marked_window_statement",
]

DEFAULT_ITEMS_SCAN_WINDOW: Final = 5000
"""Сколько строк ``th_item`` читает один запрос окна (ARCHITECTURE §15)."""

ITEM_VIEW_FIELDS: Final = (
    "id",
    "batch_id",
    "state",
    "task_name",
    "label",
    "attempt",
    "depth",
    "key",
    "weight",
    "child_batch_id",
    "result",
    "error",
    "created_at",
    "finished_at",
)
"""Колонки ``th_item`` для ``ItemView``; ``payload`` и ``options`` в окно не попадают."""

_WINDOW_NAME = "scan_window"
_Ts = TypeVarTuple("_Ts")


def item_window_statement(
    tables: Tables,
    *,
    batch_id: UUID,
    states: Collection[ItemState],
    after: UUID | None,
    window: int,
) -> Select[*tuple[object, ...]]:
    """Прочитать одно окно ``th_item`` по индексу ``(batch_id, id)``.

    Args:
        tables: Таблицы установки.
        batch_id: Батч, Items которого обходятся.
        states: Непустой набор состояний; фильтр применяется снаружи окна.
        after: Последний id предыдущего окна или ``None`` для первого.
        window: Сколько строк ``th_item`` читает statement.

    Returns:
        Запрос с границей окна и совпавшими Items.
    """
    item = tables.item
    scope = select(*_item_columns(tables)).where(item.c.batch_id == batch_id)
    if after is not None:
        scope = scope.where(item.c.id > after)
    return _window(tables, scope.order_by(item.c.id).limit(window), states, marks=False)


def marked_window_statement(
    tables: Tables,
    *,
    batch_id: UUID,
    label: str,
    states: Collection[ItemState],
    after: UUID | None,
    window: int,
) -> Select[*tuple[object, ...]]:
    """Прочитать одно окно ``th_item_mark`` по PK ``(batch_id, label, item_id)``.

    Args:
        tables: Таблицы установки.
        batch_id: Батч, помеченные Items которого обходятся.
        label: Метка итога.
        states: Дополнительный фильтр состояния; пустой набор — без фильтра.
        after: Последний ``item_id`` предыдущего окна или ``None`` для первого.
        window: Сколько строк ``th_item_mark`` читает statement.

    Returns:
        Запрос с границей окна и совпавшими Items.
    """
    mark = tables.item_mark
    scope = select(mark.c.item_id.label("id")).where(
        mark.c.batch_id == batch_id, mark.c.label == label
    )
    if after is not None:
        scope = scope.where(mark.c.item_id > after)
    return _window(tables, scope.order_by(mark.c.item_id).limit(window), states, marks=True)


def _window(
    tables: Tables,
    scope: Select[*_Ts],
    states: Collection[ItemState],
    *,
    marks: bool,
) -> Select[*tuple[object, ...]]:
    """Обернуть ограниченный ``scope`` в окно с границей и совпадениями.

    Returns:
        ``last_id``, ``scanned`` и колонки ``ITEM_VIEW_FIELDS`` совпавших Items.
    """
    item = tables.item
    # MATERIALIZED не даёт планировщику протолкнуть внешний фильтр внутрь LIMIT.
    scanned = scope.cte(_WINDOW_NAME).prefix_with("MATERIALIZED")
    if marks:
        found = select(*_item_columns(tables)).select_from(
            scanned.join(item, item.c.id == scanned.c.id)
        )
        if states:
            found = found.where(_state_in(item.c.state, states))
    else:
        own: list[ColumnElement[object]] = [scanned.c[name] for name in ITEM_VIEW_FIELDS]
        found = select(*own).where(_state_in(scanned.c.state, states))
    matched = found.subquery("matched")
    # В PostgreSQL 14-16 нет max(uuid): последнюю строку окна даёт ORDER BY … DESC LIMIT 1.
    last_id = select(scanned.c.id).order_by(scanned.c.id.desc()).limit(1).scalar_subquery()
    count = select(func.count()).select_from(scanned).scalar_subquery()
    edge = select(last_id.label("last_id"), count.label("scanned")).subquery("edge")
    columns: list[ColumnElement[object]] = [edge.c.last_id, edge.c.scanned]
    columns.extend(matched.c[name] for name in ITEM_VIEW_FIELDS)
    return select(*columns).select_from(edge.outerjoin(matched, true()))


def _item_columns(tables: Tables) -> list[ColumnElement[object]]:
    return [tables.item.c[name] for name in ITEM_VIEW_FIELDS]


def _state_in(column: ColumnElement[int], states: Collection[ItemState]) -> ColumnElement[bool]:
    # Литералы, а не bind-параметры: текст запроса не зависит от generic plan (D-020).
    codes = sorted({int(state) for state in states})
    return column.in_([literal_column(str(code), SmallInteger()) for code in codes])
