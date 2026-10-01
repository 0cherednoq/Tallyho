"""Запрос листинга корневых батчей ``th.list_batches(...)`` (ARCHITECTURE §11.2).

Только корни, от новых к старым: keyset по ``id DESC`` (UUIDv7). Батч,
созданный во время обхода, получает id больше уже пройденных и на пройденные
страницы не влияет. Фильтр по ``kind`` обслуживает индекс
``th_batch (kind, id) WHERE parent_id IS NULL``, фильтр по атрибутам —
GIN ``jsonb_path_ops`` на ``th_batch_attr.attributes`` (containment ``@>``).
Счётчики листинг не читает.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final

from sqlalchemy import Text, cast, literal, select
from sqlalchemy.dialects.postgresql import JSONB

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import Select
    from sqlalchemy.sql import ColumnElement

    from tallyho.model.attributes import AttributeValue
    from tallyho.model.states import BatchState
    from tallyho.storage.tables import Tables

__all__ = [
    "BATCH_INFO_FIELDS",
    "DEFAULT_LIST_LIMIT",
    "MAX_LIST_LIMIT",
    "list_batches_statement",
]

DEFAULT_LIST_LIMIT: Final = 100
MAX_LIST_LIMIT: Final = 1000

BATCH_INFO_FIELDS: Final = ("id", "kind", "key", "state", "created_at", "finished_at")
"""Колонки ``th_batch`` для ``BatchInfo``; следом в строке идёт ``attributes``."""


def list_batches_statement(  # ruff: ignore[too-many-arguments]  # все фильтры листинга именованные
    tables: Tables,
    *,
    kinds: Collection[str] = (),
    states: Collection[BatchState] = (),
    attributes: Mapping[str, AttributeValue] | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    before_id: UUID | None = None,
    limit: int,
) -> Select[*tuple[object, ...]]:
    """Построить запрос одной страницы листинга.

    Args:
        tables: Таблицы установки.
        kinds: Допустимые ``kind``; пусто — любые.
        states: Допустимые состояния; пусто — любые.
        attributes: Пары, которые должны содержаться в атрибутах корня.
        created_after: Нижняя граница ``created_at``, включительно.
        created_before: Верхняя граница ``created_at``, не включая.
        before_id: Последний id предыдущей страницы.
        limit: Сколько строк вернуть.

    Returns:
        Запрос: колонки ``BATCH_INFO_FIELDS`` и ``attributes``.
    """
    batch = tables.batch
    attr = tables.batch_attr
    columns: list[ColumnElement[object]] = [batch.c[name] for name in BATCH_INFO_FIELDS]
    columns.append(attr.c.attributes)
    where: list[ColumnElement[bool]] = [batch.c.parent_id.is_(None)]
    if kinds:
        where.append(batch.c.kind.in_(sorted(set(kinds))))
    if states:
        where.append(batch.c.state.in_(sorted({int(state) for state in states})))
    if created_after is not None:
        where.append(batch.c.created_at >= created_after)
    if created_before is not None:
        where.append(batch.c.created_at < created_before)
    if before_id is not None:
        where.append(batch.c.id < before_id)
    joined = batch.outerjoin(attr, attr.c.batch_id == batch.c.id)
    if attributes:
        # С фильтром строка атрибутов обязательна: внутреннее соединение даёт планировщику GIN.
        joined = batch.join(attr, attr.c.batch_id == batch.c.id)
        # Текст с приведением, а не jsonb-параметр: запрос должен рендериться литералами
        # для EXPLAIN-гарда, а значения уже нормализованы и содержат только str/int/bool.
        wanted = json.dumps(dict(attributes), ensure_ascii=False, separators=(",", ":"))
        where.append(attr.c.attributes.op("@>")(cast(literal(wanted, Text()), JSONB())))
    return (
        select(*columns).select_from(joined).where(*where).order_by(batch.c.id.desc()).limit(limit)
    )
