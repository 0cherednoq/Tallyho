"""Запрос спроса активных батчей ``th.demand(...)`` (ARCHITECTURE §11.2).

Один составной запрос по активным (``open``/``sealed``) батчам требуемых
видов. Для каждого батча коррелированными подзапросами считаются:

* ``pending = found - done`` — из ``th_counter`` плюс несвёрнутых
  ``th_counter_delta`` (§9.3): два агрегата по слотам;
* ``in_flight`` — ``count(*)`` по ``th_lease(batch_id)``;
* ``deferred`` — ``count(*)`` по ``th_outbox`` с ``defer_reason IS NOT NULL``.

Число SQL-запросов не зависит от числа батчей (всё в одном statement), но
стоимость O(активные батчи * слоты счётчиков). Фильтр ``kind`` обслуживает
индекс ``th_batch (kind) WHERE state IN (open, sealed)``, фильтр по атрибутам —
GIN ``jsonb_path_ops`` на ``th_batch_attr`` корня (containment ``@>``).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy import BigInteger, Text, func, literal, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import JSONB

from tallyho.model.states import BatchState

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

    from sqlalchemy import Select
    from sqlalchemy.sql import ColumnElement

    from tallyho.model.attributes import AttributeValue
    from tallyho.storage.tables import Tables

__all__ = ["DEMAND_FIELDS", "demand_statement"]

DEMAND_FIELDS: Final = ("id", "kind", "key", "max_in_flight")
"""Прямые колонки ``th_batch``; дальше — paused, pending, in_flight, deferred, attributes."""

_OPEN_OR_SEALED: Final = (int(BatchState.OPEN), int(BatchState.SEALED))


def demand_statement(
    tables: Tables,
    *,
    kinds: Collection[str],
    attributes: Mapping[str, AttributeValue] | None = None,
    limit: int,
) -> Select[*tuple[object, ...]]:
    """Построить запрос спроса активных батчей требуемых видов.

    Args:
        tables: Таблицы установки.
        kinds: Виды батчей (обязателен хотя бы один).
        attributes: Пары, которые должны содержаться в атрибутах корня.
        limit: Сколько строк вернуть.

    Returns:
        Запрос: колонки ``DEMAND_FIELDS``, ``paused``, ``pending``,
        ``in_flight``, ``deferred`` и ``attributes`` корня.
    """
    batch = tables.batch
    counter = tables.counter
    delta = tables.counter_delta
    lease = tables.lease
    outbox = tables.outbox
    attr = tables.batch_attr

    counter_pending = (
        select(
            sql_cast(
                func.coalesce(
                    func.sum(
                        counter.c.total
                        - counter.c.ok
                        - counter.c.skip
                        - counter.c.error
                        - counter.c.cancelled
                    ),
                    0,
                ),
                BigInteger,
            )
        )
        .where(counter.c.batch_id == batch.c.id)
        .scalar_subquery()
    )
    delta_pending = (
        select(
            sql_cast(
                func.coalesce(
                    func.sum(
                        delta.c.d_total
                        - delta.c.d_ok
                        - delta.c.d_skip
                        - delta.c.d_error
                        - delta.c.d_cancelled
                    ),
                    0,
                ),
                BigInteger,
            )
        )
        .where(delta.c.batch_id == batch.c.id)
        .scalar_subquery()
    )
    in_flight = select(func.count()).where(lease.c.batch_id == batch.c.id).scalar_subquery()
    deferred = (
        select(func.count())
        .where(outbox.c.batch_id == batch.c.id, outbox.c.defer_reason.is_not(None))
        .scalar_subquery()
    )

    extra: tuple[ColumnElement[object], ...] = (
        cast("ColumnElement[object]", batch.c.paused_at.is_not(None).label("paused")),
        cast("ColumnElement[object]", (counter_pending + delta_pending).label("pending")),
        cast("ColumnElement[object]", in_flight.label("in_flight")),
        cast("ColumnElement[object]", deferred.label("deferred")),
        cast("ColumnElement[object]", attr.c.attributes.label("attributes")),
    )
    columns: list[ColumnElement[object]] = [batch.c[name] for name in DEMAND_FIELDS]
    columns.extend(extra)

    where: list[ColumnElement[bool]] = [
        batch.c.kind.in_(sorted(set(kinds))),
        batch.c.state.in_(_OPEN_OR_SEALED),
    ]
    # Атрибуты — у корня дерева: под-батч берёт их по root_id.
    joined = batch.outerjoin(attr, attr.c.batch_id == batch.c.root_id)
    if attributes:
        joined = batch.join(attr, attr.c.batch_id == batch.c.root_id)
        # Литерал с приведением, а не bind-параметр: запрос рендерится для EXPLAIN-гарда,
        # а значения нормализованы и содержат только str/int/bool.
        wanted = json.dumps(dict(attributes), ensure_ascii=False, separators=(",", ":"))
        where.append(attr.c.attributes.op("@>")(sql_cast(literal(wanted, Text()), JSONB())))
    return select(*columns).select_from(joined).where(*where).order_by(batch.c.id).limit(limit)
