"""Запись и чтение ``th_batch_attr``: неизменяемые атрибуты и ``memo`` корня (D-038).

Строка пишется один раз в транзакции создания корня и только если есть что
хранить; читается по PK. Значения проверены и нормализованы слоем ``model``
до записи, поэтому здесь нет лимитов — только перенос в jsonb и обратно.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, insert, select

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.model.attributes import AttributeValue
    from tallyho.storage.tables import Tables

__all__ = [
    "attributes_from_json",
    "delete_batch_attributes",
    "insert_batch_attributes",
    "memo_from_json",
    "read_batch_attributes",
]


async def insert_batch_attributes(
    conn: AsyncConnection,
    tables: Tables,
    batch_id: UUID,
    *,
    attributes: Mapping[str, AttributeValue],
    memo: Mapping[str, object] | None,
) -> bool:
    """Сохранить атрибуты и ``memo`` только что созданного корня.

    Returns:
        ``False``, если хранить нечего и строка не создана.
    """
    if not attributes and memo is None:
        return False
    _ = await conn.execute(
        insert(tables.batch_attr).values(
            batch_id=batch_id,
            attributes=dict(attributes),
            memo=None if memo is None else dict(memo),
        )
    )
    return True


async def read_batch_attributes(
    conn: AsyncConnection, tables: Tables, root_id: UUID
) -> dict[str, AttributeValue]:
    """Прочитать атрибуты корня по PK.

    Returns:
        Атрибуты; пустой словарь, если строки нет.
    """
    attr = tables.batch_attr
    value: object = await conn.scalar(select(attr.c.attributes).where(attr.c.batch_id == root_id))
    return attributes_from_json(value)


async def delete_batch_attributes(
    conn: AsyncConnection, tables: Tables, batch_ids: Collection[UUID]
) -> None:
    """Удалить side-строки удаляемых батчей (retention)."""
    attr = tables.batch_attr
    _ = await conn.execute(delete(attr).where(attr.c.batch_id.in_(batch_ids)))


def attributes_from_json(value: object) -> dict[str, AttributeValue]:
    """Разобрать jsonb ``attributes``; чужие типы значений отбрасываются.

    Returns:
        Словарь ``str | int | bool``; пустой для ``NULL``.
    """
    if not isinstance(value, dict):
        return {}
    return {
        str(key): entry
        for key, entry in cast("dict[object, object]", value).items()
        if isinstance(entry, str | int | bool)
    }


def memo_from_json(value: object) -> dict[str, object] | None:
    """Разобрать jsonb ``memo``.

    Returns:
        JSON-объект или ``None``.
    """
    if not isinstance(value, dict):
        return None
    return {str(key): entry for key, entry in cast("dict[object, object]", value).items()}
