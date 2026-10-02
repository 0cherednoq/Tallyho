"""Мост между публичным API и storage-слоем одной установки."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tallyho.engine.completer import FinishResult, ItemRef
from tallyho.model.states import ResultClass
from tallyho.storage.migrations import migrate, validate_prefix, validate_schema
from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from datetime import timedelta
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.engine.completer import Completer
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import WorkerRuntime
    from tallyho.storage.tables import Tables

__all__ = ["Installation", "RuntimeServices", "create_installation", "migrate_installation"]


@dataclass(frozen=True, slots=True)
class Installation:
    """Движок пользователя и таблицы выбранных schema и prefix.

    Схема записана в таблицах, а не в опциях движка: запросы библиотеки
    находят свои таблицы и на соединении пользователя, а сессия tx-хука
    адресует доменные таблицы так же, как остальной код пользователя.
    """

    engine: AsyncEngine
    schema: str | None
    prefix: str
    tables: Tables

    @property
    def maintenance_identity(self) -> str | None:
        """Имя advisory-блокировки лидера maintenance: одна схема — один лидер.

        ``None`` для установки без схемы: имя выводится из опций движка.
        """
        return None if self.schema is None else f"{self.schema}:maintenance"


@dataclass(frozen=True, slots=True)
class RuntimeServices:
    """Worker-зависимости, которые broker adapter связывает с runtime."""

    completer: Completer
    tree_cache: TreeCache
    heartbeat_every: timedelta
    runtime: WorkerRuntime

    async def finish_dead(
        self, item_id: UUID, batch_id: UUID, *, error_type: str, detail: str
    ) -> None:
        """Завершить Item, который брокер окончательно перенёс в DLQ."""
        _ = await self.completer.finish(
            ItemRef(item_id, batch_id),
            FinishResult(
                result_class=ResultClass.ERROR,
                label="exhausted",
                error={"type": error_type, "message": detail},
            ),
        )


def create_installation(engine: AsyncEngine, schema: str | None, prefix: str) -> Installation:
    """Проверить идентификаторы и описать установку без обращения к БД.

    Returns:
        Неизменяемое описание установки.
    """
    validate_schema(schema)
    validate_prefix(prefix)
    return Installation(engine, schema, prefix, build_metadata(prefix, schema=schema))


async def migrate_installation(value: Installation, *, lock_timeout: timedelta) -> int:
    """Применить миграции описанной установки.

    Returns:
        Текущая версия схемы.
    """
    return await migrate(
        value.engine,
        value.schema,
        value.prefix,
        lock_timeout=lock_timeout,
    )
