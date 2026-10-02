"""Мост между публичным API и storage-слоем одной установки."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tallyho.model.errors import ConfigurationError
from tallyho.protocols.broker import DeadLetter
from tallyho.storage.migrations import migrate, validate_prefix, validate_schema
from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from datetime import timedelta
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.engine.completer import Completer
    from tallyho.engine.dead_letters import DeadLetterReconciler
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import WorkerRuntime
    from tallyho.storage.tables import Tables

__all__ = ["Installation", "RuntimeServices", "create_installation", "migrate_installation"]

_NO_RECONCILER = "сверка с DLQ не собрана: процессу без relay события DLQ не принадлежат"


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
    dead_letters: DeadLetterReconciler | None = None

    async def finish_dead(
        self, item_id: UUID, *, generation: int, error_type: str, detail: str
    ) -> None:
        """Применить правило сверки к джобе, которую брокер перенёс в DLQ (UC-15).

        Item завершается, только если джоба — его текущее поколение отправки,
        а lease и записи outbox нет; живой lease того же поколения получает
        ``redelivered``.

        Raises:
            ConfigurationError: установка собрана без сверки с DLQ.
        """
        if self.dead_letters is None:
            raise ConfigurationError(_NO_RECONCILER)
        _ = await self.dead_letters.settle(
            (DeadLetter(item_id, generation, detail),), error_type=error_type
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
