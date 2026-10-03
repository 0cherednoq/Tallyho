"""Мост между публичным API и storage-слоем одной установки."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from tallyho.model.errors import ConfigurationError
from tallyho.protocols.broker import DeadLetter
from tallyho.storage.migrations import migrate, validate_prefix, validate_schema
from tallyho.storage.tables import DEFAULT_PREFIX, build_metadata

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
    def maintenance_identity(self) -> str:
        """Имя advisory-блокировки лидера maintenance: одна установка — один лидер (§3.2).

        Установку задают схема и префикс. Без ``schema`` схема берётся из
        ``schema_translate_map`` движка, иначе ``public``. Для префикса по
        умолчанию имя прежнее (``<схема>:maintenance``): процессы разных версий
        при обновлении не становятся лидерами одновременно. Другой префикс
        добавляется через NUL — в имени схемы его не бывает (``validate_schema``),
        поэтому имена разных установок не совпадают.
        """
        schema = self.schema if self.schema is not None else _engine_schema(self.engine)
        identity = f"{schema}:maintenance"
        if self.prefix == DEFAULT_PREFIX:
            return identity
        return f"{identity}\x00{self.prefix}"


def _engine_schema(engine: AsyncEngine) -> str:
    mapping: object = engine.get_execution_options().get("schema_translate_map")
    if isinstance(mapping, Mapping):
        translated = cast("Mapping[object, object]", mapping).get(None)
        if isinstance(translated, str):
            return translated
    return "public"


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
