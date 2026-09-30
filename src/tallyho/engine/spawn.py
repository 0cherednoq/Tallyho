"""Кэш структуры дерева и синхронная проверка маршрута ``spawn(..., into=)``.

Воркер загружает снимок дерева до вызова пользовательской задачи. Поэтому
``th.item.spawn`` может проверить право записи без запроса к БД: задача пишет
либо в свой батч, либо в этап, для которого её батч указан источником
``fed_by``. SQL-транзакция finish всё равно сверяет идентификаторы и состояния;
кэш отвечает за раннюю, понятную пользователю ошибку (ARCHITECTURE §8.1 п.2).
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, final

from sqlalchemy import select

from tallyho.model.errors import NotFoundError, SpawnTargetError

if TYPE_CHECKING:
    from collections.abc import Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.storage.tables import Tables

__all__ = ["SpawnRoute", "TreeCache", "TreeNode", "TreeSnapshot"]

_TARGET_MISSING = "целевой под-батч spawn не найден в дереве"
_TARGET_DENIED = "задача не может писать в указанный этап: её батч не является источником"


@dataclass(frozen=True, slots=True, kw_only=True)
class TreeNode:
    """Батч в снимке дерева и его разрешённые внешние писатели."""

    id: UUID
    root_id: UUID
    parent_id: UUID | None
    key: str | None
    feeders: frozenset[UUID] = frozenset()


@dataclass(frozen=True, slots=True, kw_only=True)
class SpawnRoute:
    """Разрешённая цель spawn, подготовленная до транзакции Completer."""

    source_id: UUID
    target_id: UUID
    root_id: UUID

    @property
    def into_self(self) -> bool:
        """Spawn идёт в тот же батч, поэтому увеличивает depth."""
        return self.source_id == self.target_id


@dataclass(frozen=True, slots=True, kw_only=True)
class TreeSnapshot:
    """Неизменяемый снимок одного дерева, пригодный для sync ``spawn``."""

    root_id: UUID
    nodes: Mapping[UUID, TreeNode]
    by_key: Mapping[str, UUID]

    def __post_init__(self) -> None:
        """Скопировать отображения, чтобы снимок нельзя было изменить снаружи."""
        object.__setattr__(self, "nodes", MappingProxyType(dict(self.nodes)))
        object.__setattr__(self, "by_key", MappingProxyType(dict(self.by_key)))

    def route(self, source_id: UUID, into: str | UUID | None = None) -> SpawnRoute:
        """Проверить и вернуть цель spawn.

        ``into=None`` означает свой батч. Строка — ключ под-батча в дереве,
        UUID — уже разрешённый handle. Чужой батч и sibling без ``fed_by``
        отклоняются до обращения Completer к БД.

        Returns:
            Проверенный маршрут.

        Raises:
            NotFoundError: исходного батча нет в снимке.
            SpawnTargetError: цель отсутствует или исходный батч не может в неё писать.
        """
        source = self.nodes.get(source_id)
        if source is None:
            raise NotFoundError(str(source_id))
        target: TreeNode | None
        if into is None:
            target = source
        elif isinstance(into, str):
            target_id = self.by_key.get(into)
            target = self.nodes.get(target_id) if target_id is not None else None
        else:
            target = self.nodes.get(into)
        if target is None:
            raise SpawnTargetError(_TARGET_MISSING)
        if target.id != source.id and source.id not in target.feeders:
            raise SpawnTargetError(_TARGET_DENIED)
        return SpawnRoute(source_id=source.id, target_id=target.id, root_id=self.root_id)


@final
class TreeCache:
    """Процессный кэш снимков деревьев для ItemContext."""

    def __init__(self) -> None:
        """Создать пустой процессный кэш."""
        self._roots: dict[UUID, TreeSnapshot] = {}
        self._batch_roots: dict[UUID, UUID] = {}

    def get(self, batch_id: UUID) -> TreeSnapshot | None:
        """Вернуть уже загруженный снимок по любому батчу дерева.

        Returns:
            Снимок или ``None``, если дерево ещё не загружено.
        """
        root_id = self._batch_roots.get(batch_id)
        return self._roots.get(root_id) if root_id is not None else None

    async def load(
        self, conn: AsyncConnection, tables: Tables, batch_id: UUID, *, refresh: bool = False
    ) -> TreeSnapshot:
        """Загрузить дерево одним снимком или вернуть кэшированный результат.

        Returns:
            Неизменяемый снимок дерева ``batch_id``.

        Raises:
            NotFoundError: батч не найден.
        """
        if not refresh and (cached := self.get(batch_id)) is not None:
            return cached
        batch = tables.batch
        root_id = await conn.scalar(select(batch.c.root_id).where(batch.c.id == batch_id))
        if root_id is None:
            raise NotFoundError(str(batch_id))
        result = await conn.execute(
            select(batch.c.id, batch.c.root_id, batch.c.parent_id, batch.c.key)
            .where(batch.c.root_id == root_id)
            .order_by(batch.c.id)
        )
        batch_rows: list[tuple[UUID, UUID, UUID | None, str | None]] = []
        for node_id, node_root_id, parent_id, key in result:
            batch_rows.append((node_id, node_root_id, parent_id, key))
        feed = tables.feed
        feed_result = await conn.execute(
            select(feed.c.feeder_id, feed.c.fed_id)
            .where(feed.c.fed_id.in_([row[0] for row in batch_rows]))
            .order_by(feed.c.fed_id, feed.c.feeder_id)
        )
        feeders: dict[UUID, set[UUID]] = {}
        for feeder_id, fed_id in feed_result:
            feeders.setdefault(fed_id, set()).add(feeder_id)
        nodes = {
            node_id: TreeNode(
                id=node_id,
                root_id=node_root_id,
                parent_id=parent_id,
                key=key,
                feeders=frozenset(feeders.get(node_id, set())),
            )
            for node_id, node_root_id, parent_id, key in batch_rows
        }
        by_key = {
            node.key: node.id
            for node in nodes.values()
            if node.parent_id is not None and node.key is not None
        }
        snapshot = TreeSnapshot(root_id=root_id, nodes=nodes, by_key=by_key)
        previous = self._roots.get(root_id)
        if previous is not None:
            for node_id in previous.nodes:
                _ = self._batch_roots.pop(node_id, None)
        self._roots[root_id] = snapshot
        for node_id in nodes:
            self._batch_roots[node_id] = root_id
        return snapshot

    def invalidate(self, root_id: UUID) -> None:
        """Сбросить дерево после динамического создания под-батча."""
        snapshot = self._roots.pop(root_id, None)
        if snapshot is None:
            return
        for node_id in snapshot.nodes:
            _ = self._batch_roots.pop(node_id, None)
