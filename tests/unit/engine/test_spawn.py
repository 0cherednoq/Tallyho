"""Синхронная маршрутизация spawn по снимку дерева."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tallyho.engine.spawn import TreeNode, TreeSnapshot
from tallyho.model.errors import NotFoundError, SpawnTargetError

__all__: list[str] = []

if TYPE_CHECKING:
    from uuid import UUID


def _tree() -> tuple[TreeSnapshot, TreeNode, TreeNode, TreeNode]:
    root_id = uuid4()
    source = TreeNode(id=uuid4(), root_id=root_id, parent_id=root_id, key="pages")
    target = TreeNode(
        id=uuid4(),
        root_id=root_id,
        parent_id=root_id,
        key="cards",
        feeders=frozenset({source.id}),
    )
    sibling = TreeNode(id=uuid4(), root_id=root_id, parent_id=root_id, key="other")
    nodes = {node.id: node for node in (source, target, sibling)}
    snapshot = TreeSnapshot(
        root_id=root_id,
        nodes=nodes,
        by_key={node.key: node.id for node in nodes.values() if node.key is not None},
    )
    return snapshot, source, target, sibling


def test_routes_to_self_and_fed_stage() -> None:
    tree, source, target, _ = _tree()
    own = tree.route(source.id)
    by_key = tree.route(source.id, "cards")
    by_id = tree.route(source.id, target.id)
    assert own.into_self
    assert own.target_id == source.id
    assert by_key == by_id
    assert not by_key.into_self
    assert by_key.target_id == target.id


def test_rejects_unknown_and_unrelated_targets() -> None:
    tree, source, _, sibling = _tree()
    with pytest.raises(SpawnTargetError, match="не найден"):
        _ = tree.route(source.id, "missing")
    with pytest.raises(SpawnTargetError, match="не является источником"):
        _ = tree.route(source.id, sibling.id)
    with pytest.raises(NotFoundError):
        _ = tree.route(uuid4())


def test_snapshot_copies_input_mappings() -> None:
    root_id = uuid4()
    node = TreeNode(id=root_id, root_id=root_id, parent_id=None, key=None)
    nodes = {root_id: node}
    keys: dict[str, UUID] = {}
    snapshot = TreeSnapshot(root_id=root_id, nodes=nodes, by_key=keys)
    nodes.clear()
    assert snapshot.route(root_id).target_id == root_id
