"""Описание таблиц: DDL под PostgreSQL, снимок схемы и правила индексов."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Column, Index, MetaData, SmallInteger, Table, Uuid, create_mock_engine
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.sql import visitors

from tallyho.storage.tables import DEFAULT_PREFIX, build_metadata

if TYPE_CHECKING:
    from collections.abc import Iterable

    from tallyho.storage.tables import Tables

GOLDEN = Path(__file__).parent / "golden" / "ddl.sql"
UPDATE_GOLDEN_ENV = "TALLYHO_UPDATE_GOLDEN"

TABLE_SUFFIXES = (
    "batch",
    "batch_attr",
    "item",
    "outbox",
    "lease",
    "feed",
    "counter",
    "counter_delta",
    "metric",
    "item_mark",
    "expiry",
    "window",
    "meta",
)

# Колонки th_item, которые меняет finish: индекс по ним ломает HOT update.
ITEM_MUTABLE_COLUMNS = frozenset({"state", "label", "result", "error", "finished_at"})

AGGRESSIVE_AUTOVACUUM = {"autovacuum_vacuum_scale_factor": 0, "autovacuum_vacuum_threshold": 1000}
HOT_TABLE = {"fillfactor": 50, **AGGRESSIVE_AUTOVACUUM}


def render_ddl(tables: Tables) -> str:
    """DDL всех таблиц и индексов; строки без хвостовых пробелов, как в golden-файле."""
    dialect = create_mock_engine("postgresql+asyncpg://", executor=print).dialect
    statements: list[str] = []
    for table in tables.metadata.sorted_tables:
        statements.append(str(CreateTable(table).compile(dialect=dialect)))
        statements.extend(
            str(CreateIndex(index).compile(dialect=dialect))
            for index in sorted(table.indexes, key=lambda index: str(index.name))
        )
    lines = "\n".join(f"{statement.strip()};\n" for statement in statements).splitlines()
    return "\n".join(line.rstrip() for line in lines) + "\n"


def index_columns(index: Index) -> set[str]:
    """Имена колонок, которые индекс хранит или упоминает в условии partial-индекса."""
    names = {column.name for column in index.columns}
    where = index.dialect_options["postgresql"]["where"]
    if where is not None:
        names.update(node.name for node in visitors.iterate(where) if isinstance(node, Column))
    return names


def mutable_item_indexes(indexes: Iterable[Index]) -> list[str]:
    return [str(index.name) for index in indexes if index_columns(index) & ITEM_MUTABLE_COLUMNS]


@pytest.fixture
def tables() -> Tables:
    return build_metadata()


def test_ddl_matches_golden(tables: Tables) -> None:
    ddl = render_ddl(tables)
    if os.environ.get(UPDATE_GOLDEN_ENV):
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(ddl, encoding="utf-8", newline="\n")
    expected = GOLDEN.read_text(encoding="utf-8")
    assert ddl == expected, f"Схема изменилась: обнови golden-файл ({UPDATE_GOLDEN_ENV}=1)"


def test_default_prefix_names_all_tables(tables: Tables) -> None:
    assert DEFAULT_PREFIX == "th_"
    assert set(tables.metadata.tables) == {f"th_{suffix}" for suffix in TABLE_SUFFIXES}


def test_prefix_applies_to_tables_and_indexes() -> None:
    tables = build_metadata("acme_")
    assert set(tables.metadata.tables) == {f"acme_{suffix}" for suffix in TABLE_SUFFIXES}
    for table in tables.metadata.sorted_tables:
        for index in table.indexes:
            assert str(index.name).startswith(f"{table.name}_")


def test_each_call_builds_independent_metadata() -> None:
    first, second = build_metadata(), build_metadata()
    assert first.metadata is not second.metadata
    assert first.item.c.id is not second.item.c.id


def test_tables_have_no_schema_and_no_foreign_keys(tables: Tables) -> None:
    # Схему подставляет schema_translate_map; целостность держит библиотека.
    for table in tables.metadata.sorted_tables:
        assert table.schema is None
        assert not table.foreign_keys


def test_item_indexes_skip_mutable_columns(tables: Tables) -> None:
    assert mutable_item_indexes(tables.item.indexes) == []


def test_mutable_index_rule_catches_violations() -> None:
    item = Table("probe", MetaData(), Column("id", Uuid()), Column("state", SmallInteger()))
    Index("by_state", item.c.state)
    Index("partial", item.c.id, postgresql_where=item.c.state < 10)
    Index("clean", item.c.id)
    assert sorted(mutable_item_indexes(item.indexes)) == ["by_state", "partial"]


def test_batch_attr_has_gin_index_for_containment(tables: Tables) -> None:
    # jsonb_path_ops обслуживает только containment (@>) — им и фильтрует листинг.
    (index,) = tables.batch_attr.indexes
    options = index.dialect_options["postgresql"]
    assert [column.name for column in index.columns] == ["attributes"]
    assert options["using"] == "gin"
    assert options["ops"] == {"attributes": "jsonb_path_ops"}
    assert [column.name for column in tables.batch_attr.primary_key] == ["batch_id"]
    assert not tables.batch_attr.c.attributes.nullable
    assert tables.batch_attr.c.memo.nullable


def test_batch_kind_index_covers_only_roots(tables: Tables) -> None:
    index = next(index for index in tables.batch.indexes if index.name == "th_batch_kind_idx")
    assert [column.name for column in index.columns] == ["kind", "id"]
    assert not index.unique
    assert index_columns(index) == {"kind", "id", "parent_id"}


@pytest.mark.parametrize(
    ("suffix", "options"),
    [
        ("item", {"fillfactor": 85}),
        ("lease", AGGRESSIVE_AUTOVACUUM),
        ("outbox", AGGRESSIVE_AUTOVACUUM),
        ("window", AGGRESSIVE_AUTOVACUUM),
        ("counter_delta", AGGRESSIVE_AUTOVACUUM),
        ("counter", HOT_TABLE),
        ("metric", HOT_TABLE),
    ],
)
def test_storage_parameters(tables: Tables, suffix: str, options: dict[str, int]) -> None:
    table = tables.metadata.tables[f"th_{suffix}"]
    assert table.dialect_options["postgresql"]["with"] == options
