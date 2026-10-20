"""Миграции без БД: имена объектов, замороженная версия 1 и сумма версий против golden."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_mock_engine
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable
from sqlalchemy.sql import ClauseElement

from tallyho.model.errors import ConfigurationError
from tallyho.storage.migrations import (
    SCHEMA_VERSION,
    migration_statements,
    validate_prefix,
    validate_schema,
)

if TYPE_CHECKING:
    from sqlalchemy.sql.base import Executable

GOLDEN = Path(__file__).parent / "golden" / "ddl.sql"
GOLDEN_V1 = Path(__file__).parent / "golden" / "ddl_v1.sql"
UPDATE_GOLDEN_ENV = "TALLYHO_UPDATE_GOLDEN"
DIALECT = create_mock_engine("postgresql+asyncpg://", executor=print).dialect


def compiled(statement: Executable) -> str:
    assert isinstance(statement, ClauseElement)
    return str(statement.compile(dialect=DIALECT))


def ddl_without_schema(schema: str, *versions: int) -> str:
    """DDL таблиц и индексов указанных версий в формате golden-файла, без схемы."""
    statements = [
        compiled(statement).replace(f"{schema}.", "")
        for version in versions
        for statement in migration_statements(version, schema=schema)
        if isinstance(statement, CreateTable | CreateIndex)
    ]
    lines = "\n".join(f"{statement.strip()};\n" for statement in statements).splitlines()
    return "\n".join(line.rstrip() for line in lines) + "\n"


def objects(ddl: str) -> set[str]:
    """Операции DDL как множество: порядок между версиями контрактом не является."""
    return {block.strip() for block in ddl.split(";\n") if block.strip()}


def test_version_one_is_frozen() -> None:
    # Версия 1 опубликована (0.0.1, 0.0.2a1): её DDL меняться не может (D-088).
    ddl = ddl_without_schema("app", 1)
    if os.environ.get(UPDATE_GOLDEN_ENV):
        GOLDEN_V1.write_text(ddl, encoding="utf-8", newline="\n")
    assert ddl == GOLDEN_V1.read_text(encoding="utf-8")


def test_all_versions_together_match_tables_snapshot() -> None:
    # Все миграции вместе создают ровно то, что описывает build_metadata.
    versions = range(1, SCHEMA_VERSION + 1)
    assert objects(ddl_without_schema("app", *versions)) == objects(
        GOLDEN.read_text(encoding="utf-8")
    )


def test_later_version_creates_only_its_own_tables() -> None:
    created = {
        block.split("CREATE TABLE ")[1].split(" ")[0]
        for block in objects(ddl_without_schema("app", 2))
        if "CREATE TABLE " in block
    }
    assert created == {"th_checkpoint"}


def test_statements_order() -> None:
    statements = migration_statements(1, schema="app")
    assert compiled(statements[0]) == "SET LOCAL lock_timeout = '5000ms'"
    assert isinstance(statements[1], CreateSchema)
    last = compiled(statements[-1])
    assert last.startswith("INSERT INTO app.th_meta")
    assert "VALUES ('schema_version', '1') ON CONFLICT (key) DO UPDATE" in last


def test_schema_none_has_no_create_schema() -> None:
    statements = migration_statements(1, schema=None)
    assert not any(isinstance(statement, CreateSchema) for statement in statements)
    assert compiled(statements[1]).startswith("\nCREATE TABLE th_batch")


def test_special_schema_name_is_quoted() -> None:
    schema = 'we"ird; DROP'
    statements = migration_statements(1, schema=schema)
    assert compiled(statements[1]) == 'CREATE SCHEMA IF NOT EXISTS "we""ird; DROP"'
    assert 'CREATE TABLE "we""ird; DROP".th_batch' in compiled(statements[2])


def test_prefix_applies_to_all_objects() -> None:
    statements = migration_statements(1, schema="app", prefix="acme_")
    ddl = [compiled(s) for s in statements if isinstance(s, CreateTable | CreateIndex)]
    assert ddl
    # "th_" встречается внутри jsonb_path_ops, поэтому проверяются имена объектов.
    assert all(".th_" not in text and " th_" not in text for text in ddl)
    assert "INSERT INTO app.acme_meta" in compiled(statements[-1])


@pytest.mark.parametrize(
    ("timeout", "expected"),
    [
        (timedelta(seconds=5), "5000ms"),
        (timedelta(0), "0ms"),
        (timedelta(microseconds=1500), "1ms"),
    ],
)
def test_lock_timeout_value(timeout: timedelta, expected: str) -> None:
    statement = migration_statements(1, schema="app", lock_timeout=timeout)[0]
    assert compiled(statement) == f"SET LOCAL lock_timeout = '{expected}'"


def test_negative_lock_timeout_rejected() -> None:
    with pytest.raises(ConfigurationError):
        migration_statements(1, schema="app", lock_timeout=timedelta(seconds=-1))


@pytest.mark.parametrize("version", [0, SCHEMA_VERSION + 1])
def test_unknown_version_rejected(version: int) -> None:
    with pytest.raises(ConfigurationError):
        migration_statements(version, schema="app")


@pytest.mark.parametrize("prefix", ["th_", "_", "a", "acme_v2_", "a" * 16, "x0123456789abcde"])
def test_valid_prefix(prefix: str) -> None:
    assert validate_prefix(prefix) == prefix


@pytest.mark.parametrize(
    "prefix",
    ["", "0th_", "Th_", "th-", 'th"', "th_; drop", "a" * 17, "тх_", "th_\n"],
)
def test_invalid_prefix(prefix: str) -> None:
    with pytest.raises(ConfigurationError):
        validate_prefix(prefix)
    with pytest.raises(ConfigurationError):
        migration_statements(1, schema="app", prefix=prefix)


@pytest.mark.parametrize("schema", [None, "app", 'we"ird; DROP', "Схема", "s" * 63, "я" * 31])
def test_valid_schema(schema: str | None) -> None:
    assert validate_schema(schema) == schema


@pytest.mark.parametrize("schema", ["", "a\x00b", "s" * 64, "я" * 32])
def test_invalid_schema(schema: str) -> None:
    with pytest.raises(ConfigurationError):
        validate_schema(schema)
    with pytest.raises(ConfigurationError):
        migration_statements(1, schema=schema)
