"""Миграции без БД: проверка имён и DDL версии 1 против golden-снимка схемы."""

from __future__ import annotations

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

GOLDEN = Path(__file__).parent / "golden" / "ddl_v1.sql"
DIALECT = create_mock_engine("postgresql+asyncpg://", executor=print).dialect


def compiled(statement: Executable) -> str:
    assert isinstance(statement, ClauseElement)
    return str(statement.compile(dialect=DIALECT))


def ddl_without_schema(schema: str) -> str:
    """DDL таблиц и индексов версии 1 в формате golden-файла, без квалификатора схемы."""
    statements = [
        compiled(statement).replace(f"{schema}.", "")
        for statement in migration_statements(1, schema=schema)
        if isinstance(statement, CreateTable | CreateIndex)
    ]
    lines = "\n".join(f"{statement.strip()};\n" for statement in statements).splitlines()
    return "\n".join(line.rstrip() for line in lines) + "\n"


def test_version_one_matches_tables_snapshot() -> None:
    # Историческая схема v1 заморожена отдельным golden-снимком.
    assert ddl_without_schema("app") == GOLDEN.read_text(encoding="utf-8")


def test_version_two_adds_timestamp_and_index() -> None:
    statements = migration_statements(2, schema="app")
    sql = [compiled(statement) for statement in statements]
    assert sql[1] == (
        "ALTER TABLE app.th_counter_delta ADD COLUMN created_at "
        "TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP"
    )
    assert sql[2] == ("ALTER TABLE app.th_counter_delta ALTER COLUMN created_at DROP DEFAULT")
    assert sql[3] == (
        "CREATE INDEX th_counter_delta_created_idx ON app.th_counter_delta (created_at, id)"
    )
    assert "VALUES ('schema_version', '2')" in sql[-1]


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
    assert all("th_" not in text for text in ddl)
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
