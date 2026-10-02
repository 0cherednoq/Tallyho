"""Миграции через Alembic ``op`` в offline-режиме (``alembic upgrade --sql``)."""

from __future__ import annotations

import io

import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from tallyho.model.errors import ConfigurationError
from tallyho.storage.alembic import upgrade
from tallyho.storage.migrations import SCHEMA_VERSION


def offline_sql(*, version: int = 1, schema: str | None = "app", prefix: str = "th_") -> str:
    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": buffer}
    )
    upgrade(Operations(context), version=version, schema=schema, prefix=prefix)
    return buffer.getvalue()


def test_offline_script_contains_whole_migration() -> None:
    sql = offline_sql()
    assert sql.startswith("SET LOCAL lock_timeout = '5000ms';")
    assert "CREATE SCHEMA IF NOT EXISTS app;" in sql
    assert "CREATE TABLE app.th_batch (" in sql
    assert "CREATE INDEX th_batch_progress_idx ON app.th_batch (id)" in sql
    assert "INSERT INTO app.th_meta (key, value) VALUES ('schema_version', '1')" in sql
    assert "%(" not in sql


def test_offline_script_quotes_schema_and_uses_prefix() -> None:
    sql = offline_sql(schema='we"ird', prefix="acme_")
    assert 'CREATE SCHEMA IF NOT EXISTS "we""ird";' in sql
    assert 'CREATE TABLE "we""ird".acme_item (' in sql
    assert "th_" not in sql


def test_unknown_version_rejected() -> None:
    with pytest.raises(ConfigurationError):
        offline_sql(version=SCHEMA_VERSION + 1)


def test_version_two_offline_script_is_safe_and_complete() -> None:
    sql = offline_sql(version=2, schema='we"ird', prefix="acme_")
    assert (
        'ALTER TABLE "we""ird".acme_counter_delta ADD COLUMN created_at '
        "TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP;"
    ) in sql
    assert "CREATE INDEX acme_counter_delta_created_idx" in sql
    assert "VALUES ('schema_version', '2')" in sql


def test_version_three_offline_script_is_safe_and_complete() -> None:
    sql = offline_sql(version=3, schema='we"ird', prefix="acme_")
    assert sql.startswith("SET LOCAL lock_timeout = '5000ms';")
    assert 'CREATE TABLE "we""ird".acme_batch_attr (' in sql
    assert (
        'CREATE INDEX acme_batch_attr_attributes_idx ON "we""ird".acme_batch_attr '
        "USING gin (attributes jsonb_path_ops);"
    ) in sql
    assert (
        'CREATE INDEX acme_batch_kind_idx ON "we""ird".acme_batch (kind, id) '
        "WHERE parent_id IS NULL;"
    ) in sql
    assert "VALUES ('schema_version', '3')" in sql
    assert "%(" not in sql
    # "th_" встречается внутри jsonb_path_ops, поэтому проверяются имена объектов.
    assert ".th_" not in sql
    assert " th_" not in sql


def test_version_four_offline_script_is_safe_and_complete() -> None:
    sql = offline_sql(version=4, schema='we"ird', prefix="acme_")
    assert sql.startswith("SET LOCAL lock_timeout = '5000ms';")
    assert (
        'ALTER TABLE "we""ird".acme_lease ADD COLUMN redelivered BOOLEAN DEFAULT false NOT NULL;'
    ) in sql
    assert "VALUES ('schema_version', '4')" in sql
    assert "%(" not in sql
    assert "th_" not in sql
