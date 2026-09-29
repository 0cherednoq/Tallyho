"""Имена схем интеграционных тестов (без БД)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests.helpers.db import SCHEMA_PREFIX, unique_schema_name

if TYPE_CHECKING:
    import pytest


def test_schema_name_contains_xdist_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    assert unique_schema_name().startswith(f"{SCHEMA_PREFIX}gw3_")


def test_schema_name_without_xdist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    assert unique_schema_name().startswith(f"{SCHEMA_PREFIX}main_")


def test_schema_name_fits_postgres_identifier_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw127")
    name = unique_schema_name()
    assert len(name.encode()) <= 63  # NAMEDATALEN - 1
    assert name == name.lower()  # без кавычек PostgreSQL приводит имя к нижнему регистру
    assert name != unique_schema_name()
