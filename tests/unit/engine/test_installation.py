"""Описание установки: схема живёт в таблицах, движок пользователя не меняется."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from tallyho.engine.installation import create_installation
from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []


class _Engine:
    """Движок без методов: описание установки не должно его трогать."""


def _engine() -> AsyncEngine:
    return cast("AsyncEngine", cast("object", _Engine()))


def test_installation_keeps_user_engine_and_puts_schema_into_tables() -> None:
    engine = _engine()

    value = create_installation(engine, "app", "jobs_")

    # Тот же объект: опции выполнения (schema_translate_map) движку не навязываются.
    assert value.engine is engine
    assert (value.schema, value.prefix) == ("app", "jobs_")
    assert value.tables.batch.fullname == "app.jobs_batch"
    assert value.maintenance_identity == "app:maintenance"


def test_installation_without_schema_leaves_tables_unqualified() -> None:
    value = create_installation(_engine(), None, "th_")

    assert value.tables.batch.fullname == "th_batch"
    # Имя блокировки лидера Maintenance выведет сам: из опций движка.
    assert value.maintenance_identity is None


@pytest.mark.parametrize(("schema", "prefix"), [("x" * 64, "th_"), ("app", "bad-prefix")])
def test_installation_rejects_unsafe_identifiers(schema: str, prefix: str) -> None:
    with pytest.raises(ConfigurationError):
        _ = create_installation(_engine(), schema, prefix)
