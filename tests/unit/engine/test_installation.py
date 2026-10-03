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
    assert value.maintenance_identity == "app:maintenance\x00jobs_"


def test_default_prefix_keeps_previous_leader_identity() -> None:
    # Процессы до и после обновления спорят за одну блокировку.
    assert create_installation(_engine(), "app", "th_").maintenance_identity == "app:maintenance"


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (("app", "th_"), ("app", "jobs_")),
        (("app", "jobs_"), ("app", "mail_")),
        # Схема с двоеточием не выдаёт себя за другую установку.
        (("app:maintenance", "th_"), ("app", "maintenance")),
    ],
)
def test_leader_identity_differs_between_installations(
    first: tuple[str, str], second: tuple[str, str]
) -> None:
    left = create_installation(_engine(), *first).maintenance_identity
    right = create_installation(_engine(), *second).maintenance_identity
    assert left != right


class _OptionsEngine:
    """Движок, у которого описание установки читает только опции выполнения."""

    options: dict[str, object]

    def __init__(self, options: dict[str, object]) -> None:
        self.options = options

    def get_execution_options(self) -> dict[str, object]:
        return self.options


@pytest.mark.parametrize(
    ("options", "prefix", "identity"),
    [
        ({}, "th_", "public:maintenance"),
        ({"schema_translate_map": {None: "tenant"}}, "th_", "tenant:maintenance"),
        ({"schema_translate_map": {None: None}}, "jobs_", "public:maintenance\x00jobs_"),
        ({"schema_translate_map": "broken"}, "th_", "public:maintenance"),
    ],
)
def test_installation_without_schema_takes_leader_schema_from_engine(
    options: dict[str, object], prefix: str, identity: str
) -> None:
    engine = cast("AsyncEngine", cast("object", _OptionsEngine(options)))
    value = create_installation(engine, None, prefix)

    assert value.tables.batch.fullname == f"{prefix}batch"
    assert value.maintenance_identity == identity


@pytest.mark.parametrize(("schema", "prefix"), [("x" * 64, "th_"), ("app", "bad-prefix")])
def test_installation_rejects_unsafe_identifiers(schema: str, prefix: str) -> None:
    with pytest.raises(ConfigurationError):
        _ = create_installation(_engine(), schema, prefix)
