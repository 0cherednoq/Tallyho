"""Публичная конфигурация и сборка клиента Tallyho."""

from __future__ import annotations

import asyncio
from dataclasses import fields
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, Self, TypeVar, cast

import pytest

from tallyho import Call, Settings, Tallyho
from tallyho.engine import RuntimeServices
from tallyho.model.errors import ClosedError, ConfigurationError
from tallyho.protocols.broker import DeadLetters, Verdict

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from tallyho.model.views import BatchSummary
    from tallyho.protocols.broker import Message

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")


class FakeEngine:
    """Достаточная для не-I/O сборки замена AsyncEngine."""

    def execution_options(self, **_options: object) -> Self:
        return self


class Adapter:
    """Dispatcher с необязательным worker install hook."""

    services: object = None

    def task_name(self, fn: Callable[P, object]) -> str:
        return getattr(fn, "__name__", "task")

    async def dispatch(self, messages: Sequence[Message]) -> None:
        _ = messages

    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        return fn

    def retry_verdict(self, exc: BaseException) -> Verdict:
        _ = exc
        return Verdict.FINAL

    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)

    def install_runtime(self, services: object) -> None:
        self.services = services


def _client() -> Tallyho:
    engine = cast("AsyncEngine", cast("object", FakeEngine()))
    return Tallyho(engine)


def test_defaults_match_architecture_table() -> None:
    value = Settings()
    expected = {
        "counter_slots": 8,
        "completer_tick": timedelta(milliseconds=20),
        "completer_max_batch": 500,
        "completer_backpressure": 10_000,
        "lease_ttl": timedelta(seconds=60),
        "heartbeat_every": timedelta(seconds=20),
        "relay_grace": timedelta(seconds=5),
        "relay_claim_ttl": timedelta(seconds=30),
        "finalize_grace": timedelta(seconds=30),
        "hook_timeout": timedelta(seconds=10),
        "hook_backoff_initial": timedelta(seconds=1),
        "hook_backoff_max": timedelta(minutes=5),
        "snapshot_tick": timedelta(milliseconds=500),
        "estimate_min_basis": 20,
        "estimate_min_share": 0.05,
        "eta_window": timedelta(seconds=60),
        "max_items": None,
        "sweep_interval": timedelta(seconds=5),
        "lock_timeout": timedelta(seconds=5),
        "close_timeout": timedelta(seconds=10),
        "retention": timedelta(days=14),
        "watch_throttle": timedelta(milliseconds=500),
        "items_scan_window": 5000,
        "attributes_max_keys": 32,
        "attributes_max_key_bytes": 128,
        "attributes_max_value_bytes": 512,
        "attributes_max_bytes": 8192,
        "memo_max_bytes": 16384,
    }
    assert {field.name: getattr(value, field.name) for field in fields(value)} == expected


def test_engine_settings_carry_every_engine_field() -> None:
    value = Settings(hook_backoff_initial=timedelta(seconds=7), hook_backoff_max=timedelta(hours=1))
    engine = value.engine_settings()
    # Каждое поле DTO engine берётся из одноимённой настройки клиента.
    for field in fields(engine):
        assert getattr(engine, field.name) == getattr(value, field.name), field.name
    assert engine.hook_backoff_initial == timedelta(seconds=7)


@pytest.mark.parametrize(
    "settings",
    [
        {"counter_slots": 0},
        {"counter_slots": True},
        {"items_scan_window": 0},
        {"attributes_max_keys": 0},
        {"memo_max_bytes": True},
        {"completer_backpressure": 10, "completer_max_batch": 11},
        {"heartbeat_every": timedelta(0)},
        {"close_timeout": timedelta(0)},
        {"relay_grace": timedelta(seconds=-1)},
        {"hook_backoff_initial": timedelta(minutes=6)},
        {"estimate_min_share": 2.0},
        {"max_items": 0},
        {"retention": timedelta(0)},
        {"unknown": 1},
        {"lease_ttl": "one minute"},
    ],
)
def test_invalid_configuration_is_tallyho_error(settings: dict[str, object]) -> None:
    with pytest.raises(ConfigurationError):
        _ = Settings.overridden(settings)


@pytest.mark.parametrize(("schema", "prefix"), [("x" * 64, "th_"), (None, "bad-prefix")])
def test_invalid_installation_identifiers(schema: str | None, prefix: str) -> None:
    engine = cast("AsyncEngine", cast("object", FakeEngine()))
    with pytest.raises(ConfigurationError):
        _ = Tallyho(engine, schema=schema, prefix=prefix)


def test_install_assembles_services_once_and_requires_it_for_maintenance() -> None:
    client = _client()
    with pytest.raises(ConfigurationError, match="install"):
        _ = client.maintenance()
    adapter = Adapter()
    client.install(adapter)
    assert isinstance(adapter.services, RuntimeServices)
    assert client.maintenance() is client.maintenance()
    with pytest.raises(ConfigurationError, match="уже установлен"):
        client.install(adapter)


async def test_install_without_broker_serves_maintenance_but_not_producer() -> None:
    client = _client()
    client.install(None)

    assert client.maintenance() is client.maintenance()
    with pytest.raises(ConfigurationError, match="install"):
        _ = client.batch("kind")
    with pytest.raises(ConfigurationError, match="install"):
        _ = client.call(sample_task, 1, mode="strict")
    with pytest.raises(ConfigurationError, match="уже установлен"):
        client.install(Adapter())
    await client.aclose()


async def test_aclose_without_started_relay_is_idempotent() -> None:
    client = _client()
    client.install(Adapter())
    await client.aclose()
    await client.aclose()


async def test_closed_client_rejects_install_and_maintenance() -> None:
    never_installed = _client()
    await never_installed.aclose()
    with pytest.raises(ClosedError, match="aclose"):
        never_installed.install(Adapter())

    client = _client()
    client.install(None)
    await client.aclose()
    with pytest.raises(ClosedError):
        _ = client.maintenance()
    with pytest.raises(ClosedError):
        _ = await client.run_maintenance_once()


async def sample_task(value: int, *, mode: str) -> str:
    """Сигнатура задачи для runtime-проверки ``Tallyho.call``."""
    await asyncio.sleep(0)
    return f"{value}:{mode}"


def test_call_resolves_task_and_preserves_typed_subclass_through_opts() -> None:
    client = _client()
    with pytest.raises(ConfigurationError, match="install"):
        _ = client.call(sample_task, 1, mode="strict")
    client.install(Adapter())

    value = client.call(sample_task, 1, mode="strict").opts(
        key="one", weight=2, queue="priority", priority=9
    )

    assert isinstance(value, Call)
    assert value.task_name == "sample_task"
    assert value.args == (1,)
    assert value.kwargs == {"mode": "strict"}
    assert (value.key, value.weight, value.queue) == ("one", 2, "priority")
    assert value.options == {"priority": 9}


def test_hook_decorators_register_on_client_registry() -> None:
    client = _client()

    @client.on_finalized("mail")
    async def finalized(_session: AsyncSession, _summary: BatchSummary) -> None:
        await asyncio.sleep(0)

    assert client.hooks.finalized("mail") is finalized
