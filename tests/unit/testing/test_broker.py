"""Локальные контракты InlineBroker без PostgreSQL."""

from __future__ import annotations

import asyncio
from typing import cast
from uuid import uuid4

import pytest

from tallyho.model.errors import ConfigurationError
from tallyho.model.states import OutboxKind
from tallyho.protocols.broker import Dispatcher, Message, RelayPolicy, Runtime, Verdict
from tallyho.protocols.serialization import PayloadCodec
from tallyho.testing import InlineBroker

__all__: list[str] = []


async def task(value: int) -> int:
    await asyncio.sleep(0)
    return value


def test_inline_broker_satisfies_adapter_protocols_and_codec_round_trips() -> None:
    broker = InlineBroker(seed=42)
    assert isinstance(broker, Dispatcher)
    assert isinstance(broker, Runtime)
    assert isinstance(broker, PayloadCodec)
    assert isinstance(broker, RelayPolicy)
    assert not broker.relay_autostart
    assert broker.adapter is broker

    name = broker.task_name(task)
    payload = broker.encode(name, (1,), {"flag": True})
    assert broker.decode(name, payload) == ((1,), {"flag": True})
    assert broker.pending == 0
    assert broker.deliveries == 0


def test_task_name_rejects_collision() -> None:
    broker = InlineBroker()
    _ = broker.task_name(task)

    async def other(value: int) -> int:
        await asyncio.sleep(0)
        return value

    other.__module__ = task.__module__
    other.__qualname__ = task.__qualname__
    with pytest.raises(ConfigurationError, match="конфликт"):
        _ = broker.task_name(other)


@pytest.mark.parametrize("rate", [-0.1, 1.1, True, "all"])
def test_inline_broker_rejects_invalid_duplicate_rate(rate: object) -> None:
    with pytest.raises(ConfigurationError, match="duplicate_delivery_rate"):
        _ = InlineBroker(duplicate_delivery_rate=cast("float", rate))


def test_inline_broker_requires_install_for_runtime_operations() -> None:
    broker = InlineBroker()
    with pytest.raises(ConfigurationError, match="install"):
        _ = broker.wrap(task)
    with pytest.raises(ConfigurationError, match="доставок"):
        broker.kill_worker_after(0)


async def test_dispatch_rejects_unknown_task_and_invalid_retries() -> None:
    broker = InlineBroker()
    message = Message(
        id=uuid4(),
        batch_id=uuid4(),
        kind=OutboxKind.ITEM,
        task_name="missing",
        payload=b"{}",
    )
    with pytest.raises(ConfigurationError, match="не знает"):
        await broker.dispatch([message])

    name = broker.task_name(task)
    bad = Message(
        id=uuid4(),
        batch_id=uuid4(),
        kind=OutboxKind.ITEM,
        task_name=name,
        payload=broker.encode(name, (1,), {}),
        options={"max_retries": True},
    )
    with pytest.raises(ConfigurationError, match="max_retries"):
        await broker.dispatch([bad])


async def test_uninstalled_broker_is_final_and_close_is_noop() -> None:
    broker = InlineBroker()
    assert broker.retry_verdict(RuntimeError()) is Verdict.FINAL
    with pytest.raises(ConfigurationError, match="несовместимый"):
        broker.install_runtime(object())
    await broker.close()


async def test_inline_broker_rejects_bad_cursor() -> None:
    broker = InlineBroker()
    with pytest.raises(ConfigurationError, match="курсор"):
        _ = await broker.reconcile_dead("not-an-int")


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
async def test_inline_broker_rejects_bad_drain_concurrency(value: object) -> None:
    broker = InlineBroker()
    with pytest.raises(ConfigurationError, match="concurrency"):
        _ = await broker.drain(concurrency=cast("int", value))
