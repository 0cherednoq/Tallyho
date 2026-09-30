"""Продюсер: создание корня (UC-01) и expect."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from typing_extensions import override

from tallyho.engine.producer import CallbackName, RootSpec, StoredCallback
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError, NotFoundError
from tallyho.model.policy import FailurePolicy
from tallyho.model.states import BatchState, OnFeederFailed
from tallyho.protocols.clock import SystemClock

if TYPE_CHECKING:
    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

KIND = "campaign_deliveries"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


class FixedClock(SystemClock):
    """Часы, у которых «сейчас» в SQL — фиксированное значение."""

    @override
    def now(self) -> datetime | None:
        return NOW


def as_mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    data = cast("dict[str, object]", value)
    return dict(data)


async def test_create_root_writes_row(env: Env, registry: HookRegistry) -> None:
    @registry.on_finalized(KIND)
    async def save(session: object, summary: BatchSummary) -> None:
        del session, summary
        await asyncio.sleep(0)

    producer = replace(env.producer, clock=FixedClock())
    start = NOW + timedelta(hours=1)
    spec = RootSpec(
        kind=KIND,
        key="campaign:1",
        start_at=start,
        deadline=timedelta(hours=2),
        callbacks={CallbackName.ON_SUCCEEDED: TaskCall(task_name="notify", args=(1,))},
        failure_policy=FailurePolicy.fail_fast(),
        max_in_flight=10,
        expected_total=5,
        max_items=100,
        retention=timedelta(days=3),
        release_required=True,
    )
    async with env.transaction() as conn:
        ref = await producer.create_root(conn, spec)
    assert ref.created
    assert ref.root_id == ref.id
    row = await env.batch(ref.id)
    assert row["kind"] == KIND
    assert row["key"] == "campaign:1"
    assert row["parent_id"] is None
    assert row["state"] == BatchState.OPEN
    assert row["hooks"] == ["finalized"]
    assert row["start_at"] == start
    assert row["deadline_at"] == NOW + timedelta(hours=2)
    assert row["created_at"] == NOW
    assert row["updated_at"] == NOW
    assert row["max_in_flight"] == 10
    assert row["expected_total"] == 5
    assert row["max_items"] == 100
    assert row["retention"] == timedelta(days=3)
    assert row["release_required"] is True
    assert row["on_feeder_failed"] == OnFeederFailed.SEAL
    options = as_mapping(row["options"])
    assert options["failure_policy"] == {"kind": "fail_fast"}
    stored = StoredCallback.from_json(as_mapping(as_mapping(options["callbacks"])["on_succeeded"]))
    assert stored.task_name == "notify"
    assert producer.codec.decode("notify", stored.payload) == ((1,), {})


async def test_create_root_defaults(env: Env) -> None:
    deadline = datetime(2027, 1, 1, tzinfo=UTC)
    async with env.transaction() as conn:
        ref = await env.producer.create_root(conn, RootSpec(kind=KIND, deadline=deadline))
    row = await env.batch(ref.id)
    assert row["key"] is None
    assert row["hooks"] == []
    assert row["options"] == {}
    assert row["deadline_at"] == deadline
    assert row["retention"] is None
    assert row["release_required"] is False


async def test_create_root_same_key_returns_existing(env: Env) -> None:
    spec = RootSpec(kind=KIND, key="campaign:1")
    async with env.transaction() as conn:
        first = await env.producer.create_root(conn, spec)
    async with env.transaction() as conn:
        again = await env.producer.create_root(conn, replace(spec, max_items=5))
        other_kind = await env.producer.create_root(conn, replace(spec, kind="other"))
    assert again == replace(first, created=False)
    assert other_kind.created
    assert other_kind.id != first.id
    assert await env.count(env.tables.batch) == 2
    # Параметры повтора не применяются: батч уже создан.
    assert (await env.batch(first.id))["max_items"] is None


async def test_create_root_without_key_always_creates(env: Env) -> None:
    async with env.transaction() as conn:
        first = await env.producer.create_root(conn, RootSpec(kind=KIND))
        second = await env.producer.create_root(conn, RootSpec(kind=KIND))
    assert first.created
    assert second.created
    assert first.id != second.id
    assert await env.count(env.tables.batch) == 2


async def test_concurrent_create_root_returns_same_batch(env: Env) -> None:
    spec = RootSpec(kind=KIND, key="campaign:1")
    first_done = asyncio.Event()
    release_first = asyncio.Event()

    async def first() -> UUID:
        async with env.transaction() as conn:
            ref = await env.producer.create_root(conn, spec)
            first_done.set()
            _ = await release_first.wait()
            return ref.id

    async def second() -> UUID:
        _ = await first_done.wait()
        async with env.transaction() as conn:
            ref = await env.producer.create_root(conn, spec)
            assert not ref.created
            return ref.id

    first_task = asyncio.create_task(first())
    second_task = asyncio.create_task(second())
    _ = await first_done.wait()
    await asyncio.sleep(0.2)
    # Второй ждёт блокировку ключа, пока первый не закоммитит.
    assert not second_task.done()
    release_first.set()
    assert await first_task == await second_task
    assert await env.count(env.tables.batch) == 1


async def test_payload_limit_applies_to_callbacks(env: Env) -> None:
    producer = replace(env.producer, max_payload_bytes=64)
    call = TaskCall(task_name="notify", args=("x" * 100,))
    spec = RootSpec(kind=KIND, callbacks={CallbackName.ON_FAILED: call})
    async with env.transaction() as conn:
        with pytest.raises(ConfigurationError, match="предел 64"):
            _ = await producer.create_root(conn, spec)
    assert await env.count(env.tables.batch) == 0


async def test_expect_only_grows(env: Env) -> None:
    async with env.transaction() as conn:
        ref = await env.producer.create_root(conn, RootSpec(kind=KIND))
        await env.producer.expect(conn, ref.id, 10)
        await env.producer.expect(conn, ref.id, 4)
    assert (await env.batch(ref.id))["expected_total"] == 10
    async with env.transaction() as conn:
        await env.producer.expect(conn, ref.id, 12)
    assert (await env.batch(ref.id))["expected_total"] == 12


async def test_expect_errors(env: Env) -> None:
    async with env.transaction() as conn:
        with pytest.raises(NotFoundError):
            await env.producer.expect(conn, UUID(int=1), 1)
        with pytest.raises(ConfigurationError, match="n должен"):
            await env.producer.expect(conn, UUID(int=1), -1)
