"""Completer: буфер, backpressure, привязка к loop, закрытие и сбои транзакции."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from typing_extensions import override

from tallyho.engine.completer import ClaimOutcome, Completer
from tallyho.model.errors import CompleterError, InvalidStateError
from tallyho.protocols.observer import NullObserver
from tallyho.storage.tables import build_metadata
from tests.integration.engine.completer_env import (
    SETTINGS,
    CommitCounter,
    MovableClock,
    lease_row,
    open_completer,
    schema_engine,
    seed,
    set_batch,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.engine.completer import ItemRef
    from tests.integration.engine.conftest import Env


async def test_backpressure_limits_buffer(env: Env) -> None:
    seeded = await seed(env, 6)
    settings = replace(SETTINGS, max_batch=2, backpressure=2)
    counter = CommitCounter()
    async with open_completer(env, settings=settings, counter=counter) as completer:
        tasks = [asyncio.create_task(completer.claim(ref)) for ref in seeded.refs]
        await asyncio.sleep(0)
        assert completer.buffered <= 2
        results = await asyncio.gather(*tasks)
    assert {result.outcome for result in results} == {ClaimOutcome.CLAIMED}
    assert counter.commits == 3


async def test_close_flushes_buffer_and_rejects_new_ops(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    settings = replace(SETTINGS, tick=SETTINGS.lease_ttl)
    async with open_completer(env, settings=settings) as completer:
        # Тик длиннее теста: операцию досылает close, а не таймер.
        pending = asyncio.create_task(completer.claim(first))
        await asyncio.sleep(0)
        await completer.close()
        assert (await pending).outcome is ClaimOutcome.CLAIMED
        with pytest.raises(InvalidStateError):
            _ = await completer.claim(second)
        await completer.close()
    assert await lease_row(env, second.id) is None


async def test_close_without_operations(env: Env) -> None:
    async with open_completer(env) as completer:
        await completer.close()
    assert completer.buffered == 0


async def test_other_event_loop_is_rejected(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    async with open_completer(env) as completer:
        _ = await completer.claim(first)

        def claim_in_new_loop(ref: ItemRef) -> None:
            _ = asyncio.run(completer.claim(ref))

        with pytest.raises(InvalidStateError):
            await asyncio.to_thread(claim_in_new_loop, second)


async def test_cancelled_caller_is_skipped(env: Env) -> None:
    seeded = await seed(env, 2)
    first, second = seeded.refs
    async with open_completer(env) as completer:
        abandoned = asyncio.create_task(completer.claim(first))
        await asyncio.sleep(0)
        _ = abandoned.cancel()
        with pytest.raises(asyncio.CancelledError):
            await abandoned
        assert (await completer.claim(second)).run
    assert await lease_row(env, first.id) is None


async def test_failed_transaction_fails_every_operation(env: Env) -> None:
    seeded = await seed(env, 2)
    completer = Completer(
        tables=build_metadata(prefix="missing_"),
        engine=schema_engine(env),
        clock=MovableClock(),
        settings=SETTINGS,
    )
    try:
        results = await asyncio.gather(
            *(completer.claim(ref) for ref in seeded.refs), return_exceptions=True
        )
    finally:
        await completer.close()
    assert all(isinstance(result, CompleterError) for result in results)
    assert all(result.__cause__ is not None for result in results if isinstance(result, Exception))


class BrokenObserver(NullObserver):
    @override
    def completer_flush(self, *, items: int, duration: float) -> None:
        message = "наблюдатель сломан"
        raise RuntimeError(message)


class BrokenFinalizer:
    async def try_finalize(self, batch_id: UUID) -> bool:
        message = f"финализация {batch_id} сломана"
        raise RuntimeError(message)


async def test_observer_and_finalizer_errors_do_not_break_accounting(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    seeded = await seed(env, 1)
    await set_batch(env, seeded.batch_id, cancel_requested_at=MovableClock().value)
    completer = Completer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=MovableClock(),
        settings=SETTINGS,
        observer=BrokenObserver(),
        finalizer=BrokenFinalizer(),
    )
    with caplog.at_level(logging.ERROR, logger="tallyho.engine.completer"):
        try:
            result = await completer.claim(seeded.refs[0])
        finally:
            await completer.close()
    assert result.outcome is ClaimOutcome.CANCELLED
    messages = [record.getMessage() for record in caplog.records]
    assert any("Observer" in message for message in messages)
    assert any("try_finalize" in message for message in messages)
