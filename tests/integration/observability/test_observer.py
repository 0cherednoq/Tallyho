"""Observer receives a complete successful pipeline without leaking task payloads."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from typing_extensions import override

from tallyho import Tallyho
from tallyho.protocols.observer import NullObserver
from tallyho.testing import InlineBroker

if TYPE_CHECKING:
    from uuid import UUID

    import pytest
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.states import BatchState, ResultClass

__all__: list[str] = []


class _Spy(NullObserver):
    def __init__(self) -> None:
        self.events: set[str] = set()

    @override
    def batch_created(self, *, batch_id: UUID, kind: str) -> None:
        _ = batch_id, kind
        self.events.add("create")

    @override
    def item_claimed(self, *, batch_id: UUID, item_id: UUID, attempt: int) -> None:
        _ = batch_id, item_id, attempt
        self.events.add("claim")

    @override
    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        _ = batch_id, item_id, result, label, attempt
        self.events.add("finish")

    @override
    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        _ = batch_id, kind, state
        self.events.add("finalize")

    @override
    def relay_dispatched(self, *, messages: int, duration: float) -> None:
        _ = messages, duration
        self.events.add("relay")

    @override
    def completer_flush(self, *, items: int, duration: float) -> None:
        _ = items, duration
        self.events.add("flush")

    @override
    def relay_lag(self, *, seconds: float) -> None:
        _ = seconds
        self.events.add("relay_lag")

    @override
    def completer_buffer(self, *, items: int) -> None:
        _ = items
        self.events.add("buffer")

    @override
    def oldest_lease(self, *, seconds: float) -> None:
        _ = seconds
        self.events.add("lease_age")


async def test_successful_pipeline_emits_all_events_and_hides_payload(
    engine: AsyncEngine,
    schema: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload_marker = bytes(
        (84, 79, 80, 45, 83, 69, 67, 82, 69, 84, 45, 84, 65, 83, 75, 45, 65, 82, 71)
    ).decode()
    spy = _Spy()
    broker = InlineBroker()
    th = Tallyho(engine, schema=schema, observer=spy)
    th.install(broker.adapter)
    await th.migrate()

    async def consume(value: str) -> None:
        await asyncio.sleep(0)
        assert value == payload_marker

    try:
        with caplog.at_level(logging.DEBUG, logger="tallyho"):
            async with th.batch("observability", key="complete") as batch:
                await batch.add(consume, payload_marker)
            _ = await broker.drain()
            _ = await batch.handle.wait(timeout=timedelta(seconds=5))
            _ = await th.run_maintenance_once()
    finally:
        await th.aclose()

    assert spy.events == {
        "create",
        "claim",
        "finish",
        "finalize",
        "relay",
        "flush",
        "relay_lag",
        "buffer",
        "lease_age",
    }
    assert payload_marker not in caplog.text
