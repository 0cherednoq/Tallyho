"""CLI commands operate against a real PostgreSQL installation."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho
from tallyho.cli import app
from tallyho.engine.maintenance import Maintenance
from tallyho.storage.tables import build_metadata
from tallyho.testing import InlineBroker
from tests.helpers.db import schema_connection, schema_transaction
from tests.helpers.relay import RecordingDispatcher

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.engine.public import MaintenanceRunner

__all__: list[str] = []


async def pending_task(number: int) -> None:
    """Задача, которую в этих тестах никто не исполняет."""
    _ = number
    await asyncio.sleep(0)


async def outbox_rows(engine: AsyncEngine, schema: str) -> list[tuple[UUID, datetime, int]]:
    outbox = build_metadata().outbox
    async with schema_connection(engine, schema) as conn:
        result = await conn.execute(
            select(outbox.c.id, outbox.c.available_at, outbox.c.attempts).order_by(outbox.c.id)
        )
        return [(row_id, available_at, attempts) for row_id, available_at, attempts in result]


async def test_migrate_and_inspect_by_uuid_or_kind_key(
    postgres_dsn: str,
    schema: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert await app.run(["migrate", "--dsn", postgres_dsn, "--schema", schema]) == 0
    assert capsys.readouterr().out == f"schema={schema} version=5\n"

    engine = create_async_engine(postgres_dsn)
    broker = InlineBroker()
    client = Tallyho(engine, schema=schema)
    client.install(broker.adapter)
    try:
        async with client.batch("cli-test", key="one") as batch:
            pass
        _ = await batch.handle.wait()
    finally:
        await broker.close()
        await engine.dispose()

    assert (
        await app.run(["inspect", str(batch.handle.id), "--dsn", postgres_dsn, "--schema", schema])
        == 0
    )
    by_id = capsys.readouterr().out
    assert "cli-test key=one" in by_id
    assert f"id={batch.handle.id}" in by_id
    assert "state=succeeded" in by_id
    assert "done=0/0" in by_id

    assert (
        await app.run(["inspect", "cli-test:one", "--dsn", postgres_dsn, "--schema", schema]) == 0
    )
    assert capsys.readouterr().out == by_id

    assert await app.run(["maintenance", "--once", "--dsn", postgres_dsn, "--schema", schema]) == 0
    assert capsys.readouterr().out == "maintenance pass complete\n"


async def test_maintenance_without_broker_leaves_outbox_untouched(
    postgres_dsn: str,
    schema: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """CLI без брокера: ни захвата outbox, ни ошибок в логе на каждом проходе (Fix-10)."""
    arguments = ["--dsn", postgres_dsn, "--schema", schema]
    assert await app.run(["migrate", *arguments]) == 0
    engine = create_async_engine(postgres_dsn)
    broker = InlineBroker()
    producer = Tallyho(engine, schema=schema)
    producer.install(broker.adapter)
    outbox = build_metadata().outbox
    try:
        async with producer.batch("cli-outbox", key="one") as batch:
            await batch.add(pending_task, 1)
            await batch.add(pending_task, 2)
        async with schema_transaction(engine, schema) as conn:
            # Записи давно готовы: любой проход relay захватил бы их.
            _ = await conn.execute(
                update(outbox).values(available_at=text("now() - interval '1 hour'"))
            )
        before = await outbox_rows(engine, schema)
        assert [attempts for _id, _at, attempts in before] == [0, 0]

        passes = 0

        async def serve(runner: MaintenanceRunner) -> None:
            nonlocal passes
            assert isinstance(runner, Maintenance)
            assert runner.relay is None
            task = asyncio.create_task(runner.run())
            try:
                # Лидер успевает выполнить sweep и несколько циклов снимков.
                await asyncio.sleep(1.5)
                assert runner.is_leader
                passes += 1
            finally:
                runner.stop()
                await task

        with caplog.at_level(logging.WARNING), pytest.MonkeyPatch.context() as patcher:
            patcher.setattr(app, "serve_maintenance", serve)
            assert await app.run(["maintenance", "--once", *arguments]) == 0
            assert await app.run(["maintenance", *arguments]) == 0

        assert passes == 1
        assert await outbox_rows(engine, schema) == before
        assert [record for record in caplog.records if record.levelno >= logging.WARNING] == []

        # Сообщения дождались процесса с настоящим адаптером.
        dispatcher = RecordingDispatcher()
        sender = Tallyho(engine, schema=schema, relay_grace=timedelta(0))
        sender.install(dispatcher)
        _ = await sender.run_maintenance_once()
        assert sorted(dispatcher.ids) == [row_id for row_id, _at, _attempts in before]
        assert await outbox_rows(engine, schema) == []
    finally:
        await broker.close()
        await engine.dispose()


async def test_long_running_maintenance_uses_signal_service(
    postgres_dsn: str,
    schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    served = False

    async def serve(runner: object) -> None:
        nonlocal served
        await asyncio.sleep(0)
        _ = runner
        served = True

    monkeypatch.setattr(app, "serve_maintenance", serve)

    assert await app.run(["maintenance", "--dsn", postgres_dsn, "--schema", schema]) == 0
    assert served
