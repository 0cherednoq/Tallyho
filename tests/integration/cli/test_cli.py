"""CLI commands operate against a real PostgreSQL installation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho
from tallyho.cli import app
from tallyho.testing import InlineBroker

if TYPE_CHECKING:
    import pytest

__all__: list[str] = []


async def test_migrate_and_inspect_by_uuid_or_kind_key(
    postgres_dsn: str,
    schema: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert await app.run(["migrate", "--dsn", postgres_dsn, "--schema", schema]) == 0
    assert capsys.readouterr().out == f"schema={schema} version=3\n"

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
