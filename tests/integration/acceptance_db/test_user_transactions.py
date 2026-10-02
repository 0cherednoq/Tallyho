"""A-DB-02, A-DB-03 and A-DB-12 through the public API."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho
from tallyho.engine.finalizer import Finalizer
from tallyho.engine.producer import RootSpec
from tallyho.model.errors import BatchPurged
from tallyho.model.states import BatchState
from tallyho.protocols.clock import SystemClock
from tallyho.storage.tables import build_metadata
from tallyho.testing import InlineBroker
from tests.helpers.db import schema_connection, temporary_schema
from tests.helpers.probe import committed_ids, create_probe, insert_id

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy import RowMapping
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


async def _work(value: int) -> None:
    await asyncio.sleep(0)
    assert value >= 0


async def _batch_row(env: Env, batch_id: UUID) -> RowMapping:
    batch = build_metadata().batch
    async with schema_connection(env.engine, env.schema) as conn:
        return (await conn.execute(select(batch).where(batch.c.id == batch_id))).mappings().one()


async def test_a_db_02_batch_and_domain_row_rollback_together(env: Env) -> None:
    broker = InlineBroker()
    th = Tallyho(env.engine, schema=env.schema)
    th.install(broker.adapter)
    probe = await create_probe(env.engine, env.schema)
    scoped = env.engine.execution_options(schema_translate_map={None: env.schema})
    try:
        async with AsyncSession(scoped) as session:
            await insert_id(await session.connection(), probe, 1)
            async with th.batch("a-db-02", key="rollback", session=session) as batch:
                await batch.add(_work, 1)
            await session.rollback()

        assert await committed_ids(env.engine, probe) == []
        with pytest.raises(BatchPurged):
            _ = await batch.handle.view()
        assert await broker.drain() == 0
    finally:
        await th.aclose()


async def test_a_db_03_all_handle_mutations_rollback_with_user_transaction(env: Env) -> None:
    broker = InlineBroker()
    th = Tallyho(env.engine, schema=env.schema)
    th.install(broker.adapter)
    scoped = env.engine.execution_options(schema_translate_map={None: env.schema})
    initial_start = datetime(2035, 1, 1, tzinfo=UTC)
    try:
        async with th.batch(
            "a-db-03",
            key="mutable",
            start_at=initial_start,
        ) as batch:
            await batch.add(_work, 1)
        handle = batch.handle

        async with AsyncSession(scoped) as session:
            await handle.pause(session=session)
            await session.rollback()
        row = await _batch_row(env, handle.id)
        assert row["paused_at"] is None

        await handle.pause()
        async with AsyncSession(scoped) as session:
            await handle.resume(session=session)
            await session.rollback()
        row = await _batch_row(env, handle.id)
        assert row["paused_at"] is not None

        async with AsyncSession(scoped) as session:
            await handle.cancel(session=session)
            await session.rollback()
        row = await _batch_row(env, handle.id)
        assert row["cancel_requested_at"] is None

        moved_start = initial_start + timedelta(hours=1)
        async with AsyncSession(scoped) as session:
            _ = await handle.reschedule(moved_start, session=session)
            await session.rollback()
        row = await _batch_row(env, handle.id)
        assert row["start_at"] == initial_start

        async with env.transaction() as conn:
            terminal = await env.producer.create_root(
                conn,
                RootSpec(
                    kind="a-db-03-release",
                    key="terminal",
                    retention=timedelta(days=1),
                    release_required=True,
                ),
            )
            _ = await env.producer.seal(conn, terminal.id)
        finalizer = Finalizer(
            tables=env.tables,
            engine=scoped,
            clock=SystemClock(),
            ids=th.id_factory,
            hooks=th.hooks,
        )
        assert await finalizer.try_finalize(terminal.id)
        terminal_handle = th.handle(terminal.id)
        assert (await terminal_handle.view()).state is BatchState.SUCCEEDED
        async with AsyncSession(scoped) as session:
            await terminal_handle.release(session=session)
            await session.rollback()
        terminal_row = await _batch_row(env, terminal.id)
        assert terminal_row["released_at"] is None
    finally:
        await th.aclose()


async def test_a_db_12_hook_writes_domain_table_in_another_schema(
    engine: AsyncEngine,
    schema: str,
) -> None:
    broker = InlineBroker()
    th = Tallyho(engine, schema=schema)
    th.install(broker.adapter)
    await th.migrate()
    async with temporary_schema(engine) as domain_schema:
        results = Table(
            "domain_results",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("status", String(32), nullable=False),
            schema=domain_schema,
        )
        async with engine.begin() as conn:
            await conn.run_sync(results.create)
            _ = await conn.execute(insert(results).values(id=1, status="running"))

        @th.on_finalized("a-db-12")
        async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
            _ = await session.execute(
                update(results).where(results.c.id == 1).values(status=summary.state.name.lower())
            )

        try:
            async with th.batch("a-db-12", key="other-schema") as batch:
                pass
            view = await batch.handle.wait(timeout=5)
            async with engine.connect() as conn:
                status = await conn.scalar(select(results.c.status).where(results.c.id == 1))
            assert view.state is BatchState.SUCCEEDED
            assert status == "succeeded"
        finally:
            await th.aclose()
