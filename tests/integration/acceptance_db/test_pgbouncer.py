"""A-DB-11: transaction-pooling compatibility for both PostgreSQL drivers."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

import pytest
from sqlalchemy import insert
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker
from tests.helpers.db import temporary_schema
from tests.helpers.probe import committed_ids, create_probe, insert_id

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.views import BatchSummary

__all__: list[str] = []

PGBOUNCER_IMAGE = "edoburu/pgbouncer:v1.25.2-p0"
Driver = Literal["asyncpg", "psycopg"]


def _backend_url(postgres_dsn: str) -> tuple[str, dict[str, str]]:
    source = make_url(postgres_dsn).set(drivername="postgresql")
    host = source.host or "localhost"
    extra_hosts: dict[str, str] = {}
    if host in {"localhost", "127.0.0.1", "::1"}:
        host = "host.docker.internal"
        extra_hosts[host] = "host-gateway"
    return source.set(host=host).render_as_string(hide_password=False), extra_hosts


@pytest.fixture(scope="session")
def pgbouncer_dsn(postgres_dsn: str) -> Iterator[str]:
    """Start the plan-mandated PgBouncer image in transaction mode."""
    from testcontainers.core.container import DockerContainer  # ruff: ignore[import-outside-top-level]  # Docker is needed only for A-DB-11
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy  # ruff: ignore[import-outside-top-level]  # keep optional Docker import lazy

    backend_url, extra_hosts = _backend_url(postgres_dsn)
    source = make_url(postgres_dsn)
    username = source.username or "postgres"
    container = (
        DockerContainer(PGBOUNCER_IMAGE)
        .with_env("DATABASE_URL", backend_url)
        .with_env("POOL_MODE", "transaction")
        .with_env("AUTH_TYPE", "scram-sha-256")
        .with_env("ADMIN_USERS", username)
        .with_exposed_ports(5432)
        .with_kwargs(extra_hosts=extra_hosts)
        .waiting_for(LogMessageWaitStrategy("process up"))
    )
    with container:
        frontend = URL.create(
            "postgresql",
            username=username,
            password=source.password,
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
            database=source.database,
        )
        yield frontend.render_as_string(hide_password=False)


def _pooled_engine(dsn: str, driver: Driver, *, database: str | None = None) -> AsyncEngine:
    url = make_url(dsn).set(drivername=f"postgresql+{driver}", database=database)
    if driver == "asyncpg":
        return create_async_engine(
            url,
            connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
        )
    return create_async_engine(url, connect_args={"prepare_threshold": None})


async def _pooled_work(value: int) -> None:
    await asyncio.sleep(0)
    assert value > 0


async def _exercise_pool(engine: AsyncEngine, schema: str, driver: Driver) -> None:
    broker = InlineBroker()
    th = Tallyho(engine, schema=schema)
    th.install(broker.adapter)
    _ = await th.migrate()
    probe = await create_probe(engine, schema)

    @th.on_finalized("a-db-11")
    async def persist_result(session: AsyncSession, summary: BatchSummary) -> None:
        del summary
        _ = await session.execute(insert(probe).values(id=2))

    scoped = engine.execution_options(schema_translate_map={None: schema})
    try:
        async with AsyncSession(scoped) as session:
            await insert_id(await session.connection(), probe, 1)
            async with th.batch("a-db-11", key=driver, session=session) as batch:
                await batch.add(_pooled_work, 1)
            await session.commit()

        await batch.handle.pause()
        await batch.handle.resume()
        _ = await broker.drain()

        assert (await batch.handle.view()).state is BatchState.SUCCEEDED
        assert await committed_ids(engine, probe) == [1, 2]
    finally:
        await th.aclose()


@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
async def test_a_db_11_transaction_pooling_supports_both_drivers(
    pgbouncer_dsn: str,
    driver: Driver,
) -> None:
    engine = _pooled_engine(pgbouncer_dsn, driver)
    try:
        async with temporary_schema(engine) as schema:
            await _exercise_pool(engine, schema, driver)
    finally:
        await engine.dispose()
