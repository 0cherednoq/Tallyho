"""PostgreSQL and subprocess fixtures for Flexiq contract tests."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho.model.states import BatchState
from tests.contract.flexiq.contract_app import build_app
from tests.helpers.db import temporary_schema

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Iterator

    from tallyho.api.batch import BatchHandle
    from tallyho.model.views import BatchView
    from tests.contract.flexiq.contract_app import ContractApp

__all__ = ["FlexiqContract"]

HERE = Path(__file__).parent
ROOT = HERE.parents[2]
POSTGRES_IMAGE = "postgres:16-alpine"
TERMINAL = frozenset(
    {
        BatchState.SUCCEEDED,
        BatchState.COMPLETED_WITH_ERRORS,
        BatchState.FAILED,
        BatchState.CANCELLED,
    }
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if item.path.is_relative_to(HERE):
            item.add_marker(pytest.mark.integration)
            item.add_marker(pytest.mark.flexiq)


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    if dsn := os.environ.get("TALLYHO_TEST_DSN"):
        yield dsn
        return
    from testcontainers.community.postgres import PostgresContainer  # ruff: ignore[import-outside-top-level]  # lazy Docker dependency

    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@dataclass(slots=True)
class FlexiqContract:
    app: ContractApp
    root: Path
    worker: asyncio.subprocess.Process
    extra_workers: list[asyncio.subprocess.Process]

    def events(self, event: str | None = None) -> list[dict[str, object]]:
        path = self.root / "events.jsonl"
        if not path.exists():
            return []
        rows = [
            cast("dict[str, object]", json.loads(line))
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        return rows if event is None else [row for row in rows if row.get("event") == event]

    async def wait_events(
        self, event: str, count: int = 1, timeout_seconds: float = 15
    ) -> list[dict[str, object]]:
        async with asyncio.timeout(timeout_seconds):
            while True:
                rows = self.events(event)
                if len(rows) >= count:
                    return rows
                self.assert_worker_alive()
                await self.app.th.run_maintenance_once()
                await asyncio.sleep(0.05)

    async def wait_terminal(self, handle: BatchHandle, timeout_seconds: float = 15) -> BatchView:
        async with asyncio.timeout(timeout_seconds):
            while True:
                view = await handle.view()
                if view.state in TERMINAL:
                    return view
                self.assert_worker_alive()
                await self.app.th.run_maintenance_once()
                await asyncio.sleep(0.05)

    def assert_worker_alive(self) -> None:
        code = self.worker.returncode
        if code is not None:
            log = (self.root / "worker.log").read_text(encoding="utf-8")
            message = f"worker exited with {code}:\n{log}"
            raise AssertionError(message)

    async def start_worker(self, queue_name: str, *, workers: int = 1) -> None:
        """Start an additional isolated worker process for one Flexiq queue."""
        ready_name = f"ready-{queue_name}"
        log = (self.root / f"worker-{queue_name}.log").open("wb")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tests.contract.flexiq.contract_worker",
            "--dsn",
            self.app.dsn,
            "--tallyho-schema",
            self.app.tallyho_schema,
            "--flexiq-schema",
            self.app.flexiq_schema,
            "--root",
            str(self.root),
            "--queues",
            queue_name,
            "--workers",
            str(workers),
            "--ready-name",
            ready_name,
            cwd=ROOT,
            stdout=log,
            stderr=asyncio.subprocess.STDOUT,
        )
        self.extra_workers.append(process)
        log.close()
        async with asyncio.timeout(20):
            while not (self.root / ready_name).exists():
                if process.returncode is not None:
                    message = f"extra worker exited with {process.returncode}"
                    raise AssertionError(message)
                await asyncio.sleep(0.05)


async def _stop_worker(worker: asyncio.subprocess.Process) -> None:
    if worker.returncode is not None:
        return
    worker.terminate()
    try:
        async with asyncio.timeout(10):
            await worker.wait()
    except TimeoutError:
        worker.kill()
        await worker.wait()


@asynccontextmanager
async def _contract(postgres_dsn: str, root: Path) -> AsyncGenerator[FlexiqContract]:
    engine = create_async_engine(postgres_dsn)
    async with (
        temporary_schema(engine) as tallyho_schema,
        temporary_schema(engine) as flexiq_schema,
    ):
        await engine.dispose()
        app = build_app(
            dsn=postgres_dsn,
            tallyho_schema=tallyho_schema,
            flexiq_schema=flexiq_schema,
            root=root,
        )
        await app.th.migrate()
        log = (root / "worker.log").open("wb")
        worker = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tests.contract.flexiq.contract_worker",
            "--dsn",
            postgres_dsn,
            "--tallyho-schema",
            tallyho_schema,
            "--flexiq-schema",
            flexiq_schema,
            "--root",
            str(root),
            cwd=ROOT,
            stdout=log,
            stderr=asyncio.subprocess.STDOUT,
        )
        contract = FlexiqContract(app, root, worker, [])
        try:
            async with asyncio.timeout(20):
                while not (root / "ready").exists():
                    contract.assert_worker_alive()
                    await asyncio.sleep(0.05)
            assert (root / "ready").exists()
            yield contract
        finally:
            for extra in contract.extra_workers:
                await _stop_worker(extra)
            await _stop_worker(worker)
            log.close()
            await app.th.aclose()  # relay не должен отправлять через закрытый адаптер
            app.queue.close()
            await app.adapter.close()
            await app.engine.dispose()
    await engine.dispose()


@pytest.fixture
async def flexiq_contract(postgres_dsn: str, tmp_path: Path) -> AsyncIterator[FlexiqContract]:
    async with _contract(postgres_dsn, tmp_path) as value:
        yield value
