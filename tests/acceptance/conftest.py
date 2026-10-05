"""PostgreSQL and real-FlexIQ fixtures for the reference acceptance app."""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho.model.states import BatchState
from tests.acceptance.app.application import build_app
from tests.acceptance.app.mail import FakeMailProvider
from tests.acceptance.app.site import CatalogGenerator, FakeCatalogSite
from tests.helpers.db import temporary_schema

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from uuid import UUID

    from tallyho.model.views import BatchView
    from tests.acceptance.app.application import AcceptanceApp

__all__ = ["AcceptanceHarness"]

HERE = Path(__file__).parent
ROOT = HERE.parents[1]
SEED = 41
TERMINAL = frozenset(
    {
        BatchState.SUCCEEDED,
        BatchState.COMPLETED_WITH_ERRORS,
        BatchState.FAILED,
        BatchState.CANCELLED,
    }
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Classify executable acceptance tests consistently.

    Chaos runs (marker ``chaos``) need Docker Compose and minutes per case, so they are
    deselected unless the marker expression names them (``poe acceptance`` passes ``-m chaos``).
    """
    expression = str(config.getoption("markexpr"))
    deselected: list[pytest.Item] = []
    for collected in items:
        if not collected.path.is_relative_to(HERE):
            continue
        collected.add_marker(pytest.mark.integration)
        collected.add_marker(pytest.mark.flexiq)
        # Прогоны на compose-стенде (хаос A-CH и сценарии A-UC) собираются, только если
        # выражение маркеров называет их: ``poe acceptance`` и ``poe acceptance-uc``.
        if any(
            marker not in expression and collected.get_closest_marker(marker) is not None
            for marker in ("chaos", "usecase")
        ):
            deselected.append(collected)
    if deselected:
        items[:] = [collected for collected in items if collected not in deselected]
        config.hook.pytest_deselected(items=deselected)


@dataclass(slots=True)
class AcceptanceHarness:
    """Producer, HTTP fakes, and a separate real FlexIQ worker process."""

    app: AcceptanceApp
    generated: CatalogGenerator
    site: FakeCatalogSite
    mail: FakeMailProvider
    worker: asyncio.subprocess.Process
    root: Path

    def assert_worker_alive(self) -> None:
        """Fail with the subprocess log if the real worker has exited."""
        if self.worker.returncode is not None:
            log = (self.root / "worker.log").read_text(encoding="utf-8")
            message = f"acceptance worker exited with {self.worker.returncode}:\n{log}"
            raise AssertionError(message)

    async def wait_terminal(self, batch_id: UUID, timeout_seconds: float = 30) -> BatchView:
        """Drive maintenance until the reference batch reaches a terminal state."""
        handle = self.app.th.handle(batch_id)
        async with asyncio.timeout(timeout_seconds):
            while True:
                view = await handle.view()
                if view.state in TERMINAL:
                    return view
                self.assert_worker_alive()
                _ = await self.app.th.run_maintenance_once()
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
async def _harness(postgres_dsn: str, root: Path) -> AsyncGenerator[AcceptanceHarness]:
    engine = create_async_engine(postgres_dsn)
    async with (
        temporary_schema(engine) as tallyho_schema,
        temporary_schema(engine) as flexiq_schema,
        temporary_schema(engine) as domain_schema,
    ):
        await engine.dispose()
        generated = CatalogGenerator.build(SEED, page_count=1)
        site = FakeCatalogSite(generated)
        mail = FakeMailProvider()
        site_url = await site.start()
        mail_url = await mail.start()
        app = build_app(
            dsn=postgres_dsn,
            tallyho_schema=tallyho_schema,
            flexiq_schema=flexiq_schema,
            domain_schema=domain_schema,
            site_url=site_url,
            mail_url=mail_url,
            seed=SEED,
            network_scale=0.001,
            transient_rate=0,
            permanent_rate=0,
            worker_count=4,
        )
        await app.migrate()
        ready = root / "ready"
        log = (root / "worker.log").open("wb")
        worker = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tests.acceptance.app.worker",
            "--dsn",
            postgres_dsn,
            "--tallyho-schema",
            tallyho_schema,
            "--flexiq-schema",
            flexiq_schema,
            "--domain-schema",
            domain_schema,
            "--site-url",
            site_url,
            "--mail-url",
            mail_url,
            "--seed",
            str(SEED),
            "--network-scale",
            "0.001",
            "--transient-rate",
            "0",
            "--permanent-rate",
            "0",
            "--workers",
            "4",
            "--ready",
            str(ready),
            cwd=ROOT,
            stdout=log,
            stderr=asyncio.subprocess.STDOUT,
        )
        harness = AcceptanceHarness(app, generated, site, mail, worker, root)
        try:
            async with asyncio.timeout(20):
                while not ready.exists():
                    harness.assert_worker_alive()
                    await asyncio.sleep(0.05)
            yield harness
        finally:
            await _stop_worker(worker)
            log.close()
            await app.close()
            await mail.close()
            await site.close()
    await engine.dispose()


@pytest.fixture
async def acceptance_harness(postgres_dsn: str, tmp_path: Path) -> AsyncIterator[AcceptanceHarness]:
    """Run the small no-chaos stand against an isolated set of schemas."""
    async with _harness(postgres_dsn, tmp_path) as value:
        yield value
