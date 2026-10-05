"""Container entry point for the seeded catalog and mail HTTP services."""

from __future__ import annotations

import argparse
import asyncio
import signal

from tests.acceptance.app.mail import FakeMailProvider
from tests.acceptance.app.site import CatalogGenerator, FakeCatalogSite

__all__ = ["main"]


async def _run(seed: int, pages: int, *, empty_pdfs: bool) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for candidate in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(candidate, stop.set)
        except (NotImplementedError, RuntimeError):
            continue
    site = FakeCatalogSite(CatalogGenerator.build(seed, page_count=pages, empty_pdfs=empty_pdfs))
    mail = FakeMailProvider()
    await site.start(
        host="0.0.0.0",  # ruff: ignore[hardcoded-bind-all-interfaces]  # container service must be reachable
        port=8081,
    )
    await mail.start(
        host="0.0.0.0",  # ruff: ignore[hardcoded-bind-all-interfaces]  # container service must be reachable
        port=8082,
    )
    try:
        await stop.wait()
    finally:
        await mail.close()
        await site.close()


def main() -> None:
    """Serve both external systems until stopped."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--pages", type=int, default=50)
    parser.add_argument("--empty-pdfs", type=int, default=0, help="1: каталог без PDF (A-UC-05)")
    values = parser.parse_args()
    asyncio.run(_run(values.seed, values.pages, empty_pdfs=bool(values.empty_pdfs)))


if __name__ == "__main__":
    main()
