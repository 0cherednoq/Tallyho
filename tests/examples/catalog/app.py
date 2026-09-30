"""Executable three-stage catalog application from ARCHITECTURE section 13."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast, final

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.states import BatchState, OnFeederFailed
from tallyho.testing import FakeClock, InlineBroker
from tests.examples.catalog.domain import catalog_imports, create_domain, record

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.views import BatchSummary
    from tests.examples.catalog.domain import CatalogRecord
    from tests.examples.catalog.generator import CatalogSite

__all__ = ["KIND", "CardParseError", "CatalogApp"]

KIND = "catalog_parse"


class CardParseError(Exception):
    """Controlled source-stage failure."""


def _stage_payload(summary: BatchSummary) -> dict[str, dict[str, object]]:
    return {
        key: {
            "done": child.progress.done,
            "found": child.progress.found,
            "expected": child.progress.expected,
            "estimate": child.progress.expected_is_estimate,
            "final": child.progress.final,
            "duplicates": child.progress.duplicates,
            "skipped_by_limit": child.progress.skipped_by_limit,
            "eta_s": None if child.progress.eta is None else child.progress.eta.total_seconds(),
        }
        for key, child in summary.children.items()
    }


def _status(state: BatchState) -> str:
    return {
        BatchState.SUCCEEDED: "completed",
        BatchState.COMPLETED_WITH_ERRORS: "completed_with_errors",
        BatchState.FAILED: "failed",
        BatchState.CANCELLED: "cancelled",
    }[state]


@final
class CatalogApp:
    """One installed catalog application with fake site and worker pool."""

    def __init__(self, engine: AsyncEngine, schema: str) -> None:
        self.engine = engine.execution_options(schema_translate_map={None: schema})
        self.clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
        self.broker = InlineBroker(duplicate_delivery_rate=0.05, seed=13)
        self.th = Tallyho(
            engine,
            schema=schema,
            clock=self.clock,
            lease_ttl=timedelta(seconds=60),
            heartbeat_every=timedelta(milliseconds=20),
            finalize_grace=timedelta(0),
        )
        self.th.install(self.broker.adapter)
        self.site: CatalogSite | None = None
        self.fail_cards = False
        self.downloaded: set[str] = set()
        self.snapshots: list[float] = []
        self.hold_downloads = False
        self.download_started = asyncio.Event()
        self.download_release = asyncio.Event()

        async def parse_page(url: str, page: int) -> None:
            await self._parse_page(url, page)

        async def parse_card(url: str) -> None:
            await self._parse_card(url)

        async def download_pdf(url: str) -> None:
            await self._download_pdf(url)

        self.parse_page: Callable[[str, int], Awaitable[None]] = parse_page
        self.parse_card: Callable[[str], Awaitable[None]] = parse_card
        self.download_pdf: Callable[[str], Awaitable[None]] = download_pdf
        self._register_hooks()

    @classmethod
    async def create(cls, engine: AsyncEngine, schema: str) -> CatalogApp:
        """Create user and tallyho schemas for one isolated test."""
        app = cls(engine, schema)
        await create_domain(app.engine)
        _ = await app.th.migrate()
        return app

    async def close(self) -> None:
        """Stop pending worker-runtime tasks."""
        await self.broker.close()

    async def drain(self) -> int:
        """Run workers and one deterministic maintenance pass until idle."""
        delivered = await self.broker.drain(concurrency=100)
        _ = await self.th.run_maintenance_once()
        return delivered + await self.broker.drain(concurrency=100)

    async def start_import(
        self,
        site: CatalogSite,
        *,
        import_id: int = 1,
        max_items: int = 200_000,
        max_depth: int = 1,
        on_feeder_failed: OnFeederFailed = OnFeederFailed.SEAL,
        fail_cards: bool = False,
    ) -> UUID:
        """Atomically create the domain row and pages/cards/pdfs pipeline."""
        self.site = site
        self.fail_cards = fail_cards
        async with AsyncSession(self.engine, expire_on_commit=False) as session, session.begin():
            _ = await session.execute(
                insert(catalog_imports).values(
                    id=import_id,
                    status="running",
                    stages={},
                    progress=0.0,
                    progress_seq=0,
                )
            )
            async with self.th.batch(
                KIND,
                key=f"catalog:{import_id}",
                max_items=max_items,
                session=session,
            ) as root:
                pages = root.sub_batch("pages", max_depth=max_depth)
                cards = root.sub_batch(
                    "cards",
                    fed_by=[pages],
                    max_in_flight=100,
                )
                _ = root.sub_batch(
                    "pdfs",
                    fed_by=[cards],
                    max_in_flight=50,
                    on_feeder_failed=on_feeder_failed,
                )
                await pages.add_calls(
                    [
                        self.th.call(self.parse_page, "https://catalog.test", 1).opts(
                            key="page:1",
                            weight=1,
                            max_retries=0,
                        )
                    ]
                )
            _ = await session.execute(
                update(catalog_imports)
                .where(catalog_imports.c.id == import_id)
                .values(batch_id=root.handle.id)
            )
            return root.handle.id

    async def get(self, import_id: int = 1) -> CatalogRecord:
        """Read the domain result used by the simulated UI."""
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(catalog_imports).where(catalog_imports.c.id == import_id)
                    )
                )
                .mappings()
                .one()
            )
        return record(cast("Mapping[str, object]", row))

    async def _parse_page(self, url: str, page: int) -> None:
        del url
        site = self._site()
        if page == 1:
            item.expect(site.total_pages)
            for number in range(2, site.total_pages + 1):
                item.spawn_call(
                    self.th.call(self.parse_page, "https://catalog.test", number).opts(
                        key=f"page:{number}",
                        weight=1,
                        max_retries=0,
                    )
                )
        if site.cyclic_pages and page == 2:
            item.spawn_call(
                self.th.call(self.parse_page, "https://catalog.test", 2).opts(
                    key="cycle:depth-2",
                    weight=1,
                    max_retries=0,
                )
            )
        for card_url in site.cards(page):
            item.spawn_call(
                self.th.call(self.parse_card, card_url).opts(
                    key=card_url,
                    weight=2,
                    max_retries=0,
                ),
                into="cards",
            )

    async def _parse_card(self, url: str) -> None:
        if self.fail_cards:
            message = f"card parser failed for {url}"
            raise CardParseError(message)
        for pdf_url in self._site().pdf_links(url):
            item.spawn_call(
                self.th.call(self.download_pdf, pdf_url).opts(
                    key=pdf_url,
                    weight=4,
                    max_retries=0,
                ),
                into="pdfs",
            )

    async def _download_pdf(self, url: str) -> None:
        item.progress(40, 120)
        self.download_started.set()
        if self.hold_downloads:
            await self.download_release.wait()
        self.downloaded.add(url)
        item.ok("downloaded")

    def _site(self) -> CatalogSite:
        if self.site is None:
            message = "catalog site is not installed"
            raise CardParseError(message)
        return self.site

    def _register_hooks(self) -> None:
        @self.th.on_progress(KIND, every=timedelta(seconds=2))
        async def save_progress(session: AsyncSession, summary: BatchSummary) -> None:
            ratio = summary.progress.ratio or 0.0
            self.snapshots.append(ratio)
            _ = await session.execute(
                update(catalog_imports)
                .where(
                    catalog_imports.c.batch_id == summary.id,
                    catalog_imports.c.progress_seq < summary.seq,
                )
                .values(
                    stages=_stage_payload(summary),
                    progress=func.greatest(catalog_imports.c.progress, ratio),
                    progress_seq=summary.seq,
                )
            )

        @self.th.on_finalized(KIND)
        async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
            _ = await session.execute(
                update(catalog_imports)
                .where(catalog_imports.c.batch_id == summary.id)
                .values(
                    status=_status(summary.state),
                    stages=_stage_payload(summary),
                    progress=1.0,
                    progress_seq=summary.seq,
                )
            )
