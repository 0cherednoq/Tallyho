"""Seeded aiohttp catalog and the exact S3 oracle behind it."""

from __future__ import annotations

import random
import socket
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast, final

from aiohttp import web

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["CatalogGenerator", "CatalogTruth", "FakeCatalogSite"]


@dataclass(frozen=True, slots=True)
class CatalogTruth:
    """Exact counts produced by one seeded site graph."""

    pages: int
    cards: int
    pdfs: int
    card_duplicates: int
    pdf_duplicates: int
    card_not_found: int
    pdf_not_found: int
    not_found: int
    cyclic_links: int


@dataclass(frozen=True, slots=True)
class _Page:
    cards: tuple[str, ...]
    pages: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CatalogGenerator:
    """Immutable generated site plus its exact oracle."""

    seed: int
    pages: tuple[_Page, ...]
    card_pdfs: Mapping[str, tuple[str, ...]]
    statuses: Mapping[str, int]
    truth: CatalogTruth

    @classmethod
    def build(  # ruff: ignore[too-many-locals]  # one seeded pass must retain the complete oracle
        cls, seed: int, *, page_count: int = 50, empty_pdfs: bool = False
    ) -> CatalogGenerator:
        """Generate the ACCEPTANCE §3.2 distribution from one seed."""
        rng = random.Random(  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # deterministic test fixture generation
            seed
        )
        page_rows: list[_Page] = []
        all_cards: list[str] = []
        card_duplicates = 0
        cyclic_links = 0
        for page_index in range(1, page_count + 1):
            count = 0 if rng.random() < 0.02 else rng.randint(20, 60)
            cards: list[str] = []
            for card_index in range(count):
                if all_cards and rng.random() < 0.05:
                    cards.append(rng.choice(all_cards))
                    card_duplicates += 1
                else:
                    url = f"/cards/{page_index}-{card_index}"
                    cards.append(url)
                    all_cards.append(url)
            page_links: tuple[int, ...] = ()
            if page_index > 1 and rng.random() < 0.01:
                page_links = (rng.randint(1, page_index - 1),)
                cyclic_links += 1
            page_rows.append(_Page(cards=tuple(cards), pages=page_links))

        statuses: dict[str, int] = {}
        card_pdfs: dict[str, tuple[str, ...]] = {}
        all_pdfs: list[str] = []
        pdf_duplicates = 0
        card_not_found = 0
        pdf_not_found = 0
        for card in all_cards:
            status_roll = rng.random()
            if status_roll < 0.03:
                statuses[card] = 404
                card_pdfs[card] = ()
                card_not_found += 1
                continue
            statuses[card] = 503 if status_roll < 0.08 else 200
            count = 0 if empty_pdfs or rng.random() < 0.01 else rng.randint(0, 6)
            pdfs: list[str] = []
            for pdf_index in range(count):
                if all_pdfs and rng.random() < 0.05:
                    pdfs.append(rng.choice(all_pdfs))
                    pdf_duplicates += 1
                else:
                    pdf = f"/pdfs/{card.rsplit('/', 1)[-1]}-{pdf_index}.pdf"
                    pdfs.append(pdf)
                    all_pdfs.append(pdf)
                    pdf_roll = rng.random()
                    if pdf_roll < 0.03:
                        statuses[pdf] = 404
                        pdf_not_found += 1
                    else:
                        statuses[pdf] = 503 if pdf_roll < 0.08 else 200
            card_pdfs[card] = tuple(pdfs)

        truth = CatalogTruth(
            pages=page_count,
            cards=len(all_cards),
            pdfs=len(all_pdfs),
            card_duplicates=card_duplicates,
            pdf_duplicates=pdf_duplicates,
            card_not_found=card_not_found,
            pdf_not_found=pdf_not_found,
            not_found=card_not_found + pdf_not_found,
            cyclic_links=cyclic_links,
        )
        return cls(seed, tuple(page_rows), card_pdfs, statuses, truth)


@final
class FakeCatalogSite:
    """Real local HTTP service serving a generated catalog with a call journal."""

    def __init__(self, generated: CatalogGenerator) -> None:
        self.generated = generated
        self.calls: Counter[str] = Counter()
        self._runner: web.AppRunner | None = None
        self.base_url: str | None = None

    async def start(self, *, host: str = "127.0.0.1", port: int | None = None) -> str:
        """Start on a free loopback port and return its base URL."""
        app = web.Application()
        app.router.add_get("/pages/{page}", self._page)
        app.router.add_get("/cards/{card}", self._card)
        app.router.add_get("/pdfs/{pdf}", self._pdf)
        runner = web.AppRunner(app)
        await runner.setup()
        selected_port = _free_port() if port is None else port
        await web.TCPSite(runner, host, selected_port).start()
        self._runner = runner
        self.base_url = f"http://{host}:{selected_port}"
        return self.base_url

    async def close(self) -> None:
        """Stop accepting requests and release the listener."""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _page(self, request: web.Request) -> web.Response:
        page = int(request.match_info["page"])
        key = f"/pages/{page}"
        self.calls[key] += 1
        if page < 1 or page > len(self.generated.pages):
            return web.json_response({"error": "not found"}, status=404)
        row = self.generated.pages[page - 1]
        return web.json_response({"cards": row.cards, "pages": row.pages})

    async def _card(self, request: web.Request) -> web.Response:
        key = f"/cards/{request.match_info['card']}"
        self.calls[key] += 1
        status = self.generated.statuses.get(key, 404)
        if status == 503 and self.calls[key] == 1:
            return web.json_response({"error": "retry"}, status=503)
        if status == 404:
            return web.json_response({"error": "not found"}, status=404)
        return web.json_response({"pdfs": self.generated.card_pdfs[key]})

    async def _pdf(self, request: web.Request) -> web.Response:
        key = f"/pdfs/{request.match_info['pdf']}"
        self.calls[key] += 1
        status = self.generated.statuses.get(key, 404)
        if status == 503 and self.calls[key] == 1:
            return web.Response(status=503)
        if status == 404:
            return web.Response(status=404)
        body = f"pdf:{self.generated.seed}:{key}".encode()
        return web.Response(body=body, content_type="application/pdf")


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        address = cast("tuple[str, int]", listener.getsockname())
        return address[1]
