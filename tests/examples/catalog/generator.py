"""Deterministic fake catalog and its exact oracle."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["CatalogSite", "CatalogTruth"]


@dataclass(frozen=True, slots=True)
class CatalogTruth:
    """Exact unique work and duplicate counts emitted by a fake site."""

    pages: int
    cards: int
    pdfs: int
    card_duplicates: int


@dataclass(frozen=True, slots=True)
class CatalogSite:
    """Immutable page/card graph returned by the fake fetch layer."""

    pages: tuple[tuple[str, ...], ...]
    pdfs: dict[str, tuple[str, ...]]
    truth: CatalogTruth
    cyclic_pages: bool = False

    @classmethod
    def reference(cls) -> CatalogSite:
        """Build the §13 oracle: 24 pages, 712 cards and 1,810 PDFs."""
        card_urls = [f"https://catalog.test/cards/{index}" for index in range(712)]
        first_counts = [30, *([29] * 10), 30]
        first_references = card_urls[:350]
        last_references = [*card_urls[350:], *card_urls[:18]]
        last_counts = [*([32] * 8), *([31] * 4)]
        pages = _partition(first_references, first_counts) + _partition(
            last_references, last_counts
        )
        pdfs: dict[str, tuple[str, ...]] = {}
        next_pdf = 0
        for index, url in enumerate(card_urls):
            if index < 200:
                count = 3 if index < 110 else 2
            elif index < 600:
                count = 3 if index < 430 else 2
            else:
                count = 3 if index < 646 else 2
            pdfs[url] = tuple(
                f"https://catalog.test/pdfs/{pdf_id}.pdf"
                for pdf_id in range(next_pdf, next_pdf + count)
            )
            next_pdf += count
        assert next_pdf == 1_810
        return cls(
            pages=pages,
            pdfs=pdfs,
            truth=CatalogTruth(pages=24, cards=712, pdfs=1_810, card_duplicates=18),
        )

    @classmethod
    def empty_pdfs(cls) -> CatalogSite:
        """Build a catalog whose cards contain no PDF links."""
        cards = tuple(f"https://catalog.test/empty/{index}" for index in range(12))
        return cls(
            pages=(cards,),
            pdfs=dict.fromkeys(cards, ()),
            truth=CatalogTruth(pages=1, cards=12, pdfs=0, card_duplicates=0),
        )

    @classmethod
    def small(cls, *, cyclic_pages: bool = False) -> CatalogSite:
        """Build a compact graph for failure and limit scenarios."""
        cards = tuple(f"https://catalog.test/small/{index}" for index in range(8))
        pdfs: dict[str, tuple[str, ...]] = {
            url: (f"https://catalog.test/small/{index}.pdf",) for index, url in enumerate(cards)
        }
        return cls(
            pages=(cards[:4], cards[4:]),
            pdfs=pdfs,
            truth=CatalogTruth(pages=2, cards=8, pdfs=8, card_duplicates=0),
            cyclic_pages=cyclic_pages,
        )

    @property
    def total_pages(self) -> int:
        """Return the number discovered from page one."""
        return len(self.pages)

    def cards(self, page: int) -> tuple[str, ...]:
        """Return card references for a one-based page."""
        return self.pages[page - 1]

    def pdf_links(self, card_url: str) -> tuple[str, ...]:
        """Return PDF references for one card."""
        return self.pdfs[card_url]


def _partition(values: list[str], counts: list[int]) -> tuple[tuple[str, ...], ...]:
    pages: list[tuple[str, ...]] = []
    offset = 0
    for count in counts:
        pages.append(tuple(values[offset : offset + count]))
        offset += count
    assert offset == len(values)
    return tuple(pages)
