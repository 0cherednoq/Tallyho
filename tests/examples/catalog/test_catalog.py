"""Executable catalog pipeline scenarios from ARCHITECTURE section 13."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from tallyho.model.progress import NodeCounters, compute_progress
from tallyho.model.states import BatchState, OnFeederFailed
from tests.examples.catalog.generator import CatalogSite

if TYPE_CHECKING:
    from tests.examples.catalog.app import CatalogApp

__all__: list[str] = []

pytestmark = pytest.mark.timeout(300)

ROOT = UUID(int=1)
PAGES = UUID(int=2)
CARDS = UUID(int=3)
PDFS = UUID(int=4)


async def test_reference_pipeline_overlaps_stages_and_matches_oracle(
    catalog_app: CatalogApp,
) -> None:
    site = CatalogSite.reference()
    batch_id = await catalog_app.start_import(site)

    assert await catalog_app.broker.step(5) == 5
    running = await catalog_app.th.handle(batch_id).view()
    assert not running.children["pages"].progress.final
    assert running.children["cards"].progress.done > 0
    assert running.children["pdfs"].progress.found > 0

    _ = await catalog_app.drain()
    view = await catalog_app.th.handle(batch_id).view()
    pages = view.children["pages"].progress
    cards = view.children["cards"].progress
    pdfs = view.children["pdfs"].progress
    domain = await catalog_app.get()

    assert view.state is BatchState.SUCCEEDED
    assert (pages.found, pages.ok, pages.expected, pages.final) == (24, 24, 24, True)
    assert (cards.found, cards.ok, cards.duplicates, cards.final) == (712, 712, 18, True)
    assert (pdfs.found, pdfs.ok, pdfs.final) == (1_810, 1_810, True)
    assert len(catalog_app.downloaded) == site.truth.pdfs
    assert domain.status == "completed"
    assert domain.progress == pytest.approx(1.0)
    assert domain.stages["cards"]["duplicates"] == site.truth.card_duplicates
    assert all(child.state.is_terminal for child in view.children.values())
    for child in view.children.values():
        assert child.progress.expected == child.progress.found
        assert not child.progress.expected_is_estimate


async def test_empty_pdf_stage_cascades_to_completed(catalog_app: CatalogApp) -> None:
    batch_id = await catalog_app.start_import(CatalogSite.empty_pdfs())
    _ = await catalog_app.drain()

    view = await catalog_app.th.handle(batch_id).view()
    assert view.state is BatchState.SUCCEEDED
    assert view.children["cards"].progress.found == 12
    assert view.children["pdfs"].state is BatchState.SUCCEEDED
    assert view.children["pdfs"].progress.found == 0
    assert (await catalog_app.get()).status == "completed"


async def test_download_progress_is_visible_in_flight(catalog_app: CatalogApp) -> None:
    catalog_app.hold_downloads = True
    batch_id = await catalog_app.start_import(CatalogSite.small())
    draining = asyncio.create_task(catalog_app.drain())
    try:
        await catalog_app.download_started.wait()
        pdfs = await catalog_app.th.handle(batch_id).child("pdfs")
        async with asyncio.timeout(2):
            while True:
                active = await pdfs.in_flight()
                if active and all(entry.progress_done is not None for entry in active):
                    break
                await asyncio.sleep(0.01)
        assert {(entry.progress_done, entry.progress_total) for entry in active} == {(40, 120)}
    finally:
        catalog_app.download_release.set()
        _ = await draining

    assert (await catalog_app.th.handle(batch_id).view()).state is BatchState.SUCCEEDED


@pytest.mark.parametrize(
    ("behavior", "pdf_state"),
    [
        (OnFeederFailed.SEAL, BatchState.SUCCEEDED),
        (OnFeederFailed.CANCEL, BatchState.CANCELLED),
    ],
)
async def test_failed_card_source_closes_or_cancels_pdfs(
    catalog_app: CatalogApp,
    behavior: OnFeederFailed,
    pdf_state: BatchState,
) -> None:
    batch_id = await catalog_app.start_import(
        CatalogSite.small(),
        on_feeder_failed=behavior,
        fail_cards=True,
    )
    _ = await catalog_app.drain()

    view = await catalog_app.th.handle(batch_id).view()
    assert view.children["cards"].state is BatchState.COMPLETED_WITH_ERRORS
    assert view.children["pdfs"].state is pdf_state
    assert view.children["pdfs"].progress.found == 0
    assert view.children["pdfs"].progress.in_flight == 0
    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    assert (await catalog_app.get()).status == "completed_with_errors"


async def test_max_depth_stops_cyclic_page_expansion(catalog_app: CatalogApp) -> None:
    batch_id = await catalog_app.start_import(CatalogSite.small(cyclic_pages=True))
    _ = await catalog_app.drain()

    pages = (await catalog_app.th.handle(batch_id).view()).children["pages"].progress
    assert pages.found == 2
    assert pages.skipped_by_limit == 1
    assert pages.final


async def test_max_items_counts_every_skipped_spawn(catalog_app: CatalogApp) -> None:
    batch_id = await catalog_app.start_import(CatalogSite.small(), max_items=5)
    _ = await catalog_app.drain()

    view = await catalog_app.th.handle(batch_id).view()
    stages = list(view.children.values())
    found = sum(stage.progress.found for stage in stages)
    skipped = sum(stage.progress.skipped_by_limit for stage in stages)
    assert found == 6  # soft limit: one five-call flush may cross max_items by one
    assert skipped == 8
    assert found + skipped == 14  # initial Item + every attempted card/PDF spawn
    assert view.state.is_terminal, "; ".join(
        f"{key}={child.state.name}:{child.progress!r}" for key, child in view.children.items()
    )


OPEN, SEALED, OK = BatchState.OPEN, BatchState.SEALED, BatchState.SUCCEEDED


def _stage(
    node_id: UUID,
    counts: tuple[int, int, BatchState],
    *,
    weight: int,
    fed_by: tuple[UUID, ...] = (),
    expected_total: int | None = None,
) -> NodeCounters:
    found, done, state = counts
    return NodeCounters(
        id=node_id,
        parent_id=ROOT,
        state=state,
        total=found,
        ok=done,
        w_total=found * weight,
        w_done=done * weight,
        fed_by=fed_by,
        expected_total=expected_total,
    )


def _tree(
    *,
    root: BatchState = BatchState.SEALED,
    root_ok: int = 0,
    pages: tuple[int, int, BatchState],
    cards: tuple[int, int, BatchState],
    pdfs: tuple[int, int, BatchState],
) -> list[NodeCounters]:
    return [
        NodeCounters(id=ROOT, state=root, total=3, ok=root_ok),
        _stage(PAGES, pages, weight=1, expected_total=24),
        _stage(CARDS, cards, weight=2, fed_by=(PAGES,)),
        _stage(PDFS, pdfs, weight=4, fed_by=(CARDS,)),
    ]


@dataclass(frozen=True, slots=True)
class ProgressMoment:
    """One exact row from the section 13.3 t1-t5 table."""

    name: str
    nodes: list[NodeCounters]
    stages: tuple[tuple[int, int | None, bool], ...]
    percent: int | None


MOMENTS = [
    ProgressMoment(
        "t1",
        _tree(pages=(24, 1, OPEN), cards=(30, 0, OPEN), pdfs=(0, 0, OPEN)),
        ((1, 24, True), (0, None, False), (0, None, False)),
        None,
    ),
    ProgressMoment(
        "t2",
        _tree(pages=(24, 12, OPEN), cards=(350, 200, OPEN), pdfs=(510, 300, OPEN)),
        ((12, 24, True), (200, 700, True), (300, 1_785, True)),
        19,
    ),
    ProgressMoment(
        "t3",
        _tree(
            root_ok=1,
            pages=(24, 24, OK),
            cards=(712, 600, SEALED),
            pdfs=(1_540, 1_300, OPEN),
        ),
        ((24, 24, False), (600, 712, False), (1_300, 1_827, True)),
        73,
    ),
    ProgressMoment(
        "t4",
        _tree(
            root_ok=2,
            pages=(24, 24, OK),
            cards=(712, 712, OK),
            pdfs=(1_810, 1_700, SEALED),
        ),
        ((24, 24, False), (712, 712, False), (1_700, 1_810, False)),
        95,
    ),
    ProgressMoment(
        "t5",
        _tree(
            root=OK,
            root_ok=3,
            pages=(24, 24, OK),
            cards=(712, 712, OK),
            pdfs=(1_810, 1_810, OK),
        ),
        ((24, 24, False), (712, 712, False), (1_810, 1_810, False)),
        100,
    ),
]


@pytest.mark.parametrize("moment", MOMENTS, ids=[moment.name for moment in MOMENTS])
def test_progress_moments_match_section_13(moment: ProgressMoment) -> None:
    result = compute_progress(moment.nodes)
    for node_id, expected in zip((PAGES, CARDS, PDFS), moment.stages, strict=True):
        progress = result[node_id]
        assert (
            progress.done,
            progress.expected,
            progress.expected_is_estimate,
        ) == expected
    ratio = result[ROOT].ratio
    if moment.percent is None:
        assert ratio is None
    else:
        assert ratio is not None
        assert round(ratio * 100) == moment.percent


def test_t2_ratio_matches_documented_formula() -> None:
    result = compute_progress(MOMENTS[1].nodes)
    assert result[ROOT].ratio == pytest.approx(1_612 / 8_564)
