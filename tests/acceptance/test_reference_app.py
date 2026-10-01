"""Small no-chaos executions of all three mandatory acceptance domains."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select

from tallyho.model.states import BatchState
from tests.acceptance.app.site import CatalogGenerator

if TYPE_CHECKING:
    from tests.acceptance.app.domain import DomainTable
    from tests.acceptance.conftest import AcceptanceHarness

__all__: list[str] = []


async def _count(harness: AcceptanceHarness, table: DomainTable) -> int:
    async with harness.app.engine.connect() as connection:
        value = await connection.scalar(select(func.count()).select_from(table))
    return int(value or 0)


async def _hook_count(harness: AcceptanceHarness, batch_id: object, hook: str) -> int:
    table = harness.app.domain.hook_log
    async with harness.app.engine.connect() as connection:
        value = await connection.scalar(
            select(func.count()).where(table.c.batch_id == batch_id, table.c.hook == hook)
        )
    return int(value or 0)


async def _status_count(harness: AcceptanceHarness, table: DomainTable, status: str) -> int:
    async with harness.app.engine.connect() as connection:
        value = await connection.scalar(select(func.count()).where(table.c.status == status))
    return int(value or 0)


@pytest.mark.timeout(120)
async def test_s1_s2_s3_complete_with_real_flexiq_worker(
    acceptance_harness: AcceptanceHarness,
) -> None:
    """Execute known, expanding, and pipeline work without an inline broker."""
    harness = acceptance_harness

    s1_id = await harness.app.start_s1(range(1, 13))
    s1 = await harness.wait_terminal(s1_id)
    assert s1.state is BatchState.SUCCEEDED
    assert s1.progress.found == s1.progress.ok == 12
    assert s1.progress.expected == 12
    assert await _count(harness, harness.app.domain.invoice_files) == 12
    assert await _status_count(harness, harness.app.domain.invoices, "rendered") == 12
    assert await _hook_count(harness, s1_id, "on_finalized") == 1

    addresses = [
        "first@example.test",
        "second@example.test",
        "FIRST@example.test",
        "retry-third@example.test",
        "reject-fourth@example.test",
    ]
    s2_id = await harness.app.start_s2(1, addresses)
    s2 = await harness.wait_terminal(s2_id)
    send = s2.children["send"]
    assert s2.state is BatchState.SUCCEEDED
    assert send.progress.found == 4
    assert send.progress.duplicates == 1
    assert send.progress.error == 0
    assert await _count(harness, harness.app.domain.deliveries) == 4
    assert len(harness.mail.calls) == 5
    assert await _hook_count(harness, s2_id, "on_finalized") == 1

    truth = harness.generated.truth
    s3_id = await harness.app.start_s3(1, pages=truth.pages)
    s3 = await harness.wait_terminal(s3_id)
    cards = s3.children["cards"].progress
    pdfs = s3.children["pdfs"].progress
    assert s3.state in {BatchState.SUCCEEDED, BatchState.COMPLETED_WITH_ERRORS}
    assert cards.found == truth.cards
    assert cards.duplicates == truth.card_duplicates
    assert cards.error == truth.card_not_found
    assert pdfs.found == truth.pdfs
    assert pdfs.duplicates == truth.pdf_duplicates
    assert pdfs.error == truth.pdf_not_found
    assert await _count(harness, harness.app.domain.cards) == truth.cards - truth.card_not_found
    assert await _count(harness, harness.app.domain.pdf_files) == truth.pdfs - truth.pdf_not_found
    assert await _hook_count(harness, s3_id, "on_finalized") == 1


def test_catalog_generator_empty_pdf_variant_has_exact_truth() -> None:
    """Keep the mandatory empty-stage variant deterministic and inspectable."""
    generated = CatalogGenerator.build(7, page_count=5, empty_pdfs=True)

    assert generated.truth.pages == 5
    assert generated.truth.pdfs == 0
    assert generated.truth.pdf_duplicates == 0
    assert generated.truth.pdf_not_found == 0
    assert all(not values for values in generated.card_pdfs.values())
    assert generated == CatalogGenerator.build(7, page_count=5, empty_pdfs=True)
