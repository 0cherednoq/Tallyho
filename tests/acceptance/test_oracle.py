"""Executable proof that every I-01...I-14 oracle detects its own violation."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, insert, select, update

from tallyho.model.states import BatchState, ItemState
from tallyho.storage.tables import build_metadata
from tests.acceptance.oracle import (
    PurgedBatch,
    check_i01_terminal_batches,
    check_i02_no_tails,
    check_i03_single_finalization,
    check_i04_exact_domain_effects,
    check_i05_counter_truth,
    check_i06_domain_matches_tallyho,
    check_i07_generator_truth,
    check_i08_monotonic_snapshots,
    check_i09_tree_order,
    check_i10_broker_alignment,
    check_i11_external_effects,
    check_i12_exact_after_seal,
    check_i13_tree_consistency,
    check_i14_retention,
    recovery_timeout,
)

if TYPE_CHECKING:
    from uuid import UUID

    from tallyho.model.views import BatchView
    from tests.acceptance.conftest import AcceptanceHarness
    from tests.acceptance.oracle import InvariantReport

__all__: list[str] = []


def _flatten(view: BatchView) -> list[BatchView]:
    return [view, *(node for child in view.children.values() for node in _flatten(child))]


def _assert_detects(report: InvariantReport) -> None:
    assert not report.ok
    assert report.violations > 0
    assert report.evidence


@pytest.mark.timeout(120)
async def test_oracle_is_green_then_each_invariant_detects_corruption(  # ruff: ignore[too-many-locals, too-many-statements]  # one isolated stand proves all invariant functions against the same coherent snapshot
    acceptance_harness: AcceptanceHarness,
) -> None:
    harness = acceptance_harness
    addresses = [
        "first@example.test",
        "second@example.test",
        "FIRST@example.test",
        "retry-third@example.test",
        "reject-fourth@example.test",
    ]
    s1_id = await harness.app.start_s1(range(101, 109))
    s1 = await harness.wait_terminal(s1_id)
    s2_id = await harness.app.start_s2(101, addresses)
    s2 = await harness.wait_terminal(s2_id)
    s3_id = await harness.app.start_s3(101, pages=harness.generated.truth.pages)
    s3 = await harness.wait_terminal(s3_id)
    views = [*_flatten(s1), *_flatten(s2), *_flatten(s3)]
    tables = build_metadata()
    engine = harness.app.engine.execution_options(
        schema_translate_map={None: harness.app.tallyho_schema}
    )
    async with engine.connect() as connection:
        domain_views = {
            s1_id: (harness.app.domain.invoices, s1),
            s2_id: (harness.app.domain.campaigns, s2),
            s3_id: (harness.app.domain.catalog_runs, s3),
        }
        observed_calls = len(harness.mail.calls) + sum(harness.site.calls.values())
        mail_attempts = Counter(call.item_id for call in harness.mail.calls)
        retry_calls = sum(count - 1 for count in harness.site.calls.values()) + sum(
            count - 1 for count in mail_attempts.values()
        )
        clean: list[InvariantReport] = [
            await check_i01_terminal_batches(connection, tables),
            await check_i02_no_tails(connection, tables),
            await check_i03_single_finalization(connection, tables, harness.app.domain),
            await check_i04_exact_domain_effects(connection, tables, harness.app.domain),
            await check_i05_counter_truth(connection, tables),
            await check_i06_domain_matches_tallyho(connection, domain_views),
            check_i07_generator_truth(
                s2,
                s3,
                harness.generated.truth,
                unique_addresses=4,
                duplicate_addresses=1,
            ),
            await check_i08_monotonic_snapshots(connection, harness.app.domain),
            await check_i09_tree_order(connection, tables, harness.app.domain),
            await check_i10_broker_alignment(connection, tables, ()),
            await check_i11_external_effects(
                connection,
                tables,
                observed_calls=observed_calls,
                retry_calls=retry_calls,
            ),
            check_i12_exact_after_seal(views),
            await check_i13_tree_consistency(connection, tables),
            check_i14_retention(()),
        ]
        assert [report.invariant for report in clean] == [f"I-{index:02}" for index in range(1, 15)]
        failed = [report for report in clean if not report.ok]
        assert not failed, failed

        savepoint = await connection.begin_nested()
        _ = await connection.execute(
            update(tables.batch).where(tables.batch.c.id == s1_id).values(state=0)
        )
        _assert_detects(await check_i01_terminal_batches(connection, tables))
        await savepoint.rollback()

        savepoint = await connection.begin_nested()
        _ = await connection.execute(
            insert(tables.counter_delta).values(batch_id=s1_id, created_at=func.now())
        )
        _assert_detects(await check_i02_no_tails(connection, tables))
        await savepoint.rollback()

        savepoint = await connection.begin_nested()
        _ = await connection.execute(
            delete(harness.app.domain.hook_log).where(
                harness.app.domain.hook_log.c.batch_id == s1_id,
                harness.app.domain.hook_log.c.hook == "on_finalized",
            )
        )
        _assert_detects(await check_i03_single_finalization(connection, tables, harness.app.domain))
        await savepoint.rollback()

        savepoint = await connection.begin_nested()
        effect_id = cast(
            "UUID",
            await connection.scalar(select(harness.app.domain.invoice_files.c.item_id).limit(1)),
        )
        _ = await connection.execute(
            delete(harness.app.domain.invoice_files).where(
                harness.app.domain.invoice_files.c.item_id == effect_id
            )
        )
        _assert_detects(
            await check_i04_exact_domain_effects(connection, tables, harness.app.domain)
        )
        await savepoint.rollback()

        savepoint = await connection.begin_nested()
        counter_id = cast(
            "UUID",
            await connection.scalar(
                select(tables.counter.c.batch_id).where(tables.counter.c.batch_id == s1_id).limit(1)
            ),
        )
        _ = await connection.execute(
            update(tables.counter)
            .where(tables.counter.c.batch_id == counter_id)
            .values(ok=tables.counter.c.ok + 1)
        )
        _assert_detects(await check_i05_counter_truth(connection, tables))
        await savepoint.rollback()

        savepoint = await connection.begin_nested()
        _ = await connection.execute(
            update(harness.app.domain.invoices)
            .where(harness.app.domain.invoices.c.batch_id == s1_id)
            .values(progress_done=0)
        )
        _assert_detects(await check_i06_domain_matches_tallyho(connection, domain_views))
        await savepoint.rollback()

        wrong_truth = replace(harness.generated.truth, cards=harness.generated.truth.cards + 1)
        _assert_detects(
            check_i07_generator_truth(
                s2,
                s3,
                wrong_truth,
                unique_addresses=4,
                duplicate_addresses=1,
            ),
        )

        savepoint = await connection.begin_nested()
        hook = harness.app.domain.hook_log
        _ = await connection.execute(
            insert(hook).values(
                batch_id=s1_id,
                hook="on_progress",
                seq=999_999,
                state=int(BatchState.SUCCEEDED),
                progress_done=0,
                progress_found=0,
                txid=func.txid_current(),
                at=func.clock_timestamp(),
            )
        )
        _assert_detects(await check_i08_monotonic_snapshots(connection, harness.app.domain))
        await savepoint.rollback()

        pages_id = s3.children["pages"].id
        cards_id = s3.children["cards"].id
        pages_finished = await connection.scalar(
            select(tables.batch.c.finished_at).where(tables.batch.c.id == pages_id)
        )
        assert pages_finished is not None
        savepoint = await connection.begin_nested()
        _ = await connection.execute(
            update(tables.batch)
            .where(tables.batch.c.id == cards_id)
            .values(finished_at=pages_finished - timedelta(seconds=1))
        )
        _assert_detects(await check_i09_tree_order(connection, tables, harness.app.domain))
        await savepoint.rollback()

        ok_item = cast(
            "UUID",
            await connection.scalar(
                select(tables.item.c.id).where(tables.item.c.state == int(ItemState.OK)).limit(1)
            ),
        )
        _assert_detects(await check_i10_broker_alignment(connection, tables, (ok_item,)))
        _assert_detects(
            await check_i11_external_effects(connection, tables, observed_calls=1_000_000)
        )

        bad_progress = replace(s1.progress, expected=s1.progress.found + 1)
        _assert_detects(check_i12_exact_after_seal((replace(s1, progress=bad_progress),)))

        savepoint = await connection.begin_nested()
        _ = await connection.execute(
            update(tables.batch)
            .where(tables.batch.c.id == cards_id)
            .values(state=int(BatchState.OPEN))
        )
        _assert_detects(await check_i13_tree_consistency(connection, tables))
        await savepoint.rollback()

        _assert_detects(
            check_i14_retention(
                (
                    PurgedBatch(
                        batch_id=uuid4(),
                        release_required=True,
                        released=False,
                        retention_elapsed=False,
                        domain_intact=False,
                        raised_batch_purged=True,
                    ),
                )
            ),
        )


def test_recovery_timeout_formula() -> None:
    assert recovery_timeout(timedelta(seconds=60), timedelta(seconds=5)) == timedelta(seconds=100)
