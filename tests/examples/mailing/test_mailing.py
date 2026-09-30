"""The nine executable tests specified by ARCHITECTURE section 12.6."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho.model.errors import BatchPurged
from tallyho.model.states import BatchState
from tests.examples.mailing.dataset import (
    ContactSeed,
    bounce_heavy_dataset,
    compact_dataset,
    standard_dataset,
)

if TYPE_CHECKING:
    from tests.examples.mailing.app import MailingApp

__all__: list[str] = []

pytestmark = pytest.mark.timeout(420)

BREAKDOWN = {
    "sent": 9_100,
    "recipient_not_found": 300,
    "unsubscribed": 200,
    "suppressed": 100,
    "invalid_address": 50,
    "hard_bounce": 150,
    "rejected": 50,
    "exhausted": 50,
}


async def test_scheduled_campaign_completes_with_errors(mailing_app: MailingApp) -> None:
    campaign_id = await mailing_app.create_campaign(standard_dataset())
    batch_id = await mailing_app.schedule(
        campaign_id,
        mailing_app.clock.now() + timedelta(hours=1),
    )

    assert await mailing_app.drain() == 0
    scheduled = await mailing_app.get(campaign_id)
    assert scheduled.status == "scheduled"
    assert mailing_app.mail.sent == []

    _ = mailing_app.clock.advance(hours=1, seconds=6)
    _ = await mailing_app.drain()

    campaign = await mailing_app.get(campaign_id)
    assert campaign.status == "completed_with_errors"
    assert (campaign.sent, campaign.skipped, campaign.failed, campaign.duplicates) == (
        9_100,
        600,
        300,
        40,
    )
    assert campaign.breakdown == BREAKDOWN
    assert len(mailing_app.mail.sent) == 9_100
    assert await mailing_app.suppression_count("hard_bounce") == 150
    assert (await mailing_app.th.handle(batch_id).view()).state is BatchState.COMPLETED_WITH_ERRORS


async def test_send_starts_before_expand_finishes(mailing_app: MailingApp) -> None:
    campaign_id = await mailing_app.create_campaign(standard_dataset())
    batch_id = await mailing_app.start_now(campaign_id)

    assert await mailing_app.broker.step(3) == 3
    view = await mailing_app.th.handle(batch_id).view()

    assert not view.children["expand"].progress.final
    assert view.children["send"].progress.done > 0
    assert view.children["send"].progress.expected == 10_000
    assert view.children["send"].progress.expected_is_estimate


async def test_empty_audience_completes_immediately(mailing_app: MailingApp) -> None:
    campaign_id = await mailing_app.create_campaign([])
    _ = await mailing_app.start_now(campaign_id)

    _ = await mailing_app.drain()
    campaign = await mailing_app.get(campaign_id)

    assert campaign.status == "completed"
    assert campaign.sent == 0
    assert campaign.breakdown == {}


async def test_progress_snapshots_are_monotonic(mailing_app: MailingApp) -> None:
    campaign_id = await mailing_app.create_campaign(compact_dataset(100))
    _ = await mailing_app.start_now(campaign_id)
    seen: list[float] = []

    for _ in range(10):
        _ = await mailing_app.broker.step(10)
        _ = mailing_app.clock.advance(seconds=2)
        _ = await mailing_app.th.run_maintenance_once()
        seen.append((await mailing_app.get(campaign_id)).progress)

    _ = await mailing_app.drain()
    seen.append((await mailing_app.get(campaign_id)).progress)
    assert seen == sorted(seen)
    assert seen[-1] == pytest.approx(1.0)


async def test_result_survives_retention(mailing_app: MailingApp) -> None:
    audience = [*compact_dataset(), ContactSeed("one@reject.test")]
    campaign_id = await mailing_app.create_campaign(audience)
    batch_id = await mailing_app.start_now(campaign_id)
    _ = await mailing_app.drain()

    _ = mailing_app.clock.advance(days=15)
    _ = await mailing_app.th.run_maintenance_once()

    with pytest.raises(BatchPurged):
        _ = await mailing_app.th.handle(batch_id).view()
    campaign = await mailing_app.get(campaign_id)
    assert campaign.status == "completed_with_errors"
    assert campaign.sent == 640


async def test_failing_hook_blocks_finalization_then_recovers(
    mailing_app: MailingApp,
) -> None:
    campaign_id = await mailing_app.create_campaign(compact_dataset(64))
    batch_id = await mailing_app.start_now(campaign_id)
    mailing_app.fail_finalized = True

    _ = await mailing_app.drain()

    campaign = await mailing_app.get(campaign_id)
    view = await mailing_app.th.handle(batch_id).view()
    assert campaign.status == "running"
    assert view.state is BatchState.SEALED
    assert view.hook_error == "controlled finalized hook failure"

    mailing_app.fail_finalized = False
    _ = mailing_app.clock.advance(minutes=5)
    _ = await mailing_app.th.run_maintenance_once()
    assert (await mailing_app.get(campaign_id)).status == "completed"
    assert mailing_app.finalized_calls == 1


async def test_pause_stops_sending_and_resume_finishes(mailing_app: MailingApp) -> None:
    audience = [*compact_dataset(32), ContactSeed("one@reject.test")]
    campaign_id = await mailing_app.create_campaign(audience)
    _ = await mailing_app.start_now(campaign_id)
    assert await mailing_app.broker.step(30) == 30

    await mailing_app.pause(campaign_id)
    sent_at_pause = len(mailing_app.mail.sent)
    _ = await mailing_app.drain()
    assert len(mailing_app.mail.sent) == sent_at_pause

    await mailing_app.resume(campaign_id)
    _ = await mailing_app.drain()
    assert (await mailing_app.get(campaign_id)).status == "completed_with_errors"


async def test_auto_pause_on_bounce_rate(mailing_app: MailingApp) -> None:
    campaign_id = await mailing_app.create_campaign(bounce_heavy_dataset())
    batch_id = await mailing_app.start_now(campaign_id)

    _ = await mailing_app.drain()

    campaign = await mailing_app.get(campaign_id)
    view = await mailing_app.th.handle(batch_id).view()
    assert campaign.status == "paused"
    assert campaign.pause_reason is not None
    assert campaign.pause_reason.startswith("['hard_bounce'] rate")
    assert view.paused


async def test_worker_crash_mid_flight_recovers(mailing_app: MailingApp) -> None:
    audience = [*compact_dataset(), ContactSeed("one@reject.test")]
    campaign_id = await mailing_app.create_campaign(audience)
    _ = await mailing_app.start_now(campaign_id)
    mailing_app.broker.kill_worker_after(500)

    _ = await mailing_app.drain()
    assert (await mailing_app.get(campaign_id)).status == "running"

    _ = mailing_app.clock.advance(seconds=61)
    _ = await mailing_app.th.run_maintenance_once()
    _ = await mailing_app.drain()
    assert (await mailing_app.get(campaign_id)).status == "completed_with_errors"
