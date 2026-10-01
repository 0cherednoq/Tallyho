"""Recipe «строка на каждого получателя» (ARCHITECTURE §12.9, A-UC-21, A-UC-22)."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, cast

import pytest

from tallyho.model.errors import BatchPurged
from tallyho.model.states import BatchState, ItemState
from tests.examples.mailing.delivery_app import KIND, DeliveryApp

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []


@pytest.fixture
async def app(engine: AsyncEngine, schema: str) -> AsyncIterator[DeliveryApp]:
    value = await DeliveryApp.create(engine, schema, page=50, chunk=2)
    try:
        yield value
    finally:
        await value.close()


def audience(*, ok: int, bounce: int = 0, down: int = 0, flaky: int = 0) -> list[str]:
    return [
        *(f"user{index}@ok.test" for index in range(ok)),
        *(f"bounce{index}@bounce.test" for index in range(bounce)),
        *(f"down{index}@down.test" for index in range(down)),
        *(f"flaky{index}@flaky.test" for index in range(flaky)),
    ]


async def released_at(app: DeliveryApp, batch_id: UUID) -> object:
    from sqlalchemy import select  # ruff: ignore[import-outside-top-level]  # нужен только этой проверке

    from tallyho.storage.tables import build_metadata  # ruff: ignore[import-outside-top-level]  # нужен только этой проверке

    batch = build_metadata().batch
    async with app.engine.connect() as connection:
        return await connection.scalar(select(batch.c.released_at).where(batch.c.id == batch_id))


async def test_every_recipient_gets_one_terminal_row(app: DeliveryApp) -> None:
    emails = [*audience(ok=120, bounce=4, down=3), "USER1@ok.test"]  # последний — дубль адреса
    campaign_id = await app.create_campaign(emails)
    batch_id = await app.start(campaign_id)

    _ = await app.drain()

    campaign = await app.campaign(campaign_id)
    view = await app.th.handle(batch_id).view()
    statuses = await app.delivery_statuses(campaign_id)
    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    assert campaign["status"] == "completed_with_errors"
    assert (campaign["sent"], campaign["failed"], campaign["cancelled"]) == (120, 7, 0)
    # hard_bounce записала сама задача, exhausted — только экспорт.
    assert statuses == Counter(
        {("sent", None): 120, ("failed", "hard_bounce"): 4, ("failed", "exhausted"): 3}
    )
    assert view.children["send"].progress.duplicates == 1
    assert app.finalized == ["completed_with_errors"]
    assert await released_at(app, batch_id) is not None


async def test_attributes_and_listing_find_the_campaign_batch(app: DeliveryApp) -> None:
    first = await app.create_campaign(audience(ok=3), tenant="acme")
    second = await app.create_campaign(audience(ok=3), tenant="globex")
    first_batch = await app.start(first)
    second_batch = await app.start(second)
    _ = await app.drain()

    acme = await app.th.list_batches(kinds=[KIND], attributes={"tenant": "acme"})
    by_campaign = await app.th.list_batches(attributes={"campaign_id": second})
    done = await app.th.list_batches(kinds=[KIND], states={BatchState.SUCCEEDED})
    view = await app.th.handle(first_batch).view()

    assert [info.id for info in acme.items] == [first_batch]
    assert [info.id for info in by_campaign.items] == [second_batch]
    assert [info.id for info in done.items] == [second_batch, first_batch]
    assert dict(view.attributes) == {"campaign_id": first, "tenant": "acme"}
    assert view.memo == {"started_by": "delivery-example"}


async def test_cancel_in_the_middle_of_expansion(app: DeliveryApp) -> None:
    emails = audience(ok=130)
    campaign_id = await app.create_campaign(emails)
    batch_id = await app.start(campaign_id)
    # Первая страница развёрнута, отправлено несколько писем, остальное ещё не начато.
    assert await app.broker.step(1) == 1
    assert await app.broker.step(6) == 6

    await app.cancel(campaign_id)
    _ = await app.drain()

    campaign = await app.campaign(campaign_id)
    view = await app.th.handle(batch_id).view()
    statuses = await app.delivery_statuses(campaign_id)
    send = await app.th.handle(batch_id).child("send")
    cancelled_items = [entry async for entry in send.items(states={ItemState.CANCELLED})]
    assert view.state is BatchState.CANCELLED
    assert campaign["status"] == "cancelled"
    sent = statuses["sent", None]
    assert sent == len(app.mail.sent) == campaign["sent"]
    # Отменённые Items экспортированы, а получатели без Items закрыты запросом по остатку.
    assert statuses["cancelled", "cancelled"] == len(cancelled_items) == campaign["cancelled"]
    assert statuses["cancelled", "cancelled"] > 0
    assert statuses["cancelled", "not_dispatched"] > 0
    assert sum(statuses.values()) == len(emails)
    assert not [status for status, _reason in statuses if status == "pending"]
    assert sent + len(cancelled_items) == view.children["send"].progress.found


async def test_callback_failure_in_the_middle_is_retried(app: DeliveryApp) -> None:
    campaign_id = await app.create_campaign(audience(ok=5, down=7))
    batch_id = await app.start(campaign_id)
    app.settle_failures = 1  # упасть после первого чанка экспорта

    _ = await app.drain()

    campaign = await app.campaign(campaign_id)
    # Первая попытка откатилась целиком, вторая завершила экспорт и release.
    assert app.settle_calls == 2
    assert campaign["status"] == "completed_with_errors"
    assert await app.delivery_statuses(campaign_id) == Counter(
        {("sent", None): 5, ("failed", "exhausted"): 7}
    )
    assert await released_at(app, batch_id) is not None
    # Повторная доставка колбэка — no-op.
    assert await app.settle(campaign_id) is False


async def test_tree_waits_for_release_while_callback_keeps_failing(app: DeliveryApp) -> None:
    campaign_id = await app.create_campaign(audience(ok=2, down=5))
    batch_id = await app.start(campaign_id)
    app.settle_failures = 100  # колбэк исчерпает ретраи

    _ = await app.drain()

    campaign = await app.campaign(campaign_id)
    assert campaign["status"] == "settling"  # кампания не терминальна без экспорта
    assert (campaign["sent"], campaign["failed"]) == (2, 5)  # счётчики уже точные
    statuses = await app.delivery_statuses(campaign_id)
    assert statuses["pending", None] == 5  # откат не оставил частичного экспорта
    assert await released_at(app, batch_id) is None

    _ = app.clock.advance(days=3)
    _ = await app.th.run_maintenance_once()
    assert (await app.th.handle(batch_id).view()).state is BatchState.COMPLETED_WITH_ERRORS

    app.settle_failures = 0
    assert await app.settle(campaign_id) is True
    assert (await app.campaign(campaign_id))["status"] == "completed_with_errors"
    _ = await app.th.run_maintenance_once()
    with pytest.raises(BatchPurged):
        _ = await app.th.handle(batch_id).view()
    # Строки домена пережили retention.
    assert await app.delivery_statuses(campaign_id) == Counter(
        {("sent", None): 2, ("failed", "exhausted"): 5}
    )


async def test_retry_failed_after_settle_repeats_the_export(app: DeliveryApp) -> None:
    # flaky отвечает ошибкой на первые две попытки: первый прогон исчерпывает ретраи,
    # повтор проходит. down не проходит никогда.
    campaign_id = await app.create_campaign(audience(ok=4, down=2, flaky=3))
    batch_id = await app.start(campaign_id)
    _ = await app.drain()

    assert (await app.campaign(campaign_id))["status"] == "completed_with_errors"
    assert await app.delivery_statuses(campaign_id) == Counter(
        {("sent", None): 4, ("failed", "exhausted"): 5}
    )
    assert await released_at(app, batch_id) is not None

    app.settle_failures = 100  # второй экспорт задержан
    assert await app.retry_failed(campaign_id) == 5
    assert await released_at(app, batch_id) is None  # прежнее разрешение отменено
    _ = await app.drain()

    campaign = await app.campaign(campaign_id)
    assert app.finalized == ["completed_with_errors", "completed_with_errors"]
    assert campaign["status"] == "settling"
    assert (campaign["sent"], campaign["failed"]) == (7, 2)
    # Успешные после повтора исправили свои строки сами, кодом задачи.
    assert await app.delivery(campaign_id, "flaky0@flaky.test") == ("sent", None)

    # Retention не удаляет дерево по старому release, пока второй экспорт не прошёл.
    _ = app.clock.advance(days=3)
    _ = await app.th.run_maintenance_once()
    assert (await app.th.handle(batch_id).view()).state is BatchState.COMPLETED_WITH_ERRORS

    app.settle_failures = 0
    assert await app.settle(campaign_id) is True
    assert cast("str", (await app.campaign(campaign_id))["status"]) == "completed_with_errors"
    assert await app.delivery_statuses(campaign_id) == Counter(
        {("sent", None): 7, ("failed", "exhausted"): 2}
    )
    _ = await app.th.run_maintenance_once()
    with pytest.raises(BatchPurged):
        _ = await app.th.handle(batch_id).view()
