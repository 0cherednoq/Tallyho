"""Пробуждение отложенного: ``release_deferred`` (RM8.6, UC-20, A-WI-07)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select, text, update

from tallyho.engine.operations import Operations, OperationTriggers
from tallyho.engine.producer import RootSpec
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.storage.metric_names import METRIC_PREFIX
from tests.helpers.relay import RecordingDispatcher, relay_env

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable
    from uuid import UUID

    from tests.helpers.relay import RelayEnv
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

NOW = datetime(2030, 5, 1, 9, 0, tzinfo=UTC)
GRACE = timedelta(seconds=5)
LATER = timedelta(hours=3)
NO_ACCOUNTS = "no_accounts"
QUOTA = "quota"


class _RelaySpy:
    kicked: list[UUID]

    def __init__(self) -> None:
        self.kicked = []

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        self.kicked.extend(batch_ids)


@pytest.fixture
def rel(env: Env) -> RelayEnv:
    return relay_env(env, NOW)


def operations(rel: RelayEnv, relay: _RelaySpy | None = None) -> Operations:
    return Operations(tables=rel.tables, clock=rel.clock, triggers=OperationTriggers(relay=relay))


async def seeded(
    rel: RelayEnv, n: int, *, limit: int | None = None, start_at: datetime | None = None
) -> UUID:
    """Запечатанный батч с ``n`` Items, окном ``limit`` и отложенным стартом."""
    async with rel.env.transaction() as conn:
        root = await rel.env.producer.create_root(
            conn, RootSpec(kind="checks", max_in_flight=limit, start_at=start_at)
        )
        _ = await rel.env.producer.add_items(
            conn, root.id, [TaskCall(task_name="check", args=(i,)) for i in range(n)]
        )
        _ = await rel.env.producer.seal(conn, root.id)
    rel.clock.advance(GRACE)
    return root.id


async def defer_items(rel: RelayEnv, batch_id: UUID, *, reason: str, count: int) -> list[UUID]:
    """Привести ``count`` записей батча к состоянию после ``item.defer``.

    То же, что оставляет Completer.defer (RM2.2): запись outbox со сроком в
    будущем и причиной, у Item — счётчик отложений и момент первого.
    """
    outbox = rel.tables.outbox
    item = rel.tables.item
    async with rel.env.transaction() as conn:
        chosen = list(
            await conn.scalars(
                select(outbox.c.id)
                .where(outbox.c.batch_id == batch_id, outbox.c.defer_reason.is_(None))
                .order_by(outbox.c.id)
                .limit(count)
            )
        )
        _ = await conn.execute(
            update(outbox)
            .where(outbox.c.id.in_(chosen))
            .values(available_at=rel.clock.current + LATER, defer_reason=reason)
        )
        _ = await conn.execute(
            update(item)
            .where(item.c.id.in_(chosen))
            .values(defers=1, first_deferred_at=rel.clock.current)
        )
    return chosen


async def ready(rel: RelayEnv, batch_id: UUID) -> int:
    """Записи батча, готовые к отправке по времени теста."""
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        found = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id == batch_id, outbox.c.available_at <= rel.clock.current)
        )
        return int(found or 0)


async def parked(rel: RelayEnv, batch_id: UUID) -> int:
    """Записи батча, запаркованные паузой или окном (``available_at = ∞``)."""
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        found = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id == batch_id, outbox.c.available_at == text("'infinity'"))
        )
        return int(found or 0)


async def deferred(rel: RelayEnv, batch_id: UUID) -> int:
    """Записи батча, которые всё ещё отложены."""
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        found = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id == batch_id, outbox.c.defer_reason.is_not(None))
        )
        return int(found or 0)


async def wake(
    rel: RelayEnv,
    batch_id: UUID,
    reasons: Collection[str] | None = None,
    *,
    relay: _RelaySpy | None = None,
) -> int:
    async with rel.env.transaction() as conn:
        return await operations(rel, relay).release_deferred(conn, batch_id, reasons)


async def test_without_filter_wakes_every_reason(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 4)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=2)
    _ = await defer_items(rel, batch_id, reason=QUOTA, count=2)
    assert await deferred(rel, batch_id) == 4
    assert await ready(rel, batch_id) == 0

    spy = _RelaySpy()
    assert await wake(rel, batch_id, relay=spy) == 4
    assert spy.kicked == [batch_id]
    assert await deferred(rel, batch_id) == 0
    assert await ready(rel, batch_id) == 4
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 4


async def test_filter_wakes_only_named_reasons(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 5)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=3)
    _ = await defer_items(rel, batch_id, reason=QUOTA, count=2)

    assert await wake(rel, batch_id, [NO_ACCOUNTS]) == 3
    assert await deferred(rel, batch_id) == 2
    assert await ready(rel, batch_id) == 3
    # Второй вызов с той же причиной идемпотентен: будить больше нечего.
    assert await wake(rel, batch_id, [NO_ACCOUNTS]) == 0


async def test_empty_reasons_change_nothing(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 2)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=2)
    spy = _RelaySpy()
    assert await wake(rel, batch_id, [], relay=spy) == 0
    assert spy.kicked == []
    assert await deferred(rel, batch_id) == 2


async def test_unknown_reason_changes_nothing(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 2)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=2)
    assert await wake(rel, batch_id, ["другая"]) == 0
    assert await deferred(rel, batch_id) == 2


async def test_rollback_keeps_rows_deferred(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 3)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=3)
    spy = _RelaySpy()
    async with rel.env.connection() as conn:
        assert await operations(rel, spy).release_deferred(conn, batch_id, [NO_ACCOUNTS]) == 3
    # Транзакция не коммитилась: ни одна запись не изменилась, relay не пинали.
    assert await deferred(rel, batch_id) == 3
    assert await ready(rel, batch_id) == 0
    assert spy.kicked == []


async def test_window_releases_only_free_room(rel: RelayEnv) -> None:
    # A-WI-07: окно 3, отложенных 10 — разбужены все, выпущены три.
    batch_id = await seeded(rel, 10, limit=3)
    chosen = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=10)
    assert await rel.relay.scan_once() == 0

    assert await wake(rel, batch_id, [NO_ACCOUNTS]) == 10
    assert await deferred(rel, batch_id) == 0
    assert await ready(rel, batch_id) == 10
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 3
    assert await rel.window_size(batch_id) == 3
    # Остальные ждут места в окне, а не своего срока: relay их запарковал.
    assert await parked(rel, batch_id) == 7
    assert await rel.relay.scan_once() == 0
    assert await rel.window_size(batch_id) == 3

    item = rel.tables.item
    async with rel.env.connection() as conn:
        rows = (
            await conn.execute(
                select(item.c.defers, item.c.first_deferred_at).where(item.c.id.in_(chosen))
            )
        ).all()
    # Потолки отложений не обойти: счётчик и момент первого отложения целы.
    assert {(row[0], row[1]) for row in rows} == {(1, NOW + GRACE)}


async def test_closed_window_releases_nothing(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 4, limit=0)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=4)
    assert await wake(rel, batch_id) == 4
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 0
    assert await rel.window_size(batch_id) == 0
    assert await parked(rel, batch_id) == 4


async def test_paused_batch_does_not_dispatch_woken_work(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 3)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=3)
    async with rel.env.transaction() as conn:
        await operations(rel).pause(conn, batch_id)

    assert await wake(rel, batch_id) == 3
    rel.clock.advance(GRACE)
    # Пауза сильнее пробуждения: relay паркует разбуженное, брокер не видит ничего.
    assert await rel.relay.scan_once() == 0
    assert rel.dispatcher.messages == []
    assert await parked(rel, batch_id) == 3


async def test_delayed_start_is_respected(rel: RelayEnv) -> None:
    start_at = NOW + timedelta(hours=1)
    batch_id = await seeded(rel, 2, start_at=start_at)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=2)

    assert await wake(rel, batch_id) == 2
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        moments = set(
            await conn.scalars(select(outbox.c.available_at).where(outbox.c.batch_id == batch_id))
        )
    # Пробуждение не делает работу доступной раньше отложенного старта.
    assert moments == {start_at}
    rel.clock.advance(GRACE)
    assert await rel.relay.scan_once() == 0


async def test_concurrent_wake_is_idempotent(rel: RelayEnv) -> None:
    batch_id = await seeded(rel, 3)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=3)
    first = await wake(rel, batch_id)
    second = await wake(rel, batch_id)
    assert (first, second) == (3, 0)
    assert await ready(rel, batch_id) == 3


@pytest.mark.parametrize("reason", ["", f"{METRIC_PREFIX}скрытая"])
async def test_rejects_impossible_reasons(rel: RelayEnv, reason: str) -> None:
    batch_id = await seeded(rel, 1)
    async with rel.env.transaction() as conn:
        with pytest.raises(ConfigurationError, match="defer"):
            _ = await operations(rel).release_deferred(conn, batch_id, [reason])


async def test_other_batches_keep_their_deferred_work(rel: RelayEnv) -> None:
    mine = await seeded(rel, 2)
    other = await seeded(rel, 2)
    _ = await defer_items(rel, mine, reason=NO_ACCOUNTS, count=2)
    _ = await defer_items(rel, other, reason=NO_ACCOUNTS, count=2)

    assert await wake(rel, mine) == 2
    assert await deferred(rel, other) == 2


async def test_dispatcher_is_untouched_until_relay_runs(rel: RelayEnv) -> None:
    # Пробуждение само ничего не отправляет: выпуск — работа relay.
    batch_id = await seeded(rel, 2)
    _ = await defer_items(rel, batch_id, reason=NO_ACCOUNTS, count=2)
    assert await wake(rel, batch_id) == 2
    assert isinstance(rel.dispatcher, RecordingDispatcher)
    assert rel.dispatcher.messages == []
