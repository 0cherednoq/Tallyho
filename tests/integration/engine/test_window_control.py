"""Управление окном max_in_flight на ходу: set_max_in_flight / clear_max_in_flight (RM1.1)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import func, select, text, update

from tallyho.engine.operations import Operations, OperationTriggers
from tallyho.engine.producer import RootSpec
from tallyho.engine.relay import release_window
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState, ItemState
from tests.helpers.relay import RecordingDispatcher, relay_env

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from uuid import UUID

    from tests.helpers.relay import RelayEnv
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

NOW = datetime(2030, 3, 1, 9, 0, tzinfo=UTC)
GRACE = timedelta(seconds=5)


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
    return Operations(
        tables=rel.tables,
        clock=rel.clock,
        triggers=OperationTriggers(relay=relay),
    )


async def windowed(rel: RelayEnv, n: int, limit: int | None = None) -> UUID:
    """Запечатанный батч с ``n`` Items и начальным окном ``limit``."""
    async with rel.env.transaction() as conn:
        root = await rel.producer.create_root(conn, RootSpec(kind="k", max_in_flight=limit))
        _ = await rel.producer.add_items(
            conn, root.id, [TaskCall(task_name="render", args=(i,)) for i in range(n)]
        )
        _ = await rel.producer.seal(conn, root.id)
    rel.clock.advance(GRACE)
    return root.id


async def parked(rel: RelayEnv, batch_id: UUID) -> int:
    outbox = rel.tables.outbox
    async with rel.env.connection() as conn:
        found = await conn.scalar(
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.batch_id == batch_id, outbox.c.available_at == text("'infinity'"))
        )
        return int(found or 0)


async def finish(rel: RelayEnv, item_ids: Sequence[UUID]) -> list[UUID]:
    # Эмуляция finish Completer: Item завершён, место окна освобождено.
    item = rel.tables.item
    async with rel.env.transaction() as conn:
        _ = await conn.execute(
            update(item).where(item.c.id.in_(item_ids)).values(state=int(ItemState.OK))
        )
        return await release_window(conn, rel.tables, item_ids)


async def window_of(rel: RelayEnv, batch_id: UUID) -> int | None:
    row = await rel.env.batch(batch_id)
    return cast("int | None", row["max_in_flight"])


async def test_closed_window_parks_new_work_and_lets_running_finish(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 6, 2)
    assert await rel.relay.scan_once() == 2
    sent = list(rel.dispatcher.ids)

    async with rel.env.transaction() as conn:
        await operations(rel).set_max_in_flight(conn, batch_id, 0, reason="нет кредитов")
    assert await window_of(rel, batch_id) == 0

    # Отправленное осталось у брокера и доделывается, остальное запарковано.
    assert await rel.window_size(batch_id) == 2
    assert await rel.relay.scan_once() == 0
    assert await parked(rel, batch_id) == 4
    assert await finish(rel, sent) == [batch_id]
    assert await rel.relay.scan_once() == 0
    assert rel.dispatcher.ids == sent
    assert await parked(rel, batch_id) == 4


async def test_reopened_window_releases_at_most_limit(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 6, 0)
    assert await rel.relay.scan_once() == 0
    assert await parked(rel, batch_id) == 6

    spy = _RelaySpy()
    async with rel.env.transaction() as conn:
        await operations(rel, spy).set_max_in_flight(conn, batch_id, 2)
    assert spy.kicked == [batch_id]
    # Распарковано ровно место окна; отправка — ближайшим проходом relay.
    assert await parked(rel, batch_id) == 4
    rel.relay.kick([batch_id])
    assert await rel.relay.flush_kicked() == 2
    assert await rel.window_size(batch_id) == 2
    assert await rel.relay.scan_once() == 0


async def test_cleared_window_releases_everything(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 5, 0)
    assert await rel.relay.scan_once() == 0
    assert await parked(rel, batch_id) == 5

    async with rel.env.transaction() as conn:
        await operations(rel).set_max_in_flight(conn, batch_id, None, reason="вернулись")
    assert await window_of(rel, batch_id) is None
    assert await parked(rel, batch_id) == 0
    assert await rel.relay.scan_once() == 5
    assert await rel.window_size(batch_id) == 0


async def test_rollback_reverts_window(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 3, 2)
    subject = operations(rel)
    async with rel.env.connection() as conn:
        await subject.set_max_in_flight(conn, batch_id, 0)
        assert (
            await conn.scalar(
                select(rel.tables.batch.c.max_in_flight).where(rel.tables.batch.c.id == batch_id)
            )
            == 0
        )
    # Транзакция не коммитилась: окно осталось прежним.
    assert await window_of(rel, batch_id) == 2


async def test_window_and_pause_are_independent(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 6, 2)
    subject = operations(rel)
    async with rel.env.transaction() as conn:
        await subject.pause(conn, batch_id)
        await subject.set_max_in_flight(conn, batch_id, 4)
    # Окно изменилось, пауза держится: ничего не уехало и не распарковано.
    assert await window_of(rel, batch_id) == 4
    assert (await rel.env.batch(batch_id))["paused_at"] is not None
    assert await rel.relay.scan_once() == 0
    assert await parked(rel, batch_id) == 6

    async with rel.env.transaction() as conn:
        await subject.resume(conn, batch_id)
    rel.clock.advance(GRACE)
    # resume не снял окно: уедет не больше его.
    assert await window_of(rel, batch_id) == 4
    assert await rel.relay.scan_once() == 4
    assert await rel.window_size(batch_id) == 4


async def test_closed_window_batch_does_not_finalize(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 3, 0)
    assert await rel.relay.scan_once() == 0
    assert await parked(rel, batch_id) == 3
    row = await rel.env.batch(batch_id)
    assert BatchState(row["state"]) is BatchState.SEALED
    assert row["finished_at"] is None


async def test_rejects_negative_and_non_integer_window(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 1, 1)
    subject = operations(rel)
    async with rel.env.transaction() as conn:
        with pytest.raises(ConfigurationError, match="max_in_flight"):
            await subject.set_max_in_flight(conn, batch_id, -1)
        # bool — не окно: True выглядит как 1, но это ошибка вызывающего.
        flag = True
        with pytest.raises(ConfigurationError, match="max_in_flight"):
            await subject.set_max_in_flight(conn, batch_id, flag)


async def test_reason_is_stored_and_replaced_in_options(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 1, 1)
    subject = operations(rel)
    async with rel.env.transaction() as conn:
        await subject.set_max_in_flight(conn, batch_id, 0, reason="нет кредитов")
    assert (await rel.env.batch(batch_id))["options"] == {"max_in_flight_reason": "нет кредитов"}
    async with rel.env.transaction() as conn:
        await subject.set_max_in_flight(conn, batch_id, 3)
    # Без reason прошлая причина не остаётся: она описывала предыдущее решение.
    assert (await rel.env.batch(batch_id))["options"] == {}


async def test_terminal_batch_keeps_window(rel: RelayEnv) -> None:
    batch_id = await windowed(rel, 1, 1)
    batch = rel.tables.batch
    async with rel.env.transaction() as conn:
        _ = await conn.execute(
            update(batch).where(batch.c.id == batch_id).values(state=int(BatchState.SUCCEEDED))
        )
    async with rel.env.transaction() as conn:
        await operations(rel).set_max_in_flight(conn, batch_id, 0)
    assert await window_of(rel, batch_id) == 1


async def test_window_change_touches_only_this_batch(rel: RelayEnv) -> None:
    first = await windowed(rel, 2, 1)
    second = await windowed(rel, 2, 1)
    async with rel.env.transaction() as conn:
        await operations(rel).set_max_in_flight(conn, first, 0)
    assert await window_of(rel, first) == 0
    assert await window_of(rel, second) == 1
    dispatcher = RecordingDispatcher()
    other = rel.another_relay(dispatcher)
    assert await other.scan_once() == 1
    assert await rel.window_size(first) == 0
    assert await rel.window_size(second) == 1
