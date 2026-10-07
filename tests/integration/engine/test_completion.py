"""Путь B: завершение Item внутри транзакции пользователя."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import column, func, select, table, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho.engine.completer import (
    CompleterSettings,
    FinishResult,
    ItemRef,
    SpawnRequest,
    SubBatchRequest,
)
from tallyho.engine.completion import complete_in
from tallyho.engine.producer import SubBatchSpec
from tallyho.engine.spawn import SpawnRoute
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.calls import TaskCall
from tallyho.model.states import BatchState, ItemState, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.storage.metric_names import METRIC_PREFIX
from tallyho.storage.tx import resolve_connection
from tests.helpers.after_commit import pause_commit_polling
from tests.helpers.db import deadlocks, record_db_errors
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    SETTINGS,
    Finalized,
    MovableClock,
    RecordingProgress,
    lease_row,
    open_completer,
    schema_engine,
    seed,
)

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from tests.integration.engine.conftest import Env

__all__: list[str] = []


class RecordingFinishObserver(NullObserver):
    """Record the scalar completion event emitted after commit."""

    def __init__(self) -> None:
        self.finished: list[tuple[UUID, ResultClass, str | None, int]] = []

    @override
    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        del batch_id
        self.finished.append((item_id, result, label, attempt))


async def _state(env: Env, item_id: UUID) -> ItemState:
    async with env.connection() as conn:
        value = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.id == item_id)
        )
    assert value is not None
    return ItemState(value)


async def test_complete_in_commits_domain_item_delta_and_then_folds(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    finalizer = Finalized()
    progress = RecordingProgress()
    async with (
        open_completer(env, finalizer=finalizer, progress=progress) as completer,
        AsyncSession(schema_engine(env)) as session,
    ):
        await insert_id(await resolve_connection(session), probe, 1)
        changed = await complete_in(
            session,
            ref,
            FinishResult(
                result_class=ResultClass.OK,
                result={"message_id": "m-1"},
                metrics={"bytes": 42},
            ),
            completer=completer,
        )
        assert changed
        await session.commit()

    assert await committed_ids(env.engine, probe) == [1]
    assert await _state(env, ref.id) is ItemState.OK
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.w_done, counters.pending) == (1, 1, 0)
    async with env.connection() as conn:
        delta_count = await conn.scalar(select(func.count()).select_from(env.tables.counter_delta))
        metrics = (
            await conn.execute(
                select(
                    env.tables.metric.c.name,
                    env.tables.metric.c.slot,
                    env.tables.metric.c.value,
                ).order_by(env.tables.metric.c.name)
            )
        ).all()
    assert delta_count == 0
    assert metrics == [(METRIC_PREFIX + "bytes", COMPLETER_SLOT, 42), ("ok", COMPLETER_SLOT, 1)]
    assert finalizer.calls == [seeded.batch_id]
    assert progress.calls == [([seeded.batch_id], False)]


async def test_complete_in_outer_rollback_removes_domain_and_completion(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    async with open_completer(env) as completer, AsyncSession(schema_engine(env)) as session:
        await insert_id(await resolve_connection(session), probe, 1)
        assert await complete_in(
            session,
            ref,
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        await session.rollback()

    assert await committed_ids(env.engine, probe) == []
    assert await _state(env, ref.id) is ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 1


async def test_complete_in_savepoint_rollback_discards_callback_and_writes(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    finalizer = Finalized()
    async with (
        open_completer(env, finalizer=finalizer) as completer,
        AsyncSession(schema_engine(env)) as session,
    ):
        nested = await session.begin_nested()
        await insert_id(await resolve_connection(session), probe, 1)
        assert await complete_in(
            session,
            ref,
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        await nested.rollback()
        await session.commit()

    assert await committed_ids(env.engine, probe) == []
    assert await _state(env, ref.id) is ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 1
    assert finalizer.calls == []


async def test_complete_in_duplicate_returns_false_and_counts_once(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        async with AsyncSession(schema_engine(env)) as first:
            assert await complete_in(
                first,
                ref,
                FinishResult(result_class=ResultClass.SKIP),
                completer=completer,
            )
            await first.commit()
        async with AsyncSession(schema_engine(env)) as second:
            assert not await complete_in(
                second,
                ref,
                FinishResult(result_class=ResultClass.ERROR),
                completer=completer,
            )
            await second.commit()

    counters = await env.counters(seeded.batch_id)
    assert (counters.skip, counters.error, counters.pending) == (1, 0, 0)


async def test_complete_in_scalar_persists_fields_observer_and_batch_guard(env: Env) -> None:
    seeded = await seed(env, 2)
    error_ref, untouched_ref = seeded.refs
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.item).where(env.tables.item.c.id == error_ref.id).values(attempt=4)
        )

    observer = RecordingFinishObserver()
    value = FinishResult(
        result_class=ResultClass.ERROR,
        label="rejected",
        result={"provider": "mx-1"},
        error={"code": 550},
    )
    async with open_completer(env, observer=observer) as completer:
        async with AsyncSession(schema_engine(env)) as session:
            assert await complete_in(session, error_ref, value, completer=completer)
            await session.commit()
        async with AsyncSession(schema_engine(env)) as session:
            wrong = ItemRef(untouched_ref.id, uuid4())
            assert not await complete_in(session, wrong, value, completer=completer)
            await session.commit()

    async with env.connection() as conn:
        rows = (
            await conn.execute(
                select(env.tables.item).where(
                    env.tables.item.c.id.in_([error_ref.id, untouched_ref.id])
                )
            )
        ).mappings()
        by_id = {row["id"]: row for row in rows}
    changed = by_id[error_ref.id]
    assert (
        changed["state"],
        changed["label"],
        changed["result"],
        changed["error"],
        changed["finished_at"],
    ) == (
        ItemState.ERROR,
        "rejected",
        {"provider": "mx-1"},
        {"code": 550},
        NOW,
    )
    assert by_id[untouched_ref.id]["state"] == ItemState.ACTIVE
    assert observer.finished == [(error_ref.id, ResultClass.ERROR, "rejected", 4)]


async def test_complete_in_connection_spawns_atomically_and_folds_all_deltas(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    route = SpawnRoute(
        source_id=seeded.batch_id,
        target_id=seeded.batch_id,
        root_id=seeded.batch_id,
    )
    value = FinishResult(
        result_class=ResultClass.ERROR,
        label="retryable",
        spawns=(
            SpawnRequest(
                route=route,
                call=TaskCall(task_name="retry", key="retry:1"),
            ),
        ),
    )
    async with open_completer(env) as completer, env.transaction() as conn:
        assert await complete_in(conn, ref, value, completer=completer)

    counters = await env.counters(seeded.batch_id)
    assert (counters.total, counters.error, counters.pending, counters.tree_total) == (2, 1, 1, 2)
    assert await env.count(env.tables.counter_delta) == 0
    assert await env.count(env.tables.item_mark) == 1


async def test_complete_in_creates_sub_batch_without_spawn(env: Env) -> None:
    """Под-батч из задачи без spawn, завершённой путём B (Fix-27).

    Раньше батч самого Item блокировался только в маршрутах spawn/expect, и
    ``_sub_batches`` падал ``KeyError`` по id батча Item.
    """
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    request = SubBatchRequest(
        spec=SubBatchSpec(key="parts"),
        calls=(
            TaskCall(task_name="part", args=(1,), key="p:1"),
            TaskCall(task_name="part", args=(2,), key="p:2", weight=3),
        ),
    )
    value = FinishResult(result_class=ResultClass.OK, sub_batches=(request,))
    finalizer = Finalized()
    async with open_completer(env, finalizer=finalizer) as completer:
        assert (await completer.claim(ref)).run
        async with AsyncSession(schema_engine(env)) as session:
            assert await complete_in(session, ref, value, completer=completer, attempt=0)
            await session.commit()

    assert await _state(env, ref.id) is ItemState.OK
    assert await lease_row(env, ref.id) is None
    assert await env.count(env.tables.counter_delta) == 0
    batch = env.tables.batch
    item = env.tables.item
    async with env.connection() as conn:
        child_id = await conn.scalar(
            select(batch.c.id).where(batch.c.root_id == seeded.batch_id, batch.c.key == "parts")
        )
        assert child_id is not None
        virtual = (
            await conn.execute(
                select(item.c.state).where(
                    item.c.batch_id == seeded.batch_id, item.c.child_batch_id == child_id
                )
            )
        ).all()
    child = await env.batch(child_id)
    assert (child["parent_id"], child["state"]) == (seeded.batch_id, BatchState.SEALED)
    assert virtual == [(ItemState.ACTIVE,)]
    parent = await env.counters(seeded.batch_id)
    parts = await env.counters(child_id)
    assert (parent.total, parent.ok, parent.pending, parent.w_done) == (2, 1, 1, 1)
    assert (parts.total, parts.w_total, parts.pending) == (2, 4, 2)
    assert parent.tree_total == 3
    assert child_id in finalizer.calls


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_complete_in_under_strict_isolation_has_no_serialization_failures(
    env: Env, isolation: str
) -> None:
    seeded = await seed(env, 12)
    engine = schema_engine(env).execution_options(isolation_level=isolation)
    async with open_completer(env) as completer:

        async def finish(index: int) -> bool:
            async with AsyncSession(engine) as session:
                changed = await complete_in(
                    session,
                    seeded.refs[index],
                    FinishResult(result_class=ResultClass.OK, label=f"ok-{index}"),
                    completer=completer,
                )
                await session.commit()
                return changed

        assert all(await asyncio.gather(*(finish(index) for index in range(12))))

    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (12, 0)


async def test_open_complete_in_transaction_does_not_lock_counter(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer, AsyncSession(schema_engine(env)) as session:
        assert await complete_in(
            session,
            ref,
            FinishResult(result_class=ResultClass.OK),
            completer=completer,
        )
        async with env.connection() as conn:
            locks = table("pg_locks", column("relation"))
            locked = await conn.scalar(
                select(func.count())
                .select_from(locks)
                .where(locks.c.relation == func.to_regclass(f'"{env.schema}"."th_counter"'))
            )
        assert locked == 0
        await session.rollback()


async def test_twenty_percent_long_transactions_do_not_block_counter(env: Env) -> None:
    seeded = await seed(env, 10)
    release = asyncio.Event()
    started = asyncio.Event()
    count = 0

    async with open_completer(env) as completer:

        async def finish(index: int, *, slow: bool) -> bool:
            nonlocal count
            async with AsyncSession(schema_engine(env)) as session:
                changed = await complete_in(
                    session,
                    seeded.refs[index],
                    FinishResult(result_class=ResultClass.OK, label=f"item-{index}"),
                    completer=completer,
                )
                if slow:
                    count += 1
                    if count == 2:
                        started.set()
                    await release.wait()
                await session.commit()
                return changed

        slow = [asyncio.create_task(finish(index, slow=True)) for index in range(2)]
        await started.wait()
        assert all(await asyncio.gather(*(finish(index, slow=False) for index in range(2, 10))))
        await asyncio.sleep(2)
        async with env.connection() as conn:
            locks = table(
                "pg_locks",
                column("relation"),
                column("granted"),
                column("waitstart"),
            )
            waiting = await conn.scalar(
                select(func.count())
                .select_from(locks)
                .where(
                    locks.c.relation == func.to_regclass(f'"{env.schema}"."th_counter"'),
                    locks.c.granted.is_(False),
                    locks.c.waitstart < func.now() - timedelta(milliseconds=100),
                )
            )
        assert waiting == 0
        release.set()
        assert all(await asyncio.gather(*slow))


# --- владение lease (UC-08): завершает только попытка, владеющая Item ------------------

LATER = NOW + SETTINGS.lease_ttl + timedelta(seconds=1)
"""Момент, когда lease, взятый в ``NOW``, уже истёк."""
OTHER = CompleterSettings(worker_id="worker-2", slot=COMPLETER_SLOT + 1)
OK = FinishResult(result_class=ResultClass.OK)


class _Limits:
    """Умолчание ``max_retries`` адаптера: истёкший lease возвращает Item в outbox."""

    def max_retries(self, task_name: str) -> int:
        del task_name
        return 1


def _sweeper(env: Env, *, retries: bool = False) -> Sweeper:
    return Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=MovableClock(LATER),
        finalizer=Finalized(),
        limits=_Limits() if retries else None,
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )


async def _untouched(env: Env, item_id: UUID) -> bool:
    """Отказ ``complete_in`` ничего не записал: ни дельт, ни пометок."""
    deltas = await env.count(env.tables.counter_delta)
    marks = await env.count(env.tables.item_mark)
    return (deltas, marks) == (0, 0) and await _state(env, item_id) is ItemState.ACTIVE


async def test_complete_in_owner_attempt_finishes_item(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        claim = await completer.claim(ref)
        assert (claim.run, claim.attempt) == (True, 0)
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, OK, completer=completer, attempt=0)

    assert await _state(env, ref.id) is ItemState.OK
    assert await lease_row(env, ref.id) is None
    assert (await env.counters(seeded.batch_id)).ok == 1


async def test_close_right_after_commit_still_settles_complete_in(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Опрос COMMIT ушёл в паузу, Completer закрывают сразу после commit пользователя.

    close() доставляет колбэки уже закоммиченных транзакций до флага закрытия:
    lease снят, дельты свёрнуты сразу, а не sweeper-ом позже.
    """

    pause_commit_polling(monkeypatch)
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, OK, completer=completer, attempt=0)

    assert await _state(env, ref.id) is ItemState.OK
    assert await lease_row(env, ref.id) is None
    assert await env.count(env.tables.counter_delta) == 0


async def test_settled_right_after_commit_delivers_complete_in(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """settled() подхватывает callback внешнего COMMIT до проверки idle."""
    pause_commit_polling(monkeypatch)
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, OK, completer=completer, attempt=0)
        await asyncio.wait_for(completer.settled(), timeout=5)

        assert await _state(env, ref.id) is ItemState.OK
        assert await lease_row(env, ref.id) is None
        assert await env.count(env.tables.counter_delta) == 0


async def test_complete_in_expired_but_unclaimed_lease_still_owns_item(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        assert (await completer.claim(ref)).run
        clock.value = LATER
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, OK, completer=completer, attempt=0)

    assert await _state(env, ref.id) is ItemState.OK


async def test_complete_in_without_lease_is_rejected(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer, env.transaction() as conn:
        assert not await complete_in(conn, ref, OK, completer=completer, attempt=0)

    assert await _untouched(env, ref.id)
    assert (await env.counters(seeded.batch_id)).pending == 1


async def test_complete_in_stolen_lease_is_rejected_and_left_intact(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with (
        open_completer(env) as first,
        open_completer(env, clock=MovableClock(LATER), settings=OTHER) as second,
    ):
        assert (await first.claim(ref)).run
        stolen = await second.claim(ref)
        assert (stolen.run, stolen.attempt) == (True, 1)

        async with env.transaction() as conn:
            assert not await complete_in(conn, ref, OK, completer=first, attempt=0)
        assert await _untouched(env, ref.id)
        lease = await lease_row(env, ref.id)
        assert lease is not None
        assert (lease["worker_id"], lease["attempt"]) == (OTHER.worker_id, 1)

        # Номер попытки без своего lease тоже не даёт права на Item.
        async with env.transaction() as conn:
            assert not await complete_in(conn, ref, OK, completer=first, attempt=1)
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, OK, completer=second, attempt=1)

    assert await _state(env, ref.id) is ItemState.OK
    assert (await env.counters(seeded.batch_id)).ok == 1


async def test_complete_in_same_worker_is_fenced_by_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:
        assert (await completer.claim(ref)).run
        # Lease истёк, sweeper вернул Item в outbox, и его снова получил тот же процесс.
        assert await _sweeper(env, retries=True).expire_leases() == 1
        again = await completer.claim(ref)
        assert (again.run, again.attempt) == (True, 1)

        async with env.transaction() as conn:
            assert not await complete_in(conn, ref, OK, completer=completer, attempt=0)
        lease = await lease_row(env, ref.id)
        assert lease is not None
        assert (lease["worker_id"], lease["attempt"]) == (SETTINGS.worker_id, 1)
        assert await _state(env, ref.id) is ItemState.ACTIVE

        async with env.transaction() as conn:
            assert await complete_in(conn, ref, OK, completer=completer, attempt=1)

    assert await _state(env, ref.id) is ItemState.OK


async def test_complete_in_terminal_item_with_stale_lease_is_rejected(env: Env) -> None:
    seeded = await seed(env, 2)
    cancelled, foreign = seeded.refs
    async with open_completer(env) as completer:
        assert (await completer.claim(cancelled)).run
        assert (await completer.claim(foreign)).run
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(env.tables.item)
                .where(env.tables.item.c.id == cancelled.id)
                .values(state=int(ItemState.CANCELLED))
            )
        async with env.transaction() as conn:
            assert not await complete_in(conn, cancelled, OK, completer=completer, attempt=0)
            # Lease свой, но Item из другого батча: ссылка не та.
            wrong = ItemRef(foreign.id, uuid4())
            assert not await complete_in(conn, wrong, OK, completer=completer, attempt=0)

    assert await lease_row(env, cancelled.id) is not None
    assert await _state(env, cancelled.id) is ItemState.CANCELLED
    assert await _state(env, foreign.id) is ItemState.ACTIVE
    assert (await env.counters(seeded.batch_id)).pending == 2


# --- метрики пути B, не перенесённые после commit (Fix-23) -----------------------------

GRACE = timedelta(seconds=30)
WITH_METRICS = FinishResult(result_class=ResultClass.OK, label="sent", metrics={"bytes": 42})


def _grace_sweeper(env: Env, now: datetime) -> Sweeper:
    return Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=MovableClock(now),
        finalizer=Finalized(),
        settings=SweeperSettings(finalize_grace=GRACE),
    )


async def _metric_slots(env: Env) -> list[tuple[str, int, int]]:
    metric = env.tables.metric
    async with env.connection() as conn:
        rows = await conn.execute(
            select(metric.c.name, metric.c.slot, metric.c.value).order_by(
                metric.c.name, metric.c.slot
            )
        )
        return [(name, slot, value) for name, slot, value in rows]


def _sums(rows: list[tuple[str, int, int]]) -> dict[str, int]:
    sums: dict[str, int] = {}
    for name, _, value in rows:
        sums[name] = sums.get(name, 0) + value
    return sums


async def _complete_and_crash(env: Env, monkeypatch: pytest.MonkeyPatch, ref: ItemRef) -> None:
    """``complete_in`` закоммичен, а процесс «упал» до переноса после commit."""
    pause_commit_polling(monkeypatch)
    async with open_completer(env) as completer:
        async with env.transaction() as conn:
            assert await complete_in(conn, ref, WITH_METRICS, completer=completer)
        # Колбэк после commit ещё не доставлен; Completer обрывается, как при падении.
        await completer.abort()


async def test_sweeper_moves_metric_slot_left_after_crash(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = await seed(env, 1)
    await _complete_and_crash(env, monkeypatch, seeded.refs[0])
    before = await _metric_slots(env)
    counters = await env.counters(seeded.batch_id)
    assert len({slot for _, slot, _ in before}) == 1
    assert all(slot < 0 for _, slot, _ in before)
    assert await env.count(env.tables.counter_delta) == 1

    # Моложе grace: перенос после commit ещё возможен, строки не трогаются.
    assert await _grace_sweeper(env, NOW + GRACE - timedelta(seconds=1)).fold_stale_deltas() == 0
    assert await _metric_slots(env) == before

    assert await _grace_sweeper(env, NOW + GRACE).fold_stale_deltas() == 1
    after = await _metric_slots(env)
    assert after == [(METRIC_PREFIX + "bytes", 0, 42), ("sent", 0, 1)]
    assert _sums(after) == _sums(before)
    assert await env.counters(seeded.batch_id) == counters
    assert await env.count(env.tables.counter_delta) == 0


async def test_sweeper_skips_metric_slot_while_its_fold_is_in_progress(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = await seed(env, 1)
    await _complete_and_crash(env, monkeypatch, seeded.refs[0])
    before = await _metric_slots(env)
    sweeper = _grace_sweeper(env, NOW + GRACE)
    delta = env.tables.counter_delta
    async with env.connection() as holder:
        # Свёртка Completer держит дельту: строки её слота она заберёт сама.
        _ = await holder.execute(select(delta.c.id).with_for_update())
        assert await sweeper.fold_stale_deltas() == 0
        assert await _metric_slots(env) == before
        await holder.rollback()

    assert await sweeper.fold_stale_deltas() == 1
    after = await _metric_slots(env)
    assert all(slot == 0 for _, slot, _ in after)
    assert _sums(after) == _sums(before)


async def test_sweeper_races_settle_of_complete_in_without_deadlocks(env: Env) -> None:
    """Sweeper без grace сворачивает дельты наперегонки со свёрткой Completer."""
    items = 24
    seeded = await seed(env, items)
    sweeper = Sweeper(
        tables=env.tables,
        engine=schema_engine(env),
        clock=MovableClock(),
        finalizer=Finalized(),
        settings=SweeperSettings(finalize_grace=timedelta(0)),
    )
    finished = asyncio.Event()

    async def sweep() -> int:
        folded = 0
        while not finished.is_set():
            folded += await sweeper.fold_stale_deltas()
            await asyncio.sleep(0)
        return folded

    with record_db_errors(env.engine) as errors:
        async with open_completer(env) as completer:

            async def finish(ref: ItemRef) -> bool:
                async with AsyncSession(schema_engine(env)) as session:
                    changed = await complete_in(session, ref, WITH_METRICS, completer=completer)
                    await session.commit()
                    return changed

            sweeping = asyncio.create_task(sweep())
            try:
                assert all(await asyncio.gather(*(finish(ref) for ref in seeded.refs)))
                await completer.settled()
            finally:
                finished.set()
                _ = await sweeping
        _ = await sweeper.fold_stale_deltas()

    assert deadlocks(errors) == []
    rows = await _metric_slots(env)
    assert all(slot >= 0 for _, slot, _ in rows)
    assert _sums(rows) == {METRIC_PREFIX + "bytes": 42 * items, "sent": items}
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (items, 0)
    assert await env.count(env.tables.counter_delta) == 0
