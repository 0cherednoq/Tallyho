"""Finalizer: tx-хук, CAS, callback-outbox и каскад этапов (T4.4)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, select, text, update
from typing_extensions import override

from tallyho.engine.finalizer import Finalizer, FinalizerSettings
from tallyho.engine.operations import Operations
from tallyho.engine.producer import CallbackName, RootSpec, SubBatchSpec
from tallyho.hooks.registry import HookRegistry
from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError
from tallyho.model.policy import FailurePolicy
from tallyho.model.states import (
    BatchState,
    CancelReason,
    ItemState,
    OnFeederFailed,
    OutboxKind,
)
from tallyho.protocols.clock import SystemClock
from tallyho.protocols.observer import NullObserver
from tallyho.storage.counters import CounterDelta, upsert_slots
from tallyho.storage.tx import RetryPolicy
from tests.helpers.db import backend_pid, wait_blocked_by
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.integration.engine.completer_env import (
    NOW,
    RecordingProgress,
    schema_engine,
    set_batch,
)

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.model.views import BatchSummary
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


@dataclass
class RecordingObserver(NullObserver):
    """Записывает отсутствующие хуки."""

    missing: list[tuple[UUID, str, str]] = field(default_factory=list)

    @override
    def hook_missing(self, *, batch_id: UUID, kind: str, hook: str) -> None:
        self.missing.append((batch_id, kind, hook))


def finalizer(
    env: Env,
    registry: HookRegistry,
    *,
    progress: RecordingProgress | None = None,
) -> Finalizer:
    """Finalizer над схемой конкретного интеграционного теста."""
    return Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
        progress=progress,
    )


async def test_empty_batch_finalizes_once_and_writes_callbacks(
    env: Env, registry: HookRegistry
) -> None:
    calls = {
        CallbackName.ON_SUCCEEDED: TaskCall(task_name="success", args=(), kwargs={}),
        CallbackName.ON_FINALIZED_TASK: TaskCall(
            task_name="always", args=(), kwargs={}, queue="hooks"
        ),
        CallbackName.ON_FAILED: TaskCall(task_name="wrong", args=(), kwargs={}),
    }
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="empty", callbacks=calls))
        assert await env.producer.seal(conn, root.id)
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(hook_error="old", updated_at=NOW - timedelta(days=1))
        )

    progress = RecordingProgress()
    subject = finalizer(env, registry, progress=progress)
    assert await subject.try_finalize(root.id)
    assert not await subject.try_finalize(root.id)

    row = await env.batch(root.id)
    assert row["state"] == BatchState.SUCCEEDED
    assert row["snap_seq"] == 1
    assert row["finished_at"] is not None
    assert row["updated_at"] > NOW - timedelta(days=1)
    assert row["hook_error"] is None
    assert progress.calls == [([root.id], True)]
    outbox = env.tables.outbox
    async with env.connection() as conn:
        records = (
            await conn.execute(
                select(
                    outbox.c.id,
                    outbox.c.kind,
                    outbox.c.task_name,
                    outbox.c.options,
                ).order_by(outbox.c.task_name)
            )
        ).all()
    assert len({record.id for record in records}) == 2
    assert [(record.kind, record.task_name) for record in records] == [
        (OutboxKind.CALLBACK, "always"),
        (OutboxKind.CALLBACK, "success"),
    ]
    assert records[0].options == {"queue": "hooks"}


async def test_concurrent_finalizers_commit_one_hook(env: Env, registry: HookRegistry) -> None:
    probe = await create_probe(env.engine, env.schema)
    entered = asyncio.Event()
    calls = 0

    @registry.on_finalized("race")
    async def save(session: AsyncSession, summary: BatchSummary) -> None:
        nonlocal calls
        calls += 1
        value = calls
        if calls == 2:
            entered.set()
        await entered.wait()
        assert summary.state is BatchState.SUCCEEDED
        await insert_id(await session.connection(), probe, value)

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="race"))
        _ = await env.producer.seal(conn, root.id)

    subject = finalizer(env, registry)
    results = await asyncio.gather(
        subject.try_finalize(root.id),
        subject.try_finalize(root.id),
    )
    assert sorted(results) == [False, True]
    assert calls == 2
    assert len(await committed_ids(env.engine, probe)) == 1


async def test_terminal_state_wins_while_finalizer_hook_is_running(
    env: Env, registry: HookRegistry
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @registry.on_finalized("state-race")
    async def save(_session: AsyncSession, _summary: BatchSummary) -> None:
        entered.set()
        await release.wait()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="state-race"))
        _ = await env.producer.seal(conn, root.id)

    running = asyncio.create_task(finalizer(env, registry).try_finalize(root.id))
    await entered.wait()
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(state=int(BatchState.SUCCEEDED), finished_at=NOW)
        )
    release.set()
    assert not await running
    assert (await env.batch(root.id))["snap_seq"] == 0


async def test_changed_snapshot_retries_finalizer_hook(env: Env, registry: HookRegistry) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    @registry.on_finalized("snapshot-race")
    async def save(_session: AsyncSession, _summary: BatchSummary) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await release.wait()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="snapshot-race"))
        _ = await env.producer.seal(conn, root.id)

    running = asyncio.create_task(finalizer(env, registry).try_finalize(root.id))
    await entered.wait()
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.batch)
            .where(env.tables.batch.c.id == root.id)
            .values(snap_seq=env.tables.batch.c.snap_seq + 1)
        )
    release.set()
    assert await running
    assert calls == 2
    assert (await env.batch(root.id))["snap_seq"] == 2


async def test_hook_transaction_error_rolls_back_and_is_recorded(
    env: Env, registry: HookRegistry
) -> None:
    @registry.on_finalized("broken")
    async def broken(session: AsyncSession, _summary: BatchSummary) -> None:
        await session.commit()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="broken"))
        _ = await env.producer.seal(conn, root.id)

    assert not await finalizer(env, registry).try_finalize(root.id)
    row = await env.batch(root.id)
    assert row["state"] == BatchState.SEALED
    assert row["hook_attempts"] == 1
    assert "commit()" in row["hook_error"]


async def test_missing_required_hook_defers_finalization(env: Env, registry: HookRegistry) -> None:
    @registry.on_finalized("owned")
    async def present(_session: AsyncSession, _summary: BatchSummary) -> None:
        await asyncio.sleep(0)

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="owned"))
        _ = await env.producer.seal(conn, root.id)

    observer = RecordingObserver()
    subject = Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=HookRegistry(),
        observer=observer,
    )
    assert not await subject.try_finalize(root.id)
    assert (await env.batch(root.id))["state"] == BatchState.SEALED
    assert observer.missing == [(root.id, "owned", "on_finalized")]


async def test_cancel_reason_and_failure_policy_choose_terminal_state(
    env: Env, registry: HookRegistry
) -> None:
    async with env.transaction() as conn:
        cancelled = await env.producer.create_root(conn, RootSpec(kind="cancelled"))
        failed = await env.producer.create_root(
            conn,
            RootSpec(kind="failed", failure_policy=FailurePolicy.fail_fast()),
        )
        _ = await env.producer.add_items(
            conn,
            failed.id,
            [TaskCall(task_name="work", args=(), kwargs={})],
        )
        _ = await env.producer.seal(conn, failed.id)
        item = env.tables.item
        _ = await conn.execute(
            update(item)
            .where(item.c.batch_id == failed.id)
            .values(state=int(ItemState.ERROR), label="error", finished_at=NOW)
        )
        await upsert_slots(
            conn,
            env.tables,
            {(failed.id, 9): CounterDelta(error=1, w_done=1)},
        )
    await set_batch(
        env,
        cancelled.id,
        cancel_requested_at=NOW,
        cancel_reason=CancelReason.CANCEL.value,
    )

    subject = finalizer(env, registry)
    assert await subject.try_finalize(cancelled.id)
    assert await subject.try_finalize(failed.id)
    assert (await env.batch(cancelled.id))["state"] == BatchState.CANCELLED
    assert (await env.batch(failed.id))["state"] == BatchState.FAILED


async def test_empty_stages_finalize_in_order_and_finish_parent_items(
    env: Env, registry: HookRegistry
) -> None:
    order: list[str] = []

    def register(kind: str) -> None:
        @registry.on_finalized(kind)
        async def save(_session: AsyncSession, summary: BatchSummary) -> None:
            await asyncio.sleep(0)
            order.append(summary.key or "root")

    for kind in ("pipe", "pipe.pages", "pipe.cards", "pipe.pdfs"):
        register(kind)

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="pipe"))
        pages = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="pages"))
        cards = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="cards", fed_by=(pages.id,))
        )
        pdfs = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="pdfs", fed_by=(cards.id,))
        )
        _ = await env.producer.seal(conn, pages.id)
        _ = await env.producer.seal(conn, root.id)

    assert await finalizer(env, registry).try_finalize(pages.id)
    assert order == ["pages", "cards", "pdfs", "root"]
    assert (await env.batch(root.id))["state"] == BatchState.SUCCEEDED
    for batch_id in (pages.id, cards.id, pdfs.id):
        assert (await env.batch(batch_id))["state"] == BatchState.SUCCEEDED
    item = env.tables.item
    async with env.connection() as conn:
        states = list(
            await conn.scalars(
                select(item.c.state).where(item.c.batch_id == root.id).order_by(item.c.id)
            )
        )
    assert states == [ItemState.OK, ItemState.OK, ItemState.OK]
    counters = await env.counters(root.id)
    assert (counters.total, counters.ok, counters.pending) == (3, 3, 0)


async def test_child_errors_propagate_to_parent_state_but_virtual_item_is_ok(
    env: Env, registry: HookRegistry
) -> None:
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="tree"))
        child = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(key="send"),
        )
        inserted = await env.producer.add_items(
            conn,
            child.id,
            [TaskCall(task_name="send", args=(), kwargs={})],
        )
        _ = await env.producer.seal(conn, child.id)
        _ = await env.producer.seal(conn, root.id)
        assert inserted.found == 1
        task_id = await conn.scalar(
            select(env.tables.item.c.id).where(env.tables.item.c.batch_id == child.id)
        )
        assert task_id is not None
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == task_id)
            .values(state=int(ItemState.ERROR), label="rejected", finished_at=NOW)
        )
        await upsert_slots(
            conn,
            env.tables,
            {(child.id, 9): CounterDelta(error=1, w_done=1)},
        )

    assert await finalizer(env, registry).try_finalize(child.id)
    assert (await env.batch(child.id))["state"] == BatchState.COMPLETED_WITH_ERRORS
    assert (await env.batch(root.id))["state"] == BatchState.COMPLETED_WITH_ERRORS
    async with env.connection() as conn:
        virtual_state = await conn.scalar(
            select(env.tables.item.c.state).where(env.tables.item.c.child_batch_id == child.id)
        )
    assert virtual_state == ItemState.OK


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            (OnFeederFailed.SEAL, BatchState.SUCCEEDED),
            id="seal",
        ),
        pytest.param(
            (OnFeederFailed.CANCEL, BatchState.CANCELLED),
            id="cancel",
        ),
    ],
)
async def test_completed_with_errors_feeder_honors_failure_behavior(
    env: Env,
    registry: HookRegistry,
    case: tuple[OnFeederFailed, BatchState],
) -> None:
    behavior, expected = case
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="pipeline"))
        source = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(key="source"),
        )
        downstream = await env.producer.create_sub_batch(
            conn,
            root.id,
            SubBatchSpec(
                key="downstream",
                fed_by=(source.id,),
                on_feeder_failed=behavior,
            ),
        )
        inserted = await env.producer.add_items(
            conn,
            source.id,
            [TaskCall(task_name="source", args=(), kwargs={})],
        )
        _ = await env.producer.seal(conn, source.id)
        assert inserted.found == 1
        task_id = await conn.scalar(
            select(env.tables.item.c.id).where(env.tables.item.c.batch_id == source.id)
        )
        assert task_id is not None
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.id == task_id)
            .values(state=int(ItemState.ERROR), label="rejected", finished_at=NOW)
        )
        await upsert_slots(
            conn,
            env.tables,
            {(source.id, 9): CounterDelta(error=1, w_done=1)},
        )

    assert await finalizer(env, registry).try_finalize(source.id)
    assert (await env.batch(source.id))["state"] == BatchState.COMPLETED_WITH_ERRORS
    assert (await env.batch(downstream.id))["state"] == expected


async def test_two_sources_racing_always_close_stage(env: Env, registry: HookRegistry) -> None:
    pairs: list[tuple[UUID, UUID, UUID]] = []
    async with env.transaction() as conn:
        for index in range(100):
            root = await env.producer.create_root(conn, RootSpec(kind=f"race-{index}"))
            left = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="left"))
            right = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="right"))
            stage = await env.producer.create_sub_batch(
                conn,
                root.id,
                SubBatchSpec(key="stage", fed_by=(left.id, right.id)),
            )
            _ = await env.producer.seal(conn, left.id)
            _ = await env.producer.seal(conn, right.id)
            pairs.append((left.id, right.id, stage.id))

    subject = finalizer(env, registry)
    await asyncio.gather(
        *(subject.try_finalize(source) for left, right, _stage in pairs for source in (left, right))
    )
    for _left, _right, stage_id in pairs:
        assert (await env.batch(stage_id))["state"] == BatchState.SUCCEEDED


def _retry_recording_finalizer(env: Env, registry: HookRegistry, retried: list[str]) -> Finalizer:
    return Finalizer(
        tables=env.tables,
        engine=schema_engine(env),
        clock=SystemClock(),
        ids=env.producer.ids,
        hooks=registry,
        settings=FinalizerSettings(retry=RetryPolicy(on_retry=retried.append)),
    )


async def test_finalizer_locks_fed_stage_before_parent_counters(
    env: Env, registry: HookRegistry
) -> None:
    # Fix-4: финализатор источника брал слот счётчика родителя раньше строки этапа,
    # а Completer и финализатор этапа идут в порядке th_batch → th_counter (§9.2).
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="lock-order"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        stage = await env.producer.create_sub_batch(
            conn, root.id, SubBatchSpec(key="stage", fed_by=(source.id,))
        )
        _ = await env.producer.seal(conn, source.id)

    retried: list[str] = []
    subject = _retry_recording_finalizer(env, registry, retried)
    batch = env.tables.batch
    async with env.connection() as watcher, env.transaction() as holder:
        # Как транзакция Completer со spawn в этап: строка этапа под FOR SHARE,
        # слот счётчика корня (tree_total) — последним шагом.
        _ = await holder.execute(
            select(batch.c.id).where(batch.c.id == stage.id).with_for_update(read=True)
        )
        finishing = asyncio.create_task(subject.try_finalize(source.id))
        await wait_blocked_by(watcher, await backend_pid(holder))
        await upsert_slots(
            holder,
            env.tables,
            {(root.id, subject.settings.slot): CounterDelta(tree_total=1)},
        )

    assert await asyncio.wait_for(finishing, timeout=10)
    assert retried == []
    assert (await env.batch(source.id))["state"] == BatchState.SUCCEEDED
    assert (await env.batch(stage.id))["state"] == BatchState.SUCCEEDED
    counters = await env.counters(root.id)
    assert (counters.ok, counters.tree_total) == (2, 1)


async def test_finalizer_locks_batch_and_fed_stages_in_id_order(
    env: Env, registry: HookRegistry
) -> None:
    # Этап старше своего источника (связь добавлена позже через add_feed): обе
    # строки th_batch берутся одним FOR UPDATE по возрастанию id, как в pause/cancel.
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="id-order"))
        stage = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="stage"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        await env.producer.add_feed(conn, stage.id, [source.id])
        _ = await env.producer.seal(conn, source.id)
    assert stage.id < source.id

    retried: list[str] = []
    subject = _retry_recording_finalizer(env, registry, retried)
    batch = env.tables.batch
    async with env.connection() as watcher, env.transaction() as holder:
        _ = await holder.execute(select(batch.c.id).where(batch.c.id == stage.id).with_for_update())
        finishing = asyncio.create_task(subject.try_finalize(source.id))
        await wait_blocked_by(watcher, await backend_pid(holder))
        # Операция над поддеревом идёт дальше по id: строка источника свободна.
        _ = await holder.execute(
            select(batch.c.id).where(batch.c.id == source.id).with_for_update(nowait=True)
        )

    assert await asyncio.wait_for(finishing, timeout=10)
    assert retried == []
    assert (await env.batch(stage.id))["state"] == BatchState.SUCCEEDED


async def test_feed_committed_before_batch_lock_restarts_finalization(
    env: Env, registry: HookRegistry
) -> None:
    # add_feed держит строку источника; финализатор прочитал связи до его commit и
    # ждёт CAS. После commit связь видна только повторному чтению: попытка
    # начинается заново, и новый этап закрывается этой же финализацией.
    calls = 0

    @registry.on_finalized("late-feed.source")
    async def save(_session: AsyncSession, _summary: BatchSummary) -> None:
        nonlocal calls
        await asyncio.sleep(0)
        calls += 1

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="late-feed"))
        source = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="source"))
        stage = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="stage"))
        _ = await env.producer.seal(conn, source.id)

    async with env.connection() as watcher, env.transaction() as holder:
        await env.producer.add_feed(holder, stage.id, [source.id])
        running = asyncio.create_task(finalizer(env, registry).try_finalize(source.id))
        await wait_blocked_by(watcher, await backend_pid(holder))

    assert await asyncio.wait_for(running, timeout=10)
    assert calls == 2
    assert (await env.batch(source.id))["state"] == BatchState.SUCCEEDED
    assert (await env.batch(stage.id))["state"] == BatchState.SUCCEEDED


def _recording_hook(
    registry: HookRegistry, kind: str, *, hold_first: bool = False
) -> tuple[list[tuple[BatchState, CancelReason | None]], asyncio.Event, asyncio.Event]:
    """Хук ``kind``: пишет ``(state, reason)`` каждого вызова; первый вызов можно задержать."""
    seen: list[tuple[BatchState, CancelReason | None]] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    @registry.on_finalized(kind)
    async def save(_session: AsyncSession, summary: BatchSummary) -> None:
        seen.append((summary.state, summary.reason))
        if hold_first and len(seen) == 1:
            entered.set()
            await release.wait()

    return seen, entered, release


async def test_cancel_between_batch_read_and_counters_finalizes_cancelled(
    env: Env, registry: HookRegistry
) -> None:
    # Fix-9: финализация после seal прочитала строку батча (sealed, отмены нет) и
    # ждёт чтения счётчиков; cancel() коммитит флаг и отменяет все Items. Счётчики
    # дают pending = 0 при строке без флага отмены: раньше это давало succeeded.
    # Итог обязан выбираться по строке, прочитанной под блокировкой.
    seen, _entered, _release = _recording_hook(registry, "late-cancel")
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="late-cancel"))
        calls = [TaskCall(task_name="work", args=(index,), kwargs={}) for index in range(4)]
        _ = await env.producer.add_items(conn, root.id, calls)
        _ = await env.producer.seal(conn, root.id)

    counter = f'"{env.schema}"."{env.tables.counter.name}"'
    async with env.connection() as watcher, env.transaction() as holder:
        _ = await holder.execute(text(f"LOCK TABLE {counter} IN ACCESS EXCLUSIVE MODE"))
        running = asyncio.create_task(finalizer(env, registry).try_finalize(root.id))
        await wait_blocked_by(watcher, await backend_pid(holder))
        async with env.transaction() as conn:
            assert await Operations(tables=env.tables, clock=SystemClock()).cancel(conn, root.id)

    assert await asyncio.wait_for(running, timeout=10)
    row = await env.batch(root.id)
    assert row["state"] == BatchState.CANCELLED
    # Первая попытка показала хуку итог по устаревшей строке и откатилась.
    assert seen == [
        (BatchState.SUCCEEDED, None),
        (BatchState.CANCELLED, CancelReason.CANCEL),
    ]
    counters = await env.counters(root.id)
    assert (counters.cancelled, counters.ok, counters.pending) == (4, 0, 0)


@pytest.mark.parametrize(
    "case",
    [
        pytest.param((CancelReason.CANCEL, BatchState.CANCELLED), id="cancel"),
        pytest.param((CancelReason.DEADLINE, BatchState.FAILED), id="deadline"),
        pytest.param((CancelReason.FAIL_FAST, BatchState.FAILED), id="fail_fast"),
        pytest.param((CancelReason.POLICY, BatchState.FAILED), id="policy"),
    ],
)
async def test_cancel_request_while_hook_runs_restarts_finalization(
    env: Env, registry: HookRegistry, case: tuple[CancelReason, BatchState]
) -> None:
    # Запрос отмены (cancel, дедлайн sweeper-а, fail_fast, политика) закоммичен между
    # хуком и CAS: попытка повторяется, хук получает итог, который и будет записан.
    reason, expected = case
    seen, entered, release = _recording_hook(registry, "hook-cancel", hold_first=True)
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="hook-cancel"))
        _ = await env.producer.seal(conn, root.id)

    running = asyncio.create_task(finalizer(env, registry).try_finalize(root.id))
    await entered.wait()
    async with env.transaction() as conn:
        _ = await Operations(tables=env.tables, clock=SystemClock()).cancel(
            conn, root.id, reason=reason
        )
    release.set()

    assert await asyncio.wait_for(running, timeout=10)
    assert seen == [(BatchState.SUCCEEDED, None), (expected, reason)]
    row = await env.batch(root.id)
    assert (row["state"], row["snap_seq"]) == (expected, 1)


async def test_pause_while_hook_runs_does_not_restart_finalization(
    env: Env, registry: HookRegistry
) -> None:
    # Пауза на итог не влияет (§6.1): повторять попытку и хук незачем.
    seen, entered, release = _recording_hook(registry, "hook-pause", hold_first=True)
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="hook-pause"))
        _ = await env.producer.seal(conn, root.id)

    running = asyncio.create_task(finalizer(env, registry).try_finalize(root.id))
    await entered.wait()
    async with env.transaction() as conn:
        await Operations(tables=env.tables, clock=SystemClock()).pause(conn, root.id)
    release.set()

    assert await asyncio.wait_for(running, timeout=10)
    assert seen == [(BatchState.SUCCEEDED, None)]
    row = await env.batch(root.id)
    assert row["state"] == BatchState.SUCCEEDED
    assert row["paused_at"] is not None


async def test_child_retry_while_parent_hook_runs_keeps_parent_open(
    env: Env, registry: HookRegistry
) -> None:
    # retry_failed() потомка вернул виртуальный Item родителя в active, пока шёл хук
    # родителя: под блокировкой pending уже не 0, родитель не финализируется, а
    # изменения хука откатываются вместе с транзакцией.
    probe = await create_probe(env.engine, env.schema)
    entered = asyncio.Event()
    release = asyncio.Event()

    @registry.on_finalized("reopened")
    async def save(session: AsyncSession, _summary: BatchSummary) -> None:
        await insert_id(await session.connection(), probe, 1)
        entered.set()
        await release.wait()

    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind="reopened"))
        child = await env.producer.create_sub_batch(conn, root.id, SubBatchSpec(key="send"))
        _ = await env.producer.add_items(
            conn, child.id, [TaskCall(task_name="send", args=(), kwargs={})]
        )
        _ = await env.producer.seal(conn, child.id)
        _ = await env.producer.seal(conn, root.id)
        _ = await conn.execute(
            update(env.tables.item)
            .where(env.tables.item.c.batch_id == child.id)
            .values(state=int(ItemState.ERROR), label="rejected", finished_at=NOW)
        )
        _ = await conn.execute(
            delete(env.tables.outbox).where(env.tables.outbox.c.batch_id == child.id)
        )
        await upsert_slots(conn, env.tables, {(child.id, 9): CounterDelta(error=1, w_done=1)})

    running = asyncio.create_task(finalizer(env, registry).try_finalize(child.id))
    await entered.wait()
    async with env.transaction() as conn:
        retried = await Operations(tables=env.tables, clock=SystemClock()).retry_failed(
            conn, child.id
        )
    assert retried == 1
    release.set()

    assert await asyncio.wait_for(running, timeout=10)
    assert (await env.batch(child.id))["state"] == BatchState.SEALED
    row = await env.batch(root.id)
    assert (row["state"], row["snap_seq"]) == (BatchState.SEALED, 0)
    assert (await env.counters(root.id)).pending == 1
    assert await committed_ids(env.engine, probe) == []


def test_finalizer_settings_validate_ranges() -> None:
    with pytest.raises(ConfigurationError, match="slot"):
        _ = FinalizerSettings(slot=-1)
    with pytest.raises(ConfigurationError, match="hook_timeout"):
        _ = FinalizerSettings(hook_timeout=timedelta(0))
