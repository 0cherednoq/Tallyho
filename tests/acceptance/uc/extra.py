"""Дополнительные сценарии A-UC-07…20 (ACCEPTANCE §7.2)."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, cast

from sqlalchemy import func, select

from tallyho.model.errors import DownstreamFinalized, SpawnTargetError
from tallyho.model.states import BatchState, CancelReason
from tests.acceptance.app.usecases import HOOK_MISSING_MARK, NEST_LEVELS, UC_KIND, UC_NOHOOK_KIND
from tests.acceptance.chaos.load import Root, audience_for
from tests.acceptance.oracle import PurgedBatch
from tests.acceptance.uc.helpers import (
    TreeOptions,
    domain_status,
    hooks,
    start_tree,
    tree,
    work_calls,
)
from tests.acceptance.uc.mandatory import failing_source

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from tallyho import BatchBuilder
    from tallyho.model.views import BatchView
    from tests.acceptance.uc.context import UcContext

__all__ = [
    "purge_record",
    "uc07",
    "uc08",
    "uc09",
    "uc10",
    "uc11",
    "uc12",
    "uc13",
    "uc14",
    "uc15",
    "uc16",
    "uc17",
    "uc18",
    "uc19",
    "uc20",
]

_TREE_LEASES = """
    SELECT l.item_id FROM th.th_lease l
      JOIN th.th_item i ON i.id = l.item_id
      JOIN th.th_batch b ON b.id = i.batch_id
     WHERE b.root_id = :root
"""
_TREE_PARKED = """
    SELECT count(*) FROM th.th_outbox o
      JOIN th.th_item i ON i.id = o.item_id
      JOIN th.th_batch b ON b.id = i.batch_id
     WHERE b.root_id = :root AND o.available_at = 'infinity'
"""
_TREE_DONE = """
    SELECT count(*) FROM th.th_item i JOIN th.th_batch b ON b.id = i.batch_id
     WHERE b.root_id = :root AND i.state >= 10 AND i.child_batch_id IS NULL
"""
_TREE_FOUND = """
    SELECT count(*) FROM th.th_item i JOIN th.th_batch b ON b.id = i.batch_id
     WHERE b.root_id = :root AND i.child_batch_id IS NULL
"""
_RUN_EFFECTS = "SELECT count(*), min(at) FROM app.uc_effects WHERE run_id = :root"
_DOMAIN_RUN = """
    SELECT status, progress_done, progress_found, pause_reason
      FROM app.uc_runs WHERE batch_id = :root
"""


async def _number(ctx: UcContext, sql: str, key: UUID | int) -> int:
    rows = await ctx.root_items_sql(sql, key)
    return int(cast("int", rows[0][0]) or 0)


def _flatten(view: BatchView) -> list[BatchView]:
    return [view, *(node for child in view.children.values() for node in _flatten(child))]


# ---------------------------------------------------------------------- A-UC-07


async def uc07(ctx: UcContext) -> None:
    """Вложенные под-батчи глубиной 3 из задачи: родитель финализируется после детей."""
    fanout = 3

    async def build(root: BatchBuilder, run: int) -> None:
        th = ctx.app.th
        await root.add_calls(
            [th.call(ctx.app.uc.nest, run, 0, fanout).opts(key=f"n:{i}") for i in range(fanout)]
        )

    # Путь A (итог фиксирует Completer): обход Fix-NEW-complete-in-sub-batch.
    await ctx.set_flag(f"nest_path_a:{ctx.upcoming_run()}", 1)
    _run, batch_id = await start_tree(ctx, build)
    view = await ctx.wait_terminal(batch_id)
    nodes = await tree(ctx, batch_id)
    by_id = {node.id: node for node in nodes}
    early = [
        str(node.id)
        for node in nodes
        if node.parent_id is not None
        and node.finished_at is not None
        and (parent := by_id[node.parent_id]).finished_at is not None
        and parent.finished_at < node.finished_at
    ]
    expected_children = fanout + fanout**2 + fanout**3
    ctx.expect(
        "дерево глубиной 3 создано задачами и финализировано целиком",
        view.state is BatchState.SUCCEEDED and len(nodes) == 1 + expected_children,
        f"state={view.state.name}, батчей={len(nodes)}, ожидалось={1 + expected_children}",
    )
    ctx.expect("родитель финализирован не раньше детей", not early, f"раньше детей: {early[:5]}")

    async def single(root: BatchBuilder, run: int) -> None:
        await root.add_calls([ctx.app.th.call(ctx.app.uc.nest, run, NEST_LEVELS - 1, 1)])

    # Путь B: sub_batch и итог задачи - в транзакции пользователя через complete_in (§3.1).
    _run, probe_id = await start_tree(ctx, single)
    probe = await ctx.wait_terminal(probe_id)
    ctx.expect(
        "sub_batch из задачи, завершённой через complete_in, создаётся",
        probe.state is BatchState.SUCCEEDED and len(probe.children) == 1,
        f"state={probe.state.name}, детей={len(probe.children)}, метки={dict(probe.labels)}",
    )


# ---------------------------------------------------------------------- A-UC-08


async def uc08(ctx: UcContext) -> None:  # ruff: ignore[too-many-locals]  # сценарий - одна последовательность шагов
    """``start_at``, ``reschedule`` и отмена до старта."""
    count = 20
    now = await ctx.db_now()

    async def build(root: BatchBuilder, run: int) -> None:
        await root.add_calls(work_calls(ctx, run, range(1, count + 1)))

    run, delayed = await start_tree(ctx, build, TreeOptions(start_at=now + timedelta(seconds=40)))
    _ = run
    await asyncio.sleep(10)
    before = await _number(ctx, _RUN_EFFECTS, run)
    leases = len(await ctx.root_items_sql(_TREE_LEASES, delayed))
    ctx.expect(
        "до start_at ни одного выполнения",
        before == 0 and leases == 0,
        f"эффектов={before}, lease={leases}",
    )
    moved_to = await ctx.db_now() + timedelta(seconds=10)
    async with ctx.transaction() as session:
        dispatched = await ctx.app.th.handle(delayed).reschedule(moved_to, session=session)
    early = 0
    while await ctx.db_now() < moved_to - timedelta(seconds=0.5):
        early += await _number(ctx, _RUN_EFFECTS, run)
        early += len(await ctx.root_items_sql(_TREE_LEASES, delayed))
        await asyncio.sleep(0.5)
    view = await ctx.wait_terminal(delayed)
    rows = await ctx.root_items_sql(_RUN_EFFECTS, run)
    first = cast("datetime | None", rows[0][1])
    ctx.expect(
        "после переноса - старт в новое время",
        dispatched == 0
        and early == 0
        and view.state is BatchState.SUCCEEDED
        and first is not None
        and first >= moved_to,
        (
            f"уже отправлено={dispatched}, выполнений до переноса={early}, ",
            f"первый эффект={first}, новое время={moved_to}, state={view.state.name}",
        ),
    )
    far = await ctx.db_now() + timedelta(minutes=10)
    run, never = await start_tree(ctx, build, TreeOptions(start_at=far))
    await asyncio.sleep(2)
    await ctx.app.th.handle(never).cancel()
    cancelled = await ctx.wait_terminal(never, timeout_seconds=120)
    finals = await hooks(ctx, never, "on_finalized")
    executed = await _number(ctx, _RUN_EFFECTS, run)
    ctx.expect(
        "отмена до старта: on_finalized(cancelled) один раз и 0 выполнений",
        cancelled.state is BatchState.CANCELLED
        and [state for state, _ in finals] == [int(BatchState.CANCELLED)]
        and executed == 0
        and cancelled.progress.cancelled == count,
        (
            f"state={cancelled.state.name}, хуки={finals}, выполнений={executed}, ",
            f"cancelled={cancelled.progress.cancelled}",
        ),
    )


# ---------------------------------------------------------------------- A-UC-09


async def _pause_resume(ctx: UcContext, name: str, root_id: UUID) -> None:
    async def far_enough() -> bool:
        found = await _number(ctx, _TREE_FOUND, root_id)
        return found > 0 and await _number(ctx, _TREE_DONE, root_id) >= 0.15 * found

    _ = await ctx.wait_for(far_enough, 600, what=f"{name}: 15% выполнено")
    handle = ctx.app.th.handle(root_id)
    await handle.pause()
    paused_at = time.monotonic()
    await asyncio.sleep(1.0)
    running = {row[0] for row in await ctx.root_items_sql(_TREE_LEASES, root_id)}
    late: set[object] = set()
    while time.monotonic() - paused_at < 16:
        current = {row[0] for row in await ctx.root_items_sql(_TREE_LEASES, root_id)}
        late |= current - running
        await asyncio.sleep(0.5)
    done_before = await _number(ctx, _TREE_DONE, root_id)
    await asyncio.sleep(4)
    done_after = await _number(ctx, _TREE_DONE, root_id)
    leases = len(await ctx.root_items_sql(_TREE_LEASES, root_id))
    parked = await _number(ctx, _TREE_PARKED, root_id)
    ctx.expect(
        f"{name}: после pause новые выполнения прекратились ≤ 1 с, присланное запарковано",
        not late and done_before == done_after and leases == 0 and parked > 0,
        (
            f"новых lease через 1 с={len(late)}, выполнявшихся={len(running)}, ",
            f"done {done_before}->{done_after}, lease={leases}, parked={parked}",
        ),
    )
    await handle.resume()
    view = await ctx.wait_terminal(root_id, timeout_seconds=900)
    ctx.expect(
        f"{name}: после resume всё доделано",
        view.state.is_terminal and view.state is not BatchState.CANCELLED,
        f"state={view.state.name}",
    )


async def uc09(ctx: UcContext) -> None:
    """Пауза и продолжение под нагрузкой на S1, S2 и S3 одновременно."""
    size = ctx.config.volume(20_000)
    s1 = await ctx.app.start_s1(range(1, size + 1))
    ctx.roots.append(Root(s1, "S1", 0, size=size))
    audience = ctx.config.volume(20_000)
    page = max(10, audience // 20)
    addresses = audience_for(ctx.config.seed, 1, audience)
    s2 = await ctx.app.start_s2(1, addresses, page=page)
    ctx.roots.append(Root(s2, "S2", 0, addresses, page=page))
    s3 = await ctx.app.start_s3(1, pages=ctx.config.pages)
    ctx.roots.append(Root(s3, "S3", 0))
    _ = await asyncio.gather(
        _pause_resume(ctx, "S1", s1), _pause_resume(ctx, "S2", s2), _pause_resume(ctx, "S3", s3)
    )


# ---------------------------------------------------------------------- A-UC-10


async def _policy_fail_order(ctx: UcContext) -> None:
    """Политика ``action="fail"`` (``fail_fast`` - её частный случай) проваливает дерево.

    Дерево обязано финализироваться снизу вверх (I-09): корень - после детей, этап - после
    своего источника.
    """
    fail_id, _view = await failing_source(ctx, "seal", policy=ctx.app.th.FailurePolicy.fail_fast())
    by_key = {node.key: node for node in await tree(ctx, fail_id)}
    root = next(node for node in by_key.values() if node.parent_id is None)
    src, dst = by_key["src"], by_key["dst"]
    ordered = (
        root.finished_at is not None
        and src.finished_at is not None
        and dst.finished_at is not None
        and src.finished_at <= dst.finished_at <= root.finished_at
    )
    ctx.expect(
        "провал дерева политикой: финализация снизу вверх",
        ordered,
        f"src@{src.finished_at}, dst@{dst.finished_at}, корень@{root.finished_at}",
    )


async def uc10(ctx: UcContext) -> None:
    """Авто-пауза по политике: 8% ``hard_bounce`` ставят на паузу всё дерево."""
    count = ctx.config.volume(4_000, floor=200)
    minimum = 50
    policy = ctx.app.th.FailurePolicy.threshold(
        ratio=0.05, min_processed=minimum, labels=["hard_bounce"], action="pause"
    )

    async def build(root: BatchBuilder, run: int) -> None:
        send = root.sub_batch("send", failure_policy=policy)
        other = root.sub_batch("other")
        await send.add_calls(work_calls(ctx, run, range(1, count + 1)))
        await other.add_calls(work_calls(ctx, run, range(count + 1, 2 * count + 1)))

    run = ctx.upcoming_run()
    await ctx.plan(run, {n: "error:hard_bounce" for n in range(1, count + 1) if n % 12 == 0})
    _run, batch_id = await start_tree(ctx, build)

    async def breached() -> bool:
        rows = await ctx.root_items_sql(_DOMAIN_RUN, batch_id)
        return bool(rows and rows[0][3])

    _ = await ctx.wait_for(breached, 600, what="on_policy_breach записал причину")
    view = await ctx.app.th.handle(batch_id).view()
    reason = (await ctx.root_items_sql(_DOMAIN_RUN, batch_id))[0][3]
    nodes = _flatten(view)
    processed = view.children["send"].progress
    ctx.expect(
        "пауза всего дерева после min_processed, причина записана хуком",
        all(node.paused_at is not None for node in nodes)
        and processed.ok + processed.error + processed.skip >= minimum
        and str(reason).startswith("pause:hard_bounce"),
        (
            f"на паузе={sum(n.paused_at is not None for n in nodes)}/{len(nodes)}, ",
            f"обработано send={processed.ok + processed.error}, причина={reason}",
        ),
    )
    await ctx.app.th.handle(batch_id).cancel()
    final = await ctx.wait_terminal(batch_id)
    ctx.expect("дерево на паузе отменяется", final.state is BatchState.CANCELLED, final.state.name)
    await _policy_fail_order(ctx)


# ---------------------------------------------------------------------- A-UC-11


async def uc11(ctx: UcContext) -> None:
    """Отмена посреди работы S1: остаток cancelled, выполнявшиеся доделаны, хук один раз."""
    size = ctx.config.volume(20_000)
    batch_id = await ctx.app.start_s1(range(1, size + 1))
    ctx.roots.append(Root(batch_id, "S1", 0, size=size))
    handle = ctx.app.th.handle(batch_id)

    async def third() -> bool:
        return (await handle.view()).progress.done >= 0.3 * size

    _ = await ctx.wait_for(third, 600, what="30% выполнено")
    at_cancel = (await handle.view()).progress
    await handle.cancel()
    view = await ctx.wait_terminal(batch_id)
    progress = view.progress
    finals = await hooks(ctx, batch_id, "on_finalized")
    ctx.expect(
        "остаток cancelled, выполнявшиеся доделаны, on_finalized(cancelled) один раз",
        view.state is BatchState.CANCELLED
        and progress.cancelled > 0
        and progress.ok + progress.error + progress.skip + progress.cancelled == size
        and progress.ok >= at_cancel.ok
        and [state for state, _ in finals] == [int(BatchState.CANCELLED)],
        (
            f"при отмене ok={at_cancel.ok}; итог ok={progress.ok} error={progress.error} ",
            f"cancelled={progress.cancelled}; хуки={finals}",
        ),
    )


# ---------------------------------------------------------------------- A-UC-12


async def uc12(ctx: UcContext) -> None:
    """Дедлайн: по истечении ``failed(reason=deadline)``, остаток отменён."""
    count = ctx.config.volume(6_000, floor=300)

    async def build(root: BatchBuilder, run: int) -> None:
        await root.add_calls(work_calls(ctx, run, range(1, count + 1)))

    _run, batch_id = await start_tree(
        ctx, build, TreeOptions(deadline=timedelta(seconds=20), max_in_flight=20)
    )
    view = await ctx.wait_terminal(batch_id, timeout_seconds=300)
    status = (await ctx.root_items_sql(_DOMAIN_RUN, batch_id))[0][0]
    ctx.expect(
        "по дедлайну failed(reason=deadline), остаток отменён",
        view.state is BatchState.FAILED
        and view.reason is CancelReason.DEADLINE
        and view.progress.cancelled > 0
        and status == domain_status(view),
        (
            f"state={view.state.name}, reason={view.reason}, ok={view.progress.ok}, ",
            f"cancelled={view.progress.cancelled}, домен={status}",
        ),
    )
    await _deadline_tree(ctx, count)


async def _deadline_tree(ctx: UcContext, count: int) -> None:
    """Дедлайн корня конвейера: поддерево отменено и финализировано снизу вверх."""
    run = ctx.upcoming_run()
    await ctx.plan(run, dict.fromkeys(range(1, count + 1), "feed:2"))

    async def build(root: BatchBuilder, run_id: int) -> None:
        src = root.sub_batch("src", max_in_flight=20)
        _ = root.sub_batch("dst", fed_by=[src])
        await src.add_calls(work_calls(ctx, run_id, range(1, count + 1)))

    _run, batch_id = await start_tree(ctx, build, TreeOptions(deadline=timedelta(seconds=20)))
    started = time.monotonic()
    view = await ctx.wait_terminal(batch_id, timeout_seconds=900)
    elapsed = round(time.monotonic() - started)
    ctx.stats["uc12.tree_terminal_seconds"] = elapsed
    limit = 20 + ctx.stand.settings.recovery.total_seconds()
    ctx.expect(
        "дерево с дедлайном терминально не позже T_rec после дедлайна",
        elapsed <= limit,
        f"терминально через {elapsed} с, предел {limit:.0f} с",
    )
    by_key = {node.key: node for node in await tree(ctx, batch_id)}
    src, dst = by_key["src"], by_key["dst"]
    root = by_key[f"uc:{run}"]
    ctx.expect(
        "дедлайн конвейера: failed(deadline), финализация снизу вверх",
        view.state is BatchState.FAILED
        and view.reason is CancelReason.DEADLINE
        and src.finished_at is not None
        and dst.finished_at is not None
        and root.finished_at is not None
        and src.finished_at <= dst.finished_at <= root.finished_at,
        (
            f"корень={view.state.name}/{view.reason}, src@{src.finished_at}, ",
            f"dst@{dst.finished_at}, корень@{root.finished_at}",
        ),
    )


# ---------------------------------------------------------------------- A-UC-13


async def uc13(ctx: UcContext) -> None:  # ruff: ignore[too-many-locals]  # сценарий - одна последовательность шагов
    """``retry_failed`` батча и корня конвейера; ``DownstreamFinalized`` у источника."""
    count = ctx.config.volume(2_000, floor=100)
    run = ctx.upcoming_run()
    failing = {n: "error:boom" for n in range(1, count + 1) if n % 10 == 0}
    await ctx.plan(run, failing)

    async def flat(root: BatchBuilder, run_id: int) -> None:
        await root.add_calls(work_calls(ctx, run_id, range(1, count + 1)))

    _run, batch_id = await start_tree(ctx, flat)
    first = await ctx.wait_terminal(batch_id)
    await ctx.plan(run, dict.fromkeys(failing, "ok"))
    retried = await ctx.app.th.handle(batch_id).retry_failed()
    ctx.retry_failed[batch_id] += 1
    second = await ctx.wait_terminal(batch_id)
    finals = await hooks(ctx, batch_id, "on_finalized")
    ctx.expect(
        "retry_failed перезапускает только упавшие, on_finalized - снова с новым итогом",
        first.state is BatchState.COMPLETED_WITH_ERRORS
        and retried == len(failing)
        and second.state is BatchState.SUCCEEDED
        and second.progress.ok == count
        and [state for state, _ in finals]
        == [int(BatchState.COMPLETED_WITH_ERRORS), int(BatchState.SUCCEEDED)],
        (
            f"первый={first.state.name} error={first.progress.error}, повторено={retried}, ",
            f"второй={second.state.name} ok={second.progress.ok}, хуки={finals}",
        ),
    )
    feeders = ctx.config.volume(400, floor=20)
    pipeline_run = ctx.upcoming_run()
    broken = {n: "error:boom" for n in range(1, feeders + 1) if n % 5 == 0}
    await ctx.plan(pipeline_run, dict.fromkeys(range(1, feeders + 1), "feed:2") | broken)

    async def pipeline(root: BatchBuilder, run_id: int) -> None:
        src = root.sub_batch("src")
        _ = root.sub_batch("dst", fed_by=[src])
        await src.add_calls(work_calls(ctx, run_id, range(1, feeders + 1)))

    _run, pipeline_id = await start_tree(ctx, pipeline)
    done = await ctx.wait_terminal(pipeline_id)
    src_handle = await ctx.app.th.handle(pipeline_id).child("src")
    raised = ""
    try:
        _ = await src_handle.retry_failed()
    except DownstreamFinalized as exc:
        raised = type(exc).__name__
    ctx.expect(
        "retry_failed источника с финализированным получателем - DownstreamFinalized",
        raised == "DownstreamFinalized",
        f"исключение={raised or 'нет'}, src={done.children['src'].state.name}",
    )
    await ctx.plan(pipeline_run, dict.fromkeys(broken, "feed:2"))
    again = await ctx.app.th.handle(pipeline_id).retry_failed()
    for node in (pipeline_id, done.children["src"].id, done.children["dst"].id):
        ctx.retry_failed[node] += 1
    final = await ctx.wait_terminal(pipeline_id)
    ctx.expect(
        "retry_failed корня переоткрывает конвейер, новые Items доходят до получателя",
        again == len(broken)
        and final.state is BatchState.SUCCEEDED
        and final.children["dst"].progress.found == 2 * feeders,
        (
            f"повторено={again}, итог={final.state.name}, ",
            f"dst.found={final.children['dst'].progress.found}",
        ),
    )


# ---------------------------------------------------------------------- A-UC-14


async def purge_record(
    ctx: UcContext, view: BatchView, *, release_required: bool, released: bool, retention: float
) -> PurgedBatch:
    rows = await ctx.root_items_sql(_DOMAIN_RUN, view.id)
    intact = bool(rows) and (int(cast("int", rows[0][1])), int(cast("int", rows[0][2]))) == (
        view.progress.done,
        view.progress.found,
    )
    now = await ctx.db_now()
    elapsed = view.finished_at is not None and (now - view.finished_at).total_seconds() >= retention
    return PurgedBatch(
        batch_id=view.id,
        release_required=release_required,
        released=released,
        retention_elapsed=elapsed,
        domain_intact=intact,
        raised_batch_purged=await ctx.is_purged(view.id),
    )


async def uc14(ctx: UcContext) -> None:
    """Retention: без ``release_required`` - по TTL, с ним - только после ``release()``."""
    retention = 5.0

    async def build(root: BatchBuilder, run: int) -> None:
        await root.add_calls(work_calls(ctx, run, range(1, 21)))

    _a, plain = await start_tree(ctx, build, TreeOptions(retention=timedelta(seconds=retention)))
    _b, held = await start_tree(
        ctx,
        build,
        TreeOptions(retention=timedelta(seconds=retention), release_required=True),
    )
    plain_view = await ctx.wait_terminal(plain)
    held_view = await ctx.wait_terminal(held)
    ctx.frozen |= {plain: plain_view, held: held_view}

    async def plain_purged() -> bool:
        return await ctx.is_purged(plain)

    waited = await ctx.wait_for(plain_purged, 120, what="удаление батча без release_required")
    record = await purge_record(
        ctx, plain_view, release_required=False, released=False, retention=retention
    )
    ctx.purged.append(record)
    await asyncio.sleep(2 * ctx.config.sweep_interval)
    held_alive = not await ctx.is_purged(held)
    ctx.expect(
        "без release_required дерево удалено по TTL, с ним - ждёт release()",
        record.raised_batch_purged and held_alive,
        f"удалён через {waited} с, release_required жив={held_alive}",
    )
    async with ctx.transaction() as session:
        await ctx.app.th.handle(held).release(session=session)

    async def held_purged() -> bool:
        return await ctx.is_purged(held)

    _ = await ctx.wait_for(held_purged, 120, what="удаление после release()")
    ctx.purged.append(
        await purge_record(
            ctx, held_view, release_required=True, released=True, retention=retention
        )
    )


# ---------------------------------------------------------------------- A-UC-15


async def uc15(ctx: UcContext) -> None:
    """Лимиты: цикл упирается в ``max_depth``, ``max_items`` мягкий, ``skipped_by_limit`` точный."""
    depth = 3
    run = ctx.upcoming_run()
    await ctx.set_flag(f"crawl_depth:{run}", depth)

    async def chain(root: BatchBuilder, run_id: int) -> None:
        stage = root.sub_batch("chain", max_depth=depth)
        await stage.add_calls([ctx.app.th.call(ctx.app.uc.crawl, run_id, 0).opts(key="c:0")])

    _run, chain_id = await start_tree(ctx, chain)
    view = (await ctx.wait_terminal(chain_id)).children["chain"].progress
    ctx.truth[chain_id] = {
        "chain.found": (view.found, depth + 1),
        "chain.duplicates": (view.duplicates, depth),
        "chain.skipped_by_limit": (view.skipped_by_limit, 1),
    }
    ctx.expect(
        "циклическая цепочка упирается в max_depth",
        (view.found, view.duplicates, view.skipped_by_limit) == (depth + 1, depth, 1),
        f"found={view.found}, duplicates={view.duplicates}, skipped={view.skipped_by_limit}",
    )
    feeders, fanout, limit = 40, 9, 150
    attempts = feeders * (fanout + 1)
    run = ctx.upcoming_run()
    await ctx.plan(run, dict.fromkeys(range(1, feeders + 1), f"spawn:{fanout}"))

    async def fan(root: BatchBuilder, run_id: int) -> None:
        stage = root.sub_batch("fan")
        await stage.add_calls(work_calls(ctx, run_id, range(1, feeders + 1)))

    _run, fan_id = await start_tree(ctx, fan, TreeOptions(max_items=limit))
    progress = (await ctx.wait_terminal(fan_id)).children["fan"].progress
    slack = 500 * 4
    ctx.truth[fan_id] = {
        "fan.found+skipped": (progress.found + progress.skipped_by_limit, attempts),
        "fan.duplicates": (progress.duplicates, 0),
    }
    ctx.expect(
        "max_items превышен не больше чем на flush на процесс, skipped_by_limit сходится",
        limit - 2 <= progress.found <= limit + slack
        and progress.found + progress.skipped_by_limit == attempts,
        (
            f"found={progress.found}, skipped={progress.skipped_by_limit}, попыток={attempts}, ",
            f"max_items={limit}",
        ),
    )


# ---------------------------------------------------------------------- A-UC-16


async def uc16(ctx: UcContext) -> None:
    """Идемпотентность ``batch(kind, key)``, ``sub_batch(key)`` и spawn с тем же ключом."""
    th = ctx.app.th

    async def build(root: BatchBuilder, run: int) -> None:
        await root.add_calls(
            [
                th.call(ctx.app.uc.probe, run, 1, "shared_stage").opts(key="p:1"),
                th.call(ctx.app.uc.probe, run, 2, "shared_stage").opts(key="p:2"),
                th.call(ctx.app.uc.probe, run, 3, "duplicate_spawn").opts(key="p:3"),
            ]
        )

    run, batch_id = await start_tree(ctx, build)
    async with (
        ctx.transaction() as session,
        th.batch(UC_KIND, key=f"uc:{run}", session=session) as again,
    ):
        pass
    view = await ctx.wait_terminal(batch_id)
    shared = view.children.get("shared")
    ctx.expect(
        "повторный batch(kind, key) возвращает тот же батч",
        again.handle.id == batch_id,
        f"{again.handle.id} vs {batch_id}",
    )
    ctx.expect(
        "повторный sub_batch(key) - тот же этап",
        list(view.children) == ["shared"] and shared is not None and shared.progress.found == 2,
        (
            f"дети={list(view.children)}, ",
            f"shared.found={None if shared is None else shared.progress.found}",
        ),
    )
    ctx.expect(
        "spawn с тем же key - дубль, а не новый Item",
        view.progress.duplicates == 2 and view.state is BatchState.SUCCEEDED,
        (
            f"duplicates={view.progress.duplicates}, found={view.progress.found}, ",
            f"state={view.state.name}",
        ),
    )


# ---------------------------------------------------------------------- A-UC-17


async def uc17(ctx: UcContext) -> None:
    """Хук не импортирован у воркеров: финализирует maintenance, воркер пишет th_hook_missing."""

    async def build(root: BatchBuilder, run: int) -> None:
        await root.add_calls(work_calls(ctx, run, range(1, 31)))

    _run, batch_id = await start_tree(ctx, build, TreeOptions(kind=UC_NOHOOK_KIND))
    view = await ctx.wait_terminal(batch_id)
    finals = await hooks(ctx, batch_id, "on_finalized")
    logs = await ctx.stand.logs("worker-1", "worker-2", "worker-3", "worker-4")
    missing = [
        line for line in logs.splitlines() if HOOK_MISSING_MARK in line and str(batch_id) in line
    ]
    ctx.expect(
        "воркер без хука не финализирует батч и пишет th_hook_missing",
        bool(missing),
        f"строк th_hook_missing по батчу={len(missing)}",
    )
    ctx.expect(
        "финализирует maintenance, в котором хук есть",
        view.state is BatchState.SUCCEEDED and len(finals) == 1 and finals[0][1].startswith("api-"),
        f"state={view.state.name}, хуки={finals}",
    )


# ---------------------------------------------------------------------- A-UC-18


async def uc18(ctx: UcContext) -> None:
    """Правило записи в этап: ``SpawnTargetError`` у задачи и у продюсера."""
    th = ctx.app.th

    async def build(root: BatchBuilder, run: int) -> None:
        src = root.sub_batch("src")
        feeder = root.sub_batch("feeder")
        _ = root.sub_batch("sink", fed_by=[feeder])
        await src.add_calls([th.call(ctx.app.uc.probe, run, 1, "wrong_target").opts(key="p:1")])
        await feeder.add_calls(work_calls(ctx, run, [2]))

    run, batch_id = await start_tree(ctx, build)
    _ = await ctx.wait_terminal(batch_id)
    rows = await ctx.root_items_sql(
        "SELECT event, detail FROM app.uc_events WHERE run_id = :root ORDER BY id",
        run,
    )
    events = [(str(event), str(detail)) for event, detail in rows]
    ctx.expect(
        "spawn into= в этап, для которого батч не источник, - SpawnTargetError",
        events
        == [
            ("spawn_target_error", "sink:SpawnTargetError"),
            ("spawn_target_error", "missing:SpawnTargetError"),
        ],
        f"события={events}",
    )
    runs = ctx.app.domain.uc_runs
    upcoming = ctx.upcoming_run()
    raised = await _producer_into_stage(ctx)
    leftovers = await ctx.count(select(func.count()).select_from(runs).where(runs.c.id == upcoming))
    ctx.expect(
        "продюсер не может add в этап с fed_by",
        raised == "SpawnTargetError" and leftovers == 0,
        f"исключение={raised or 'нет'}, строк домена после отката={leftovers}",
    )


async def _producer_into_stage(ctx: UcContext) -> str:
    """Продюсер добавляет в этап с ``fed_by``; имя исключения или пустая строка."""
    th = ctx.app.th

    async def build(root: BatchBuilder, run: int) -> None:
        first = root.sub_batch("a")
        second = root.sub_batch("b", fed_by=[first])
        await first.add_calls(work_calls(ctx, run, [1]))
        await second.add_calls(work_calls(ctx, run, [2]))

    try:
        async with ctx.transaction() as session:
            producer_run = await ctx.new_run(session, UC_KIND)
            async with th.batch(UC_KIND, key=f"uc:{producer_run}", session=session) as root:
                await build(root, producer_run)
    except SpawnTargetError as exc:
        return type(exc).__name__
    return ""


# ---------------------------------------------------------------------- A-UC-19


async def uc19(ctx: UcContext) -> None:
    """Прогресс: оценка по ``min(20, 5%)``, ETA при известном expected, доля не откатывается."""
    feeders = ctx.config.volume(800, floor=40)
    run = ctx.upcoming_run()
    await ctx.plan(run, dict.fromkeys(range(1, feeders + 1), "feed:5"))

    async def build(root: BatchBuilder, run_id: int) -> None:
        src = root.sub_batch("src", max_in_flight=16)
        _ = root.sub_batch("dst", fed_by=[src])
        await src.add_calls(work_calls(ctx, run_id, range(1, feeders + 1)))

    _run, batch_id = await start_tree(ctx, build)
    early: list[str] = []
    estimates = 0
    etas = 0
    ratios: list[int] = []
    async for view in ctx.app.th.handle(batch_id).watch():
        src, dst = view.children["src"].progress, view.children["dst"]
        if dst.state is BatchState.OPEN and src.expected:
            threshold = min(20, 0.05 * src.expected)
            if dst.progress.expected is not None:
                estimates += 1
                if (dst.progress.estimate_basis or 0) < threshold:
                    early.append(f"{dst.progress.estimate_basis}<{threshold}")
        etas += sum(
            1
            for node in _flatten(view)
            if node.progress.expected is not None and node.progress.done and node.progress.eta
        )
        rows = await ctx.root_items_sql(
            "SELECT progress_ratio FROM app.uc_runs WHERE batch_id = :root", batch_id
        )
        ratios.append(int(cast("int", rows[0][0])))
        if view.state.is_terminal:
            break
    ctx.expect(
        "оценка появляется по правилу min(20, 5%)",
        estimates > 0 and not early,
        f"снимков с оценкой={estimates}, раньше правила={early[:3]}",
    )
    snapshot_etas = await _number(
        ctx, "SELECT snapshots_with_eta FROM app.uc_runs WHERE batch_id = :root", batch_id
    )
    ctx.expect(
        "ETA есть в снимках on_progress при известном expected",
        snapshot_etas > 0,
        f"снимков on_progress с ETA={snapshot_etas}",
    )
    # ARCHITECTURE §9.4: ETA «считается в Snapshotter и в watch()».
    ctx.expect("ETA есть в watch() при известном expected", etas > 0, f"снимков с ETA={etas}")
    drops = [(a, b) for a, b in pairwise(ratios) if b < a]
    ctx.expect(
        "доля в домене не откатывается (GREATEST)",
        not drops and ratios[-1] > 0,
        f"падений={drops[:3]}, последняя доля={ratios[-1] if ratios else None}",
    )


# ---------------------------------------------------------------------- A-UC-20


async def uc20(ctx: UcContext) -> None:
    """``watch()``: обновления не чаще ``watch_throttle``, финальное состояние не пропущено."""
    count = ctx.config.volume(20_000, floor=500)
    run = ctx.upcoming_run()
    await ctx.plan(run, dict.fromkeys(range(1, count + 1), "fast"))
    throttle = 0.010  # watch_throttle стенда (build_app)

    async def build(root: BatchBuilder, run_id: int) -> None:
        await root.add_calls(work_calls(ctx, run_id, range(1, count + 1)))

    started = time.monotonic()
    _run, batch_id = await start_tree(ctx, build)
    stamps: list[float] = []
    last: BatchView | None = None
    async for view in ctx.app.th.handle(batch_id).watch():
        stamps.append(time.monotonic())
        last = view
    elapsed = time.monotonic() - started
    gaps = [b - a for a, b in pairwise(stamps)]
    fastest = min(gaps) if gaps else 0.0
    rate = round(count / max(elapsed, 1e-6))
    ctx.stats["uc20.updates"] = len(stamps)
    ctx.stats["uc20.items_per_second"] = rate
    ctx.expect(
        "подписчик получает обновления не чаще watch_throttle и видит финал",
        last is not None and last.state.is_terminal and fastest >= 0.5 * throttle,
        (
            f"обновлений={len(stamps)}, минимальный интервал={fastest * 1000:.1f} мс, ",
            f"изменений/с≈{rate}, финал={None if last is None else last.state.name}",
        ),
    )
