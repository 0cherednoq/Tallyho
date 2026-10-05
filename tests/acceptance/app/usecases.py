"""Задачи и хуки сценариев A-UC (ACCEPTANCE §7) поверх того же графа, что S1/S2/S3.

Поведение задач задаётся доменом, а не аргументами: ``uc_plan`` назначает режим каждому
логическому Item сценария, ``uc_flags`` - переключатели, которые сценарий меняет между
фазами (например, «почтовый ящик снова доступен» перед ``retry_failed``). Так один и тот же
код задачи выполняет все сценарии, а воркеры стенда не перезапускаются.

Каждая задача пишет доменный эффект (``uc_effects`` или ``uc_delivery``) в одной транзакции
со своим завершением через ``item.complete_in`` - как S1/S2/S3 (ACCEPTANCE §3.1).
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing_extensions import override

from tallyho import item
from tallyho.model.errors import SpawnTargetError
from tallyho.model.states import BatchState, ItemState
from tallyho.protocols.observer import NullObserver
from tests.acceptance.app.common import TransientError, network

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

    from tallyho import Tallyho
    from tallyho.adapters.flexiq import FlexiqAdapter
    from tallyho.model.policy import PolicyBreach
    from tallyho.model.views import BatchSummary
    from tests.acceptance.app.domain import DomainTables

__all__ = [
    "HOOK_MISSING_MARK",
    "NEST_LEVELS",
    "UC_EXPORT_KIND",
    "UC_KIND",
    "UC_NOHOOK_KIND",
    "HookMissingPrinter",
    "UcTasks",
    "register_usecases",
]

UC_KIND = "acceptance.uc"
UC_NOHOOK_KIND = "acceptance.uc.nohook"
"""Хуки этого вида есть у API-процессов и у процесса нагрузки, но не у воркеров (A-UC-17)."""
UC_EXPORT_KIND = "acceptance.uc.export"
"""Кампания со строкой на каждого получателя (ARCHITECTURE §12.9, A-UC-21/22)."""
NEST_LEVELS = 3
"""Глубина вложенных под-батчей A-UC-07."""
HOOK_MISSING_MARK = "th_hook_missing"

_SETTLE_DELAYS = [12.0] * 10
"""Пауза между попытками экспорта: окно, в котором дерево без release() обязано уцелеть."""


class HookMissingPrinter(NullObserver):
    """Метрика ``th_hook_missing`` стенда: строка в stdout процесса (журнал контейнера)."""

    @override
    def hook_missing(self, *, batch_id: UUID, kind: str, hook: str) -> None:
        _ = sys.stdout.write(f"{HOOK_MISSING_MARK} batch={batch_id} kind={kind} hook={hook}\n")
        sys.stdout.flush()


@dataclass(frozen=True, slots=True)
class UcTasks:
    """Задачи сценариев A-UC, зарегистрированные одинаково во всех процессах стенда."""

    work: Callable[[int, int], Awaitable[None]]
    nest: Callable[[int, int, int], Awaitable[None]]
    crawl: Callable[[int, int], Awaitable[None]]
    probe: Callable[[int, int, str], Awaitable[None]]
    deliver: Callable[[int, str], Awaitable[None]]
    settle: Callable[[int], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _Env:
    th: Tallyho
    engine: AsyncEngine
    domain: DomainTables
    seed: int
    network_scale: float


async def _mode(env: _Env, run_id: int, n: int) -> str:
    plan = env.domain.uc_plan
    async with env.engine.connect() as connection:
        value = await connection.scalar(
            select(plan.c.mode).where(plan.c.run_id == run_id, plan.c.n == n)
        )
    return "ok" if value is None else str(value)


async def _flag(env: _Env, name: str) -> int:
    flags = env.domain.uc_flags
    async with env.engine.connect() as connection:
        value = await connection.scalar(select(flags.c.value).where(flags.c.name == name))
    return 0 if value is None else int(cast("int", value))


async def _event(
    connection: AsyncConnection, env: _Env, run_id: int, *, event: str, detail: str
) -> None:
    _ = await connection.execute(
        pg_insert(env.domain.uc_events).values(run_id=run_id, event=event, detail=detail)
    )


async def _effect(
    connection: AsyncConnection, env: _Env, *, run_id: int, n: int, task: str
) -> None:
    item_id = _item_id()
    statement = pg_insert(env.domain.uc_effects).values(
        item_id=item_id,
        run_id=run_id,
        n=n,
        task=task,
        started_at=func.now(),
    )
    _ = await connection.execute(statement.on_conflict_do_nothing())


def _item_id() -> UUID:
    value = item.id()
    if value is None:
        message = "задача сценария A-UC выполнена вне контекста Item"
        raise TransientError(message)
    return value


async def _sleep_network(env: _Env, namespace: str) -> None:
    _ = await network(env.seed, _item_id(), namespace=namespace, scale=env.network_scale)


def register_usecases(  # ruff: ignore[complex-structure, too-many-statements, too-many-arguments]  # один реестр задач и хуков для всех процессов стенда
    th: Tallyho,
    adapter: FlexiqAdapter,
    *,
    engine: AsyncEngine,
    domain: DomainTables,
    seed: int,
    network_scale: float,
    job_timeout: int,
    worker: bool,
) -> UcTasks:
    """Зарегистрировать задачи и хуки A-UC.

    ``worker=True`` - процесс воркера: хуки вида :data:`UC_NOHOOK_KIND` не регистрируются,
    чтобы финализацию таких батчей выполнял maintenance (ARCHITECTURE §7.5, A-UC-17).
    """
    env = _Env(th, engine, domain, seed, network_scale)
    delivery = domain.uc_delivery
    runs = domain.uc_runs
    flags = domain.uc_flags

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def uc_work(run_id: int, n: int) -> None:
        mode = await _mode(env, run_id, n)
        if not mode.startswith("fast"):
            await _sleep_network(env, "uc_work")
        kind, _sep, argument = mode.partition(":")
        if kind == "transient":
            message = f"uc_work {run_id}:{n} transient"
            raise TransientError(message)
        if kind == "sleep":
            await asyncio.sleep(float(argument))
        if kind == "late_error":
            await asyncio.sleep(float(argument))
        if kind in {"spawn", "feed"}:
            # spawn - в свой батч (лимиты), feed - в этап-получатель dst (конвейер).
            into = "dst" if kind == "feed" else None
            for index in range(int(argument)):
                child = n * 1000 + index + 1
                item.spawn_call(th.call(uc_work, run_id, child).opts(key=f"w:{child}"), into=into)
        async with engine.begin() as connection:
            if kind == "late_error":
                item.error("broken")
            elif kind == "error":
                item.error(argument)
            elif kind == "skip":
                item.skip(argument)
            else:
                await _effect(connection, env, run_id=run_id, n=n, task="uc_work")
                item.ok("done")
            await item.complete_in(connection)

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def uc_nest(run_id: int, level: int, fanout: int) -> None:
        item_id = _item_id()
        await _sleep_network(env, "uc_nest")
        if level < NEST_LEVELS:
            async with item.sub_batch(f"L{level + 1}:{item_id.hex}", kind=UC_KIND) as child:
                for index in range(fanout):
                    child.add_call(
                        th.call(uc_nest, run_id, level + 1, fanout).opts(key=f"n:{index}")
                    )
        # Путь B: под-батч, итог и эффект - одна транзакция пользователя (A-UC-07).
        async with engine.begin() as connection:
            await _effect(connection, env, run_id=run_id, n=level, task="uc_nest")
            item.ok("done")
            await item.complete_in(connection)

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def uc_crawl(run_id: int, n: int) -> None:
        # Цепочка c:0 -> c:1 -> ... в своём этапе и ссылка назад на c:0 (цикл, дубль ключа).
        # Последнее звено упирается в max_depth этапа (A-UC-15).
        limit = await _flag(env, f"crawl_depth:{run_id}")
        await _sleep_network(env, "uc_crawl")
        item.spawn_call(th.call(uc_crawl, run_id, n + 1).opts(key=f"c:{n + 1}"))
        if n < limit:
            item.spawn_call(th.call(uc_crawl, run_id, 0).opts(key="c:0"))
        async with engine.begin() as connection:
            await _effect(connection, env, run_id=run_id, n=n, task="uc_crawl")
            item.ok("done")
            await item.complete_in(connection)

    @adapter.task(max_retries=3, timeout=job_timeout, retry_delays=[0.01, 0.02, 0.03])
    async def uc_probe(run_id: int, n: int, role: str) -> None:
        await _sleep_network(env, "uc_probe")
        events: list[tuple[str, str]] = []
        if role == "shared_stage":
            async with item.sub_batch("shared", kind=UC_KIND) as stage:
                stage.add_call(th.call(uc_work, run_id, 500 + n).opts(key=f"shared:{n}"))
        elif role == "duplicate_spawn":
            for _attempt in range(3):
                item.spawn_call(th.call(uc_work, run_id, 900).opts(key="dup"))
        elif role == "wrong_target":
            for target in ("sink", "missing"):
                try:
                    item.spawn_call(th.call(uc_work, run_id, 950).opts(key="x"), into=target)
                except SpawnTargetError as exc:
                    events.append(("spawn_target_error", f"{target}:{type(exc).__name__}"))
                else:
                    events.append(("spawn_accepted", target))
        async with engine.begin() as connection:
            for event, detail in events:
                await _event(connection, env, run_id, event=event, detail=detail)
            await _effect(connection, env, run_id=run_id, n=n, task="uc_probe")
            item.ok("done")
            # shared_stage: sub_batch без spawn - тоже путь B (A-UC-16).
            await item.complete_in(connection)

    @adapter.task(
        max_retries=2,
        timeout=job_timeout,
        retry_delays=[0.01, 0.02],
        retry_on=[TransientError],
    )
    async def uc_deliver(run_id: int, email: str) -> None:
        # ok-* доставлено; bounce-* - hard_bounce, строку пишет сама задача; down-* - ящик
        # недоступен до флага heal (исчерпанные попытки переносит экспорт); slow-* - долго.
        await _sleep_network(env, "uc_deliver")
        if email.startswith("slow-"):
            await asyncio.sleep(15)
        if email.startswith("down-") and not await _flag(env, f"heal:{run_id}"):
            message = f"mailbox {email} unavailable"
            raise TransientError(message)
        status, reason = (
            ("failed", "hard_bounce") if email.startswith("bounce-") else ("sent", None)
        )
        async with engine.begin() as connection:
            _ = await connection.execute(
                update(delivery)
                .where(delivery.c.run_id == run_id, delivery.c.email == email)
                .values(status=status, reason=reason)
            )
            if reason is None:
                item.ok("sent")
            else:
                item.error(reason)
            await item.complete_in(connection)

    @adapter.task(max_retries=10, timeout=job_timeout, retry_delays=_SETTLE_DELAYS)
    async def uc_settle(run_id: int) -> None:
        # Рецепт ARCHITECTURE §12.9: экспорт исходов, запрос по остатку, итоговый статус и
        # release() - одна транзакция; повторная доставка колбэка - no-op.
        fail_name = f"settle_fail:{run_id}"
        async with engine.begin() as connection:
            status, outcome, batch_id = (
                await connection.execute(
                    select(runs.c.status, runs.c.outcome, runs.c.batch_id)
                    .where(runs.c.id == run_id)
                    .with_for_update()
                )
            ).one()
            if status != "settling":
                await _event(connection, env, run_id, event="settle_noop", detail=str(status))
                return
            root = th.handle(cast("UUID", batch_id))
            send = await root.child("send")
            exported = 0
            async for entry in send.items(states={ItemState.ERROR, ItemState.CANCELLED}):
                cancelled = entry.state is ItemState.CANCELLED
                _ = await connection.execute(
                    update(delivery)
                    .where(delivery.c.run_id == run_id, delivery.c.email == entry.key)
                    .values(
                        status="cancelled" if cancelled else "failed",
                        reason=entry.label,
                    )
                )
                exported += 1
                if exported == 1 and await _flag(env, fail_name):
                    # Падение посередине экспорта: транзакция откатывается, флаг снимается
                    # отдельно, повтор колбэка обязан дать тот же итог.
                    async with engine.begin() as flag_connection:
                        _ = await flag_connection.execute(
                            delete(flags).where(flags.c.name == fail_name)
                        )
                        await _event(
                            flag_connection,
                            env,
                            run_id,
                            event="settle_failed",
                            detail=str(exported),
                        )
                    message = f"uc_settle {run_id}: падение посередине экспорта"
                    raise TransientError(message)
            _ = await connection.execute(
                update(delivery)
                .where(delivery.c.run_id == run_id, delivery.c.status == "pending")
                .values(status="cancelled", reason="not_dispatched")
            )
            _ = await connection.execute(
                update(runs).where(runs.c.id == run_id).values(status=outcome)
            )
            await _event(connection, env, run_id, event="settled", detail=str(exported))
            await root.release(session=connection)

    _register_hooks(th, domain, UC_KIND)
    _register_hooks(th, domain, UC_EXPORT_KIND, export=True)
    if not worker:
        _register_hooks(th, domain, UC_NOHOOK_KIND)
    return UcTasks(
        work=uc_work,
        nest=uc_nest,
        crawl=uc_crawl,
        probe=uc_probe,
        deliver=uc_deliver,
        settle=uc_settle,
    )


async def _hook_row(
    session: AsyncSession, domain: DomainTables, summary: BatchSummary, *, name: str
) -> bool:
    hook_log = domain.hook_log
    statement = pg_insert(hook_log).values(
        batch_id=summary.id,
        hook=name,
        seq=summary.seq,
        state=int(summary.state),
        progress_done=summary.progress.done,
        progress_found=summary.progress.found,
        txid=func.txid_current(),
    )
    result = await session.execute(statement.on_conflict_do_nothing().returning(hook_log.c.id))
    return result.scalar_one_or_none() is not None


def _register_hooks(th: Tallyho, domain: DomainTables, kind: str, *, export: bool = False) -> None:
    runs = domain.uc_runs

    @th.on_finalized(kind)
    async def finalized(session: AsyncSession, summary: BatchSummary) -> None:
        if not await _hook_row(session, domain, summary, name="on_finalized"):
            return
        values: dict[str, object] = {
            "batch_status": summary.state.name.lower(),
            "progress_done": summary.progress.done,
            "progress_found": summary.progress.found,
        }
        if export:
            # Счётчики точные уже здесь; терминальный статус поставит колбэк экспорта.
            values |= {"status": "settling", "outcome": summary.state.name.lower()}
        elif summary.state is not BatchState.FAILED or summary.reason is None:
            values |= {"status": summary.state.name.lower()}
        else:
            values |= {"status": f"failed:{summary.reason.value}"}
        changed = await session.execute(
            update(runs).where(runs.c.batch_id == summary.id).values(**values).returning(runs.c.id)
        )
        run_id = changed.scalar_one_or_none()
        if export and run_id is not None:
            _ = await session.execute(
                pg_insert(domain.uc_events).values(
                    run_id=int(cast("int", run_id)), event="settling", detail=values["outcome"]
                )
            )

    @th.on_progress(kind, every=timedelta(seconds=1))
    async def progress(session: AsyncSession, summary: BatchSummary) -> None:
        if not await _hook_row(session, domain, summary, name="on_progress"):
            return
        ratio = round(1000 * (summary.progress.ratio or 0.0))
        _ = await session.execute(
            update(runs)
            .where(runs.c.batch_id == summary.id)
            .values(
                progress_done=summary.progress.done,
                progress_found=summary.progress.found,
                # Рецепт §9.4: доля в домене не откатывается, даже если оценка выросла.
                progress_ratio=func.greatest(runs.c.progress_ratio, ratio),
                snapshots_with_eta=runs.c.snapshots_with_eta
                + int(summary.progress.eta is not None),
            )
        )

    @th.on_policy_breach(kind)
    async def policy(session: AsyncSession, summary: BatchSummary, breach: PolicyBreach) -> None:
        if not await _hook_row(session, domain, summary, name="on_policy_breach"):
            return
        reason = f"{breach.action.value}:{','.join(breach.labels)}:{breach.ratio:.3f}"
        _ = await session.execute(
            update(runs)
            .where(runs.c.batch_id == summary.id)
            .values(policy_breaches=runs.c.policy_breaches + 1, pause_reason=reason)
        )
