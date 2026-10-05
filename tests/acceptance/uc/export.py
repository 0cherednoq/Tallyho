"""A-UC-21/22: строка на каждого получателя и экспорт исходов (ARCHITECTURE §12.9)."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from sqlalchemy import insert, select

from tallyho.model.states import BatchState
from tests.acceptance.app.usecases import UC_EXPORT_KIND
from tests.acceptance.uc.extra import purge_record
from tests.acceptance.uc.helpers import TreeOptions, hooks, start_tree, tree

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    from tallyho import BatchBuilder
    from tallyho.model.views import BatchView
    from tests.acceptance.uc.context import UcContext

__all__ = ["uc21", "uc22"]

_RETENTION = 1.0
_RETRY_RETENTION = 20.0
_EVENTS = "SELECT event FROM app.uc_events WHERE run_id = :root ORDER BY id"
_STATUS = "SELECT status FROM app.uc_runs WHERE id = :root"


def _recipients(prefix_counts: Sequence[tuple[str, int]]) -> list[str]:
    return [
        f"{prefix}-{index}@uc.test" for prefix, count in prefix_counts for index in range(count)
    ]


async def _campaign(
    ctx: UcContext,
    emails: Sequence[str],
    *,
    extra_rows: Sequence[str] = (),
    retention: float = _RETENTION,
) -> tuple[int, UUID]:
    """Кампания: строки получателей ``pending`` и Items этапа ``send`` в одной транзакции.

    ``extra_rows`` - получатели, которые не станут Items (дубль ключа, не разосланы).
    """
    th = ctx.app.th
    delivery = ctx.app.domain.uc_delivery
    run = ctx.upcoming_run()

    async def build(root: BatchBuilder, run_id: int) -> None:
        async with ctx.app.engine.begin() as connection:
            _ = await connection.execute(
                insert(delivery),
                [{"run_id": run_id, "email": email} for email in (*emails, *extra_rows)],
            )
        send = root.sub_batch("send")
        await send.add_calls(
            [
                th.call(ctx.app.uc.deliver, run_id, email).opts(key=email.casefold())
                for email in (*emails, *[row for row in extra_rows if row.casefold() != row])
            ]
        )

    _ = run
    return await start_tree(
        ctx,
        build,
        TreeOptions(
            kind=UC_EXPORT_KIND,
            release_required=True,
            retention=timedelta(seconds=retention),
            settle=True,
            attributes={"campaign": "uc"},
        ),
    )


async def _rows(ctx: UcContext, run: int) -> dict[str, tuple[str, str | None]]:
    delivery = ctx.app.domain.uc_delivery
    async with ctx.app.engine.connect() as connection:
        rows = (
            await connection.execute(
                select(delivery.c.email, delivery.c.status, delivery.c.reason).where(
                    delivery.c.run_id == run
                )
            )
        ).all()
    return {str(email): (str(status), cast("str | None", reason)) for email, status, reason in rows}


async def _events(ctx: UcContext, run: int) -> list[str]:
    return [str(row[0]) for row in await ctx.root_items_sql(_EVENTS, run)]


async def _status(ctx: UcContext, run: int) -> str:
    return str((await ctx.root_items_sql(_STATUS, run))[0][0])


async def _settle(
    ctx: UcContext, run: int, batch_id: UUID, *, settled: int
) -> tuple[BatchView, list[str]]:
    """Дождаться финализации и ``settled``-го экспорта; снимок корня до его удаления."""
    final = await ctx.wait_terminal(batch_id)
    ctx.frozen[batch_id] = final
    statuses: list[str] = []

    async def exported() -> bool:
        statuses.append(await _status(ctx, run))
        return (await _events(ctx, run)).count("settled") >= settled

    _ = await ctx.wait_for(exported, 240, what=f"экспорт №{settled}", interval=0.3)
    return final, statuses


def _expected_row(email: str, *, healed: bool = False) -> tuple[str, str | None]:
    if email.startswith("bounce-"):
        return "failed", "hard_bounce"
    if email.startswith("down-") and not healed:
        return "failed", "exhausted"
    return "sent", None


async def uc21(ctx: UcContext) -> None:
    """Строка на каждого получателя; падение колбэка и повтор; дерево ждёт release()."""
    count = ctx.config.volume(400, floor=40)
    emails = _recipients([("ok", count), ("bounce", 4), ("down", 4)])
    duplicate = "OK-0@uc.test"
    run = ctx.upcoming_run()
    await ctx.set_flag(f"settle_fail:{run}", 1)
    run, batch_id = await _campaign(ctx, emails, extra_rows=[duplicate])

    async def failed_once() -> bool:
        return "settle_failed" in await _events(ctx, run)

    _ = await ctx.wait_for(failed_once, 600, what="первая попытка экспорта упала")
    # Повтор колбэка - через 12 с. Всё это время release() не вызван, retention (1 с) истёк,
    # sweeper прошёл минимум раз: дерево обязано уцелеть.
    await asyncio.sleep(2 * ctx.config.sweep_interval)
    nodes = await tree(ctx, batch_id)
    roots_released = [node.released_at for node in nodes if node.parent_id is None]
    ctx.expect(
        "дерево не удалено до release()",
        bool(nodes) and roots_released == [None] and not await ctx.is_purged(batch_id),
        f"узлов={len(nodes)}, released_at корня={roots_released}",
    )
    final, statuses = await _settle(ctx, run, batch_id, settled=1)
    rows = await _rows(ctx, run)
    wrong = {email: rows.get(email) for email in emails if rows.get(email) != _expected_row(email)}
    if rows.get(duplicate) != ("cancelled", "not_dispatched"):
        wrong[duplicate] = rows.get(duplicate)
    ctx.expect(
        "после settle у каждого получателя ровно одна строка в терминальном статусе",
        not wrong
        and len(rows) == len(emails) + 1
        and "pending" not in {s for s, _ in rows.values()},
        f"неверных={len(wrong)}: {dict(list(wrong.items())[:5])}, строк={len(rows)}",
    )
    events = await _events(ctx, run)
    ctx.expect(
        "падение колбэка посередине и повтор не меняют итог",
        events.count("settle_failed") == 1 and events.count("settled") == 1,
        f"события={Counter(events)}",
    )
    ctx.expect(
        "кампания терминальна только после экспорта",
        "settling" in statuses and await _status(ctx, run) == final.state.name.lower(),
        f"статусы домена по ходу={sorted(set(statuses))}, итог={await _status(ctx, run)}",
    )

    async def purged() -> bool:
        return await ctx.is_purged(batch_id)

    _ = await ctx.wait_for(purged, 120, what="удаление после release()")
    ctx.purged.append(
        await purge_record(ctx, final, release_required=True, released=True, retention=_RETENTION)
    )
    await _cancelled_campaign(ctx)


async def _cancelled_campaign(ctx: UcContext) -> None:
    """Отмена посреди рассылки: отменённые Items и не разосланные строки закрывает экспорт."""
    emails = _recipients([("slow", 6), ("ok", 6)])
    late = _recipients([("late", 5)])
    run, batch_id = await _campaign(ctx, emails, extra_rows=late)
    await asyncio.sleep(3)
    await ctx.app.th.handle(batch_id).cancel()
    final, _statuses = await _settle(ctx, run, batch_id, settled=1)
    rows = await _rows(ctx, run)
    statuses = Counter(status for status, _ in rows.values())
    ctx.expect(
        "отмена: каждая строка терминальна, не ставшие Items закрыты запросом по остатку",
        final.state is BatchState.CANCELLED
        and len(rows) == len(emails) + len(late)
        and statuses["pending"] == 0
        and all(rows[email] == ("cancelled", "not_dispatched") for email in late),
        f"итог={final.state.name}, статусы={dict(statuses)}",
    )

    async def purged() -> bool:
        return await ctx.is_purged(batch_id)

    _ = await ctx.wait_for(purged, 120, what="удаление отменённой кампании после release()")
    ctx.purged.append(
        await purge_record(ctx, final, release_required=True, released=True, retention=_RETENTION)
    )


async def uc22(ctx: UcContext) -> None:  # ruff: ignore[too-many-locals]  # сценарий - одна последовательность шагов
    """``retry_failed`` после экспорта и ``release``: цикл экспорта повторяется."""
    count = ctx.config.volume(400, floor=40)
    emails = _recipients([("ok", count), ("bounce", 4), ("down", 6)])
    # retention дольше паузы между первым release() и retry_failed(), но короче сценария:
    # дерево удаляется только после второго release().
    run, batch_id = await _campaign(ctx, emails, retention=_RETRY_RETENTION)
    first, _ = await _settle(ctx, run, batch_id, settled=1)
    nodes = await tree(ctx, batch_id)
    released_before = all(node.released_at is not None for node in nodes if node.parent_id is None)
    await ctx.set_flag(f"heal:{run}", 1)
    async with ctx.transaction() as session:
        retried = await ctx.app.th.handle(batch_id).retry_failed(
            labels=["exhausted"], session=session
        )
    started = time.monotonic()
    nodes = await tree(ctx, batch_id)
    reset = all(node.released_at is None for node in nodes if node.parent_id is None)
    send_id = first.children["send"].id
    ctx.retry_failed[batch_id] += 1
    ctx.retry_failed[send_id] += 1
    ctx.expect(
        "retry_failed сбрасывает released_at",
        released_before and reset and retried == 6,
        f"до={released_before}, после сброса={reset}, повторено={retried}",
    )
    second, _ = await _settle(ctx, run, batch_id, settled=2)
    rows = await _rows(ctx, run)
    wrong = {e: rows.get(e) for e in emails if rows.get(e) != _expected_row(e, healed=True)}
    finals = await hooks(ctx, batch_id, "on_finalized")
    ctx.expect(
        "on_finalized и колбэк выполнены снова, строки исправлены задачей и повторным экспортом",
        len(finals) == 2 and not wrong and second.children["send"].progress.error == 4,
        (
            f"хуков={finals}, неверных строк={dict(list(wrong.items())[:5])}, ",
            f"error={second.children['send'].progress.error}, {time.monotonic() - started:.0f} с",
        ),
    )

    async def purged() -> bool:
        return await ctx.is_purged(batch_id)

    _ = await ctx.wait_for(purged, 120, what="удаление после второго release()")
    ctx.purged.append(
        await purge_record(
            ctx, second, release_required=True, released=True, retention=_RETRY_RETENTION
        )
    )
