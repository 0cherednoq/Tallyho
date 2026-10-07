"""Обязательные сценарии A-UC-01…06 (ACCEPTANCE §7.1) на эталонных доменах S1/S2/S3."""

from __future__ import annotations

import asyncio
import time
from itertools import pairwise
from typing import TYPE_CHECKING, cast

from sqlalchemy import func, select

from tallyho.model.states import BatchState
from tests.acceptance.chaos.load import Root, audience_for
from tests.acceptance.uc.helpers import (
    TreeOptions,
    domain_status,
    item_bounds,
    start_tree,
    work_calls,
)

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from tallyho import BatchBuilder
    from tallyho.model.policy import FailurePolicy
    from tallyho.model.views import BatchView
    from tests.acceptance.uc.context import UcContext

__all__ = ["failing_source", "uc01", "uc02", "uc03", "uc04", "uc05", "uc06"]

_POLL = 0.5


def _s2_page(audience: int) -> int:
    """Страница ``expand_audience``: цепочка разворачивания - около 20 страниц."""
    return max(10, audience // 20)


async def uc01(ctx: UcContext) -> None:
    """S1: известное число задач, ``seal`` вместе с доменной записью «запуск»."""
    size = ctx.config.volume(20_000)
    batch_id = await ctx.app.start_s1(range(1, size + 1))
    ctx.roots.append(Root(batch_id, "S1", 0, size=size))
    first = await ctx.app.th.handle(batch_id).view()
    progress = first.progress
    ctx.expect(
        "found и точный expected видны сразу после запуска",
        progress.found == size and progress.expected == size and not progress.expected_is_estimate,
        (
            f"found={progress.found}, expected={progress.expected}, ",
            f"estimate={progress.expected_is_estimate}, size={size}",
        ),
    )
    invoices = ctx.app.domain.invoices
    seen: list[int] = []
    view = first
    while not view.state.is_terminal:
        await asyncio.sleep(2)
        seen.append(
            await ctx.count(
                select(func.max(invoices.c.progress_done)).where(invoices.c.batch_id == batch_id)
            )
        )
        view = await ctx.app.th.handle(batch_id).view()
    drops = [(a, b) for a, b in pairwise(seen) if b < a]
    ctx.expect(
        "прогресс в домене не убывает", not drops, f"падения={drops[:5]}, снимков={len(seen)}"
    )
    share = view.progress.error / size
    ctx.expect(
        "итог completed_with_errors с ≈1% ошибок",
        view.state is BatchState.COMPLETED_WITH_ERRORS and 0.002 <= share <= 0.03,
        (
            f"state={view.state.name}, error={view.progress.error}, ok={view.progress.ok}, ",
            f"share={share:.4f}",
        ),
    )
    ctx.stats["s1.size"] = size


async def _start_s2(ctx: UcContext, *, announced: bool) -> tuple[UUID, int, int]:
    audience = ctx.config.volume(20_000)
    page = _s2_page(audience)
    addresses = audience_for(ctx.config.seed, 1, audience)
    batch_id = await ctx.app.start_s2(
        1, addresses, expected_total=audience if announced else None, page=page
    )
    ctx.roots.append(Root(batch_id, "S2", 0, addresses, page=page))
    ctx.stats["s2.audience"] = audience
    ctx.stats["s2.page"] = page
    return batch_id, audience, page


async def _sample(ctx: UcContext, batch_id: UUID) -> list[BatchView]:
    """Снимки дерева до терминального состояния с шагом ``_POLL``."""
    samples: list[BatchView] = []
    handle = ctx.app.th.handle(batch_id)
    while True:
        view = await handle.view()
        samples.append(view)
        if view.state.is_terminal:
            return samples
        await asyncio.sleep(_POLL)


async def _s2_common(ctx: UcContext, batch_id: UUID, final: BatchView) -> None:
    expand, send = final.children["expand"], final.children["send"]
    first_send, _ = await item_bounds(ctx, send.id)
    ctx.expect(
        "send стартует до конца expand",
        first_send is not None
        and expand.finished_at is not None
        and first_send < expand.finished_at,
        f"первый send={first_send}, expand.finished_at={expand.finished_at}",
    )
    ctx.expect(
        "send закрылся сам после expand и финализирован",
        send.state.is_terminal
        and send.finished_at is not None
        and expand.finished_at is not None
        and send.finished_at >= expand.finished_at,
        f"send={send.state.name}@{send.finished_at}, expand@{expand.finished_at}",
    )
    deliveries = ctx.app.domain.deliveries
    async with ctx.app.engine.connect() as connection:
        rows = (
            await connection.execute(
                select(deliveries.c.label, func.count())
                .where(deliveries.c.campaign_id == 1)
                .group_by(deliveries.c.label)
            )
        ).all()
    breakdown = {str(label): int(count) for label, count in rows}
    labels = {name: send.labels.get(name, 0) for name in ("sent", "rejected")}
    ctx.expect(
        "breakdown доставок в домене совпадает с метками send",
        breakdown == {name: value for name, value in labels.items() if value},
        f"домен={breakdown}, метки={labels}",
    )
    _ = batch_id


async def uc02(ctx: UcContext) -> None:
    """S2 с ``expected_total``: send работает параллельно, закрывается сам, дубли по эталону."""
    batch_id, audience, _page = await _start_s2(ctx, announced=True)
    samples = await _sample(ctx, batch_id)
    final = samples[-1]
    await _s2_common(ctx, batch_id, final)
    announced = [
        s.children["send"].progress for s in samples if s.children["send"].state is BatchState.OPEN
    ]
    ctx.expect(
        "expected send равен заявленному размеру аудитории до seal",
        all(p.expected is not None and p.expected >= audience for p in announced),
        f"снимков={len(announced)}, первые={[p.expected for p in announced[:3]]}",
    )
    send = final.children["send"].progress
    expected_duplicates = audience // 100
    ctx.expect(
        "дубли составляют ровно 1% аудитории",
        send.duplicates == expected_duplicates,
        f"duplicates={send.duplicates}, expected={expected_duplicates}, audience={audience}",
    )


async def uc03(ctx: UcContext) -> None:
    """S2 без ``expected_total``: оценка по правилу ``min(20, 5%)``, точный итог после expand."""
    batch_id, _audience, _page = await _start_s2(ctx, announced=False)
    samples = await _sample(ctx, batch_id)
    final = samples[-1]
    await _s2_common(ctx, batch_id, final)
    early: list[str] = []
    missing: list[str] = []
    estimates = 0
    for view in samples:
        expand, send = view.children["expand"].progress, view.children["send"]
        if send.state is not BatchState.OPEN or expand.expected is None:
            continue
        basis = send.progress.estimate_basis or expand.done
        threshold = min(20, 0.05 * expand.expected)
        shown = send.progress.expected is not None
        estimates += int(shown)
        if shown and basis < threshold:
            early.append(f"basis={basis}<{threshold:.2f}")
        if not shown and basis >= threshold and send.progress.found:
            missing.append(f"basis={basis}>={threshold:.2f}")
    ctx.expect(
        "оценка итога появляется по правилу min(20, 5%) и не раньше",
        estimates > 0 and not early and not missing,
        f"снимков с оценкой={estimates}, раньше={early[:3]}, не показана={missing[:3]}",
    )
    exact = final.children["send"].progress
    ctx.expect(
        "после финализации expand итог send точный",
        exact.expected == exact.found and not exact.expected_is_estimate,
        f"expected={exact.expected}, found={exact.found}, estimate={exact.expected_is_estimate}",
    )


async def uc04(ctx: UcContext) -> None:
    """S3: этапы работают параллельно, каскад seal, ``in_flight`` показывает прогресс PDF."""
    batch_id = await ctx.app.start_s3(1, pages=ctx.config.pages)
    ctx.roots.append(Root(batch_id, "S3", 0))
    handle = ctx.app.th.handle(batch_id)
    pdfs = await handle.child("pdfs")
    progress_seen = 0
    in_flight_seen = 0
    while not (view := await handle.view()).state.is_terminal:
        for entry in await pdfs.in_flight(limit=500):
            in_flight_seen += 1
            if entry.progress_total == 2 and entry.progress_done == 1:
                progress_seen += 1
        await asyncio.sleep(0.3)
    ctx.expect(
        "in_flight показывает item.progress скачивания PDF",
        progress_seen > 0,
        f"in_flight записей={in_flight_seen}, с прогрессом 1/2={progress_seen}",
    )
    pages, cards, pdf_view = (view.children[key] for key in ("pages", "cards", "pdfs"))
    first_card, _ = await item_bounds(ctx, cards.id)
    first_pdf, _ = await item_bounds(ctx, pdf_view.id)
    ctx.expect(
        "cards и pdfs работают параллельно с pages",
        first_card is not None
        and pages.finished_at is not None
        and first_card < pages.finished_at
        and first_pdf is not None
        and cards.finished_at is not None
        and first_pdf < cards.finished_at,
        (
            f"первая карточка={first_card}, pages@{pages.finished_at}; ",
            f"первый PDF={first_pdf}, cards@{cards.finished_at}",
        ),
    )
    order = [node.finished_at for node in (pages, cards, pdf_view)]
    ctx.expect(
        "каскад seal: pages -> cards -> pdfs финализированы по порядку",
        all(node.state.is_terminal for node in (pages, cards, pdf_view))
        and None not in order
        and order == sorted(cast("list[datetime]", order)),
        f"{[(n.state.name, n.finished_at) for n in (pages, cards, pdf_view)]}",
    )


async def uc05(ctx: UcContext) -> None:
    """S3 на каталоге без PDF: пустой этап закрыт и финализирован, корень тоже."""
    batch_id = await ctx.app.start_s3(1, pages=ctx.config.pages)
    ctx.roots.append(Root(batch_id, "S3", 0))
    view = await ctx.wait_terminal(batch_id)
    pdfs = view.children["pdfs"]
    ctx.expect(
        "pdfs закрыт и финализирован при found = 0",
        pdfs.state is BatchState.SUCCEEDED and pdfs.progress.found == 0,
        f"pdfs={pdfs.state.name}, found={pdfs.progress.found}",
    )
    runs = ctx.app.domain.catalog_runs
    status = await ctx.scalar(select(runs.c.batch_status).where(runs.c.batch_id == batch_id))
    expected = "completed" if view.state is BatchState.SUCCEEDED else "completed_with_errors"
    ctx.expect(
        "корень финализирован, статус в домене соответствует итогу",
        view.state.is_terminal and status == expected,
        (
            f"корень={view.state.name}, домен={status}, ",
            f"cards.error={view.children['cards'].progress.error}",
        ),
    )
    ctx.stats["uc05.root_succeeded"] = int(view.state is BatchState.SUCCEEDED)


async def failing_source(
    ctx: UcContext, mode: str, *, policy: FailurePolicy | None = None
) -> tuple[UUID, BatchView]:
    """Конвейер ``src -> dst``: половина источника наполняет dst, половина падает.

    Без ``policy`` источник завершается ``completed_with_errors`` - это «упавший источник»
    §8.1 п.4. Получатели выполняются 8+ с, поэтому заняты, когда источник финализируется.
    Возвращает id корня и его снимок, когда терминально всё дерево.
    """
    feeders = ctx.config.volume(80, floor=8)
    half = feeders // 2

    async def build(root: BatchBuilder, run: int) -> None:
        src = root.sub_batch("src", failure_policy=policy)
        _ = root.sub_batch("dst", fed_by=[src], on_feeder_failed=mode)
        await src.add_calls(work_calls(ctx, run, range(1, feeders + 1)))

    run_id = ctx.upcoming_run()
    modes = dict.fromkeys(range(1, half + 1), "feed:3")
    modes |= dict.fromkeys(range(half + 1, feeders + 1), "late_error:15")
    modes |= {n * 1000 + i: "sleep:8" for n in range(1, half + 1) for i in range(1, 4)}
    await ctx.plan(run_id, modes)
    _run, batch_id = await start_tree(ctx, build, TreeOptions())
    view = await ctx.wait_terminal(batch_id)

    async def settled() -> bool:
        current = await ctx.app.th.handle(batch_id).view()
        return all(child.state.is_terminal for child in current.children.values())

    _ = await ctx.wait_for(settled, 300, what="все этапы конвейера терминальны")
    return batch_id, view


async def uc06(ctx: UcContext) -> None:
    """Упавший источник: ``on_feeder_failed="seal"``, затем ``"cancel"``."""
    _, sealed = await failing_source(ctx, "seal")
    sealed = await ctx.app.th.handle(sealed.id).view()
    src, dst = sealed.children["src"], sealed.children["dst"]
    ctx.expect(
        "seal: источник с ошибками, получатель закрылся и доделал полученное",
        src.state is BatchState.COMPLETED_WITH_ERRORS
        and dst.state.is_terminal
        and dst.state is not BatchState.CANCELLED
        and dst.progress.found > 0
        and dst.progress.ok == dst.progress.found,
        (
            f"src={src.state.name}/{src.reason}, dst={dst.state.name} found={dst.progress.found} ",
            f"ok={dst.progress.ok} cancelled={dst.progress.cancelled}",
        ),
    )
    started = time.monotonic()
    _, cancelled = await failing_source(ctx, "cancel")
    cancelled = await ctx.app.th.handle(cancelled.id).view()
    src, dst = cancelled.children["src"], cancelled.children["dst"]
    ctx.expect(
        "cancel: источник с ошибками, получатель получил запрос отмены",
        src.state is BatchState.COMPLETED_WITH_ERRORS and dst.state is BatchState.CANCELLED,
        (
            f"src={src.state.name}, dst={dst.state.name} found={dst.progress.found} ",
            f"ok={dst.progress.ok} cancelled={dst.progress.cancelled}, ",
            f"{time.monotonic() - started:.0f} с",
        ),
    )
    runs = ctx.app.domain.uc_runs
    async with ctx.app.engine.connect() as connection:
        rows = (
            await connection.execute(select(runs.c.batch_id, runs.c.status).order_by(runs.c.id))
        ).all()
    statuses = [str(status) for _, status in rows]
    ctx.expect(
        "итоги корней в домене соответствуют финализации",
        statuses == [domain_status(sealed), domain_status(cancelled)],
        f"домен={statuses}, корни={[sealed.state.name, cancelled.state.name]}",
    )
