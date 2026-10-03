"""Метки итога и метрики ``item.incr`` — разные словари (ARCHITECTURE §11.2, §12.4)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from tallyho.model.views import BatchSummary

__all__: list[str] = []


async def send(index: int) -> None:
    """Письмо: метка итога и метрика с тем же именем, что у метки."""
    await asyncio.sleep(0)
    item.incr("sent", 10)
    item.incr("hard_bounce")
    item.incr("bytes", index)
    item.ok("sent")


async def test_labels_hold_only_outcomes_and_metrics_only_increments(
    engine: AsyncEngine, schema: str
) -> None:
    broker = InlineBroker(seed=1)
    th = Tallyho(engine, schema=schema)
    th.install(broker.adapter)
    _ = await th.migrate()
    seen: list[tuple[dict[str, int], dict[str, int]]] = []

    @th.on_finalized("mailing")
    async def finalized(_session: AsyncSession, summary: BatchSummary) -> None:
        await asyncio.sleep(0)
        out = summary.children["send"]
        seen.append((dict(out.labels), dict(out.metrics)))

    _ = finalized
    try:
        # Порог по метке hard_bounce: метрика с тем же именем его не задевает.
        policy = th.FailurePolicy.threshold(ratio=0.5, min_processed=1, labels=["hard_bounce"])
        async with th.batch("mailing", key="campaign:1") as root:
            out = root.sub_batch("send", failure_policy=policy)
            await out.map(send, range(4))
        _ = await broker.drain()
        view = await root.handle.view()
    finally:
        await th.aclose()

    stage = view.children["send"]
    assert stage.state is BatchState.SUCCEEDED
    # Разбивку итогов можно сохранять целиком (§12.4 breakdown: send.labels).
    assert dict(stage.labels) == {"sent": 4}
    assert dict(stage.metrics) == {"sent": 40, "hard_bounce": 4, "bytes": 6}
    assert seen == [({"sent": 4}, {"sent": 40, "hard_bounce": 4, "bytes": 6})]
