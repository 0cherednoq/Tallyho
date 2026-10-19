"""A-RM: при нехватке ресурса работа ждёт через defer, попытки не горят (RM7.1, §13).

Сквозной сценарий domain-checker из §13 исходного документа: 20 доменов,
ёмкость ресурса приходит волнами 5 + 3 + 0 (и финальное пополнение). Окно
батча держит не больше одного запроса с ресурса. Без ``item.defer`` 12 ждавших
доменов сожгли бы 10 ретраев; с ним — ноль, ровно 20 платных вызовов, 20 из 20
завершены после пополнения.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.storage.tables import build_metadata
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []

DOMAINS = 20
DEFER_DELAY = timedelta(seconds=1)


@dataclass
class _ResourcePool:
    """Пул кредитов дефицитного ресурса с учётом платных вызовов и параллелизма."""

    available: int = 0
    paid: int = 0
    deferred: int = 0
    errors: int = 0
    concurrent: int = 0
    max_concurrent: int = 0

    def reset(self) -> None:
        """Обнулить пул перед сценарием (объект переиспользуется задачей брокера)."""
        self.available = self.paid = self.deferred = self.errors = 0
        self.concurrent = self.max_concurrent = 0

    def acquire(self) -> bool:
        """Снять один кредит, если он есть.

        Returns:
            ``True``, если кредит был и списан.
        """
        if self.available <= 0:
            return False
        self.available -= 1
        return True


_POOL = _ResourcePool()


async def check_domain(index: int) -> None:
    """Проверка одного домена: платный вызов при наличии кредита, иначе defer."""
    del index
    await asyncio.sleep(0)
    pool = _POOL
    if not pool.acquire():
        pool.deferred += 1
        item.defer(reason="no_credits", delay=DEFER_DELAY)
        return
    pool.concurrent += 1
    pool.max_concurrent = max(pool.max_concurrent, pool.concurrent)
    pool.paid += 1
    pool.concurrent -= 1
    item.ok("checked")


async def _max_attempt(engine: AsyncEngine, schema: str) -> int:
    """Максимум ``th_item.attempt`` в схеме: 0 означает «ни одного ретрая»."""
    tables = build_metadata()
    translated = engine.execution_options(schema_translate_map={None: schema})
    async with translated.connect() as conn:
        value = await conn.scalar(select(func.max(tables.item.c.attempt)))
    return int(value or 0)


async def test_a_rm_resource_shortage_defers_without_burning_retries(
    engine: AsyncEngine, schema: str
) -> None:
    _POOL.reset()
    clock = FakeClock(datetime(2026, 11, 1, 9, tzinfo=UTC))
    broker = InlineBroker(seed=7)
    th = Tallyho(engine, schema=schema, clock=clock)
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        # Окно 1: максимум один запрос с дефицитного ресурса одновременно.
        async with th.batch("domain-check", key="batch:1", max_in_flight=1) as root:
            await root.map(check_domain, range(DOMAINS))

        # Волна ёмкости 5 → 3 → 0, затем финальное пополнение до завершения.
        for grant in (5, 3, 0, DOMAINS):
            _POOL.available += grant
            clock.advance(DEFER_DELAY * 2)
            _ = await broker.drain()

        view = await root.handle.view()
        max_attempt = await _max_attempt(engine, schema)
    finally:
        await th.aclose()

    # Контрольный замер A-RM.
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == DOMAINS  # 20 из 20 завершены
    assert view.progress.error == 0  # ни одного провала
    assert view.progress.deferred == 0  # очередь отложений пуста
    assert _POOL.paid == DOMAINS  # ровно 20 платных вызовов
    assert _POOL.max_concurrent == 1  # не больше одного параллельного с ресурса
    assert _POOL.errors == 0
    # Ноль сгоревших ретраев: defer не тратит попытку, attempt у всех остался 0.
    assert max_attempt == 0
    assert _POOL.deferred > 0  # работа действительно ждала ресурса
