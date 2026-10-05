"""Нагрузка S1/S2/S3 для хаос-прогонов: размер от длительности, данные от seed."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from tests.acceptance.app.application import PAGE
from tests.acceptance.app.common import rng_for
from tests.acceptance.app.site import CatalogGenerator
from tests.acceptance.chaos.stand import CONNECTION_ERRORS

if TYPE_CHECKING:
    from uuid import UUID

    from tests.acceptance.app.application import AcceptanceApp
    from tests.acceptance.chaos.journal import ChaosJournal
    from tests.acceptance.chaos.stand import StandSettings

__all__ = ["LoadDriver", "LoadProfile", "Root", "audience_for", "plan_load"]

# Средняя задача: сон 1-5 с плюс 20% транзакций, открытых ещё на 1-5 с (ACCEPTANCE §3.1).
MEAN_TASK_SECONDS = 3.6
_BATCH_ITEMS = 160
_INVOICE_SPAN = 100_000


@dataclass(frozen=True, slots=True)
class LoadProfile:
    """Объём нагрузки одного прогона.

    Attributes:
        scenario: ``S1``, ``S2`` или ``S3``.
        batches: Сколько корневых батчей запустить.
        size: Счетов (S1), адресов (S2) или страниц каталога (S3) на батч.
        stagger: Пауза между запусками батчей, секунды.
        expected_items: Оценка числа Items во всех батчах.
    """

    scenario: str
    batches: int
    size: int
    stagger: float
    expected_items: int


@dataclass(frozen=True, slots=True)
class Root:
    """Запущенный корневой батч и то, что нужно оракулу для сверки с эталоном."""

    batch_id: UUID
    scenario: str
    index: int
    addresses: tuple[str, ...] = ()
    page: int = PAGE
    """S2: сколько контактов читает одна страница ``expand_audience``."""
    size: int = 0
    """S1: сколько счетов в батче."""


def plan_load(
    scenario: str,
    seed: int,
    duration: float,
    *,
    settings: StandSettings,
    load_factor: float = 0.6,
) -> LoadProfile:
    """Подобрать объём так, чтобы без отказов работа заняла ``load_factor * duration``.

    Батчи запускаются по одному через равные промежутки: финализации идут всё окно
    хаоса, а не только в его конце.
    """
    target = max(60.0, load_factor * duration * settings.concurrency / MEAN_TASK_SECONDS)
    if scenario == "S3":
        truth = CatalogGenerator.build(seed, page_count=settings.pages).truth
        per_batch = max(1, truth.pages + truth.cards + truth.pdfs)
        batches = max(2, round(target / per_batch))
        size = settings.pages
    else:
        batches = max(2, round(target / _BATCH_ITEMS))
        size = max(10, round(target / batches))
        per_batch = size if scenario == "S1" else size + size // 10 + 1
    return LoadProfile(
        scenario=scenario,
        batches=batches,
        size=size,
        stagger=round(load_factor * duration / batches, 3),
        expected_items=per_batch * batches,
    )


def audience_for(seed: int, campaign_id: int, size: int) -> tuple[str, ...]:
    """Адреса кампании S2: 1% дублей, часть адресов с 503 и отказом провайдера."""
    rng = rng_for(seed, campaign_id, namespace="chaos-audience")
    addresses: list[str] = []
    for index in range(size):
        roll = rng.random()
        if addresses and roll < 0.01:
            addresses.append(rng.choice(addresses).upper())
        elif roll < 0.04:
            addresses.append(f"retry-{campaign_id}-{index}@example.test")
        elif roll < 0.05:
            addresses.append(f"reject-{campaign_id}-{index}@example.test")
        else:
            addresses.append(f"user-{campaign_id}-{index}@example.test")
    return tuple(addresses)


@dataclass(slots=True)
class LoadDriver:
    """Создаёт батчи сценария по расписанию и переживает отказы PostgreSQL."""

    app: AcceptanceApp
    profile: LoadProfile
    seed: int
    journal: ChaosJournal
    roots: list[Root] = field(default_factory=list[Root])
    finished: bool = False

    async def run(self) -> None:
        """Запустить все батчи профиля; первый — сразу, остальные через ``stagger``."""
        started = self.journal.elapsed()
        for index in range(self.profile.batches):
            delay = started + index * self.profile.stagger - self.journal.elapsed()
            if delay > 0:
                await asyncio.sleep(delay)
            root = await self._launch(index)
            self.roots.append(root)
            self.journal.record(
                "batch_started", self.profile.scenario, batch_id=str(root.batch_id), index=index
            )
        self.finished = True

    async def _launch(self, index: int) -> Root:
        """Создать батч; при отказе БД повторять, не создавая второй батч того же номера."""
        addresses = (
            audience_for(self.seed, index + 1, self.profile.size)
            if self.profile.scenario == "S2"
            else ()
        )
        while True:
            try:
                existing = await self._existing(index)
                batch_id = existing or await self._start(index, addresses)
            except (*CONNECTION_ERRORS, IntegrityError) as exc:
                self.journal.record(
                    "batch_start_retry",
                    self.profile.scenario,
                    index=index,
                    error=type(exc).__name__,
                )
                await asyncio.sleep(1)
                continue
            return Root(batch_id, self.profile.scenario, index, addresses, size=self.profile.size)

    async def _start(self, index: int, addresses: tuple[str, ...]) -> UUID:
        if self.profile.scenario == "S1":
            first = index * _INVOICE_SPAN + 1
            return await self.app.start_s1(range(first, first + self.profile.size))
        if self.profile.scenario == "S2":
            return await self.app.start_s2(index + 1, addresses)
        return await self.app.start_s3(index + 1, pages=self.profile.size)

    async def _existing(self, index: int) -> UUID | None:
        """Батч этого номера, если прошлый commit дошёл, а ответ потерялся."""
        domain = self.app.domain
        if self.profile.scenario == "S1":
            table, row_id = domain.invoices, index * _INVOICE_SPAN + 1
        elif self.profile.scenario == "S2":
            table, row_id = domain.campaigns, index + 1
        else:
            table, row_id = domain.catalog_runs, index + 1
        async with self.app.engine.connect() as connection:
            found = await connection.scalar(select(table.c.batch_id).where(table.c.id == row_id))
        return cast("UUID | None", found)
