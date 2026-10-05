"""Общий стенд сценариев с явными фазами (P-04, P-07, P-08, P-11): схемы, воркеры, maintenance."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.app import KIND_PIPELINE, AppConfig, build_app
from benchmarks.metrics import LatencyLog
from benchmarks.stand import ident, schemas, sql
from benchmarks.workers import WorkerPool

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

    from benchmarks.app import BenchApp, BenchTasks
    from benchmarks.context import RunContext
    from benchmarks.stand import Schemas
    from tallyho import Tallyho

__all__ = ["Harness", "create_pipeline", "harness"]

_POLL: Final = 0.5
_TALLYHO_TABLES: Final = ("batch", "item", "lease", "outbox", "counter", "counter_delta")
_DOMAIN_TABLES: Final = ("finalized", "progress_log", "invoice_done")


@dataclass(slots=True)
class Harness:
    """Запущенный стенд одного сценария.

    Attributes:
        app: приложение producer.
        pool: воркеры.
        names: схемы.
        observer: отдельный движок для служебных запросов харнесса.
        latencies: задержки операций producer (моменты — секунды от ``origin``).
        origin: ``time.monotonic()`` начала сценария.
        wall_origin: ``time.time()`` того же момента (для событий воркеров).
    """

    app: BenchApp
    pool: WorkerPool
    names: Schemas
    observer: AsyncEngine
    latencies: LatencyLog = field(default_factory=LatencyLog)
    origin: float = field(default_factory=time.monotonic)
    wall_origin: float = field(default_factory=time.time)
    maintenance_errors: int = 0

    @property
    def th(self) -> Tallyho:
        """Установка tallyho producer."""
        return self.app.tallyho()[0]

    @property
    def tasks(self) -> BenchTasks:
        """Задачи."""
        return self.app.tallyho()[1]

    def now(self) -> float:
        """Секунды от начала сценария.

        Returns:
            Секунды.
        """
        return time.monotonic() - self.origin

    @property
    def idents(self) -> dict[str, str]:
        """Таблицы для шаблонов :func:`benchmarks.stand.sql` (``{batch}``, ``{item}`` и т. д.)."""
        tallyho = {name: ident(self.names.tallyho, f"th_{name}") for name in _TALLYHO_TABLES}
        domain = {name: ident(self.names.domain, name) for name in _DOMAIN_TABLES}
        return {**tallyho, **domain}

    async def scalar(self, template: str, params: dict[str, object] | None = None) -> int:
        """Целочисленный скаляр служебного запроса; таблицы — из :attr:`idents`.

        Returns:
            Значение; ``NULL`` — 0.
        """
        async with self.observer.connect() as connection:
            value = cast(
                "int | None",
                await connection.scalar(sql(template, **self.idents), params or {}),
            )
        return int(value or 0)

    async def active_roots(self, kind: str) -> int:
        """Сколько корней вида ``kind`` ещё не терминальны.

        Returns:
            Число корней.
        """
        return await self.scalar(
            "SELECT count(*) FROM {batch} WHERE kind = :kind AND parent_id IS NULL AND state < 10",
            {"kind": kind},
        )

    async def wait_roots(self, kind: str, *, within: float) -> float:
        """Дождаться терминальности всех корней ``kind``; вернуть момент (секунды сценария).

        Returns:
            Секунды сценария к моменту, когда все корни терминальны.
        """
        async with asyncio.timeout(within):
            while await self.active_roots(kind):
                self.pool.assert_alive()
                await asyncio.sleep(_POLL)
        return self.now()

    async def maintenance_loop(self, interval: float) -> None:
        """Maintenance вручную: ``run_maintenance_once`` раз в ``interval`` (таймер операции)."""
        while True:
            started = time.monotonic()
            _ = await self.th.run_maintenance_once()
            finished = time.monotonic()
            self.latencies.add("maintenance", finished - self.origin, finished - started)
            await asyncio.sleep(max(0.0, interval - (finished - started)))


@contextlib.asynccontextmanager
async def harness(
    ctx: RunContext,
    *,
    name: str,
    processes: int,
    concurrency: int,
    configure: Callable[[AppConfig], AppConfig] | None = None,
    maintenance_interval: float | None = 1.0,
) -> AsyncGenerator[Harness]:
    """Свежие схемы, приложение, воркеры и (по умолчанию) maintenance раз в секунду.

    Yields:
        Запущенный стенд.
    """
    engine = create_async_engine(ctx.stand.dsn)
    try:
        async with schemas(engine, name) as names:
            config = AppConfig(
                dsn=ctx.stand.dsn,
                schemas=names,
                concurrency=concurrency,
                seed=ctx.seed,
                record_ops=True,
            )
            if configure is not None:
                config = configure(config)
            app = build_app(replace(config))
            pool = WorkerPool(ctx.out / name, config)
            maintenance: asyncio.Task[None] | None = None
            try:
                await app.migrate()
                await asyncio.to_thread(app.queue.stats)
                await pool.start(processes)
                stand = Harness(app, pool, names, engine)
                if maintenance_interval is not None:
                    maintenance = asyncio.create_task(
                        stand.maintenance_loop(maintenance_interval), name="bench-maintenance"
                    )
                yield stand
            finally:
                if maintenance is not None:
                    _ = maintenance.cancel()
                    _ = await asyncio.wait([maintenance])
                await pool.stop()
                await app.close()
    finally:
        await engine.dispose()


async def create_pipeline(th: Tallyho, tasks: BenchTasks, run: int, *, pages: int) -> UUID:
    """Конвейер S3: ``pages`` → ``cards`` → ``pdfs`` (``fed_by``), страницы — сразу.

    Returns:
        Id корня.
    """
    async with th.batch(KIND_PIPELINE, key=f"pipeline:{run}") as root:
        page_stage = root.sub_batch("pages")
        card_stage = root.sub_batch("cards", fed_by=[page_stage])
        _ = root.sub_batch("pdfs", fed_by=[card_stage])
        await page_stage.add_calls(
            th.call(tasks.page, run, page).opts(key=f"page:{page}") for page in range(pages)
        )
    return root.handle.id
