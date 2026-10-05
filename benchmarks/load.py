"""Устойчивая нагрузка S1 (ACCEPTANCE §3.2): батчи счетов при ограниченном числе в работе.

Producer держит в работе не больше ``in_flight`` Items: как только завершённых становится
достаточно, создаёт следующий батч ``bench.s1`` на ``batch_size`` задач (таймер операции
``create``). Раз в тик читает ``handle.view()`` незавершённых батчей (таймер ``read_progress``)
и пишет точку «сколько Items завершено к моменту t». Maintenance (лидер) работает в этом же
процессе, как в API-процессе эталонного приложения. После прогона — оракул на выборке:
каждый батч ``succeeded``, ровно одна финализация, доменных строк ровно столько, сколько Items.
"""

from __future__ import annotations

import asyncio
import contextlib
import operator
import time
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Final, TypeAlias, cast

from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from benchmarks.app import KIND_S1, AppConfig, build_app
from benchmarks.metrics import LatencyLog, rate_series, steady_rate
from benchmarks.prose import prose
from benchmarks.stand import BenchError, ident, schemas, sql
from benchmarks.workers import WorkerEvents, WorkerPool
from tallyho.model.errors import TallyhoError
from tallyho.model.states import BatchState

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine
    from pathlib import Path
    from uuid import UUID

    from benchmarks.app import BenchApp
    from benchmarks.context import RunContext
    from benchmarks.stand import Schemas

__all__ = ["Action", "OracleReport", "SteadyLoad", "SteadyRun", "SteadySpec", "steady_load"]

_TICK: Final = 0.25
_DRAIN_TIMEOUT: Final = 300.0
_TERMINAL: Final = frozenset(
    {
        BatchState.SUCCEEDED,
        BatchState.COMPLETED_WITH_ERRORS,
        BatchState.FAILED,
        BatchState.CANCELLED,
    }
)

Action: TypeAlias = "tuple[float, Callable[[], Coroutine[object, object, None]]]"


def _raise_failed(tasks: list[asyncio.Task[None]]) -> None:
    for task in tasks:
        if task.done() and not task.cancelled() and (error := task.exception()) is not None:
            raise error


@dataclass(frozen=True, slots=True, kw_only=True)
class SteadySpec:
    """Параметры устойчивой нагрузки.

    Attributes:
        processes: процессов-воркеров.
        concurrency: потоков (и ``async_concurrency``) flexiq в процессе.
        duration: длительность замера, секунды.
        warmup: начало прогона, не входящее в устойчивую скорость, секунды.
        batch_size: Items в одном батче S1.
        in_flight: сколько Items держать в работе (созданы, но не завершены).
        window: окно расчёта скорости, секунды.
    """

    processes: int
    concurrency: int
    duration: float
    warmup: float
    batch_size: int
    in_flight: int
    window: float = 2.0

    @property
    def parallelism(self) -> int:
        """Одновременно выполняемых задач: процессы x потоки."""
        return self.processes * self.concurrency


@dataclass(slots=True)
class OracleReport:
    """Оракул на выборке: что не сошлось после прогона."""

    batches: int = 0
    items: int = 0
    violations: list[str] = field(default_factory=list[str])

    @property
    def ok(self) -> bool:
        """Нарушений нет."""
        return not self.violations

    def describe(self) -> str:
        """Одна строка для отчёта.

        Returns:
            Текст для отчёта.
        """
        if self.ok:
            return f"нарушений нет ({self.batches} батчей, {self.items} Items)"
        return "; ".join(self.violations[:5])


@dataclass(slots=True)
class SteadyRun:
    """Результат прогона: точки завершений, задержки операций producer, события воркеров."""

    spec: SteadySpec
    done_points: list[tuple[float, int]] = field(default_factory=list[tuple[float, int]])
    created_points: list[tuple[float, int]] = field(default_factory=list[tuple[float, int]])
    latencies: LatencyLog = field(default_factory=LatencyLog)
    events: WorkerEvents = field(default_factory=WorkerEvents)
    wall_origin: float = 0.0
    outage_ticks: int = 0
    oracle: OracleReport = field(default_factory=OracleReport)

    def rates(self) -> list[tuple[float, float]]:
        """Завершений в секунду по окнам ``spec.window``.

        Returns:
            Пары ``(конец окна, завершений в секунду)``.
        """
        return rate_series(self.done_points, window=self.spec.window)

    def steady(self) -> float:
        """Устойчивая скорость: медиана окон после прогрева.

        Returns:
            Завершений в секунду.
        """
        return steady_rate(self.rates(), warmup=self.spec.warmup)

    def worker_ops(self) -> LatencyLog:
        """Операции воркеров (``claim``, ``finish``) и транзакции Completer за время прогона.

        Returns:
            Журнал задержек с моментами от начала прогона.
        """
        log = LatencyLog()
        end = self.spec.duration
        for at, name, duration in self.events.ops:
            if 0 <= at - self.wall_origin <= end:
                log.add(name, at - self.wall_origin, duration)
        for at, _, duration in self.events.flushes:
            if 0 <= at - self.wall_origin <= end:
                log.add("completer_flush", at - self.wall_origin, duration)
        return log

    def flushes(self) -> list[tuple[float, int, float]]:
        """Транзакции Completer: ``(t от начала прогона, Items, длительность)``.

        Returns:
            Транзакции в порядке событий.
        """
        return [(at - self.wall_origin, items, d) for at, items, d in self.events.flushes]

    def buffers(self) -> list[tuple[float, int]]:
        """Размер буфера Completer: ``(t от начала прогона, Items)``.

        Returns:
            Замеры в порядке событий.
        """
        return [(at - self.wall_origin, items) for at, items in self.events.buffers]


@dataclass(slots=True)
class SteadyLoad:
    """Producer, воркеры и maintenance одного прогона S1."""

    config: AppConfig
    spec: SteadySpec
    root: Path
    app: BenchApp | None = None
    pool: WorkerPool | None = None
    _batches: dict[UUID, int] = field(default_factory=dict["UUID", int])
    _closed_done: int = 0
    _created: int = 0
    _next_invoice: int = 0
    _maintenance: asyncio.Task[None] | None = None
    _stop_maintenance: Callable[[], None] | None = None

    async def start(self) -> None:
        """Мигрировать, поднять воркеры и maintenance."""
        app = build_app(self.config)
        await app.migrate()
        await asyncio.to_thread(app.queue.stats)
        self.app = app
        self.pool = WorkerPool(self.root, self.config)
        await self.pool.start(self.spec.processes)
        th, _ = app.tallyho()
        runner = th.maintenance()
        self._maintenance = asyncio.create_task(runner.run(), name="bench-maintenance")
        self._stop_maintenance = runner.stop

    def require(self) -> tuple[BenchApp, WorkerPool]:
        """Приложение и пул; упасть, если прогон не запущен.

        Returns:
            Приложение и пул воркеров.

        Raises:
            BenchError: :meth:`start` ещё не вызван.
        """
        if self.app is None or self.pool is None:
            message = "нагрузка не запущена"
            raise BenchError(message)
        return self.app, self.pool

    async def _create_batch(self, run: SteadyRun, origin: float) -> None:
        app, _ = self.require()
        th, tasks = app.tallyho()
        first = self._next_invoice
        self._next_invoice += self.spec.batch_size
        started = time.monotonic()
        async with th.batch(
            KIND_S1, key=f"invoices:{first}", expected_total=self.spec.batch_size
        ) as batch:
            await batch.add_calls(
                th.call(tasks.s1, invoice).opts(key=f"invoice:{invoice}")
                for invoice in range(first, first + self.spec.batch_size)
            )
        finished = time.monotonic()
        run.latencies.add("create", finished - origin, finished - started)
        self._batches[batch.handle.id] = 0
        self._created += self.spec.batch_size

    async def _refresh(self, run: SteadyRun, origin: float) -> int:
        app, _ = self.require()
        th, _ = app.tallyho()
        for batch_id in list(self._batches):
            started = time.monotonic()
            view = await th.handle(batch_id).view()
            finished = time.monotonic()
            run.latencies.add("read_progress", finished - origin, finished - started)
            if view.state in _TERMINAL:
                self._closed_done += view.progress.done
                del self._batches[batch_id]
            else:
                self._batches[batch_id] = view.progress.done
        return self._closed_done + sum(self._batches.values())

    async def _tick(self, run: SteadyRun, origin: float, *, feed: bool) -> bool:
        """Один тик: прогресс и, если есть место, новый батч. ``True`` — батч создан.

        Returns:
            Создан ли батч.
        """
        done = await self._refresh(run, origin)
        at = time.monotonic() - origin
        run.done_points.append((at, done))
        run.created_points.append((at, self._created))
        if feed and self._created - done < self.spec.in_flight:
            await self._create_batch(run, origin)
            return True
        return False

    async def run(
        self,
        duration: float,
        *,
        actions: tuple[Action, ...] = (),
        feed: bool = True,
        tolerate_outage: bool = False,
    ) -> SteadyRun:
        """Держать нагрузку ``duration`` секунд; ``actions`` — ``(момент, действие)``.

        ``tolerate_outage`` — ошибки соединения producer с PostgreSQL (P-09 гасит БД)
        не прерывают прогон: тик пропускается и записывается в ``run.outage_ticks``.

        Ошибки producer при выключенном ``tolerate_outage`` и ошибки действий пробрасываются.

        Returns:
            Точки завершений, задержки producer и события воркеров.
        """
        _, pool = self.require()
        run = SteadyRun(self.spec, wall_origin=time.time())
        origin = time.monotonic()
        pending = sorted(actions, key=operator.itemgetter(0))
        background: list[asyncio.Task[None]] = []
        try:
            while (now := time.monotonic() - origin) < duration:
                while pending and pending[0][0] <= now:
                    _, action = pending.pop(0)
                    background.append(asyncio.create_task(action(), name="bench-action"))
                if await self._guarded_tick(run, origin, feed=feed, tolerate=tolerate_outage):
                    continue
                _raise_failed(background)
                if all(task.done() for task in background):
                    pool.assert_alive()
                await asyncio.sleep(_TICK)
        finally:
            if background:
                _ = await asyncio.wait(background)
        _raise_failed(background)
        run.events = pool.events()
        return run

    async def _guarded_tick(
        self, run: SteadyRun, origin: float, *, feed: bool, tolerate: bool
    ) -> bool:
        try:
            return await self._tick(run, origin, feed=feed)
        except (OSError, DBAPIError, TallyhoError):
            if not tolerate:
                raise
            run.outage_ticks += 1
            return False

    async def drain(self, run: SteadyRun, within: float = _DRAIN_TIMEOUT) -> None:
        """Дождаться финализации всех созданных батчей и проверить оракул."""
        origin = time.monotonic() - (time.time() - run.wall_origin)
        async with asyncio.timeout(within):
            while self._batches:
                _ = await self._refresh(run, origin)
                await asyncio.sleep(_TICK)
        _, pool = self.require()
        run.events = pool.events()
        run.oracle = await self.oracle()

    async def oracle(self) -> OracleReport:
        """Оракул на выборке: состояния батчей, единственность финализации, эффект один раз.

        Returns:
            Найденные нарушения.
        """
        app, _ = self.require()
        report = OracleReport()
        batch = sql(
            "SELECT state, count(*) FROM {batch} WHERE kind = :kind GROUP BY state",
            batch=ident(self.config.schemas.tallyho, "th_batch"),
        )
        domain = app.domain
        async with app.engine.connect() as connection:
            states = {
                int(cast("int", row[0])): int(cast("int", row[1]))
                for row in await connection.execute(batch, {"kind": KIND_S1})
            }
            report.batches = sum(states.values())
            not_succeeded = report.batches - states.get(int(BatchState.SUCCEEDED), 0)
            if not_succeeded:
                report.violations.append(f"{not_succeeded} батчей не succeeded: {states}")
            finals = select(domain.finalized.c.batch_id, func.count()).group_by(
                domain.finalized.c.batch_id
            )
            counts = [int(row[1]) for row in await connection.execute(finals)]
            if len(counts) != report.batches or any(count != 1 for count in counts):
                report.violations.append(
                    prose(
                        f"""
                        финализаций: {len(counts)} батчей с хуком, повторных
                        {sum(1 for count in counts if count != 1)} (ожидалось ровно 1 на батч)
                        """
                    )
                )
            rows = int(
                cast(
                    "int",
                    await connection.scalar(select(func.count()).select_from(domain.invoices)),
                )
            )
        report.items = self._created
        if rows != self._created:
            report.violations.append(f"доменных строк {rows}, а Items создано {self._created}")
        return report

    async def stop(self) -> None:
        """Остановить maintenance, воркеры и приложение."""
        if self._maintenance is not None and self._stop_maintenance is not None:
            self._stop_maintenance()
            _ = await asyncio.wait([self._maintenance])
            self._maintenance = None
        if self.pool is not None:
            await self.pool.stop()
        if self.app is not None:
            await self.app.close()


@contextlib.asynccontextmanager
async def steady_load(
    ctx: RunContext,
    spec: SteadySpec,
    *,
    name: str,
    configure: Callable[[AppConfig], AppConfig] | None = None,
    dsn: str | None = None,
) -> AsyncGenerator[tuple[SteadyLoad, Schemas]]:
    """Свежие схемы, запущенная нагрузка S1; всё убирается по выходе.

    ``dsn`` — DSN воркеров и producer (например, через toxiproxy); схемы создаются по
    прямому DSN стенда.

    Yields:
        Нагрузку и её схемы.
    """
    engine = create_async_engine(ctx.stand.dsn)
    try:
        async with schemas(engine, name) as names:
            config = AppConfig(
                dsn=dsn or ctx.stand.dsn,
                schemas=names,
                concurrency=spec.concurrency,
                seed=ctx.seed,
                record_ops=True,
            )
            if configure is not None:
                config = configure(config)
            load = SteadyLoad(replace(config), spec, ctx.out / name)
            try:
                await load.start()
                yield load, names
            finally:
                await load.stop()
    finally:
        await engine.dispose()
