"""Варианты нагрузки «N пустых задач» и их замер: P-01 и база для T11.6.

:class:`LoadVariant` — общий интерфейс варианта: поставить задачи ``[first, first + count)``,
дождаться их выполнения, отдать задержки по задачам. :func:`measure` прогоняет вариант по
:class:`OverheadSpec` (прогрев, повторы) и возвращает :class:`RunMeasurement` на каждый
повтор. Реализации здесь — «голый» flexiq и tallyho поверх той же конфигурации flexiq;
T11.6 добавляет варианты taskiq (InMemory, Redis), реализуя тот же протокол.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Protocol, cast
from uuid import UUID

from typing_extensions import override

from benchmarks.app import KIND_NOOP, NOOP_TASK, AppConfig, Variant, build_app
from benchmarks.metrics import summarize
from benchmarks.stand import BenchError, ident, sql
from benchmarks.workers import WorkerPool
from tallyho.model.states import BatchState

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from sqlalchemy.ext.asyncio import AsyncEngine

    from benchmarks.app import BenchApp
    from benchmarks.metrics import Summary
    from benchmarks.stand import Schemas

__all__ = [
    "Completion",
    "FlexiqVariant",
    "LoadVariant",
    "OverheadSpec",
    "RunMeasurement",
    "TallyhoVariant",
    "TaskTimings",
    "measure",
    "median_run",
]

_ENQUEUE_CHUNK: Final = 5_000
_POLL: Final = 0.05
_TERMINAL: Final = frozenset(
    {
        BatchState.SUCCEEDED,
        BatchState.COMPLETED_WITH_ERRORS,
        BatchState.FAILED,
        BatchState.CANCELLED,
    }
)


@dataclass(frozen=True, slots=True, kw_only=True)
class OverheadSpec:
    """Параметры замера варианта.

    Attributes:
        tasks: задач в одном повторе.
        processes: процессов-воркеров.
        concurrency: ``workers``/``async_concurrency`` flexiq одного процесса.
        warmup: задач прогрева (не входят в результат).
        repeats: повторов; итог — медиана по пропускной способности.
        timeout: предел ожидания одного повтора, секунды.
        print_output: задача печатает ``task {i}`` (T11.6), вывод воркеров — в их лог.
        scheduler_batch: ``scheduler_batch_size`` flexiq (сколько джоб за круг опроса).
    """

    tasks: int
    processes: int
    concurrency: int
    warmup: int
    repeats: int
    timeout: float
    print_output: bool = False
    scheduler_batch: int = 1


@dataclass(frozen=True, slots=True)
class Completion:
    """Когда повтор закончился (``time.monotonic``)."""

    done_at: float
    finalized_at: float | None = None


@dataclass(slots=True)
class TaskTimings:
    """Задержки по задачам, секунды.

    Attributes:
        latency: «поставлена → выполнена» (итог записан брокером или tallyho).
        dispatch: «вызов функции задачи воркером → начало тела» (у tallyho — обёртка и claim).
        service: «вызов функции задачи → итог записан»: у flexiq — возврат из функции, у
            tallyho — ``max(finished_at Item, возврат из функции)``.
    """

    latency: list[float] = field(default_factory=list[float])
    dispatch: list[float] = field(default_factory=list[float])
    service: list[float] = field(default_factory=list[float])


class _EnqueueMany(Protocol):
    def __call__(self, task_name: str, /, *, args_list: list[tuple[int]]) -> object: ...


class LoadVariant(Protocol):
    """Вариант нагрузки «N одинаковых задач» (P-01; T11.6 добавляет taskiq)."""

    @property
    def name(self) -> str:
        """Короткое имя варианта для отчёта."""
        ...

    async def start(self, spec: OverheadSpec, root: Path) -> None:
        """Подготовить хранилище и запустить воркеры; ``root`` — каталог логов варианта."""
        ...

    async def submit(self, first: int, count: int) -> None:
        """Поставить задачи ``first … first + count - 1``; вернуться после фиксации."""
        ...

    async def wait(self, first: int, count: int, within: float) -> Completion:
        """Дождаться выполнения всех задач повтора.

        Returns:
            Момент завершения.
        """
        ...

    async def timings(self, first: int, count: int) -> TaskTimings:
        """Задержки задач повтора (после :meth:`wait`).

        Returns:
            Задержки задач повтора.
        """
        ...

    async def stop(self) -> None:
        """Остановить воркеры и освободить ресурсы."""
        ...


@dataclass(frozen=True, slots=True)
class RunMeasurement:
    """Один повтор варианта."""

    variant: str
    repeat: int
    tasks: int
    enqueue_s: float
    total_s: float
    finalize_s: float | None
    latency: Summary
    dispatch: Summary
    service: Summary

    @property
    def throughput(self) -> float:
        """Задач в секунду: от начала постановки до выполнения последней."""
        return self.tasks / self.total_s if self.total_s > 0 else 0.0


async def measure(variant: LoadVariant, spec: OverheadSpec, root: Path) -> list[RunMeasurement]:
    """Прогреть вариант и выполнить ``spec.repeats`` повторов.

    Returns:
        Замер каждого повтора.
    """
    await variant.start(spec, root)
    runs: list[RunMeasurement] = []
    try:
        first = 0
        if spec.warmup > 0:
            await variant.submit(first, spec.warmup)
            _ = await variant.wait(first, spec.warmup, spec.timeout)
            first += spec.warmup
        for repeat in range(spec.repeats):
            started = time.monotonic()
            await variant.submit(first, spec.tasks)
            enqueued = time.monotonic()
            completion = await variant.wait(first, spec.tasks, spec.timeout)
            timings = await variant.timings(first, spec.tasks)
            runs.append(
                RunMeasurement(
                    variant=variant.name,
                    repeat=repeat,
                    tasks=spec.tasks,
                    enqueue_s=enqueued - started,
                    total_s=completion.done_at - started,
                    finalize_s=None
                    if completion.finalized_at is None
                    else completion.finalized_at - started,
                    latency=summarize(timings.latency),
                    dispatch=summarize(timings.dispatch),
                    service=summarize(timings.service),
                )
            )
            first += spec.tasks
    finally:
        await variant.stop()
    return runs


def median_run(runs: Sequence[RunMeasurement]) -> RunMeasurement:
    """Повтор с медианной пропускной способностью.

    Returns:
        Медианный повтор.

    Raises:
        BenchError: повторов нет.
    """
    if not runs:
        message = "нет ни одного повтора"
        raise BenchError(message)
    target = statistics.median_low([run.throughput for run in runs])
    return next(run for run in runs if run.throughput == target)


_ARCHIVED: Final = """
SELECT id, created_at, started_at, completed_at FROM {jobs}
WHERE task_name = :task AND created_at >= :since AND completed_at IS NOT NULL
"""
_ARCHIVED_COUNT: Final = """
SELECT count(*) FROM {jobs}
WHERE task_name = :task AND created_at >= :since AND completed_at IS NOT NULL
"""
_ITEMS: Final = """
SELECT id, extract(epoch FROM created_at), extract(epoch FROM finished_at) FROM {item}
WHERE batch_id = :batch AND finished_at IS NOT NULL
"""


async def _archived_jobs(
    engine: AsyncEngine, schema: str, since_ms: int
) -> list[tuple[str, int, int, int]]:
    """Завершённые джобы flexiq: ``(id, created_at, started_at, completed_at)``, мс.

    Returns:
        Джобы задачи ``bench.noop``, поставленные не раньше ``since_ms``.
    """
    statement = sql(_ARCHIVED, jobs=ident(schema, "archived_jobs"))
    async with engine.connect() as connection:
        rows = (await connection.execute(statement, {"task": NOOP_TASK, "since": since_ms})).all()
    return [
        (cast("str", row[0]), cast("int", row[1]), cast("int", row[2]), cast("int", row[3]))
        for row in rows
    ]


@dataclass(slots=True)
class _Base:
    """Общая часть вариантов на flexiq: приложение, пул воркеров, окно времени повтора."""

    dsn: str
    schemas: Schemas
    app: BenchApp | None = None
    pool: WorkerPool | None = None
    _submitted_ms: dict[int, int] = field(default_factory=dict[int, int])

    def _config(self, spec: OverheadSpec, variant: Variant) -> AppConfig:
        return AppConfig(
            dsn=self.dsn,
            schemas=self.schemas,
            variant=variant,
            concurrency=spec.concurrency,
            scheduler_batch=spec.scheduler_batch,
            print_output=spec.print_output,
            record_bodies=True,
        )

    async def _start(self, spec: OverheadSpec, root: Path, variant: Variant) -> BenchApp:
        config = self._config(spec, variant)
        app = build_app(config)
        await app.migrate()
        # flexiq создаёт свои таблицы при первом обращении; воркеры не должны гоняться за это.
        await asyncio.to_thread(app.queue.stats)
        self.app = app
        self.pool = WorkerPool(root, config)
        await self.pool.start(spec.processes)
        return app

    def _require(self) -> tuple[BenchApp, WorkerPool]:
        if self.app is None or self.pool is None:
            message = "вариант не запущен"
            raise BenchError(message)
        return self.app, self.pool

    async def stop(self) -> None:
        """Остановить воркеры и закрыть приложение."""
        if self.pool is not None:
            await self.pool.stop()
        if self.app is not None:
            await self.app.close()


@dataclass(slots=True)
class FlexiqVariant(_Base):
    """«Голый» flexiq: ``queue.enqueue_many`` той же пустой задачи, без tallyho."""

    @property
    def name(self) -> str:
        """Имя варианта."""
        return "flexiq"

    async def start(self, spec: OverheadSpec, root: Path) -> None:
        """Запустить воркеры варианта."""
        _ = await self._start(spec, root, Variant.FLEXIQ)

    async def submit(self, first: int, count: int) -> None:
        """Поставить задачи пачками по 5 000 (одна транзакция flexiq на пачку)."""
        app, _ = self._require()
        self._submitted_ms[first] = int(time.time() * 1000) - 1
        for start in range(first, first + count, _ENQUEUE_CHUNK):
            stop = min(start + _ENQUEUE_CHUNK, first + count)
            enqueue_many = cast("_EnqueueMany", app.queue.enqueue_many)
            _ = await asyncio.to_thread(
                enqueue_many, NOOP_TASK, args_list=[(i,) for i in range(start, stop)]
            )

    async def wait(self, first: int, count: int, within: float) -> Completion:
        """Ждать, пока все джобы повтора попадут в архив flexiq выполненными.

        Returns:
            Момент завершения.
        """
        app, pool = self._require()
        since = self._submitted_ms[first]
        statement = sql(_ARCHIVED_COUNT, jobs=ident(self.schemas.flexiq, "archived_jobs"))
        async with asyncio.timeout(within):
            while True:
                async with app.engine.connect() as connection:
                    done = int(
                        cast(
                            "int",
                            await connection.scalar(statement, {"task": NOOP_TASK, "since": since}),
                        )
                    )
                if done >= count:
                    return Completion(time.monotonic())
                pool.assert_alive()
                await asyncio.sleep(_POLL)

    async def timings(self, first: int, count: int) -> TaskTimings:
        """Задержки по архиву flexiq и моментам middleware и тела задачи.

        Returns:
            Задержки задач повтора.
        """
        _ = count
        app, pool = self._require()
        events = pool.events()
        bodies = {job: at for at, job, _ in events.bodies}
        result = TaskTimings()
        for job_id, created, _, completed in await _archived_jobs(
            app.engine, self.schemas.flexiq, self._submitted_ms[first]
        ):
            result.latency.append((completed - created) / 1000)
            before = events.before.get(job_id)
            after = events.after.get(job_id)
            body = bodies.get(job_id)
            if before is None or after is None or body is None:
                continue
            result.dispatch.append(body - before)
            result.service.append(after - before)
        return result


@dataclass(slots=True)
class TallyhoVariant(_Base):
    """tallyho поверх той же конфигурации flexiq: один батч на повтор через публичный API."""

    batches: dict[int, UUID] = field(default_factory=dict[int, UUID])
    _maintenance: asyncio.Task[None] | None = None
    _stop_maintenance: Callable[[], None] | None = None

    @property
    def name(self) -> str:
        """Имя варианта."""
        return "tallyho"

    async def start(self, spec: OverheadSpec, root: Path) -> None:
        """Запустить воркеры и maintenance в этом процессе (как в API-процессе)."""
        app = await self._start(spec, root, Variant.TALLYHO)
        th, _ = app.tallyho()
        runner = th.maintenance()
        self._maintenance = asyncio.create_task(runner.run(), name="bench-maintenance")
        self._stop_maintenance = runner.stop

    async def submit(self, first: int, count: int) -> None:
        """Создать батч ``bench.noop`` на ``count`` задач одной транзакцией."""
        app, _ = self._require()
        th, tasks = app.tallyho()
        self._submitted_ms[first] = int(time.time() * 1000) - 1
        async with th.batch(KIND_NOOP, key=f"run:{first}", expected_total=count) as batch:
            await batch.add_calls(th.call(tasks.noop, i) for i in range(first, first + count))
        self.batches[first] = batch.handle.id

    async def wait(self, first: int, count: int, within: float) -> Completion:
        """Ждать завершения всех Items, затем финализации батча.

        Returns:
            Момент завершения.
        """
        app, pool = self._require()
        th, _ = app.tallyho()
        handle = th.handle(self.batches[first])
        done_at: float | None = None
        async with asyncio.timeout(within):
            while True:
                view = await handle.view()
                now = time.monotonic()
                if done_at is None and view.progress.done >= count:
                    done_at = now
                if view.state in _TERMINAL:
                    return Completion(done_at or now, now)
                pool.assert_alive()
                await asyncio.sleep(_POLL)

    async def timings(self, first: int, count: int) -> TaskTimings:
        """Задержки по ``th_item`` (создан → итог записан) и моментам воркеров.

        Returns:
            Задержки задач повтора.
        """
        _ = count
        app, pool = self._require()
        items_sql = sql(_ITEMS, item=ident(self.schemas.tallyho, "th_item"))
        async with app.engine.connect() as connection:
            rows = (await connection.execute(items_sql, {"batch": self.batches[first]})).all()
        finished = {
            str(cast("UUID", row[0])): (float(cast("float", row[1])), float(cast("float", row[2])))
            for row in rows
        }
        events = pool.events()
        result = TaskTimings(latency=[done - created for created, done in finished.values()])
        for at, job_id, item_id in events.bodies:
            item_times = finished.get(item_id)
            before = events.before.get(job_id)
            after = events.after.get(job_id)
            if item_times is None or before is None or after is None:
                continue
            result.dispatch.append(at - before)
            result.service.append(max(item_times[1], after) - before)
        return result

    @override
    async def stop(self) -> None:
        """Остановить maintenance, воркеры и приложение."""
        if self._maintenance is not None and self._stop_maintenance is not None:
            self._stop_maintenance()
            _ = await asyncio.wait([self._maintenance])
            self._maintenance = None
        await _Base.stop(self)
