"""Приложение бенчмарка: одни и те же задачи и хуки в producer и в процессах воркеров.

Два варианта на одной конфигурации flexiq (P-01, T11.6): ``flexiq`` — «голая» задача через
``queue.task``; ``tallyho`` — те же задачи через ``FlexiqAdapter`` и учёт tallyho. Задачи:

* ``noop(i)`` — пустая задача (P-01); при ``print_output`` печатает ``task {i}`` (T11.6);
* ``s1(invoice_id)`` — S1 из ACCEPTANCE §3.2: доменная строка и ``complete_in`` одной
  транзакцией, опционально «сеть» и удержание транзакции (P-02, P-05, P-06);
* ``page`` → ``card`` → ``pdf`` — конвейер S3 через ``spawn`` в этапы (P-07);
* ``step(i)`` — медленная задача с прогрессом для снимков (P-08).

Воркер пишет в ``stats_path`` события наблюдателя tallyho (длительность групповых
транзакций Completer, размер его буфера, отправки relay) и моменты начала тела задач.
"""

from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from flexiq import Queue, current_job
from flexiq.middleware import TaskMiddleware
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    Text,
    Uuid,
    func,
    insert,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

from benchmarks.stand import Schemas
from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.protocols.observer import NullObserver

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from flexiq.context import JobContext
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
    from sqlalchemy.sql.base import ReadOnlyColumnCollection

    from tallyho.model.states import BatchState
    from tallyho.model.views import BatchSummary

    DomainTable = Table[ReadOnlyColumnCollection[str, Column[object]]]

__all__ = [
    "KIND_HISTORY",
    "KIND_NOOP",
    "KIND_PIPELINE",
    "KIND_PROGRESS",
    "KIND_S1",
    "NOOP_TASK",
    "AppConfig",
    "BenchApp",
    "BenchTasks",
    "Domain",
    "Variant",
    "WorkerStats",
    "build_app",
    "flexiq_url",
]

KIND_NOOP: Final = "bench.noop"
KIND_S1: Final = "bench.s1"
KIND_PIPELINE: Final = "bench.pipeline"
KIND_PROGRESS: Final = "bench.progress"
KIND_HISTORY: Final = "bench.history"
NOOP_TASK: Final = "bench.noop"
_FINALIZED_KINDS: Final = (KIND_NOOP, KIND_S1, KIND_PIPELINE, KIND_PROGRESS)
_DUMP_EVERY: Final = 1.0


class Variant(StrEnum):
    """Вариант нагрузки P-01: та же конфигурация flexiq с учётом tallyho и без."""

    FLEXIQ = "flexiq"
    TALLYHO = "tallyho"


@dataclass(frozen=True, slots=True, kw_only=True)
class AppConfig:
    """Конфигурация, общая для producer и воркеров (передаётся воркеру JSON-файлом).

    Attributes:
        dsn: DSN SQLAlchemy (``postgresql+asyncpg://``).
        schemas: схемы tallyho, flexiq и домена.
        variant: ``tallyho`` или «голый» ``flexiq``.
        concurrency: ``workers`` и ``async_concurrency`` flexiq одного процесса.
        seed: seed «сети» и выбора удерживаемых транзакций.
        sleep_min: нижняя граница «сети» в задаче S1, секунды.
        sleep_max: верхняя граница «сети» в задаче S1, секунды.
        hold_share: доля задач S1, держащих доменную транзакцию открытой ещё одну «сеть».
        print_output: ``noop`` печатает ``task {i}`` в stdout воркера (T11.6).
        record_bodies: записывать момент начала тела ``noop`` (задержки P-01).
        record_ops: записывать длительности операций в воркере: ``claim`` (вызов функции
            задачи → начало тела, т. е. обёртка и claim tallyho) и ``finish`` (``complete_in``
            и commit доменной транзакции S1).
        cards_per_page: сколько карточек порождает страница конвейера.
        pdfs_per_card: сколько PDF порождает карточка (0 — пустой этап).
        progress_every: ``every`` хука ``on_progress`` вида ``bench.progress``, секунды.
        step_seconds: длительность задачи ``step``.
        sweep_interval: ``sweep_interval`` tallyho, секунды.
        stats_path: куда воркер пишет события (JSON lines); ``None`` — не писать.
    """

    dsn: str
    schemas: Schemas
    variant: Variant = Variant.TALLYHO
    concurrency: int = 10
    seed: int = 1
    sleep_min: float = 0.0
    sleep_max: float = 0.0
    hold_share: float = 0.0
    print_output: bool = False
    record_bodies: bool = False
    record_ops: bool = False
    cards_per_page: int = 0
    pdfs_per_card: int = 0
    progress_every: float = 2.0
    step_seconds: float = 0.0
    sweep_interval: float = 5.0
    stats_path: str | None = None

    def to_json(self) -> str:
        """Сериализовать для воркера.

        Returns:
            JSON-строка конфигурации.
        """
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> AppConfig:
        """Восстановить из :meth:`to_json`.

        Returns:
            Конфигурация.
        """
        data = cast("dict[str, object]", json.loads(raw))
        schemas = cast("dict[str, str]", data["schemas"])
        stats_path = data["stats_path"]

        def number(name: str) -> float:
            return float(cast("float", data[name]))

        return cls(
            dsn=str(data["dsn"]),
            schemas=Schemas(**schemas),
            variant=Variant(str(data["variant"])),
            concurrency=int(number("concurrency")),
            seed=int(number("seed")),
            sleep_min=number("sleep_min"),
            sleep_max=number("sleep_max"),
            hold_share=number("hold_share"),
            print_output=bool(data["print_output"]),
            record_bodies=bool(data["record_bodies"]),
            record_ops=bool(data["record_ops"]),
            cards_per_page=int(number("cards_per_page")),
            pdfs_per_card=int(number("pdfs_per_card")),
            progress_every=number("progress_every"),
            step_seconds=number("step_seconds"),
            sweep_interval=number("sweep_interval"),
            stats_path=None if stats_path is None else str(stats_path),
        )


@dataclass(slots=True)
class WorkerStats:
    """Буфер событий воркера; фоновый поток дописывает их в файл раз в секунду."""

    path: Path | None
    _events: list[list[object]] = field(default_factory=list[list[object]])
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None
    _started: dict[str, float] = field(default_factory=dict[str, float])

    def called(self, job_id: str, at: float) -> None:
        """Момент вызова функции задачи (из middleware)."""
        self._started[job_id] = at

    def body_started(self, job_id: str) -> None:
        """Начало тела задачи: записать операцию ``claim`` (обёртка tallyho и claim)."""
        called = self._started.pop(job_id, None)
        if called is not None:
            now = time.time()
            self.add("op", now, "claim", now - called)

    def add(self, *event: object) -> None:
        """Записать событие ``[тип, момент time.time(), ...]``."""
        if self.path is None:
            return
        with self._lock:
            self._events.append(list(event))

    def start(self) -> None:
        """Запустить фоновую запись."""
        if self.path is None:
            return
        self._thread = threading.Thread(target=self._loop, name="bench-stats", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(_DUMP_EVERY):
            self.dump()

    def dump(self) -> None:
        """Дописать накопленные события в файл."""
        if self.path is None:
            return
        with self._lock:
            events, self._events = self._events, []
        if events:
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.writelines(json.dumps(event) + "\n" for event in events)

    def close(self) -> None:
        """Остановить поток и дописать остаток."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.dump()


class _StatsObserver(NullObserver):
    """Наблюдатель tallyho, пишущий события Completer и relay в :class:`WorkerStats`."""

    def __init__(self, stats: WorkerStats) -> None:
        self._stats: WorkerStats = stats

    @override
    def completer_flush(self, *, items: int, duration: float) -> None:
        self._stats.add("flush", time.time(), items, duration)

    @override
    def completer_buffer(self, *, items: int) -> None:
        self._stats.add("buffer", time.time(), items)

    @override
    def relay_dispatched(self, *, messages: int, duration: float) -> None:
        self._stats.add("relay", time.time(), messages, duration)


@dataclass(frozen=True, slots=True)
class Domain:
    """Доменные таблицы бенчмарка (схема ``schemas.domain``)."""

    metadata: MetaData
    invoices: DomainTable
    finalized: DomainTable
    progress: DomainTable


def _domain(schema: str) -> Domain:
    metadata = MetaData(schema=schema)
    invoices = Table(
        "invoice_done",
        metadata,
        Column("item_id", Uuid(), primary_key=True),
        Column("invoice_id", BigInteger(), nullable=False),
    )
    finalized = Table(
        "finalized",
        metadata,
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("batch_id", Uuid(), nullable=False, index=True),
        Column("kind", Text(), nullable=False),
        Column("state", Integer(), nullable=False),
        Column(
            "at", DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
        ),
    )
    progress = Table(
        "progress_log",
        metadata,
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("batch_id", Uuid(), nullable=False, index=True),
        Column("seq", Integer(), nullable=False),
        Column("done", BigInteger(), nullable=False),
        Column(
            "at", DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
        ),
    )
    return Domain(metadata, invoices, finalized, progress)


@dataclass(frozen=True, slots=True)
class BenchTasks:
    """Зарегистрированные задачи варианта ``tallyho``."""

    noop: Callable[[int], Awaitable[None]]
    s1: Callable[[int], Awaitable[None]]
    page: Callable[[int, int], Awaitable[None]]
    card: Callable[[int, int, int], Awaitable[None]]
    pdf: Callable[[int, int, int], Awaitable[None]]
    step: Callable[[int], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BenchApp:
    """Процесс приложения: flexiq, при варианте ``tallyho`` — установка tallyho и задачи."""

    config: AppConfig
    engine: AsyncEngine
    queue: Queue
    domain: Domain
    stats: WorkerStats
    th: Tallyho | None
    adapter: FlexiqAdapter | None
    tasks: BenchTasks | None

    def tallyho(self) -> tuple[Tallyho, BenchTasks]:
        """Установка tallyho и задачи; только для варианта ``tallyho``.

        Returns:
            Пара «установка, задачи».

        Raises:
            TypeError: вариант ``flexiq``.
        """
        if self.th is None or self.tasks is None:
            message = "вариант flexiq не использует tallyho"
            raise TypeError(message)
        return self.th, self.tasks

    async def migrate(self) -> None:
        """Создать доменные таблицы и таблицы tallyho; flexiq создаёт свои сам."""
        async with self.engine.begin() as connection:
            await connection.run_sync(self.domain.metadata.create_all)
        if self.th is not None:
            _ = await self.th.migrate()

    async def close(self) -> None:
        """Закрыть установку и клиентов (в том же event loop, где ими пользовались)."""
        if self.th is not None:
            await self.th.aclose()
        self.queue.close()
        if self.adapter is not None:
            await self.adapter.close()
        await self.engine.dispose()
        self.stats.close()


class _TimingMiddleware(TaskMiddleware):
    """Моменты до и после вызова зарегистрированной функции задачи (вместе с обёрткой tallyho)."""

    def __init__(self, stats: WorkerStats) -> None:
        super().__init__()
        self._stats: WorkerStats = stats

    @override
    def before(self, ctx: JobContext) -> None:
        now = time.time()
        self._stats.called(ctx.id, now)
        self._stats.add("before", now, ctx.id)

    @override
    def after(self, ctx: JobContext, result: object, error: Exception | None) -> None:
        _ = result, error
        self._stats.add("after", time.time(), ctx.id)


def flexiq_url(dsn: str) -> str:
    """DSN без драйвера SQLAlchemy — так его ждёт flexiq.

    Returns:
        DSN вида ``postgresql://…``.
    """
    return dsn.replace("postgresql+asyncpg://", "postgresql://", 1)


def _queue(config: AppConfig, stats: WorkerStats) -> Queue:
    # Одинаковая конфигурация flexiq для обоих вариантов P-01 (и для T11.6). Замер моментов —
    # middleware без сторожа времени (middleware_timeout=0), чтобы не платить за поток-сторож.
    timed = config.record_bodies or config.record_ops
    middleware: list[TaskMiddleware] = [_TimingMiddleware(stats)] if timed else []
    return Queue(
        backend="postgres",
        db_url=flexiq_url(config.dsn),
        schema=config.schemas.flexiq,
        workers=config.concurrency,
        async_concurrency=config.concurrency,
        drain_timeout=5,
        middleware=middleware,
        middleware_timeout=0,
    )


def _record_body(stats: WorkerStats, item_id: UUID | None) -> None:
    stats.add("body", time.time(), current_job.id, "" if item_id is None else str(item_id))


def _build_flexiq(config: AppConfig, base: BenchApp) -> BenchApp:
    stats = base.stats

    async def noop(i: int) -> None:  # ruff: ignore[unused-async]  # задача flexiq обязана быть корутиной
        if config.record_bodies:
            _record_body(stats, None)
        if config.print_output:
            print(f"task {i}")  # ruff: ignore[print]  # T11.6: вывод воркера идёт в его лог-файл

    decorator: object = base.queue.task(name=NOOP_TASK, max_retries=0)  # pyright: ignore[reportUnknownMemberType]  # Queue.task flexiq типизирован через Any
    register = cast("Callable[[Callable[[int], Awaitable[None]]], object]", decorator)
    _ = register(noop)
    return base


@dataclass(frozen=True, slots=True)
class _Env:
    """Что нужно задачам варианта ``tallyho`` в процессе."""

    config: AppConfig
    stats: WorkerStats
    th: Tallyho
    adapter: FlexiqAdapter
    domain: Domain
    tx_engine: AsyncEngine

    def network(self, identity: int, namespace: str) -> float:
        """Детерминированная «сеть» задачи S1, секунды.

        Returns:
            0, если «сеть» выключена.
        """
        if self.config.sleep_max <= 0:
            return 0.0
        rng = random.Random(f"{self.config.seed}:{namespace}:{identity}")  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # детерминированная нагрузка, не криптография
        return rng.uniform(self.config.sleep_min, self.config.sleep_max)

    def holds(self, identity: int) -> bool:
        """Держит ли задача S1 доменную транзакцию открытой (доля ``hold_share``).

        Returns:
            Решение для этой задачи, одинаковое при повторах.
        """
        rng = random.Random(f"{self.config.seed}:hold:{identity}")  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # детерминированная нагрузка, не криптография
        return rng.random() < self.config.hold_share

    def body_started(self) -> None:
        """Записать ``claim`` для текущей джобы, если операции записываются."""
        if self.config.record_ops:
            self.stats.body_started(current_job.id)


def _simple_tasks(
    env: _Env,
) -> tuple[Callable[[int], Awaitable[None]], Callable[[int], Awaitable[None]]]:
    config, stats = env.config, env.stats

    @env.adapter.task(name=NOOP_TASK, max_retries=0)
    async def noop(i: int) -> None:  # ruff: ignore[unused-async]  # задача flexiq обязана быть корутиной
        env.body_started()
        if config.record_bodies:
            _record_body(stats, item.id())
        if config.print_output:
            print(f"task {i}")  # ruff: ignore[print]  # T11.6: вывод воркера идёт в его лог-файл

    @env.adapter.task(name="bench.step", max_retries=3)
    async def step(i: int) -> None:
        _ = i
        env.body_started()
        await asyncio.sleep(config.step_seconds)

    return noop, step


def _s1_task(env: _Env) -> Callable[[int], Awaitable[None]]:
    @env.adapter.task(name="bench.s1", max_retries=3)
    async def s1(invoice_id: int) -> None:
        env.body_started()
        if (delay := env.network(invoice_id, "s1")) > 0:
            await asyncio.sleep(delay)
        item_id = item.id()
        async with env.tx_engine.begin() as connection:
            if env.holds(invoice_id):
                await asyncio.sleep(env.network(invoice_id, "s1-hold"))
            statement = pg_insert(env.domain.invoices).values(
                item_id=item_id, invoice_id=invoice_id
            )
            _ = await connection.execute(statement.on_conflict_do_nothing())
            item.ok("rendered")
            finish_started = time.perf_counter()
            await item.complete_in(connection)
        if env.config.record_ops:
            env.stats.add("op", time.time(), "finish", time.perf_counter() - finish_started)

    return s1


def _pipeline_tasks(
    env: _Env,
) -> tuple[
    Callable[[int, int], Awaitable[None]],
    Callable[[int, int, int], Awaitable[None]],
    Callable[[int, int, int], Awaitable[None]],
]:
    config, th = env.config, env.th

    @env.adapter.task(name="bench.page", max_retries=3)
    async def page(run: int, page_no: int) -> None:  # ruff: ignore[unused-async]  # задача flexiq обязана быть корутиной
        env.body_started()
        for card_no in range(config.cards_per_page):
            item.spawn_call(
                th.call(card, run, page_no, card_no).opts(key=f"card:{page_no}:{card_no}"),
                into="cards",
            )

    @env.adapter.task(name="bench.card", max_retries=3)
    async def card(run: int, page_no: int, card_no: int) -> None:  # ruff: ignore[unused-async]  # задача flexiq обязана быть корутиной
        _ = run
        env.body_started()
        for pdf_no in range(config.pdfs_per_card):
            item.spawn_call(
                th.call(pdf, page_no, card_no, pdf_no).opts(
                    key=f"pdf:{page_no}:{card_no}:{pdf_no}"
                ),
                into="pdfs",
            )

    @env.adapter.task(name="bench.pdf", max_retries=3)
    async def pdf(page_no: int, card_no: int, pdf_no: int) -> None:  # ruff: ignore[unused-async]  # задача flexiq обязана быть корутиной
        _ = page_no, card_no, pdf_no
        env.body_started()

    return page, card, pdf


def _register_hooks(env: _Env) -> None:
    domain = env.domain

    async def write_finalized(session: AsyncSession, summary: BatchSummary) -> None:
        state: BatchState = summary.state
        _ = await session.execute(
            insert(domain.finalized).values(
                batch_id=summary.id, kind=summary.kind, state=int(state)
            )
        )

    for kind in _FINALIZED_KINDS:
        _ = env.th.on_finalized(kind)(write_finalized)

    async def progress(session: AsyncSession, summary: BatchSummary) -> None:
        _ = await session.execute(
            insert(domain.progress).values(
                batch_id=summary.id,
                seq=summary.seq,
                done=summary.progress.done,
                # Часы процесса, как у finished_at Items: P-08 сравнивает их между собой.
                at=datetime.now(UTC),
            )
        )

    every = timedelta(seconds=env.config.progress_every)
    _ = env.th.on_progress(KIND_PROGRESS, every=every)(progress)


def build_app(config: AppConfig) -> BenchApp:
    """Собрать процесс приложения бенчмарка: одни и те же задачи в producer и воркерах.

    Returns:
        Приложение процесса.
    """
    stats = WorkerStats(None if config.stats_path is None else Path(config.stats_path))
    engine = create_async_engine(config.dsn, pool_size=10, max_overflow=20)
    domain = _domain(config.schemas.domain)
    queue = _queue(config, stats)
    base = BenchApp(config, engine, queue, domain, stats, None, None, None)
    if config.variant is Variant.FLEXIQ:
        return _build_flexiq(config, base)
    adapter = FlexiqAdapter(queue)
    th = Tallyho(
        engine,
        schema=config.schemas.tallyho,
        observer=_StatsObserver(stats),
        sweep_interval=timedelta(seconds=config.sweep_interval),
    )
    th.install(adapter)
    env = _Env(
        config=config,
        stats=stats,
        th=th,
        adapter=adapter,
        domain=domain,
        tx_engine=engine.execution_options(schema_translate_map={None: config.schemas.tallyho}),
    )
    noop, step = _simple_tasks(env)
    page, card, pdf = _pipeline_tasks(env)
    tasks = BenchTasks(noop=noop, s1=_s1_task(env), page=page, card=card, pdf=pdf, step=step)
    _register_hooks(env)
    return BenchApp(config, engine, queue, domain, stats, th, adapter, tasks)
