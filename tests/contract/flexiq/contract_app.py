"""Shared real Flexiq application used by producer and subprocess workers."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, cast, final

from flexiq import GzipCodec, Queue, SmartSerializer, TaskMiddleware, current_job
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter

if TYPE_CHECKING:
    from pathlib import Path

    from flexiq.context import JobContext
    from flexiq.predicates.context import PredicateContext
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = [
    "ContractApp",
    "DataClassPayload",
    "ModelPayload",
    "RetryableError",
    "build_app",
    "flexiq_dsn",
]


class Probe(Protocol):
    async def __call__(self, *args: object, **kwargs: object) -> None:
        """Record arbitrary JSON-compatible arguments."""
        ...


class NamedProbe(Probe, Protocol):
    @property
    def name(self) -> str:
        """Return the registered Flexiq task name."""
        ...


class RetryableError(Exception):
    """Failure selected by retry_on in the contract application."""


class FinalError(Exception):
    """Failure selected by dont_retry_on in the contract application."""


@dataclass(frozen=True, slots=True)
class DataClassPayload:
    value: str


class ModelPayload(BaseModel):  # type: ignore[explicit-any]  # pydantic BaseModel generates Any-typed compatibility methods
    value: str


def _json_value(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        raw_dataclass = cast("dict[str, object]", dataclasses.asdict(value))
        return {key: _json_value(item) for key, item in raw_dataclass.items()}
    if isinstance(value, BaseModel):
        raw_model = cast("dict[str, object]", value.model_dump())
        return {key: _json_value(item) for key, item in raw_model.items()}
    if isinstance(value, tuple):
        raw_tuple = cast("tuple[object, ...]", value)
        return [_json_value(item) for item in raw_tuple]
    if isinstance(value, dict):
        raw_dict = cast("dict[object, object]", value)
        return {str(key): _json_value(item) for key, item in raw_dict.items()}
    return value


def flexiq_dsn(dsn: str) -> str:
    """Return the driver-neutral PostgreSQL URL expected by Flexiq."""
    return dsn.replace("postgresql+asyncpg://", "postgresql://", 1)


def _record(root: Path, event: str, **values: object) -> None:
    row = json.dumps({"event": event, **values}, ensure_ascii=False, default=repr)
    with (root / "events.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(f"{row}\n")
        stream.flush()


@final
class _RecordingMiddleware(TaskMiddleware):
    def __init__(self, root: Path, scope: str) -> None:
        super().__init__()
        self._root = root
        self._scope = scope

    @override
    def before(self, ctx: JobContext) -> None:
        _record(self._root, "middleware-before", scope=self._scope, job_id=ctx.id)

    @override
    def after(self, ctx: JobContext, result: object, error: Exception | None) -> None:
        _record(
            self._root,
            "middleware-after",
            scope=self._scope,
            job_id=ctx.id,
            error=type(error).__name__ if error is not None else None,
        )

    @override
    def on_enqueue(
        self,
        task_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        options: dict[str, object],
    ) -> None:
        _record(
            self._root,
            "middleware-enqueue",
            scope=self._scope,
            task_name=task_name,
            has_th="_th" in kwargs,
        )


@dataclass(frozen=True, slots=True)
class ContractApp:
    """All process-local objects and registered functions of the contract app."""

    engine: AsyncEngine
    queue: Queue
    adapter: FlexiqAdapter
    th: Tallyho
    probe: Probe
    tasks: dict[str, Probe]
    ordinary: NamedProbe
    flexiq_schema: str
    dsn: str
    tallyho_schema: str


def build_app(  # ruff: ignore[complex-structure, too-many-statements]  # one factory registers the identical worker/producer task set
    *,
    dsn: str,
    tallyho_schema: str,
    flexiq_schema: str,
    root: Path,
    worker_count: int = 4,
) -> ContractApp:
    """Build one producer or worker instance against shared PostgreSQL schemas."""
    engine = create_async_engine(dsn)
    queue = Queue(
        backend="postgres",
        db_url=flexiq_dsn(dsn),
        schema=flexiq_schema,
        workers=worker_count,
        async_concurrency=worker_count,
        drain_timeout=1,
        scheduler_poll_interval_ms=20,
        scheduler_reap_interval=1,
        middleware=[_RecordingMiddleware(root, "global")],
        codecs={"gzip": GzipCodec()},
    )
    adapter = FlexiqAdapter(queue)
    th = Tallyho(
        engine,
        schema=tallyho_schema,
        relay_grace=timedelta(0),
        finalize_grace=timedelta(0),
        sweep_interval=timedelta(milliseconds=100),
        snapshot_tick=timedelta(milliseconds=50),
        watch_throttle=timedelta(milliseconds=10),
    )
    th.install(adapter)

    @queue.worker_resource("contract_resource")
    def contract_resource() -> str:
        return "injected"

    @queue.before_task
    def before_task(task_name: str, _args: tuple[object, ...], kwargs: dict[str, object]) -> None:
        _record(root, "before-task", task_name=task_name, has_th="_th" in kwargs)

    @adapter.task()
    async def probe(*args: object, **kwargs: object) -> None:
        job = cast("object", current_job)
        _record(
            root,
            "probe",
            args=args,
            kwargs=kwargs,
            task_name=getattr(job, "task_name", None),
            metadata=getattr(job, "metadata", None),
            notes=getattr(job, "notes", None),
            retry_count=getattr(job, "retry_count", None),
            at=time.time(),
        )
        await asyncio.sleep(0)

    @adapter.task(serializer=SmartSerializer(), codecs=["gzip"])
    async def signature(
        required: object,
        default: object = "default",
        *extra: object,
        option: object = None,
        **rest: object,
    ) -> None:
        _record(
            root,
            "signature",
            required=_json_value(required),
            default=_json_value(default),
            extra=_json_value(extra),
            option=_json_value(option),
            rest=_json_value(rest),
            task_name=current_job.task_name,
        )
        await asyncio.sleep(0)

    @adapter.task(
        max_retries=3,
        retry_backoff=0.01,
        retry_delays=[0.01, 0.02, 0.03],
        max_retry_delay=1,
        retry_on=[RetryableError],
        dont_retry_on=[FinalError],
    )
    async def flaky(key: str, fail_until: int, *, final: bool = False) -> None:
        attempt = current_job.retry_count
        _record(root, "flaky", key=key, attempt=attempt, at=time.time())
        if final:
            raise FinalError(key)
        if attempt < fail_until:
            raise RetryableError(key)
        await asyncio.sleep(0)

    @adapter.task(soft_timeout=0.1, max_retries=1, retry_delays=[0.01])
    async def soft_timeout(key: str) -> None:
        _record(root, "soft-start", key=key, attempt=current_job.retry_count)
        await asyncio.sleep(0.15)
        current_job.check_timeout()

    @adapter.task(timeout=1, max_retries=1, retry_delays=[0.01])
    async def hard_timeout(key: str) -> None:
        _record(root, "hard-start", key=key, attempt=current_job.retry_count, at=time.time())
        await asyncio.sleep(2)
        _record(root, "hard-finish", key=key, attempt=current_job.retry_count, at=time.time())

    @adapter.task(max_retries=2, retry_delays=[0.01, 0.01], retry_on=[RetryableError])
    async def requeued(key: str) -> None:
        # Первое выполнение ждёт сигнала теста и падает с повторяемой ошибкой,
        # следующее завершается успешно.
        _record(root, "requeue-start", key=key, job_id=current_job.id)
        failed = root / f"requeue-failed-{key}"
        if failed.exists():
            await asyncio.sleep(0)
            return
        release = root / f"requeue-release-{key}"
        for _ in range(400):  # сигнал приходит из процесса теста файлом; ждём не дольше 20 с
            if release.exists():
                break
            await asyncio.sleep(0.05)
        _ = failed.write_text("failed", encoding="utf-8")
        raise RetryableError(key)

    @adapter.task(max_retries=0)
    async def cancellable(key: str) -> None:
        _record(root, "cancel-start", key=key, job_id=current_job.id)
        while True:
            await asyncio.sleep(0.05)
            current_job.check_cancelled()

    @adapter.task(max_concurrent=2, max_in_flight_per_task=2, rate_limit="100/s")
    async def limited(key: str, seconds: float = 0.2) -> None:
        _record(root, "limited-start", key=key, at=time.time())
        await asyncio.sleep(seconds)
        _record(root, "limited-finish", key=key, at=time.time())

    @adapter.task(rate_limit="2/s")
    async def rate_limited(key: str) -> None:
        _record(root, "rate", key=key, at=time.time())
        await asyncio.sleep(0)

    @adapter.task(max_retries=10, retry_delays=[0.01] * 10, retry_budget="1/m")
    async def budget(key: str) -> None:
        _record(root, "budget", key=key, attempt=current_job.retry_count)
        await asyncio.sleep(0)
        raise RetryableError(key)

    @adapter.task(
        max_retries=2,
        retry_delays=[0.01, 0.01],
        circuit_breaker={"threshold": 1, "window": 60, "cooldown": 1},
    )
    async def breaker(key: str) -> None:
        attempt = current_job.retry_count
        _record(root, "breaker", key=key, attempt=attempt, at=time.time())
        if attempt == 0:
            raise RetryableError(key)
        await asyncio.sleep(0)

    def contract_predicate(ctx: PredicateContext) -> bool:
        _record(root, "predicate", has_th="_th" in ctx.kwargs)
        return True

    @adapter.task(
        middleware=[_RecordingMiddleware(root, "task")],
        inject=["contract_resource"],
        predicate=contract_predicate,
    )
    async def integrated(value: str, *, contract_resource: object = None) -> None:
        _record(root, "integrated", value=value, resource=contract_resource)
        await asyncio.sleep(0)

    @queue.task()
    async def ordinary(value: str) -> None:
        _record(root, "ordinary", value=value, item_is_none=item.current() is None)
        await asyncio.sleep(0)

    tasks = {
        "probe": cast("Probe", probe),
        "signature": cast("Probe", signature),
        "flaky": cast("Probe", flaky),
        "soft_timeout": cast("Probe", soft_timeout),
        "hard_timeout": cast("Probe", hard_timeout),
        "requeued": cast("Probe", requeued),
        "cancellable": cast("Probe", cancellable),
        "limited": cast("Probe", limited),
        "rate_limited": cast("Probe", rate_limited),
        "budget": cast("Probe", budget),
        "breaker": cast("Probe", breaker),
        "integrated": cast("Probe", integrated),
    }

    return ContractApp(
        engine,
        queue,
        adapter,
        th,
        cast("Probe", probe),
        tasks,
        cast("NamedProbe", ordinary),
        flexiq_schema,
        dsn,
        tallyho_schema,
    )
