"""Спайк T8.0: факты ARCHITECTURE §11.3 о flexiq на живом воркере ``pool="thread"`` и PostgreSQL.

Запуск (нужен Docker): ``uv run python -m tests.contract.flexiq.spike_facts``
из корня репозитория.
Каждая строка вывода — ``<факт>: <наблюдение>``; разбор — в docs/plan/FLEXIQ_SPIKE.md.
Не тест и не часть CI: pytest собирает только ``test_*.py``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import json
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from flexiq import EventType, JsonSerializer, Queue, TaskMiddleware, current_job
from typing_extensions import override

from tests.contract.flexiq.spike_support import postgres_url, say, wait_until, worker_thread

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from flexiq import TaskWrapper
    from flexiq._flexiq import PyJob
    from flexiq.context import JobContext
    from flexiq.pagination import Page

__all__ = ["main"]

P = ParamSpec("P")
R = TypeVar("R")

META = '{"user":  "как есть" , "n": [1,2]}'  # лишние пробелы: проверяем «байт в байт»
LOCK = threading.Lock()
SEEN: list[dict[str, object]] = []  # что видела обёртка tracked
RECEIVED: dict[str, tuple[tuple[object, ...], dict[str, object]]] = {}  # что получила функция
HOOKS: list[dict[str, object]] = []  # вызовы middleware и событий
ACTIVE = [0, 0]  # [сейчас, максимум] для sleeper


@dataclasses.dataclass(frozen=True)
class Point:
    x: int
    y: int


def record(bucket: list[dict[str, object]], **fields: object) -> None:
    with LOCK:
        bucket.append(fields)


def tracked(fn: Callable[P, Awaitable[R]]) -> Callable[P, Coroutine[object, object, R]]:
    """Модель ``th.tracked``: async-обёртка с ``functools.wraps``, вынимает ``_th``."""

    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        th = kwargs.pop("_th", None)
        record(
            SEEN,
            task=current_job.task_name,
            job=current_job.id,
            retry_count=current_job.retry_count,
            th=th,
            thread=threading.current_thread().name,
            loop=id(asyncio.get_running_loop()),
            at=time.monotonic(),
        )
        return await fn(*args, **kwargs)

    # Не декоратором: mypy disallow_any_decorated ругается на Any в типе _Wrapped.
    functools.update_wrapper(wrapper, fn)
    return wrapper


class Recorder(TaskMiddleware):
    """Пишет, какие хуки middleware зовёт flexiq, с каким ctx и в каком потоке."""

    def _hook(self, hook: str, ctx: JobContext, **extra: object) -> None:
        record(
            HOOKS,
            hook=hook,
            ctx_type=type(ctx).__name__,
            ctx_id=getattr(ctx, "id", None),
            ctx_task=getattr(ctx, "task_name", None),
            ctx_attrs=sorted(vars(ctx)) if hasattr(ctx, "__dict__") else None,
            thread=threading.current_thread().name,
            **extra,
        )

    @override
    def before(self, ctx: JobContext) -> None:
        self._hook("before", ctx, retry_count=ctx.retry_count)

    @override
    def after(self, ctx: JobContext, result: object, error: Exception | None) -> None:
        del result
        self._hook("after", ctx, error=repr(error))

    @override
    def on_retry(self, ctx: JobContext, error: Exception, retry_count: int) -> None:
        self._hook("on_retry", ctx, error=repr(error)[:60], retry_count=retry_count)

    @override
    def on_dead_letter(self, ctx: JobContext, error: Exception) -> None:
        self._hook(
            "on_dead_letter",
            ctx,
            error=repr(error)[:60],
            error_type=type(error).__name__,
            retry_count=ctx.retry_count,
        )

    @override
    def on_timeout(self, ctx: JobContext) -> None:
        self._hook("on_timeout", ctx)

    @override
    def on_cancel(self, ctx: JobContext) -> None:
        self._hook("on_cancel", ctx)


# --- функции задач -----------------------------------------------------------------------


async def echo(*args: object, **kwargs: object) -> None:
    await asyncio.sleep(0)
    with LOCK:
        RECEIVED[current_job.id] = (args, kwargs)


async def plain_echo(*args: object, **kwargs: object) -> None:
    await asyncio.sleep(0)
    with LOCK:
        RECEIVED[current_job.id] = (args, kwargs)


async def sleeper(i: int) -> int:
    with LOCK:
        ACTIVE[0] += 1
        ACTIVE[1] = max(ACTIVE)
    await asyncio.sleep(0.3)
    with LOCK:
        ACTIVE[0] -= 1
    return i


async def always_fail(i: int) -> None:
    await asyncio.sleep(0)
    msg = f"fail {i}"
    raise RuntimeError(msg)


async def budget_fail(i: int) -> None:
    await asyncio.sleep(0)
    msg = f"budget {i}"
    raise RuntimeError(msg)


async def config_probe() -> None:
    await asyncio.sleep(0)


async def no_retry(i: int) -> None:
    await asyncio.sleep(0)
    msg = f"no retry {i}"
    raise ValueError(msg)


async def breaker_fail(i: int) -> None:
    await asyncio.sleep(0)
    msg = f"breaker {i}"
    raise RuntimeError(msg)


async def slow_timeout(i: int) -> None:
    record(HOOKS, hook="slow_start", i=i, retry=current_job.retry_count, at=time.monotonic())
    await asyncio.sleep(12)  # заметно дольше timeout=1 и цикла reaper (~5 с)
    record(HOOKS, hook="slow_end", i=i, retry=current_job.retry_count, at=time.monotonic())


async def cancellable() -> None:
    for _ in range(100):
        current_job.check_cancelled()
        await asyncio.sleep(0.1)


async def json_echo(x: int) -> int:
    await asyncio.sleep(0)
    return x


@dataclasses.dataclass(frozen=True)
class Tasks:
    echo: TaskWrapper
    plain_echo: TaskWrapper
    sleeper: TaskWrapper
    always_fail: TaskWrapper
    budget_fail: TaskWrapper
    config_probe: TaskWrapper
    slow_timeout: TaskWrapper
    cancellable: TaskWrapper
    json_echo: TaskWrapper
    no_retry: TaskWrapper
    breaker_fail: TaskWrapper


def register(queue: Queue) -> Tasks:
    delays = [0.2] * 20
    return Tasks(
        echo=queue.task(max_retries=0)(tracked(echo)),
        plain_echo=queue.task(max_retries=0)(plain_echo),
        sleeper=queue.task(max_retries=0)(tracked(sleeper)),
        always_fail=queue.task(max_retries=2, retry_delays=delays)(tracked(always_fail)),
        budget_fail=queue.task(max_retries=10, retry_delays=delays, retry_budget="1/m")(
            tracked(budget_fail)
        ),
        config_probe=queue.task(max_retries=5, timeout=77, priority=9)(tracked(config_probe)),
        slow_timeout=queue.task(max_retries=1, timeout=1, retry_delays=delays)(
            tracked(slow_timeout)
        ),
        cancellable=queue.task(max_retries=0)(tracked(cancellable)),
        json_echo=queue.task(max_retries=0, serializer=JsonSerializer())(tracked(json_echo)),
        no_retry=queue.task(max_retries=3, dont_retry_on=[ValueError])(tracked(no_retry)),
        breaker_fail=queue.task(
            max_retries=5,
            retry_delays=delays,
            circuit_breaker={"threshold": 2, "window": 60, "cooldown": 300},
        )(tracked(breaker_fail)),
    )


# --- проверки -----------------------------------------------------------------------------


def check_names(queue: Queue, tasks: Tasks) -> None:
    entry = cast("object", queue._task_registry[tasks.echo.name])  # ruff: ignore[private-member-access]  # реестр задач flexiq
    say(
        "F1.name",
        {
            "tracked": tasks.echo.name,
            "expected_for_bare_fn": f"{Path(__file__).stem}.{echo.__qualname__}",
            "plain": tasks.plain_echo.name,
            "registry_is_async": getattr(entry, "_flexiq_is_async", None),
            "async_fn_is_wrapper": getattr(entry, "_flexiq_async_fn", None) is not echo,
        },
    )


def decode_payload(queue: Queue, py_job: PyJob) -> tuple[tuple[object, ...], dict[str, object]]:
    """Декодирует payload джобы так же, как воркер flexiq (кодеки + сериализатор задачи)."""
    return cast(
        "tuple[tuple[object, ...], dict[str, object]]",
        queue._deserialize_payload(py_job.task_name, py_job.payload_bytes),  # ruff: ignore[private-member-access]  # декодер flexiq
    )


def on_job_dead(_event: EventType, payload: dict[str, object]) -> None:
    record(HOOKS, hook="event.job.dead", payload=payload, thread=threading.current_thread().name)


def job_seen(job_id: str) -> list[dict[str, object]]:
    with LOCK:
        return [s for s in SEEN if s["job"] == job_id]


def check_args(queue: Queue, tasks: Tasks) -> str:
    args: tuple[object, ...] = (
        1,
        "юникод ✓",
        None,
        (1, (2, 3)),
        [1, (2, 3)],
        {1: "a", "k": b"\x00\xff"},
        datetime(2026, 9, 30, 12, 0, tzinfo=UTC),
        Decimal("1.10"),
        frozenset({1, 2}),
        Point(1, 2),
    )
    kwargs: dict[str, object] = {"flag": True, "nested": {"t": (1,)}}
    th = {"i": "item-1", "b": "batch-1"}
    name = tasks.echo.name
    direct = queue.enqueue_many(
        task_name=name,
        args_list=[args],
        kwargs_list=[{**kwargs, "_th": th}],
        metadata_list=[META],
    )[0]
    # Путь relay: payload Item хранится кодеком задачи flexiq и восстанавливается перед отправкой.
    serializer = queue._get_serializer(name)  # ruff: ignore[private-member-access]  # сериализатор задачи
    stored = serializer.dumps((args, kwargs))
    loaded = cast("tuple[list[object], dict[str, object]]", serializer.loads(stored))
    relayed = queue.enqueue_many(
        task_name=name, args_list=[tuple(loaded[0])], kwargs_list=[{**loaded[1], "_th": th}]
    )[0]
    wait_until(lambda: direct.id in RECEIVED and relayed.id in RECEIVED, 15)
    for label, job in (("direct", direct), ("relay_roundtrip", relayed)):
        got_args, got_kwargs = RECEIVED.get(job.id, ((), {}))
        say(
            f"F2.args.{label}",
            {
                "exact_repr": repr((got_args, got_kwargs)) == repr((args, kwargs)),
                "th_in_function_kwargs": "_th" in got_kwargs,
                "th_seen_by_wrapper": [s["th"] for s in job_seen(job.id)],
                "payload_codec_tag": stored[:1].hex(),
            },
        )
    try:
        json.dumps([list(args), kwargs])
        json_verdict = "ok"
    except TypeError as exc:
        json_verdict = f"TypeError: {exc}"
    say(
        "F2.args.json_codec",
        {
            "full_args": json_verdict,
            "tuple_roundtrip": json.loads(json.dumps([(1, 2)])),
            "int_key_roundtrip": json.loads(json.dumps({1: "a"})),
        },
    )
    return direct.id


def check_metadata_and_payload(queue: Queue, job_id: str) -> None:
    job = queue.get_job(job_id)
    assert job is not None
    py_job = job._py_job  # ruff: ignore[private-member-access]  # сырой PyJob: payload_bytes
    args, kwargs = decode_payload(queue, py_job)
    say(
        "F2.metadata",
        {
            "byte_for_byte": job.metadata == META,
            "stored": job.metadata,
            "payload_kwargs_has_th": "_th" in kwargs,
            "payload_args_len": len(args),
        },
    )


def check_json_serializer(queue: Queue, tasks: Tasks) -> None:
    job = queue.enqueue_many(
        task_name=tasks.json_echo.name, args_list=[(7,)], kwargs_list=[{"_th": {"i": "x"}}]
    )[0]
    wait_until(lambda: bool(job_seen(job.id)), 10)
    job.refresh()
    say(
        "F2.json_serializer_task", {"status": job.status, "th": [s["th"] for s in job_seen(job.id)]}
    )


def check_loop(queue: Queue, tasks: Tasks) -> None:
    jobs = queue.enqueue_many(task_name=tasks.sleeper.name, args_list=[(i,) for i in range(20)])
    ids = {j.id for j in jobs}
    wait_until(lambda: len([s for s in SEEN if s["job"] in ids]) == len(ids), 30)
    with LOCK:
        seen = [s for s in SEEN if s["job"] in ids]
    say(
        "F3.loop",
        {
            "jobs": len(seen),
            "distinct_loops": len({s["loop"] for s in seen}),
            "threads": sorted({str(s["thread"]) for s in seen}),
            "max_concurrent": ACTIVE[1],
        },
    )


def check_enqueue_many_defaults(queue: Queue, tasks: Tasks) -> None:
    parked = queue.enqueue_many(task_name=tasks.config_probe.name, args_list=[()], queue="parked")
    job = queue.get_job(parked[0].id)
    assert job is not None
    py_job = job._py_job  # ruff: ignore[private-member-access]  # поля джобы
    say(
        "F4.enqueue_many_none_defaults",
        {
            "task_config": {"max_retries": 5, "timeout_ms": 77000, "priority": 9},
            "job": {
                "max_retries": py_job.max_retries,
                "timeout_ms": py_job.timeout_ms,
                "priority": py_job.priority,
            },
        },
    )


def check_idempotency(queue: Queue, tasks: Tasks) -> None:
    name = tasks.config_probe.name
    first = queue.enqueue_many(
        task_name=name, args_list=[()], queue="parked", idempotency_keys=["th:k1"]
    )[0]
    single = queue.enqueue(task_name=name, queue="parked", idempotency_key="th:k1")
    before = queue.stats_by_queue("parked")
    try:
        queue.enqueue_many(
            task_name=name,
            args_list=[(), ()],
            queue="parked",
            idempotency_keys=["th:k1-new", "th:k1"],
        )
        batch_dup = "ok"
    except RuntimeError as exc:
        batch_dup = f"RuntimeError: {exc}"
    after = queue.stats_by_queue("parked")
    done = queue.enqueue_many(task_name=name, args_list=[()], idempotency_keys=["th:k2"])[0]
    wait_until(lambda: bool(job_seen(done.id)), 10)
    time.sleep(0.5)
    again = queue.enqueue_many(task_name=name, args_list=[()], idempotency_keys=["th:k2"])[0]
    say(
        "F7.idempotency",
        {
            "enqueue_single_dup_returns_existing_id": single.id == first.id,
            "enqueue_many_dup_while_pending": batch_dup,
            "enqueue_many_dup_batch_is_atomic": before == after,
            "pending_before_after": [before, after],
            "after_complete_same_key_new_id": again.id != done.id,
        },
    )


def check_retries_and_dlq(queue: Queue, tasks: Tasks) -> str:
    job = queue.enqueue_many(
        task_name=tasks.always_fail.name,
        args_list=[(1,)],
        max_retries=2,
        kwargs_list=[{"_th": {"i": "item-dlq"}}],
        metadata_list=[META],
    )[0]
    wait_until(
        lambda: any(h["hook"] == "on_dead_letter" and h["ctx_id"] == job.id for h in HOOKS), 30
    )
    time.sleep(0.5)
    say("F5.retry_count_per_attempt", [s["retry_count"] for s in job_seen(job.id)])
    with LOCK:
        hooks = [h for h in HOOKS if h.get("ctx_id") == job.id and h["hook"] != "before"]
    say("F5.hooks", hooks)
    page = cast("Page[dict[str, object]]", queue.dead_letters_after(limit=50))
    entry = next((e for e in page.items if e.get("original_job_id") == job.id), None)
    say(
        "F5.dead_letters_after",
        {
            "page_type": type(page).__name__,
            "next_cursor": page.next_cursor,
            "entry_keys": sorted(entry) if entry else None,
            "entry": {k: v for k, v in (entry or {}).items() if k != "payload"},
            "metadata_byte_for_byte": entry is not None and entry.get("metadata") == META,
        },
    )
    # В записи DLQ нет payload: _th достаём из исходной джобы по original_job_id.
    dead_job = queue.get_job(job.id)
    assert dead_job is not None
    dead_py_job = dead_job._py_job  # ruff: ignore[private-member-access]  # сырой PyJob
    say(
        "F5.dead_job_payload",
        {"status": dead_job.status, "kwargs": decode_payload(queue, dead_py_job)[1]},
    )
    return str(entry["id"]) if entry else ""


def check_retry_dead_and_replay(queue: Queue, *, dead_id: str, done_id: str) -> None:
    new_id = queue.retry_dead(dead_id)
    new_job = queue.get_job(new_id)
    assert new_job is not None
    py_job = new_job._py_job  # ruff: ignore[private-member-access]  # payload новой джобы
    _args, kwargs = decode_payload(queue, py_job)
    replayed = queue.replay(done_id)
    wait_until(lambda: bool(job_seen(replayed.id)), 10)
    say(
        "F9.retry_dead_replay",
        {
            "retry_dead_new_id": new_id != dead_id,
            "retry_dead_kwargs_th": kwargs.get("_th"),
            "retry_dead_metadata": new_job.metadata,
            "replay_new_id": replayed.id != done_id,
            "replay_wrapper_th": [s["th"] for s in job_seen(replayed.id)],
            "replay_metadata": replayed.metadata,
        },
    )


def check_retry_budget(queue: Queue, tasks: Tasks) -> None:
    jobs = queue.enqueue_many(
        task_name=tasks.budget_fail.name, args_list=[(i,) for i in range(3)], max_retries=10
    )
    ids = {j.id for j in jobs}

    def all_dead() -> bool:
        return sum(1 for h in HOOKS if h["hook"] == "on_dead_letter" and h["ctx_id"] in ids) == 3

    wait_until(all_dead, 30)
    time.sleep(0.5)
    with LOCK:
        attempts = {jid: [s["retry_count"] for s in SEEN if s["job"] == jid] for jid in ids}
        hooks = [
            (h["hook"], h.get("error_type"))
            for h in HOOKS
            if h.get("ctx_id") in ids and h["hook"] not in {"before", "after"}
        ]
    statuses = []
    for jid in ids:
        job = queue.get_job(jid)
        statuses.append(job.status if job else None)
    say(
        "F6.retry_budget",
        {"attempts_retry_counts": sorted(attempts.values()), "hooks": hooks, "status": statuses},
    )


def check_timeout(queue: Queue, tasks: Tasks) -> None:
    job = queue.enqueue_many(
        task_name=tasks.slow_timeout.name, args_list=[(1,)], max_retries=1, timeout=1
    )[0]
    wait_until(lambda: sum(1 for h in HOOKS if h["hook"] == "slow_end") >= 2, 40)
    time.sleep(1)
    t0 = min(float(cast("float", s["at"])) for s in job_seen(job.id))
    with LOCK:
        events = [
            (h["hook"], h.get("retry"), round(float(cast("float", h["at"])) - t0, 2))
            for h in HOOKS
            if h["hook"] in {"slow_start", "slow_end"}
        ]
        hooks = [h["hook"] for h in HOOKS if h.get("ctx_id") == job.id]
    job.refresh()
    say("F8.hard_timeout", {"timeline": events, "hooks": hooks, "status": job.status})


def outcome(queue: Queue, ids: set[str]) -> dict[str, object]:
    with LOCK:
        attempts = sorted([s["retry_count"] for s in SEEN if s["job"] == jid] for jid in ids)
        hooks = [
            (h["hook"], h.get("retry_count"))
            for h in HOOKS
            if h.get("ctx_id") in ids and h["hook"] not in {"before", "after"}
        ]
    jobs = [queue.get_job(jid) for jid in ids]
    return {
        "attempts_retry_counts": attempts,
        "hooks": hooks,
        "status": sorted(j.status for j in jobs if j is not None),
    }


def check_dont_retry_on(queue: Queue, tasks: Tasks) -> None:
    job = queue.enqueue_many(task_name=tasks.no_retry.name, args_list=[(1,)], max_retries=3)[0]
    wait_until(
        lambda: any(h["hook"] == "on_dead_letter" and h["ctx_id"] == job.id for h in HOOKS), 10
    )
    say("F5.dont_retry_on", outcome(queue, {job.id}))


def check_circuit_breaker(queue: Queue, tasks: Tasks) -> None:
    jobs = queue.enqueue_many(
        task_name=tasks.breaker_fail.name, args_list=[(i,) for i in range(4)], max_retries=5
    )
    time.sleep(6)
    say(
        "F6.circuit_breaker",
        {
            **outcome(queue, {j.id for j in jobs}),
            "breakers": cast("list[dict[str, object]]", queue.circuit_breakers()),
        },
    )


def check_cancel(queue: Queue, tasks: Tasks) -> None:
    job = queue.enqueue_many(task_name=tasks.cancellable.name, args_list=[()])[0]
    wait_until(lambda: bool(job_seen(job.id)), 10)
    requested = queue.cancel_running_job(job.id)
    time.sleep(1.5)
    job.refresh()
    with LOCK:
        hooks = [h["hook"] for h in HOOKS if h.get("ctx_id") == job.id]
    say("F10.cancel_running", {"requested": requested, "status": job.status, "hooks": hooks})


async def check_aenqueue_many(queue: Queue, tasks: Tasks) -> None:
    jobs = await queue.aenqueue_many(task_name=tasks.config_probe.name, args_list=[(), ()])
    executor = queue._executor  # ruff: ignore[private-member-access]  # общий пул aenqueue_many
    say(
        "F11.aenqueue_many",
        {"jobs": len(jobs), "executor_max_workers": getattr(executor, "_max_workers", None)},
    )


def run_all(queue: Queue, tasks: Tasks) -> None:
    check_names(queue, tasks)
    check_enqueue_many_defaults(queue, tasks)
    with worker_thread(queue, ["default"]):
        direct_id = check_args(queue, tasks)
        check_metadata_and_payload(queue, direct_id)
        check_json_serializer(queue, tasks)
        check_loop(queue, tasks)
        check_idempotency(queue, tasks)
        dead_id = check_retries_and_dlq(queue, tasks)
        check_retry_dead_and_replay(queue, dead_id=dead_id, done_id=direct_id)
        check_retry_budget(queue, tasks)
        check_dont_retry_on(queue, tasks)
        check_circuit_breaker(queue, tasks)
        check_timeout(queue, tasks)
        check_cancel(queue, tasks)
        asyncio.run(check_aenqueue_many(queue, tasks))
    with LOCK:
        dead_events = [h for h in HOOKS if h["hook"] == "event.job.dead"]
    say("F5.job_dead_events", {"count": len(dead_events), "sample": dead_events[:1]})


def main() -> None:
    with postgres_url() as url:
        queue = Queue(
            backend="postgres",
            db_url=url,
            workers=4,
            default_retry=3,
            drain_timeout=5,
            middleware=[Recorder()],
        )
        queue.on_event(EventType.JOB_DEAD, on_job_dead)
        run_all(queue, register(queue))


if __name__ == "__main__":
    main()
