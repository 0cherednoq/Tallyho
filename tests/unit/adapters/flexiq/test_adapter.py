from __future__ import annotations

import asyncio
import itertools
import json
import logging
import threading
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, cast, final
from uuid import uuid4

import pytest
from flexiq import EventType
from flexiq.exceptions import TaskCancelledError

import tallyho.adapters.flexiq.adapter as adapter_module
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.engine import RuntimeServices
from tallyho.model.errors import (
    ClosedError,
    CompleterError,
    ConfigurationError,
    InvalidStateError,
    TallyhoError,
    UnsupportedOption,
)
from tallyho.model.states import OutboxKind
from tallyho.protocols.broker import (
    DeadLetter,
    Dispatcher,
    Message,
    RetryLimits,
    Runtime,
    Verdict,
)
from tallyho.protocols.serialization import PayloadCodec
from tallyho.runtime.tracked import TaskRuntime, bind_runtime
from tests.helpers.loops import LoopThread, library_tasks

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from uuid import UUID

    from flexiq import Queue

    from tallyho.engine.completer import Completer
    from tallyho.engine.dead_letters import DeadLetterReconciler
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import DeadLetters, WorkerRuntime

__all__: list[str] = []


_DUPLICATE = "duplicate idempotency key"
_FAILURE = "fake flexiq failure"
_DEFAULT = object()


class _CallableTask(Protocol):
    __module__: str
    __qualname__: str

    def __call__(self, *args: object, **kwargs: object) -> object: ...


@final
class _FakeTask:
    def __init__(self, name: str, fn: _CallableTask) -> None:
        self.name = name
        self._fn = fn

    def __call__(self, *args: object, **kwargs: object) -> object:
        return self._fn(*args, **kwargs)


@dataclass(slots=True)
class _StoredJob:
    task_name: str
    payload_bytes: bytes


@dataclass(slots=True)
class _Job:
    _py_job: _StoredJob


@dataclass(slots=True)
class _Page:
    items: list[object]
    next_cursor: str | None


@final
class _FakeQueue:
    def __init__(self) -> None:
        self.task_options: list[dict[str, object]] = []
        self.many: list[dict[str, object]] = []
        self.one: list[dict[str, object]] = []
        self.events: dict[object, Callable[[object, object], None]] = {}
        self.payloads: dict[bytes, tuple[tuple[object, ...], dict[str, object]]] = {}
        self.jobs: dict[str, _Job] = {}
        self.page = _Page([], None)
        self.pages: dict[str | None, _Page] = {}
        self.page_calls: list[tuple[int, str | None]] = []
        self.job_calls: list[str] = []
        self.job_error: Exception | None = None
        self.reject_many_once = False
        self.task_error: Exception | None = None
        self.many_error: Exception | None = None
        self.one_error: Exception | None = None
        self.page_error: Exception | None = None
        self.encode_error: Exception | None = None
        self.decode_error: Exception | None = None
        self.decode_result: object = _DEFAULT

    def task(self, **options: object) -> Callable[[_CallableTask], _FakeTask]:
        if self.task_error is not None:
            raise self.task_error
        self.task_options.append(dict(options))

        def decorate(fn: _CallableTask) -> _FakeTask:
            configured = options.get("name")
            name = (
                configured if isinstance(configured, str) else f"{fn.__module__}.{fn.__qualname__}"
            )
            return _FakeTask(name, fn)

        return decorate

    def enqueue_many(self, **options: object) -> object:
        if self.many_error is not None:
            raise self.many_error
        if self.reject_many_once:
            self.reject_many_once = False
            raise RuntimeError(_DUPLICATE)
        self.many.append(dict(options))
        return []

    def enqueue(self, **options: object) -> object:
        if self.one_error is not None:
            raise self.one_error
        self.one.append(dict(options))
        return object()

    def on_event(self, event_type: object, callback: Callable[[object, object], None]) -> None:
        self.events[event_type] = callback

    async def aget_job(self, job_id: str) -> object | None:
        await asyncio.sleep(0)
        self.job_calls.append(job_id)
        if self.job_error is not None:
            raise self.job_error
        return self.jobs.get(job_id)

    async def adead_letters_after(self, *, limit: int, after: str | None) -> object:
        await asyncio.sleep(0)
        self.page_calls.append((limit, after))
        if self.page_error is not None:
            raise self.page_error
        return self.pages.get(after, self.page)

    def _encode_payload(  # pyright: ignore[reportUnusedFunction]  # flexiq SPI вызывается адаптером динамически
        self, task_name: str, args: tuple[object, ...], kwargs: dict[str, object]
    ) -> bytes:
        if self.encode_error is not None:
            raise self.encode_error
        key = f"{task_name}:{len(self.payloads)}".encode()
        self.payloads[key] = (args, dict(kwargs))
        return key

    def _deserialize_payload(  # pyright: ignore[reportUnusedFunction]  # flexiq SPI вызывается адаптером динамически
        self, task_name: str, payload: bytes
    ) -> object:
        if self.decode_error is not None:
            raise self.decode_error
        if self.decode_result is not _DEFAULT:
            return self.decode_result
        assert payload.startswith(task_name.encode())
        return self.payloads[payload]


@dataclass(slots=True)
class _CurrentJob:
    retry_count: int
    task_name: str = "tests.probe"


@final
class _FakeDeadLetters:
    """Сверка с DLQ движка: запоминает, что событие передало правилу UC-15."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.settled: list[tuple[tuple[DeadLetter, ...], str]] = []

    async def settle(self, entries: Sequence[DeadLetter], *, error_type: str) -> int:
        await asyncio.sleep(0)
        if self.fail:
            raise RuntimeError(_FAILURE)
        self.settled.append((tuple(entries), error_type))
        return len(entries)


def _services(
    completer: object | None = None,
    *,
    adapter: FlexiqAdapter | None = None,
    dead_letters: _FakeDeadLetters | None = None,
) -> RuntimeServices:
    raw_completer = object() if completer is None else completer
    tree_cache = object()
    runtime: WorkerRuntime
    if adapter is None:
        runtime = cast("WorkerRuntime", object())
    else:
        built = TaskRuntime(
            completer=cast("Completer", raw_completer),
            broker=adapter,
            dispatcher=adapter,
            tree_cache=cast("TreeCache", tree_cache),
            heartbeat_every=timedelta(seconds=10),
        )
        bind_runtime(built)
        runtime = built
    return RuntimeServices(
        completer=cast("Completer", raw_completer),
        tree_cache=cast("TreeCache", tree_cache),
        heartbeat_every=timedelta(seconds=10),
        runtime=runtime,
        dead_letters=cast("DeadLetterReconciler | None", dead_letters),
    )


def _adapter(
    *,
    pool: str = "thread",
    completer: object | None = None,
    dead_letters: _FakeDeadLetters | None = None,
) -> tuple[FlexiqAdapter, _FakeQueue]:
    queue = _FakeQueue()
    adapter = FlexiqAdapter(cast("Queue", cast("object", queue)), pool=pool)
    if pool == "thread":
        adapter.install_runtime(_services(completer, adapter=adapter, dead_letters=dead_letters))
    return adapter, queue


async def _echo(value: object) -> object:
    await asyncio.sleep(0)
    return value


def _message(
    adapter: FlexiqAdapter,
    task_name: str,
    *,
    options: Mapping[str, object] | None = None,
    kind: OutboxKind = OutboxKind.ITEM,
) -> Message:
    item_id = uuid4()
    return Message(
        id=item_id,
        batch_id=uuid4(),
        kind=kind,
        task_name=task_name,
        payload=adapter.encode(task_name, (str(item_id),), {"unicode": "привет"}),
        options={} if options is None else options,
    )


def test_adapter_satisfies_protocols_and_registers_task_options() -> None:
    adapter, queue = _adapter()
    options: dict[str, object] = {
        "name": "custom.echo",
        "max_retries": 7,
        "retry_backoff": 2.0,
        "retry_delays": [1.0, 3.0],
        "max_retry_delay": 30,
        "retry_budget": "10/m",
        "circuit_breaker": {"threshold": 3},
        "soft_timeout": 4.5,
        "rate_limit": "5/s",
        "max_concurrent": 8,
        "max_in_flight_per_task": 9,
        "middleware": [],
        "inject": ["ctx"],
        "codecs": ["zstd"],
        "predicate": lambda: True,
    }
    task = adapter.task(**options)(_echo)

    assert isinstance(adapter, Dispatcher)
    assert isinstance(adapter, Runtime)
    assert isinstance(adapter, PayloadCodec)
    assert adapter.task_name(task) == "custom.echo"
    assert queue.task_options == [options]


def test_retry_limits_expose_decorator_default_to_sweeper() -> None:
    adapter, _queue = _adapter()
    explicit = adapter.task(name="custom.explicit", max_retries=7)(_echo)
    implicit = adapter.task(name="custom.implicit")(_echo)

    assert isinstance(adapter, RetryLimits)
    assert adapter.max_retries(adapter.task_name(explicit)) == 7
    # Без max_retries в декораторе flexiq берёт своё умолчание задачи — 3.
    assert adapter.max_retries(adapter.task_name(implicit)) == 3
    assert adapter.max_retries("custom.unknown") == 0


async def test_dispatch_maps_options_markers_and_user_keys_exactly() -> None:
    adapter, queue = _adapter()
    task = adapter.task(max_retries=4, timeout=90, priority=2, queue="base", expires=30)(_echo)
    name = adapter.task_name(task)
    metadata = '{"spacing":  [1, 2]}'
    notes = {"trace": "точно", "nested": {"n": 1}}
    first = _message(
        adapter,
        name,
        options={
            "delay": 1.5,
            "metadata": metadata,
            "notes": notes,
            "priority": 5,
            "queue": "urgent",
            "max_retries": 6,
            "timeout": 12,
            "expires": 2.5,
            "result_ttl": 44,
        },
    )
    second = _message(
        adapter,
        name,
        options={"queue": "urgent", "priority": 5, "max_retries": 6, "timeout": 12},
    )
    third = _message(
        adapter,
        name,
        options={
            "queue": "urgent",
            "priority": 5,
            "max_retries": 6,
            "timeout": 12,
            "idempotency_key": "user-idem",
            "unique_key": "user-unique",
            "idempotent": True,
        },
    )

    await adapter.dispatch([first, second, third])

    assert len(queue.many) == 2  # idempotent=True образует отдельную совместимую группу
    batch = next(value for value in queue.many if value["idempotent"] is False)
    assert batch["task_name"] == name
    assert batch["args_list"] == [(str(first.id),), (str(second.id),)]
    assert batch["priority"] == 5
    assert batch["queue"] == "urgent"
    assert batch["max_retries"] == 6
    assert batch["timeout"] == 12
    assert batch["delay_list"] == [1.5, None]
    assert batch["metadata_list"] == [metadata, None]
    assert batch["notes_list"] == [notes, None]
    assert batch["expires_list"] == [2.5, 30.0]
    assert batch["result_ttl_list"] == [44, None]
    assert batch["idempotency_keys"] == [f"th:{first.id}:0", f"th:{second.id}:0"]
    markers = [kwargs["_th"] for kwargs in cast("list[dict[str, object]]", batch["kwargs_list"])]
    assert markers == [
        {"i": str(first.id), "b": str(first.batch_id), "r": 6},
        {"i": str(second.id), "b": str(second.batch_id), "r": 6},
    ]
    user = next(value for value in queue.many if value["idempotent"] is True)
    assert user["idempotency_keys"] == ["user-idem"]
    assert user["unique_keys"] == ["user-unique"]
    await adapter.close()


async def test_dispatch_groups_by_task_and_chunks_at_one_thousand() -> None:
    adapter, queue = _adapter()
    first_task = adapter.task(name="first")(_echo)
    second_task = adapter.task(name="second")(_echo)
    first_name = adapter.task_name(first_task)
    messages = [_message(adapter, first_name) for _ in range(1_001)]
    messages.append(_message(adapter, adapter.task_name(second_task)))

    await adapter.dispatch(messages)

    assert sorted(len(cast("list[object]", call["args_list"])) for call in queue.many) == [
        1,
        1,
        1_000,
    ]
    assert {call["task_name"] for call in queue.many} == {"first", "second"}
    await adapter.close()


async def test_duplicate_batch_falls_back_to_idempotent_single_enqueue() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    messages = [_message(adapter, name), replace(_message(adapter, name), generation=4)]
    queue.reject_many_once = True

    await adapter.dispatch(messages)

    # D-013: дубль ключа в пачке — поштучный повтор с теми же ключами поколений.
    assert queue.many == []
    assert [item["idempotency_key"] for item in queue.one] == [
        f"th:{messages[0].id}:0",
        f"th:{messages[1].id}:4",
    ]
    await adapter.close()


@pytest.mark.parametrize(
    ("options", "option"),
    [
        ({"depends_on": "job"}, "depends_on"),
        ({"debounce": 1}, "debounce"),
        ({"batch": True}, "batch"),
        ({"unknown": 1}, "unknown"),
    ],
)
async def test_dispatch_rejects_unsupported_call_options(
    options: Mapping[str, object], option: str
) -> None:
    adapter, _queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    with pytest.raises(UnsupportedOption) as info:
        await adapter.dispatch([_message(adapter, name, options=options)])
    assert info.value.option == option
    if option == "depends_on":
        assert info.value.hint is not None
        assert "fed_by" in info.value.hint
    await adapter.close()


@pytest.mark.parametrize("options", [{"batch": True}, {"debounce_key": "same"}])
def test_task_rejects_aggregation_options(options: Mapping[str, object]) -> None:
    adapter, _queue = _adapter()
    with pytest.raises(UnsupportedOption):
        _ = adapter.task(**options)(_echo)


def test_task_rejects_weight_with_hint_to_call_options() -> None:
    """Вес — опция вызова tallyho, а не декоратора flexiq (ARCHITECTURE §11.4)."""
    adapter, _queue = _adapter()
    with pytest.raises(UnsupportedOption) as info:
        _ = adapter.task(max_retries=3, weight=2)
    assert info.value.option == "weight"
    assert info.value.hint is not None
    assert ".opts(weight=" in info.value.hint


def test_task_rejects_sync_function() -> None:
    adapter, _queue = _adapter()

    def sync_task() -> None:
        return None

    disguised = cast("Callable[[], Awaitable[object]]", cast("object", sync_task))
    with pytest.raises(ConfigurationError, match="async def"):
        _ = adapter.task()(disguised)


async def test_retry_verdict_uses_effective_limit_and_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _queue = _adapter()
    verdicts: list[Verdict] = []

    @adapter.task(max_retries=5, retry_on=[ValueError], dont_retry_on=[KeyError])
    async def probe(exc: BaseException) -> None:
        await asyncio.sleep(0)
        verdicts.append(adapter.retry_verdict(exc))

    monkeypatch.setattr(adapter_module, "current_job", _CurrentJob(retry_count=1))
    await probe(ValueError())
    await probe(TypeError())
    await probe(KeyError())
    monkeypatch.setattr(adapter_module, "current_job", _CurrentJob(retry_count=5))
    await probe(ValueError())

    assert verdicts == [Verdict.RETRY, Verdict.FINAL, Verdict.FINAL, Verdict.FINAL]
    await adapter.close()


@pytest.mark.parametrize(
    ("retry_on", "registered"),
    [
        # Белый список пользователя дополняется: отказ PostgreSQL на claim — не повод для DLQ.
        ([ValueError], [ValueError, CompleterError, ClosedError]),
        ((ValueError, KeyError), [ValueError, KeyError, CompleterError, ClosedError]),
        # Закрытие установки посреди задачи (Fix-11) дополняется отдельно.
        ([CompleterError], [CompleterError, ClosedError]),
        ([InvalidStateError], [InvalidStateError, CompleterError]),
        # Список уже покрывает ошибки библиотеки — остаётся как есть.
        ([TallyhoError], [TallyhoError]),
        ([Exception], [Exception]),
    ],
)
def test_task_retry_on_also_retries_completer_errors(
    retry_on: Sequence[type[Exception]], registered: list[type[Exception]]
) -> None:
    adapter, queue = _adapter()

    _ = adapter.task(max_retries=2, retry_on=retry_on, dont_retry_on=[KeyError])(_echo)

    assert queue.task_options == [
        {"max_retries": 2, "retry_on": registered, "dont_retry_on": [KeyError]}
    ]


@pytest.mark.parametrize("options", [{}, {"retry_on": None}, {"retry_on": []}])
def test_task_without_retry_filter_is_registered_unchanged(options: dict[str, object]) -> None:
    # Пустой retry_on во flexiq значит «повторять всё»: дополнять нечего.
    adapter, queue = _adapter()

    _ = adapter.task(**options)(_echo)

    assert queue.task_options == [options]


async def test_retry_verdict_matches_registered_filter_for_completer_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _queue = _adapter()
    verdicts: list[Verdict] = []

    @adapter.task(max_retries=5, retry_on=[ValueError])
    async def retried(exc: BaseException) -> None:
        await asyncio.sleep(0)
        verdicts.append(adapter.retry_verdict(exc))

    @adapter.task(max_retries=5, retry_on=[ValueError], dont_retry_on=[TallyhoError])
    async def forbidden(exc: BaseException) -> None:
        await asyncio.sleep(0)
        verdicts.append(adapter.retry_verdict(exc))

    monkeypatch.setattr(adapter_module, "current_job", _CurrentJob(retry_count=1))
    await retried(CompleterError())
    await retried(ClosedError())
    await retried(TypeError())
    # dont_retry_on пользователя сильнее: его решение не переопределяется.
    await forbidden(CompleterError())

    assert verdicts == [Verdict.RETRY, Verdict.RETRY, Verdict.FINAL, Verdict.FINAL]
    await adapter.close()


def _dead_job(
    adapter: FlexiqAdapter, queue: _FakeQueue, job_id: str, *, generation: int | None = None
) -> UUID:
    """Положить в фейковую очередь мёртвую джобу Item и вернуть id Item."""
    item_id = uuid4()
    marker: dict[str, object] = {"i": str(item_id), "b": str(uuid4()), "r": 3}
    if generation is not None:
        marker["g"] = generation
    payload = adapter.encode("echo", (), {"_th": marker})
    queue.jobs[job_id] = _Job(_StoredJob("echo", payload))
    return item_id


def _letter(job_id: str, failed_at: int, **extra: object) -> dict[str, object]:
    return {"id": f"dl-{job_id}", "original_job_id": job_id, "failed_at": failed_at, **extra}


def _cursor(result: DeadLetters) -> dict[str, object]:
    assert result.cursor is not None
    return cast("dict[str, object]", json.loads(result.cursor))


async def test_reconcile_dead_decodes_items_generation_and_detail() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    assert name == "echo"
    first = _dead_job(adapter, queue, "job-1")
    second = _dead_job(adapter, queue, "job-2", generation=2)
    queue.page = _Page(
        [
            _letter("job-2", 300, error="x" * 5_000),
            _letter("job-1", 200, error=""),
            _letter("missing", 100),
            {"other": 1},
        ],
        None,
    )

    result = await adapter.reconcile_dead(None)

    assert result.entries == (
        DeadLetter(second, 2, "x" * 1_000),
        DeadLetter(first, 0, None),
    )
    assert result.item_ids == (second, first)
    assert not result.more
    assert _cursor(result) == {"w": 300, "h": None, "r": None}
    assert queue.page_calls == [(200, None)]
    assert EventType.JOB_DEAD in queue.events
    await adapter.close()


async def test_reconcile_dead_first_walk_reads_history_page_by_page() -> None:
    adapter, queue = _adapter()
    newest = _dead_job(adapter, queue, "job-3")
    middle = _dead_job(adapter, queue, "job-2")
    oldest = _dead_job(adapter, queue, "job-1")
    queue.pages = {
        None: _Page([_letter("job-3", 300), _letter("job-2", 200)], "page-2"),
        "page-2": _Page([_letter("job-1", 100)], None),
    }

    head = await adapter.reconcile_dead(None)
    # Обход не закончен: водяной знак ещё прежний, курсор помнит страницу flexiq.
    assert head.item_ids == (newest, middle)
    assert head.more
    assert _cursor(head) == {"w": None, "h": 300, "r": "page-2"}

    tail = await adapter.reconcile_dead(head.cursor)
    assert tail.item_ids == (oldest,)
    assert not tail.more
    assert _cursor(tail) == {"w": 300, "h": None, "r": None}
    assert queue.page_calls == [(200, None), (200, "page-2")]
    await adapter.close()


async def test_reconcile_dead_rewalks_overlap_and_stops_below_it() -> None:
    queue = _FakeQueue()
    adapter = FlexiqAdapter(
        cast("Queue", cast("object", queue)), dead_letter_overlap=timedelta(milliseconds=100)
    )
    adapter.install_runtime(_services(adapter=adapter))
    fresh = _dead_job(adapter, queue, "fresh")
    # failed_at ставит воркер: запись воркера с отстающими часами появляется
    # ниже водяного знака и находится только благодаря перекрытию.
    late = _dead_job(adapter, queue, "late")
    _ = _dead_job(adapter, queue, "old")
    queue.pages = {
        None: _Page([_letter("fresh", 1_100), _letter("late", 950), _letter("old", 850)], "deep"),
    }
    since = json.dumps({"w": 1_000, "h": None, "r": None})

    result = await adapter.reconcile_dead(since)

    assert result.item_ids == (fresh, late)
    # Запись ниже перекрытия закрывает обход: глубже flexiq не читается.
    assert not result.more
    assert _cursor(result) == {"w": 1_100, "h": None, "r": None}
    assert queue.job_calls == ["fresh", "late"]

    # Следующий обход отдаёт записи перекрытия повторно, но джобы уже не читает.
    again = await adapter.reconcile_dead(result.cursor)
    assert again.item_ids == (fresh,)
    assert _cursor(again) == {"w": 1_100, "h": None, "r": None}
    assert queue.job_calls == ["fresh", "late"]
    assert queue.page_calls == [(200, None), (200, None)]
    await adapter.close()


async def test_reconcile_dead_walk_continues_through_full_overlap_pages() -> None:
    adapter, queue = _adapter()
    first = _dead_job(adapter, queue, "job-2")
    second = _dead_job(adapter, queue, "job-1")
    queue.pages = {
        None: _Page([_letter("job-2", 2_000)], "next"),
        "next": _Page([_letter("job-1", 1_500)], None),
    }
    since = json.dumps({"w": 1_000, "h": None, "r": None})

    head = await adapter.reconcile_dead(since)
    assert (head.item_ids, head.more) == ((first,), True)
    # Водяной знак не двигается, пока обход не дошёл до уже разобранного.
    assert _cursor(head) == {"w": 1_000, "h": 2_000, "r": "next"}
    tail = await adapter.reconcile_dead(head.cursor)
    assert (tail.item_ids, tail.more) == ((second,), False)
    assert _cursor(tail) == {"w": 2_000, "h": None, "r": None}
    await adapter.close()


async def test_reconcile_dead_watermark_never_moves_back() -> None:
    adapter, queue = _adapter()
    _ = _dead_job(adapter, queue, "job-1")
    since = json.dumps({"w": 5_000, "h": None, "r": None})

    # DLQ очищен retention или в нём остались только старые записи.
    empty = await adapter.reconcile_dead(since)
    assert _cursor(empty) == {"w": 5_000, "h": None, "r": None}
    queue.page = _Page([_letter("job-1", 4_990)], None)
    older = await adapter.reconcile_dead(empty.cursor)

    assert len(older.entries) == 1
    assert _cursor(older) == {"w": 5_000, "h": None, "r": None}
    await adapter.close()


async def test_reconcile_dead_does_not_trust_future_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _FakeQueue()
    adapter = FlexiqAdapter(
        cast("Queue", cast("object", queue)), dead_letter_overlap=timedelta(milliseconds=100)
    )
    adapter.install_runtime(_services(adapter=adapter))
    _ = _dead_job(adapter, queue, "job-1")
    queue.page = _Page([_letter("job-1", 10**15)], None)

    def now_ms() -> int:
        return 1_000

    monkeypatch.setattr(adapter_module, "_now_ms", now_ms)

    result = await adapter.reconcile_dead(None)

    # Запись «из будущего» разобрана, но водяной знак не уходит дальше часов процесса.
    assert len(result.entries) == 1
    assert _cursor(result) == {"w": 1_100, "h": None, "r": None}
    await adapter.close()


@pytest.mark.parametrize(
    "since",
    ["not-json", "[]", "{}", '{"w":"1","h":null,"r":null}', '{"w":true,"h":null,"r":null}', "7"],
)
async def test_reconcile_dead_restarts_from_unknown_cursor(
    since: str, caplog: pytest.LogCaptureFixture
) -> None:
    adapter, queue = _adapter()
    item_id = _dead_job(adapter, queue, "job-1")
    queue.page = _Page([_letter("job-1", 100)], "ignored")

    with caplog.at_level(logging.WARNING):
        result = await adapter.reconcile_dead(since)

    assert "не распознан" in caplog.text
    assert result.item_ids == (item_id,)
    assert queue.page_calls == [(200, None)]
    await adapter.close()


async def test_reconcile_dead_skips_undecodable_and_malformed_jobs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter, queue = _adapter()
    _ = _dead_job(adapter, queue, "bad-generation", generation=-1)
    payload = adapter.encode("echo", (), {"_th": {"i": str(uuid4()), "b": str(uuid4()), "g": True}})
    queue.jobs["bool-generation"] = _Job(_StoredJob("echo", payload))
    callback = adapter.encode("echo", (), {"_th": {"c": str(uuid4()), "b": str(uuid4())}})
    queue.jobs["callback"] = _Job(_StoredJob("echo", callback))
    queue.page = _Page(
        [
            _letter("bad-generation", 4),
            _letter("bool-generation", 3),
            _letter("callback", 2),
            {"original_job_id": "no-stamp"},
        ],
        None,
    )
    assert (await adapter.reconcile_dead(None)).entries == ()

    _ = _dead_job(adapter, queue, "broken")
    queue.page = _Page([_letter("broken", 5)], None)
    queue.decode_error = RuntimeError(_FAILURE)
    with caplog.at_level(logging.WARNING):
        result = await adapter.reconcile_dead(None)
    # Одна нечитаемая запись не останавливает сверку: курсор идёт дальше.
    assert result.entries == ()
    assert _cursor(result) == {"w": 5, "h": None, "r": None}
    assert "не декодируется" in caplog.text
    await adapter.close()


async def test_reconcile_dead_fails_when_job_cannot_be_read() -> None:
    adapter, queue = _adapter()
    queue.page = _Page([_letter("job-1", 1)], None)
    queue.job_error = RuntimeError(_FAILURE)

    with pytest.raises(TallyhoError) as info:
        _ = await adapter.reconcile_dead(None)

    assert info.value.__cause__ is queue.job_error
    # Сбой чтения не запоминается: следующий проход прочитает джобу снова.
    queue.job_error = None
    item_id = _dead_job(adapter, queue, "job-1")
    assert (await adapter.reconcile_dead(None)).item_ids == (item_id,)
    await adapter.close()


async def test_reconcile_dead_cache_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, queue = _adapter()
    monkeypatch.setattr(adapter_module, "_DLQ_CACHE", 1)
    first = _dead_job(adapter, queue, "job-1")
    second = _dead_job(adapter, queue, "job-2")
    queue.page = _Page([_letter("job-2", 2), _letter("job-1", 1)], None)

    for _ in range(2):
        assert (await adapter.reconcile_dead(None)).item_ids == (second, first)

    # Влезает одна запись: вытесненную джобу адаптер читает заново.
    assert queue.job_calls == ["job-2", "job-1", "job-2", "job-1"]
    await adapter.close()


def test_negative_dead_letter_overlap_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="dead_letter_overlap"):
        _ = FlexiqAdapter(
            cast("Queue", cast("object", _FakeQueue())), dead_letter_overlap=timedelta(seconds=-1)
        )


async def test_item_marker_carries_generation_only_after_redispatch() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    first = _message(adapter, name)
    again = replace(_message(adapter, name), generation=2)

    await adapter.dispatch([first, again])

    markers = [
        kwargs["_th"] for kwargs in cast("list[dict[str, object]]", queue.many[0]["kwargs_list"])
    ]
    assert markers == [
        {"i": str(first.id), "b": str(first.batch_id), "r": 3},
        {"i": str(again.id), "b": str(again.batch_id), "r": 3, "g": 2},
    ]
    await adapter.close()


async def test_own_idempotency_key_separates_send_generations() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    first = _message(adapter, name)
    resent = replace(first, generation=1)
    custom = replace(first, generation=1, options={"idempotency_key": "user-idem"})
    unique = replace(first, generation=1, options={"unique_key": "user-unique"})

    await adapter.dispatch([first, first, resent, custom, unique])

    # Повтор relay той же записи outbox — тот же ключ; новое поколение — свой ключ,
    # иначе flexiq слил бы его с ещё живой джобой прошлой отправки. Ключи
    # пользователя не меняются, а при unique_key свой ключ не подставляется.
    keys = [key for call in queue.many for key in cast("list[object]", call["idempotency_keys"])]
    assert keys == [
        f"th:{first.id}:0",
        f"th:{first.id}:0",
        f"th:{first.id}:1",
        "user-idem",
        None,
    ]
    await adapter.close()


async def test_callback_marker_contains_stable_callback_identity() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    message = _message(adapter, name, kind=OutboxKind.CALLBACK)
    await adapter.dispatch([message])
    kwargs = cast("list[dict[str, object]]", queue.many[0]["kwargs_list"])[0]
    # Без ключа "s": сводки в контексте колбэка нет (D-058).
    assert kwargs["_th"] == {"c": str(message.id), "b": str(message.batch_id), "r": 3}
    await adapter.close()


def test_install_rejects_prefork_and_incompatible_queue() -> None:
    prefork, _queue = _adapter(pool="prefork")
    with pytest.raises(ConfigurationError, match="prefork"):
        prefork.install_runtime(_services())

    bad = FlexiqAdapter(cast("Queue", object()))
    with pytest.raises(ConfigurationError, match="flexiq"):
        bad.install_runtime(_services())


def test_codec_round_trip_uses_queue_codec() -> None:
    adapter, _queue = _adapter()
    payload = adapter.encode("echo", (1, None), {"unicode": "я"})
    assert adapter.decode("echo", payload) == ((1, None), {"unicode": "я"})


def test_uuid_marker_parser_rejects_bad_dlq_identity() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    payload = adapter.encode(name, (), {"_th": {"i": "not-a-uuid", "b": str(uuid4())}})
    queue.jobs["job"] = _Job(_StoredJob(name, payload))
    queue.page = _Page([{"original_job_id": "job"}], None)

    result = asyncio.run(adapter.reconcile_dead(None))

    assert result.entries == ()


def test_uninstalled_operations_and_unknown_task_are_rejected() -> None:
    queue = _FakeQueue()
    adapter = FlexiqAdapter(cast("Queue", cast("object", queue)))
    assert adapter.retry_verdict(RuntimeError()) is Verdict.FINAL
    with pytest.raises(ConfigurationError, match="install"):
        _ = adapter.wrap(_echo)
    with pytest.raises(ConfigurationError, match="зарегистрирована"):
        _ = adapter.task_name(_echo)


async def test_empty_dispatch_is_a_noop_and_unknown_message_is_rejected() -> None:
    adapter, _queue = _adapter()
    await adapter.dispatch([])
    with pytest.raises(ConfigurationError, match="зарегистрирована"):
        await adapter.dispatch(
            [
                Message(
                    id=uuid4(),
                    batch_id=uuid4(),
                    kind=OutboxKind.ITEM,
                    task_name="missing",
                    payload=b"missing",
                )
            ]
        )
    await adapter.close()


def test_task_registration_wraps_queue_failure() -> None:
    adapter, queue = _adapter()
    queue.task_error = RuntimeError(_FAILURE)
    with pytest.raises(ConfigurationError, match="опции"):
        _ = adapter.task(name="broken")(_echo)


@pytest.mark.parametrize(
    "options",
    [
        {"priority": True},
        {"queue": ""},
        {"max_retries": -1},
        {"timeout": 0},
        {"expires": -0.1},
        {"idempotent": "yes"},
        {"retry_on": ValueError},
        {"retry_on": ["ValueError"]},
    ],
)
def test_task_validates_defaults_used_by_runtime(options: Mapping[str, object]) -> None:
    adapter, _queue = _adapter()
    with pytest.raises(ConfigurationError):
        _ = adapter.task(**options)(_echo)


@pytest.mark.parametrize(
    "options",
    [
        {"priority": True},
        {"max_retries": -1},
        {"timeout": 0},
        {"queue": 1},
        {"delay": -1},
        {"metadata": 1},
        {"notes": []},
        {"expires": True},
        {"result_ttl": -1},
        {"unique_key": ""},
        {"idempotency_key": 1},
        {"idempotent": 1},
    ],
)
async def test_dispatch_validates_option_types(options: Mapping[str, object]) -> None:
    adapter, _queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    with pytest.raises(ConfigurationError):
        await adapter.dispatch([_message(adapter, name, options=options)])
    await adapter.close()


def test_producer_validation_enforces_flexiq_notes_limits_and_cancellation_type() -> None:
    adapter, _queue = _adapter()
    adapter.validate_options({"notes": {"trace": "ok"}, "priority": 1})
    with pytest.raises(ConfigurationError):
        adapter.validate_options({"notes": {str(index): index for index in range(16)}})
    with pytest.raises(ConfigurationError):
        adapter.validate_options({"notes": {"large": "x" * 4097}})
    assert adapter.is_cancelled(TaskCancelledError("cancelled"))
    assert not adapter.is_cancelled(RuntimeError("ordinary"))


@pytest.mark.parametrize(
    "decoded",
    [((),), ((), {1: "bad-key"}), ([], {}), object()],
)
def test_decode_rejects_malformed_flexiq_payload(decoded: object) -> None:
    adapter, queue = _adapter()
    queue.decode_result = decoded
    with pytest.raises(TallyhoError):
        _ = adapter.decode("echo", b"bad")


def test_codec_wraps_external_failures() -> None:
    adapter, queue = _adapter()
    queue.encode_error = RuntimeError(_FAILURE)
    with pytest.raises(TallyhoError) as encoded:
        _ = adapter.encode("echo", (), {})
    assert isinstance(encoded.value.__cause__, RuntimeError)

    queue.encode_error = None
    queue.decode_error = RuntimeError(_FAILURE)
    with pytest.raises(TallyhoError) as decoded:
        _ = adapter.decode("echo", b"bad")
    assert isinstance(decoded.value.__cause__, RuntimeError)


@pytest.mark.parametrize("error", [RuntimeError(_FAILURE), TallyhoError(_FAILURE)])
async def test_reconcile_dead_preserves_library_errors_and_wraps_external_errors(
    error: Exception,
) -> None:
    adapter, queue = _adapter()
    queue.page_error = error
    with pytest.raises(TallyhoError) as info:
        _ = await adapter.reconcile_dead(None)
    if isinstance(error, TallyhoError):
        assert info.value is error
    else:
        assert info.value.__cause__ is error
    await adapter.close()


@pytest.mark.parametrize(
    ("many_error", "one_error"),
    [
        (RuntimeError("connection lost"), None),
        (ValueError(_FAILURE), None),
        (RuntimeError(_DUPLICATE), ValueError(_FAILURE)),
    ],
)
async def test_dispatch_wraps_flexiq_enqueue_failures(
    many_error: Exception, one_error: Exception | None
) -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    queue.many_error = many_error
    queue.one_error = one_error
    with pytest.raises(TallyhoError):
        await adapter.dispatch([_message(adapter, name)])
    await adapter.close()


def test_install_rejects_bad_services_and_version(monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _FakeQueue()
    adapter = FlexiqAdapter(cast("Queue", cast("object", queue)))
    with pytest.raises(ConfigurationError, match="flexiq"):
        adapter.install_runtime(object())

    def bad_version(_package: str) -> str:
        return "not-a-version"

    monkeypatch.setattr(adapter_module, "version", bad_version)
    with pytest.raises(ConfigurationError, match="flexiq"):
        adapter.install_runtime(_services())


@pytest.mark.parametrize("generation", [0, 3])
async def test_job_dead_event_passes_item_and_generation_to_reconciliation_rule(
    generation: int,
) -> None:
    dead_letters = _FakeDeadLetters()
    adapter, queue = _adapter(dead_letters=dead_letters)
    task = adapter.task(name="echo")(_echo)
    await task("bind-loop")
    item_id = uuid4()
    batch_id = uuid4()
    marker: dict[str, object] = {"i": item_id, "b": batch_id, "r": 3}
    if generation:
        marker["g"] = generation
    payload = adapter.encode("echo", (), {"_th": marker})
    queue.jobs["dead-job"] = _Job(_StoredJob("echo", payload))
    callback = queue.events[EventType.JOB_DEAD]

    callback(EventType.JOB_DEAD, {"job_id": 1})
    callback(EventType.JOB_DEAD, {"job_id": "dead-job", "error": "boom"})
    await asyncio.sleep(0)
    await adapter.close()

    # Безусловного finish нет: поколение, lease и outbox проверяет правило сверки.
    assert dead_letters.settled == [
        ((DeadLetter(item_id, generation, "boom"),), "FlexiqDeadLetter")
    ]


async def test_job_dead_event_without_reconciler_is_reported() -> None:
    adapter, queue = _adapter()
    task = adapter.task(name="echo")(_echo)
    await task("bind-loop")
    payload = adapter.encode("echo", (), {"_th": {"i": str(uuid4()), "b": str(uuid4())}})
    queue.jobs["dead-job"] = _Job(_StoredJob("echo", payload))

    queue.events[EventType.JOB_DEAD](EventType.JOB_DEAD, {"job_id": "dead-job"})
    await asyncio.sleep(0)
    with pytest.raises(ConfigurationError, match="сверка с DLQ не собрана"):
        await _services().finish_dead(uuid4(), generation=0, error_type="t", detail="d")
    await adapter.close()


async def test_job_dead_event_is_best_effort_for_missing_job_and_finish_failure() -> None:
    dead_letters = _FakeDeadLetters(fail=True)
    adapter, queue = _adapter(dead_letters=dead_letters)
    task = adapter.task(name="echo")(_echo)
    await task("bind-loop")
    callback = queue.events[EventType.JOB_DEAD]

    callback(EventType.JOB_DEAD, {"job_id": "missing"})
    await asyncio.sleep(0)
    item_id = uuid4()
    batch_id = uuid4()
    payload = adapter.encode("echo", (), {"_th": {"i": str(item_id), "b": str(batch_id), "r": 3}})
    queue.jobs["dead-job"] = _Job(_StoredJob("echo", payload))
    callback(EventType.JOB_DEAD, {"job_id": "dead-job"})
    await asyncio.sleep(0)
    await adapter.close()

    assert dead_letters.settled == []


async def test_close_does_not_wait_for_dlq_tasks_of_the_worker_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # DLQ-задачи живут в loop исполнителя flexiq, а close могут вызвать из другого loop.
    adapter, queue = _adapter(dead_letters=_FakeDeadLetters())
    task = adapter.task(name="echo")(_echo)
    entered = threading.Event()
    release = threading.Event()

    async def bind_worker_loop() -> None:
        _ = await task("bind-loop")

    async def held_job(job_id: str) -> object | None:
        _ = job_id
        entered.set()
        for _ in itertools.count():
            if release.is_set():
                break
            await asyncio.sleep(0.005)
        return None

    monkeypatch.setattr(queue, "aget_job", held_job)
    worker = LoopThread()
    try:
        await worker.run(bind_worker_loop())
        queue.events[EventType.JOB_DEAD](EventType.JOB_DEAD, {"job_id": "dead-job"})
        assert await asyncio.to_thread(entered.wait, 5)

        await asyncio.wait_for(adapter.close(), timeout=5)

        assert library_tasks(worker.loop) == ["tallyho-flexiq-dlq"]
    finally:
        release.set()
        async with asyncio.timeout(5):
            for _ in itertools.count():
                if not library_tasks(worker.loop):
                    break
                await asyncio.sleep(0.005)
        worker.close()
