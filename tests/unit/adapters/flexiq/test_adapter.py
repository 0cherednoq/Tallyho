from __future__ import annotations

import asyncio
from dataclasses import dataclass
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
    CompleterError,
    ConfigurationError,
    TallyhoError,
    UnsupportedOption,
)
from tallyho.model.states import OutboxKind
from tallyho.protocols.broker import Dispatcher, Message, RetryLimits, Runtime, Verdict
from tallyho.protocols.serialization import PayloadCodec
from tallyho.runtime.tracked import TaskRuntime, bind_runtime

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from flexiq import Queue

    from tallyho.engine.completer import Completer, FinishResult, ItemRef
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import WorkerRuntime

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
        return self.jobs.get(job_id)

    async def adead_letters_after(self, *, limit: int, after: str | None) -> object:
        assert limit == 1_000
        assert after in {None, "cursor-1"}
        await asyncio.sleep(0)
        if self.page_error is not None:
            raise self.page_error
        return self.page

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
class _FakeCompleter:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.finished: list[tuple[ItemRef, FinishResult]] = []

    async def finish(self, ref: ItemRef, value: FinishResult) -> bool:
        await asyncio.sleep(0)
        if self.fail:
            raise RuntimeError(_FAILURE)
        self.finished.append((ref, value))
        return True


def _services(
    completer: object | None = None,
    *,
    adapter: FlexiqAdapter | None = None,
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
    )


def _adapter(
    *, pool: str = "thread", completer: object | None = None
) -> tuple[FlexiqAdapter, _FakeQueue]:
    queue = _FakeQueue()
    adapter = FlexiqAdapter(cast("Queue", cast("object", queue)), pool=pool)
    if pool == "thread":
        adapter.install_runtime(_services(completer, adapter=adapter))
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
    assert batch["idempotency_keys"] == [f"th:{first.id}", f"th:{second.id}"]
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
    messages = [_message(adapter, name), _message(adapter, name)]
    queue.reject_many_once = True

    await adapter.dispatch(messages)

    assert queue.many == []
    assert [item["idempotency_key"] for item in queue.one] == [
        f"th:{messages[0].id}",
        f"th:{messages[1].id}",
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
        ([ValueError], [ValueError, CompleterError]),
        ((ValueError, KeyError), [ValueError, KeyError, CompleterError]),
        # Список уже покрывает CompleterError — остаётся как есть.
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
    await retried(TypeError())
    # dont_retry_on пользователя сильнее: его решение не переопределяется.
    await forbidden(CompleterError())

    assert verdicts == [Verdict.RETRY, Verdict.FINAL, Verdict.FINAL]
    await adapter.close()


async def test_reconcile_dead_decodes_items_and_preserves_cursor() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    item_id = uuid4()
    batch_id = uuid4()
    payload = adapter.encode(name, (), {"_th": {"i": str(item_id), "b": str(batch_id), "r": 3}})
    queue.jobs["job-1"] = _Job(_StoredJob(name, payload))
    queue.page = _Page(
        [{"original_job_id": "job-1"}, {"original_job_id": "missing"}, {"other": 1}],
        "cursor-2",
    )

    result = await adapter.reconcile_dead("cursor-1")

    assert result.item_ids == (item_id,)
    assert result.cursor == "cursor-2"
    assert EventType.JOB_DEAD in queue.events
    await adapter.close()


async def test_callback_marker_contains_stable_callback_identity() -> None:
    adapter, queue = _adapter()
    name = adapter.task_name(adapter.task(name="echo")(_echo))
    message = _message(adapter, name, kind=OutboxKind.CALLBACK)
    await adapter.dispatch([message])
    kwargs = cast("list[dict[str, object]]", queue.many[0]["kwargs_list"])[0]
    assert kwargs["_th"] == {
        "c": str(message.id),
        "b": str(message.batch_id),
        "r": 3,
        "s": None,
    }
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

    assert result.item_ids == ()


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


async def test_job_dead_event_finishes_item_on_worker_loop() -> None:
    completer = _FakeCompleter()
    adapter, queue = _adapter(completer=completer)
    task = adapter.task(name="echo")(_echo)
    await task("bind-loop")
    item_id = uuid4()
    batch_id = uuid4()
    payload = adapter.encode("echo", (), {"_th": {"i": item_id, "b": batch_id, "r": 3}})
    queue.jobs["dead-job"] = _Job(_StoredJob("echo", payload))
    callback = queue.events[EventType.JOB_DEAD]

    callback(EventType.JOB_DEAD, {"job_id": 1})
    callback(EventType.JOB_DEAD, {"job_id": "dead-job", "error": "boom"})
    await asyncio.sleep(0)
    await adapter.close()

    assert len(completer.finished) == 1
    ref, result = completer.finished[0]
    assert ref.id == item_id
    assert result.label == "exhausted"
    assert result.error == {"type": "FlexiqDeadLetter", "message": "boom"}


async def test_job_dead_event_is_best_effort_for_missing_job_and_finish_failure() -> None:
    completer = _FakeCompleter(fail=True)
    adapter, queue = _adapter(completer=completer)
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

    assert completer.finished == []
