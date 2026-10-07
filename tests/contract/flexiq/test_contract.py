"""Executable A-FQ contracts against PostgreSQL and a separate Flexiq worker process."""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from tallyho import Tallyho
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.engine.dead_letters import CURSOR_KEY
from tallyho.model.errors import ConfigurationError, UnsupportedOption
from tallyho.model.states import BatchState, OutboxKind
from tallyho.protocols.broker import Message
from tallyho.storage.tables import build_metadata
from tests.contract.flexiq.contract_app import DataClassPayload, ModelPayload
from tests.helpers.db import schema_connection
from tests.helpers.flexiq_version import installed_flexiq_supported

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from flexiq import Queue

    from tests.contract.flexiq.conftest import FlexiqContract
    from tests.contract.flexiq.contract_app import ContractApp

__all__: list[str] = []


async def test_a_fq_01_arguments_and_task_identity(flexiq_contract: FlexiqContract) -> None:
    app = flexiq_contract.app
    signature = app.tasks["signature"]
    large = "Ж" * 400_000
    async with app.th.batch("a-fq-01", key="arguments") as batch:
        await batch.add_calls(
            [
                app.th.call(signature, DataClassPayload("dataclass")),
                app.th.call(
                    signature,
                    ModelPayload(value="pydantic"),
                    None,
                    "Привет",
                    large,
                    option=None,
                    named="значение",
                ),
            ]
        )

    events = await flexiq_contract.wait_events("signature", count=2)
    view = await flexiq_contract.wait_terminal(batch.handle)

    # Порядок Items одного батча в брокере не гарантирован: fast-path после
    # commit и страховочный scan могут отправить их разными пачками.
    plain, rich = sorted(events, key=lambda event: str(event["required"]))
    assert plain == {
        "event": "signature",
        "required": {"value": "dataclass"},
        "default": "default",
        "extra": [],
        "option": None,
        "rest": {},
        "task_name": app.adapter.task_name(signature),
    }
    assert rich["required"] == {"value": "pydantic"}
    assert rich["default"] is None
    assert rich["extra"] == ["Привет", large]
    assert rich["option"] is None
    assert rich["rest"] == {"named": "значение"}
    assert "_th" not in str(events)
    assert view.state is BatchState.SUCCEEDED


async def test_a_fq_02_metadata_is_exact_in_job_and_dlq(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    metadata = '{"raw":"\\u0000 Привет","spacing":  true}'
    notes = {"trace": "abc", "nested": {"value": None}}
    async with app.th.batch("a-fq-02", key="metadata") as batch:
        await batch.add_calls(
            [
                app.th.call(app.probe, "none").opts(metadata=None, notes=notes),
                app.th.call(app.tasks["flaky"], "dead", 0, final=True).opts(
                    metadata=metadata,
                    notes=notes,
                    max_retries=3,
                ),
            ]
        )

    view = await flexiq_contract.wait_terminal(batch.handle)
    task_name = app.adapter.task_name(app.tasks["flaky"])
    jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name, limit=10)
    probe_jobs = await asyncio.to_thread(
        app.queue.list_jobs,
        task_name=app.adapter.task_name(app.probe),
        limit=10,
    )
    assert len(jobs) == 1
    assert len(probe_jobs) == 1
    assert jobs[0].metadata == metadata
    assert jobs[0].notes == notes
    assert probe_jobs[0].metadata is None
    assert probe_jobs[0].notes == notes

    def read_dead() -> list[dict[str, object]]:
        return cast("list[dict[str, object]]", app.queue.dead_letters(10, 0))

    async with asyncio.timeout(10):
        while True:
            dead = await asyncio.to_thread(read_dead)
            matching = [entry for entry in dead if entry.get("original_job_id") == jobs[0].id]
            if matching:
                break
            await asyncio.sleep(0.05)
    assert matching[0]["metadata"] == metadata
    assert view.state is BatchState.COMPLETED_WITH_ERRORS


@pytest.mark.parametrize(
    "notes",
    [
        {str(index): index for index in range(16)},
        {"oversized": "x" * 4097},
    ],
)
async def test_a_fq_03_invalid_notes_fail_in_producer(
    flexiq_contract: FlexiqContract, notes: dict[str, object]
) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-03", key=str(len(notes))) as batch:
        with pytest.raises(ConfigurationError):
            await batch.add_calls([app.th.call(app.probe, "never").opts(notes=notes)])

    view = await flexiq_contract.wait_terminal(batch.handle)
    assert view.progress.found == 0
    assert flexiq_contract.events("probe") == []


async def test_a_fq_04_enqueue_options_and_expiry(flexiq_contract: FlexiqContract) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-04", key="options") as batch:
        await batch.add_calls(
            [
                app.th.call(app.probe, "visible").opts(
                    priority=7,
                    queue="contract",
                    max_retries=2,
                    timeout=9,
                    result_ttl=30,
                ),
                app.th.call(app.probe, "expired").opts(delay=1.0, expires=0.1),
            ]
        )

    events = await flexiq_contract.wait_events("probe")
    view = await flexiq_contract.wait_terminal(batch.handle)
    task_name = app.adapter.task_name(app.probe)
    async with asyncio.timeout(5):
        while True:
            jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name, limit=10)
            completed = [job for job in jobs if job.status == "complete"]
            if completed:
                break
            await asyncio.sleep(0.05)
    visible = completed[0]
    raw = cast("dict[str, object]", visible.to_dict())

    assert events[0]["args"] == ["visible"]
    assert raw["priority"] == 7
    assert raw["queue"] == "contract"
    assert raw["max_retries"] == 2
    assert raw["timeout_ms"] == 9_000
    ttl_sql = f"""SELECT result_ttl_ms FROM "{app.flexiq_schema}".jobs WHERE id = :id
        UNION ALL SELECT result_ttl_ms
        FROM "{app.flexiq_schema}".archived_jobs WHERE id = :id"""  # ruff: ignore[hardcoded-sql-expression]  # schema is generated by temporary_schema, never user input
    ttl_query = text(ttl_sql)
    async with app.engine.connect() as connection:
        stored_ttl = await connection.scalar(ttl_query, {"id": visible.id})
    assert stored_ttl == 30_000
    assert view.progress.ok == 1
    assert view.progress.error == 1
    expired = [item async for item in batch.handle.items(labels=["expired"])]
    assert len(expired) == 1

    async with app.th.batch("a-fq-04-priority", key="priority") as priority_batch:
        await priority_batch.add_calls(
            [
                app.th.call(app.probe, "p1").opts(queue="priority", priority=1),
                app.th.call(app.probe, "p9").opts(queue="priority", priority=9),
                app.th.call(app.probe, "p5").opts(queue="priority", priority=5),
            ]
        )
    _ = await app.th.run_maintenance_once()
    await flexiq_contract.start_worker("priority", workers=1)
    ordered = await flexiq_contract.wait_events("probe", count=4)
    _ = await flexiq_contract.wait_terminal(priority_batch.handle)
    priority_order = [
        cast("str", cast("list[object]", row["args"])[0])
        for row in ordered
        if str(cast("list[object]", row["args"])[0]).startswith("p")
    ]
    assert priority_order == ["p9", "p5", "p1"]


async def test_a_fq_05_delay_is_added_after_batch_start_at(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    start_at = datetime.now(UTC) + timedelta(seconds=0.4)
    delay = 0.3
    sent_before = time.time()
    async with app.th.batch("a-fq-05", key="delay", start_at=start_at) as batch:
        await batch.add_calls([app.th.call(app.probe, "delayed").opts(delay=delay)])

    events = await flexiq_contract.wait_events("probe")
    _ = await flexiq_contract.wait_terminal(batch.handle)
    assert float(cast("float", events[0]["at"])) >= sent_before
    assert float(cast("float", events[0]["at"])) >= start_at.timestamp() + delay - 0.05
    assert events[0]["args"] == ["delayed"]


async def test_a_fq_06_user_deduplication_and_relay_repeat(  # ruff: ignore[too-many-locals]  # contract proves stored payload and all three dedup modes together
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    options: dict[str, dict[str, object]] = {
        "idem": {"delay": 3.0, "idempotency_key": "user-idempotency"},
        "unique": {"delay": 3.0, "unique_key": "user-unique"},
        "flag": {"delay": 3.0, "idempotent": True},
    }
    async with app.th.batch("a-fq-06", key="dedup") as batch:
        await batch.add_calls(
            [
                app.th.call(app.probe, "idem").opts(delay=3.0, idempotency_key="user-idempotency"),
                app.th.call(app.probe, "unique").opts(delay=3.0, unique_key="user-unique"),
                app.th.call(app.probe, "flag").opts(delay=3.0, idempotent=True),
            ]
        )

    _ = await app.th.run_maintenance_once()
    task_name = app.adapter.task_name(app.probe)
    async with asyncio.timeout(5):
        while True:
            jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name, limit=10)
            if len(jobs) == 3:
                break
            await asyncio.sleep(0.05)

    repeated: list[Message] = []
    for job in jobs:
        full = await app.queue.aget_job(job.id)
        assert full is not None
        stored = full._py_job  # ruff: ignore[private-member-access]  # exact stored payload is the relay repeat fixture
        args, kwargs = app.adapter.decode(stored.task_name, stored.payload_bytes)
        marker = cast("dict[str, object]", kwargs.pop("_th"))
        key = cast("str", args[0])
        repeated.append(
            Message(
                id=UUID(cast("str", marker["i"])),
                batch_id=UUID(cast("str", marker["b"])),
                kind=OutboxKind.ITEM,
                task_name=stored.task_name,
                payload=app.adapter.encode(stored.task_name, args, kwargs),
                options=options[key],
            )
        )
    await app.adapter.dispatch(repeated)
    await app.adapter.dispatch(repeated)

    events = await flexiq_contract.wait_events("probe", count=3)
    view = await flexiq_contract.wait_terminal(batch.handle)
    final_jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name, limit=10)
    jobs_by_id = {job.id: job for job in final_jobs}
    raw = [cast("dict[str, object]", job.to_dict()) for job in jobs_by_id.values()]

    event_args = [cast("list[object]", row["args"]) for row in events]
    assert sorted(cast("str", args[0]) for args in event_args) == sorted(options)
    assert len(jobs_by_id) == 3
    unique_keys = {cast("str", row["unique_key"]) for row in raw}
    assert "user-idempotency" in unique_keys
    assert "user-unique" in unique_keys
    assert any(key.startswith("th:") for key in unique_keys)
    assert view.progress.ok == 3


async def test_a_fq_06_new_generation_is_not_merged_with_live_previous_job(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-06", key="generations") as batch:
        await batch.add_calls([app.th.call(app.probe, "generation").opts(delay=3.0)])

    _ = await app.th.run_maintenance_once()
    task_name = app.adapter.task_name(app.probe)
    async with asyncio.timeout(5):
        while True:
            jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name, limit=10)
            if len(jobs) == 1:
                break
            await asyncio.sleep(0.05)
    full = await app.queue.aget_job(jobs[0].id)
    assert full is not None
    stored = full._py_job  # ruff: ignore[private-member-access]  # exact stored payload is the redispatch fixture
    args, kwargs = app.adapter.decode(stored.task_name, stored.payload_bytes)
    marker = cast("dict[str, object]", kwargs.pop("_th"))
    first = Message(
        id=UUID(cast("str", marker["i"])),
        batch_id=UUID(cast("str", marker["b"])),
        kind=OutboxKind.ITEM,
        task_name=stored.task_name,
        payload=app.adapter.encode(stored.task_name, args, kwargs),
        options={"delay": 3.0},
    )
    # Повтор relay той же отправки сливается с живой джобой (D-013), а новое
    # поколение — нет: у него своя джоба и, если она умрёт, своя запись DLQ (UC-15).
    await app.adapter.dispatch([first])
    await app.adapter.dispatch([replace(first, generation=1)])

    jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name, limit=10)
    raw = [cast("dict[str, object]", job.to_dict()) for job in jobs]
    assert sorted(cast("str", row["unique_key"]) for row in raw) == [
        f"th:{first.id}:0",
        f"th:{first.id}:1",
    ]
    # Выполнит Item одна из джоб, вторую claim отсечёт как дубль или терминальный Item.
    view = await flexiq_contract.wait_terminal(batch.handle)
    assert view.progress.ok == 1
    assert len(await flexiq_contract.wait_events("probe")) >= 1


async def test_a_fq_07_incompatible_options_fail_explicitly(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app

    async def extra_task() -> None:
        await asyncio.sleep(0)

    with pytest.raises(UnsupportedOption, match="debounce"):
        _ = app.adapter.task(debounce=1)(extra_task)
    with pytest.raises(UnsupportedOption, match="batch"):
        _ = app.adapter.task(batch=True)(extra_task)

    async with app.th.batch("a-fq-07", key="depends") as batch:
        with pytest.raises(UnsupportedOption, match="fed_by"):
            await batch.add_calls([app.th.call(app.probe, "never").opts(depends_on="external-job")])
    view = await flexiq_contract.wait_terminal(batch.handle)
    assert view.progress.found == 0


async def test_a_fq_08_retry_policy_attempts_and_exhaustion(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    flaky = app.tasks["flaky"]
    async with app.th.batch("a-fq-08", key="retry") as batch:
        await batch.add_calls(
            [
                app.th.call(flaky, "eventual", 2),
                app.th.call(flaky, "exhausted", 99).opts(max_retries=1),
                app.th.call(flaky, "filtered", 0, final=True).opts(max_retries=3),
            ]
        )

    events = await flexiq_contract.wait_events("flaky", count=6)
    view = await flexiq_contract.wait_terminal(batch.handle)
    attempts = {
        key: [int(cast("int", row["attempt"])) for row in events if row["key"] == key]
        for key in ("eventual", "exhausted", "filtered")
    }

    assert attempts == {
        "eventual": [0, 1, 2],
        "exhausted": [0, 1],
        "filtered": [0],
    }
    assert view.progress.ok == 1
    assert view.progress.error == 2
    assert len([item async for item in batch.handle.items(labels=["exhausted"])]) == 2


async def test_a_fq_09_retry_budget_and_circuit_breaker(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-09-budget", key="budget") as budget_batch:
        await budget_batch.add(app.tasks["budget"], "budget")

    budget_view = await flexiq_contract.wait_terminal(budget_batch.handle)
    budget_attempts = flexiq_contract.events("budget")

    def read_dead() -> list[dict[str, object]]:
        return cast("list[dict[str, object]]", app.queue.dead_letters(20, 0))

    dead = await asyncio.to_thread(read_dead)
    assert 1 <= len(budget_attempts) < 11
    assert dead
    assert budget_view.progress.error == 1

    async with app.th.batch("a-fq-09-breaker", key="breaker") as breaker_batch:
        await breaker_batch.add(app.tasks["breaker"], "breaker")

    first = await flexiq_contract.wait_events("breaker")
    await asyncio.sleep(0.2)
    _ = await app.th.run_maintenance_once()
    during_cooldown = await breaker_batch.handle.view()
    attempts = await flexiq_contract.wait_events("breaker", count=2, timeout_seconds=10)
    breaker_view = await flexiq_contract.wait_terminal(breaker_batch.handle)

    assert first[0]["attempt"] == 0
    assert not during_cooldown.state.is_terminal
    assert [row["attempt"] for row in attempts] == [0, 1]
    assert breaker_view.state is BatchState.SUCCEEDED


async def test_a_fq_10_hard_and_soft_timeouts(flexiq_contract: FlexiqContract) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-10", key="timeouts") as batch:
        await batch.add_calls(
            [
                app.th.call(app.tasks["hard_timeout"], "hard"),
                app.th.call(app.tasks["soft_timeout"], "soft"),
            ]
        )

    hard_finished = await flexiq_contract.wait_events("hard-finish", timeout_seconds=15)
    soft_started = await flexiq_contract.wait_events("soft-start", count=2)
    view = await flexiq_contract.wait_terminal(batch.handle, timeout_seconds=15)

    assert len(flexiq_contract.events("hard-start")) == 1
    assert len(hard_finished) == 1
    assert [row["attempt"] for row in soft_started] == [0, 1]
    assert view.progress.ok == 1
    assert view.progress.error == 1


async def test_a_ch_10_requeued_running_job_does_not_orphan_item_on_retry(
    flexiq_contract: FlexiqContract,
) -> None:
    """Fix-7: ``requeue_job`` закрывает джобу no-op дублем, а исходная попытка падает."""
    app = flexiq_contract.app
    requeued = app.tasks["requeued"]
    async with app.th.batch("a-ch-10", key="requeue") as batch:
        await batch.add(requeued, "one")

    started = await flexiq_contract.wait_events("requeue-start")
    job_id = cast("str", started[0]["job_id"])
    assert await asyncio.to_thread(app.queue.requeue_job, job_id)
    # Повторная доставка той же джобы упирается в живой lease и возвращает успех:
    # flexiq считает джобу завершённой, хотя задача ещё выполняется.
    async with asyncio.timeout(15):
        while True:
            job = await app.queue.aget_job(job_id)
            if job is not None and job.status == "complete":
                break
            flexiq_contract.assert_worker_alive()
            await asyncio.sleep(0.05)
    assert len(flexiq_contract.events("requeue-start")) == 1

    # Исходное выполнение падает с повторяемой ошибкой; flexiq её уже не повторит.
    _ = (flexiq_contract.root / "requeue-release-one").write_text("go", encoding="utf-8")
    view = await flexiq_contract.wait_terminal(batch.handle, timeout_seconds=20)

    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == 1
    runs = flexiq_contract.events("requeue-start")
    assert len(runs) == 2
    # Item доделала новая джоба: исходная закрыта дублем и ретрая не получила.
    assert runs[1]["job_id"] != job_id
    jobs = await asyncio.to_thread(
        app.queue.list_jobs, task_name=app.adapter.task_name(requeued), limit=10
    )
    assert sorted(job.status for job in jobs) == ["complete", "complete"]


@asynccontextmanager
async def _claim_outage(app: ContractApp) -> AsyncGenerator[None]:
    """Пока контекст открыт, claim и finish падают: таблицы lease «нет» (отказ PostgreSQL)."""
    schema = app.engine.dialect.identifier_preparer.quote(app.tallyho_schema)
    async with app.engine.begin() as conn:
        _ = await conn.execute(text(f"ALTER TABLE {schema}.th_lease RENAME TO th_lease_down"))
    try:
        yield
    finally:
        async with app.engine.begin() as conn:
            _ = await conn.execute(text(f"ALTER TABLE {schema}.th_lease_down RENAME TO th_lease"))


async def _item_rows(app: ContractApp, batch_id: UUID) -> list[tuple[int, str | None, object, int]]:
    """``(state, label, error, generation)`` Items батча."""
    item = build_metadata().item
    async with schema_connection(app.engine, app.tallyho_schema) as conn:
        rows = await conn.execute(
            select(item.c.state, item.c.label, item.c.error, item.c.generation).where(
                item.c.batch_id == batch_id
            )
        )
        return [(int(row[0]), row[1], row[2], int(row[3])) for row in rows]


async def test_fix_6_dead_job_without_recorded_result_is_reconciled(  # ruff: ignore[too-many-locals]  # один сценарий: отказ, сверка, повтор, поздняя запись DLQ
    flexiq_contract: FlexiqContract,
) -> None:
    """Fix-6: джоба умерла на claim, событие ``JOB_DEAD`` не записало итог — Item завершает сверка.

    Очередь ``quarantine`` обслуживает отдельный воркер: его можно остановить,
    чтобы повторно отправленная джоба осталась ждать в брокере.
    """
    app = flexiq_contract.app
    doomed = app.tasks["doomed"]
    task_name = app.adapter.task_name(doomed)
    await flexiq_contract.start_worker("quarantine")

    def dead_letters() -> list[dict[str, object]]:
        entries = cast("list[dict[str, object]]", app.queue.dead_letters(20, 0))
        return [entry for entry in entries if entry.get("task_name") == task_name]

    worker_log = flexiq_contract.root / "worker-quarantine.log"
    async with _claim_outage(app):
        async with app.th.batch("fix-6", key="dlq") as batch:
            await batch.add_calls([app.th.call(doomed, "one").opts(queue="quarantine")])
        # Обе попытки падают на claim с CompleterError, flexiq отправляет джобу в DLQ;
        # обработчик события JOB_DEAD в воркере тоже не может записать итог.
        async with asyncio.timeout(20):
            while True:
                dead = await asyncio.to_thread(dead_letters)
                if dead and "JOB_DEAD" in worker_log.read_text(encoding="utf-8", errors="replace"):
                    break
                await asyncio.sleep(0.05)
        await flexiq_contract.stop_extra_workers()
        # Дефект: Item active без lease, outbox и живой джобы; задача не вызывалась.
        assert await _item_rows(app, batch.handle.id) == [(0, None, None, 0)]
    (letter,) = dead
    assert flexiq_contract.events("doomed") == []

    # Сверка в цикле relay (и в run_maintenance_once) завершает Item и батч.
    view = await flexiq_contract.wait_terminal(batch.handle)

    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    assert (view.progress.error, view.labels) == (1, {"exhausted": 1})
    ((state, label, error, generation),) = await _item_rows(app, batch.handle.id)
    assert (state, label, generation) == (12, "exhausted", 0)
    # Итог записала сверка, а не обработчик события: у него тип FlexiqDeadLetter.
    assert cast("dict[str, object]", error)["type"] == "DeadLetter"
    meta = build_metadata().meta
    async with schema_connection(app.engine, app.tallyho_schema) as conn:
        stored = await conn.scalar(select(meta.c.value).where(meta.c.key == CURSOR_KEY))
    cursor = cast("dict[str, object]", json.loads(stored or ""))
    # Водяной знак — failed_at самой новой разобранной записи; обход закончен.
    assert cursor == {"w": letter["failed_at"], "h": None, "r": None}

    # Повтор: новая джоба несёт поколение 1 и ждёт в очереди без воркера. По
    # данным tallyho Item выглядит так же, как осиротевший, а запись DLQ прошлой
    # отправки сверка перечитывает на каждом обходе — Item она трогать не должна.
    assert await batch.handle.retry_failed() == 1
    async with asyncio.timeout(10):
        while True:
            jobs = await asyncio.to_thread(app.queue.list_jobs, task_name=task_name)
            if len(jobs) > 1:  # мёртвая джоба первой отправки и новая
                break
            await asyncio.sleep(0.05)
    assert sorted(job.status for job in jobs) == ["dead", "pending"]
    for _ in range(5):
        _ = await app.th.run_maintenance_once()
        await asyncio.sleep(0.1)
    assert await _item_rows(app, batch.handle.id) == [(0, None, None, 1)]

    await flexiq_contract.start_worker("quarantine")
    final = await flexiq_contract.wait_terminal(batch.handle)

    assert final.state is BatchState.SUCCEEDED
    (run,) = flexiq_contract.events("doomed")
    assert run["job_id"] != letter["original_job_id"]


@asynccontextmanager
async def _result_outage(app: ContractApp) -> AsyncGenerator[None]:
    """Пока контекст открыт, flexiq не может записать результат: таблицы джоб «нет»."""
    schema = app.engine.dialect.identifier_preparer.quote(app.flexiq_schema)
    async with app.engine.begin() as conn:
        _ = await conn.execute(text(f"ALTER TABLE {schema}.jobs RENAME TO jobs_down"))
    try:
        yield
    finally:
        async with app.engine.begin() as conn:
            _ = await conn.execute(text(f"ALTER TABLE {schema}.jobs_down RENAME TO jobs"))


async def _without_executor(app: ContractApp) -> tuple[int, int]:
    """Сколько строк ``th_lease`` и ``th_outbox`` осталось у Items."""
    tables = build_metadata()
    async with schema_connection(app.engine, app.tallyho_schema) as conn:
        leases = await conn.scalar(select(func.count()).select_from(tables.lease))
        outbox = await conn.scalar(select(func.count()).select_from(tables.outbox))
    return int(leases or 0), int(outbox or 0)


async def test_fix_19_lost_result_is_recovered_by_flexiq_timeout(
    flexiq_contract: FlexiqContract,
) -> None:
    """Fix-19: flexiq не записал результат попытки без lease — Item ждёт ``timeout`` джобы.

    Факт о flexiq (ARCHITECTURE §11.3): отчёт о результате, который воркер не смог
    записать, не повторяется, джоба остаётся ``running`` за живым воркером, и её
    подбирает только реапер таймаута. Item без lease и outbox при этом не видят ни
    sweeper, ни сверка с DLQ; после реапа повтор проходит обычный claim.
    """
    app = flexiq_contract.app
    stranded = app.tasks["stranded"]
    async with app.th.batch("fix-19", key="lost-result") as batch:
        await batch.add(stranded, "one")
    (first,) = await flexiq_contract.wait_events("stranded-start")
    job_id = cast("str", first["job_id"])

    worker_log = flexiq_contract.root / "worker.log"
    async with _result_outage(app):
        # Попытка падает с повторяемой ошибкой: release удаляет lease, а flexiq не может
        # записать ни ошибку, ни ретрай.
        _ = (flexiq_contract.root / "stranded-release-one").write_text("go", encoding="utf-8")
        async with asyncio.timeout(15):
            while "result handling error" not in worker_log.read_text(
                encoding="utf-8", errors="replace"
            ):
                flexiq_contract.assert_worker_alive()
                await asyncio.sleep(0.05)

    # Item active без lease и outbox, джоба числится выполняющейся; обслуживание его не видит.
    for _ in range(5):
        _ = await app.th.run_maintenance_once()
    assert await _item_rows(app, batch.handle.id) == [(0, None, None, 0)]
    assert await _without_executor(app) == (0, 0)
    job = await app.queue.aget_job(job_id)
    assert job is not None
    assert job.status == "running"

    # Реапер таймаута flexiq (timeout=3 с у задачи) повторяет ту же джобу; claim проходит.
    view = await flexiq_contract.wait_terminal(batch.handle, timeout_seconds=20)

    assert view.state is BatchState.SUCCEEDED
    runs = flexiq_contract.events("stranded-start")
    assert [(run["job_id"], run["attempt"]) for run in runs] == [(job_id, 0), (job_id, 1)]
    # Повтор пришёл не раньше timeout задачи от старта первой попытки.
    assert cast("float", runs[1]["at"]) - cast("float", first["at"]) >= 3 - 0.5


async def test_a_fq_11_flexiq_cancellation_finishes_item_cancelled(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-11", key="cancel") as batch:
        await batch.add(app.tasks["cancellable"], "cancel-me")

    started = await flexiq_contract.wait_events("cancel-start")
    job_id = cast("str", started[0]["job_id"])
    assert await app.queue.acancel_running_job(job_id)
    view = await flexiq_contract.wait_terminal(batch.handle)

    assert view.state.is_terminal
    assert view.progress.cancelled == 1


async def test_a_fq_12_broker_and_tallyho_concurrency_limits_compose(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    limited = app.tasks["limited"]
    async with app.th.batch("a-fq-12", key="limits", max_in_flight=3) as batch:
        await batch.add_calls([app.th.call(limited, str(index)) for index in range(6)])

    _ = await flexiq_contract.wait_events("limited-finish", count=6)
    view = await flexiq_contract.wait_terminal(batch.handle)
    starts = {
        cast("str", row["key"]): float(cast("float", row["at"]))
        for row in flexiq_contract.events("limited-start")
    }
    finishes = {
        cast("str", row["key"]): float(cast("float", row["at"]))
        for row in flexiq_contract.events("limited-finish")
    }
    points = sorted({*starts.values(), *finishes.values()})
    maximum = max(sum(starts[key] <= point < finishes[key] for key in starts) for point in points)

    assert maximum == 2
    assert view.progress.ok == 6

    async with app.th.batch("a-fq-12-rate", key="rate") as rate_batch:
        await rate_batch.add_calls(
            [app.th.call(app.tasks["rate_limited"], str(index)) for index in range(6)]
        )
    rate_events = await flexiq_contract.wait_events("rate", count=6, timeout_seconds=10)
    _ = await flexiq_contract.wait_terminal(rate_batch.handle)
    rate_times = [float(cast("float", row["at"])) for row in rate_events]
    rate_windows = Counter(int(at) for at in rate_times)
    assert len(rate_windows) >= 3
    assert max(rate_windows.values()) <= 2


async def test_a_fq_13_middleware_injection_and_predicate_are_preserved(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    async with app.th.batch("a-fq-13", key="extensions") as batch:
        await batch.add(app.tasks["integrated"], "value")

    integrated = await flexiq_contract.wait_events("integrated")
    view = await flexiq_contract.wait_terminal(batch.handle)
    before = flexiq_contract.events("middleware-before")
    after = flexiq_contract.events("middleware-after")
    enqueue = flexiq_contract.events("middleware-enqueue")
    predicates = flexiq_contract.events("predicate")
    hooks = flexiq_contract.events("before-task")

    assert integrated[0]["resource"] == "injected"
    assert {row["scope"] for row in before} >= {"global", "task"}
    assert {row["scope"] for row in after} >= {"global", "task"}
    assert all(row["has_th"] is True for row in enqueue)
    assert any(row["has_th"] is True for row in predicates)
    assert any(row["has_th"] is True for row in hooks)
    assert view.state is BatchState.SUCCEEDED


async def test_a_fq_14_broker_replay_is_noop_and_retry_failed_reexecutes(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    metadata = '{"user":"kept-on-original"}'
    async with app.th.batch("a-fq-14", key="replay") as batch:
        await batch.add_calls(
            [app.th.call(app.tasks["flaky"], "replay", 99).opts(max_retries=0, metadata=metadata)]
        )

    _ = await flexiq_contract.wait_terminal(batch.handle)
    original_events = await flexiq_contract.wait_events("flaky")

    def read_dead() -> list[dict[str, object]]:
        return cast("list[dict[str, object]]", app.queue.dead_letters(20, 0))

    async with asyncio.timeout(5):
        while True:
            entries = await asyncio.to_thread(read_dead)
            matching = [entry for entry in entries if entry.get("metadata") == metadata]
            if matching:
                break
            await asyncio.sleep(0.05)
    dead_id = cast("str", matching[0]["id"])
    original_id = cast("str", matching[0]["original_job_id"])
    retry_dead_id = await app.queue.aretry_dead(dead_id)
    replay = await app.queue.areplay(original_id)

    async with asyncio.timeout(8):
        while True:
            retried_job = await app.queue.aget_job(retry_dead_id)
            replayed_job = await app.queue.aget_job(replay.id)
            if (
                retried_job is not None
                and replayed_job is not None
                and retried_job.status == "complete"
                and replayed_job.status == "complete"
            ):
                break
            await asyncio.sleep(0.05)
    assert retry_dead_id != original_id
    assert replay.id != original_id
    assert retried_job.metadata != metadata
    assert replayed_job.metadata != metadata
    assert flexiq_contract.events("flaky") == original_events

    assert await batch.handle.retry_failed() == 1
    retried = await flexiq_contract.wait_events("flaky", count=2)
    final = await flexiq_contract.wait_terminal(batch.handle)
    assert len(retried) == 2
    assert final.progress.error == 1


async def test_a_fq_15_prefork_and_sync_tasks_are_rejected(
    flexiq_contract: FlexiqContract,
) -> None:
    app = flexiq_contract.app
    prefork = FlexiqAdapter(app.queue, pool="prefork")
    client = Tallyho(app.engine, schema="prefork_not_migrated")
    with pytest.raises(ConfigurationError, match="thread"):
        client.install(prefork)
    await prefork.close()

    def sync_task() -> None:
        return None

    sync_candidate = cast("Callable[[], Awaitable[None]]", sync_task)
    with pytest.raises(ConfigurationError, match="async def"):
        _ = app.adapter.task()(sync_candidate)


async def test_a_fq_16_plain_flexiq_tasks_coexist(flexiq_contract: FlexiqContract) -> None:
    app = flexiq_contract.app

    def enqueue_plain() -> object:
        return app.queue.enqueue(task_name=app.ordinary.name, args=("plain",))

    _ = await asyncio.to_thread(enqueue_plain)
    events = await flexiq_contract.wait_events("ordinary")

    assert events == [{"event": "ordinary", "value": "plain", "item_is_none": True}]


async def test_a_fq_17_version_guard_is_explicit(flexiq_contract: FlexiqContract) -> None:
    # Диапазон pyproject `>=2.0,<3`: nightly-ячейка master может нести 2.1+ (Fix-32).
    assert installed_flexiq_supported(), version("flexiq")
    app = flexiq_contract.app
    invalid = FlexiqAdapter(cast("Queue", object()))
    client = Tallyho(app.engine, schema="bad_api_not_migrated")
    with pytest.raises(ConfigurationError, match=r"flexiq>=2\.0,<3"):
        client.install(invalid)
    await invalid.close()
