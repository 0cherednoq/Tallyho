"""Атрибуты и memo корня через публичный API (A-AT-01, 04, 05, 10, 11)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho import Tallyho
from tallyho.model.errors import BatchPurged, InvalidAttributesError
from tallyho.model.states import BatchState
from tallyho.protocols.observer import NullObserver
from tallyho.storage.tables import build_metadata
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Mapping

    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.views import BatchSummary

__all__: list[str] = []

SECRET = "s3cr3t-attr-value"  # ruff: ignore[hardcoded-password-string]  # маркер утечки в логи
MEMO_SECRET = "s3cr3t-memo-value"  # ruff: ignore[hardcoded-password-string]  # маркер утечки в логи
TENANT = UUID("11111111-2222-3333-4444-555555555555")
ATTRIBUTES: Mapping[str, object] = {"tenant": TENANT, "campaign_id": 42, "dry_run": False}
STORED = {"tenant": str(TENANT), "campaign_id": 42, "dry_run": False}
MEMO: Mapping[str, object] = {"requested_by": "ops", "tags": ["a", "b"], "nested": {"n": 1}}


class RecordingObserver(NullObserver):
    """Собирает всё, что engine передаёт наблюдателю."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    @override
    def __getattribute__(self, name: str) -> object:
        value = super().__getattribute__(name)
        if name.startswith("_") or name == "seen" or not callable(value):
            return value
        seen = self.seen

        def spy(*args: object, **kwargs: object) -> object:
            seen.append(repr((name, args, kwargs)))
            return value(*args, **kwargs)

        return spy


@asynccontextmanager
async def make_client(
    engine: AsyncEngine,
    schema: str,
    *,
    attributes_max_keys: int = 32,
    retention_days: int = 14,
) -> AsyncGenerator[tuple[Tallyho, InlineBroker, FakeClock, RecordingObserver]]:
    clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
    broker = InlineBroker(seed=1)
    observer = RecordingObserver()
    th = Tallyho(
        engine,
        schema=schema,
        clock=clock,
        observer=observer,
        attributes_max_keys=attributes_max_keys,
        retention=timedelta(days=retention_days),
    )
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        yield th, broker, clock, observer
    finally:
        await th.aclose()


@contextlib.contextmanager
def statements_of(engine: AsyncEngine) -> Generator[list[str]]:
    statements: list[str] = []

    def record(_conn: Connection, _cursor: object, statement: str, *args: object) -> None:
        del args
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)


async def attr_rows(engine: AsyncEngine, schema: str) -> int:
    attr = build_metadata().batch_attr
    async with engine.connect() as raw:
        conn = await raw.execution_options(schema_translate_map={None: schema})
        return int(await conn.scalar(select(func.count()).select_from(attr)) or 0)


async def noop(value: int) -> None:
    _ = value
    await asyncio.sleep(0)


async def test_attributes_and_memo_are_stored_and_visible_in_tree(
    engine: AsyncEngine, schema: str
) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock, _observer):
        async with th.batch("attrs", key="one", attributes=ATTRIBUTES, memo=MEMO) as root:
            send = root.sub_batch("send")
            await send.add(noop, 1)

        view = await root.handle.view()
        child = await (await root.handle.child("send")).view()

    assert dict(view.attributes) == STORED
    assert view.memo == MEMO
    assert dict(view.children["send"].attributes) == STORED
    assert view.children["send"].memo == MEMO
    # Чтение поддерева начиная с под-батча отдаёт те же значения корня.
    assert dict(child.attributes) == STORED
    assert child.memo == MEMO
    assert await attr_rows(engine, schema) == 1


async def test_batch_without_attributes_has_no_side_row(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock, _observer):
        async with th.batch("attrs", key="plain") as root:
            await root.add(noop, 1)
        view = await root.handle.view()

    assert dict(view.attributes) == {}
    assert view.memo is None
    assert await attr_rows(engine, schema) == 0


async def test_memo_only_and_attributes_only(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock, _observer):
        async with th.batch("attrs", key="memo", memo={"note": "x"}) as with_memo:
            pass
        async with th.batch("attrs", key="attr", attributes={"n": 1}) as with_attr:
            pass
        memo_view = await with_memo.handle.view()
        attr_view = await with_attr.handle.view()

    assert (dict(memo_view.attributes), memo_view.memo) == ({}, {"note": "x"})
    assert (dict(attr_view.attributes), attr_view.memo) == ({"n": 1}, None)


async def test_user_transaction_rollback_leaves_no_rows(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock, _observer):
        scoped = engine.execution_options(schema_translate_map={None: schema})
        async with AsyncSession(scoped) as session:
            async with th.batch(
                "attrs", key="rollback", attributes=ATTRIBUTES, memo=MEMO, session=session
            ) as root:
                await root.add(noop, 1)
            await session.rollback()

        with pytest.raises(BatchPurged):
            _ = await root.handle.view()
    assert await attr_rows(engine, schema) == 0


async def test_repeated_create_keeps_first_attributes(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock, _observer):
        async with th.batch("attrs", key="same", attributes={"n": 1}, memo={"v": 1}) as first:
            pass
        async with th.batch("attrs", key="same", attributes={"n": 2, "x": "y"}, memo=None) as again:
            pass
        async with th.batch("attrs", key="bare") as bare:
            pass
        async with th.batch("attrs", key="bare", attributes={"late": True}) as bare_again:
            pass

        assert again.handle.id == first.handle.id
        view = await again.handle.view()
        bare_view = await bare_again.handle.view()

    assert (dict(view.attributes), view.memo) == ({"n": 1}, {"v": 1})
    assert bare_again.handle.id == bare.handle.id
    assert dict(bare_view.attributes) == {}
    assert await attr_rows(engine, schema) == 1


async def test_invalid_attributes_fail_before_database(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema, attributes_max_keys=1) as (th, _b, _c, _o):
        with statements_of(engine) as statements:
            with pytest.raises(InvalidAttributesError):
                _ = th.batch("attrs", key="bad", attributes={"price": 1.5})
            with pytest.raises(InvalidAttributesError):
                _ = th.batch("attrs", key="bad", attributes={"tallyho.kind": "x"})
            with pytest.raises(InvalidAttributesError):
                _ = th.batch("attrs", key="bad", attributes={"a": 1, "b": 2})
            with pytest.raises(InvalidAttributesError):
                _ = th.batch("attrs", key="bad", memo={"at": datetime(2026, 1, 1, tzinfo=UTC)})
        assert statements == []
    assert await attr_rows(engine, schema) == 0


async def test_hooks_of_root_and_sub_batch_see_root_attributes(
    engine: AsyncEngine, schema: str
) -> None:
    finalized: dict[str, dict[str, object]] = {}
    progressed: list[dict[str, object]] = []
    async with make_client(engine, schema) as (th, broker, clock, _observer):

        @th.on_finalized("hooked")
        async def root_done(_session: AsyncSession, summary: BatchSummary) -> None:
            finalized["root"] = dict(summary.attributes)
            finalized["root.child"] = dict(summary.children["send"].attributes)
            await asyncio.sleep(0)

        @th.on_finalized("hooked.send")
        async def child_done(_session: AsyncSession, summary: BatchSummary) -> None:
            finalized["send"] = dict(summary.attributes)
            await asyncio.sleep(0)

        @th.on_progress("hooked", every=timedelta(seconds=1))
        async def root_progress(_session: AsyncSession, summary: BatchSummary) -> None:
            progressed.append(dict(summary.attributes))
            await asyncio.sleep(0)

        _ = (root_done, child_done, root_progress)

        async with th.batch("hooked", key="one", attributes=ATTRIBUTES) as root:
            send = root.sub_batch("send")
            await send.add(noop, 1)
            await send.add(noop, 2)

        _ = await broker.step(1)
        clock.advance(seconds=2)
        _ = await th.run_maintenance_once()
        _ = await broker.drain()
        _ = await th.run_maintenance_once()
        view = await root.handle.view()

    assert view.state is BatchState.SUCCEEDED
    assert finalized == {"root": STORED, "root.child": STORED, "send": STORED}
    assert progressed
    assert all(entry == STORED for entry in progressed)


async def test_secret_attribute_never_reaches_logs_or_observer(
    engine: AsyncEngine, schema: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    async with make_client(engine, schema) as (th, broker, _clock, observer):

        @th.on_finalized("secret")
        async def boom(_session: AsyncSession, _summary: BatchSummary) -> None:
            await asyncio.sleep(0)
            message = "hook failed"
            raise RuntimeError(message)

        _ = boom

        async with th.batch(
            "secret", key="one", attributes={"token": SECRET}, memo={"token": MEMO_SECRET}
        ) as root:
            await root.add(noop, 1)
        _ = await broker.drain()
        _ = await th.run_maintenance_once()
        view = await root.handle.view()

    assert view.hook_attempts >= 1
    assert view.attributes["token"] == SECRET
    assert observer.seen
    for leak in (SECRET, MEMO_SECRET):
        assert leak not in caplog.text
        assert all(leak not in entry for entry in observer.seen)


async def test_retention_removes_side_row(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema, retention_days=1) as (th, broker, clock, _o):
        async with th.batch("attrs", key="purge", attributes=ATTRIBUTES, memo=MEMO) as root:
            await root.add(noop, 1)
        async with th.batch("attrs", key="keep", attributes={"keep": True}) as keep:
            pass
        _ = await broker.drain()
        _ = await th.run_maintenance_once()
        assert await attr_rows(engine, schema) == 2

        clock.advance(days=2)
        async with th.batch("attrs", key="fresh", attributes={"fresh": True}) as fresh:
            await fresh.add(noop, 1)
        for _ in range(3):
            _ = await th.run_maintenance_once()

        with pytest.raises(BatchPurged):
            _ = await root.handle.view()
        with pytest.raises(BatchPurged):
            _ = await keep.handle.view()
        assert dict((await fresh.handle.view()).attributes) == {"fresh": True}
    assert await attr_rows(engine, schema) == 1


async def test_attributes_are_read_by_the_tree_statement(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock, _observer):
        async with th.batch("attrs", key="count", attributes=ATTRIBUTES, memo=MEMO) as root:
            _ = root.sub_batch("a")
            _ = root.sub_batch("b")
        with statements_of(engine) as statements:
            view = await root.handle.view()

    assert dict(view.children["b"].attributes) == STORED
    # Отдельного запроса за атрибутами нет: они приходят тем же statement, что и дерево.
    reading = [statement for statement in statements if "th_batch_attr" in statement]
    assert len(reading) == 1
    assert "th_counter" in reading[0]
