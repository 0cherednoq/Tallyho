"""Публичные сценарии BatchBuilder и BatchHandle из ARCHITECTURE."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ParamSpec

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import override

from tallyho import Tallyho
from tallyho.model.errors import BatchPurged
from tallyho.model.states import BatchState
from tallyho.protocols.broker import Dispatcher

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Sequence

    from tallyho import BatchBuilder, BatchHandle
    from tallyho.model.views import BatchSummary
    from tallyho.protocols.broker import Message
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")


class FakeBroker(Dispatcher):
    """Минимальный broker adapter для публичных producer-сценариев."""

    @override
    def task_name(self, fn: Callable[P, object]) -> str:
        return fn.__name__

    @override
    async def dispatch(self, messages: Sequence[Message]) -> None:
        _ = messages


class ScenarioFailedError(Exception):
    """Управляемая ошибка внутри пользовательского ``async with``."""


async def expand_audience(campaign_id: int, *, after_id: int) -> None:
    """Тестовая задача примера рассылки."""
    _ = campaign_id, after_id
    await asyncio.sleep(0)


async def parse_page(url: str, *, page: int) -> None:
    """Тестовая задача примера импорта."""
    _ = url, page
    await asyncio.sleep(0)


async def fail_after_add(builder: BatchBuilder) -> None:
    """Записать Item и упасть до успешного выхода из builder."""
    async with builder:
        await builder.add(parse_page, "url", page=1)
        raise ScenarioFailedError


@pytest.fixture
async def th(env: Env) -> AsyncGenerator[Tallyho]:
    """Установленный публичный клиент над схемой теста.

    ``aclose`` дожидается фоновых задач библиотеки и останавливает relay до
    ``DROP SCHEMA`` (Fix-11).
    """
    value = Tallyho(env.engine, schema=env.schema)
    value.install(FakeBroker())
    yield value
    await value.aclose()


async def test_schedule_example_builds_and_seals_pipeline(th: Tallyho) -> None:
    """§12.4: root и feeder закрыты, зависимый этап остаётся open."""
    at = datetime(2030, 1, 2, tzinfo=UTC)

    async with th.batch(kind="campaign_deliveries", key="campaign:7", start_at=at) as root:
        expand = root.sub_batch("expand")
        _ = root.sub_batch("send", fed_by=[expand], expected_total=3, max_in_flight=500)
        await expand.add(expand_audience, 7, after_id=0)

    view = await root.handle.view()
    assert view.state is BatchState.SEALED
    assert view.start_at == at
    assert view.children["expand"].state is BatchState.SEALED
    assert view.children["expand"].progress.found == 1
    assert view.children["send"].state is BatchState.OPEN
    assert view.children["send"].progress.expected == 3
    assert (await th.find("campaign_deliveries", "campaign:7")).id == root.handle.id
    assert (await root.handle.child("send")).id == view.children["send"].id


async def test_start_import_example_builds_three_stages(th: Tallyho) -> None:
    """§13.2: цепочка pages → cards → pdfs создаётся одним context manager."""

    async with th.batch(kind="catalog_parse", key="catalog:12", max_items=200_000) as root:
        pages = root.sub_batch("pages", max_depth=1)
        cards = root.sub_batch("cards", fed_by=[pages], max_in_flight=100)
        _ = root.sub_batch("pdfs", fed_by=[cards], max_in_flight=50)
        await pages.add(parse_page, "https://example.test", page=1)

    view = await root.handle.view()
    assert list(view.children) == ["pages", "cards", "pdfs"]
    assert view.children["pages"].state is BatchState.SEALED
    assert view.children["cards"].state is BatchState.OPEN
    assert view.children["pdfs"].state is BatchState.OPEN


async def test_own_transaction_rolls_back_on_exception(th: Tallyho) -> None:
    """Без session исключение откатывает всё дерево."""
    builder = th.batch(kind="rollback", key="own")

    with pytest.raises(ScenarioFailedError):
        await fail_after_add(builder)

    with pytest.raises(BatchPurged):
        _ = await builder.handle.view()


async def test_external_transaction_keeps_writes_but_does_not_seal_on_exception(
    env: Env, th: Tallyho
) -> None:
    """С чужой session rollback/commit решает пользователь; API не дописывает seal."""
    scoped = env.engine.execution_options(schema_translate_map={None: env.schema})
    async with AsyncSession(scoped) as session:
        builder = th.batch(kind="external", key="kept", session=session)
        with pytest.raises(ScenarioFailedError):
            await fail_after_add(builder)
        await session.commit()

    view = await builder.handle.view()
    assert view.state is BatchState.OPEN
    assert view.progress.found == 1


async def test_handle_mutations_use_own_or_external_transaction(th: Tallyho) -> None:
    """Handle делегирует pause/resume/reschedule публичному engine-фасаду."""
    # Старт отложен: relay после commit ещё не может отправить Item, и число
    # перенесённых записей не зависит от фоновой отправки.
    later = datetime.now(UTC) + timedelta(minutes=30)
    async with th.batch(kind="operations", key="one", start_at=later) as root:
        await root.add(parse_page, "url", page=1)

    handle: BatchHandle = th.handle(root.handle.id)
    await handle.pause()
    assert (await handle.view()).paused
    await handle.resume()
    assert not (await handle.view()).paused
    moved = await handle.reschedule(datetime.now(UTC) + timedelta(hours=1))
    assert moved == 0


async def test_empty_batch_is_finalized_immediately_after_commit(th: Tallyho) -> None:
    """Producer seal подталкивает Finalizer, даже если maintenance ещё не запущен."""
    async with th.batch(kind="empty", key="one") as root:
        pass

    view = await root.handle.wait(timeout=timedelta(seconds=5))
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.final


async def test_empty_batches_in_own_transaction_are_finalized_without_maintenance(
    th: Tallyho,
) -> None:
    """Fix-16: финализация после seal видит закоммиченный батч, а не ждёт sweeper."""
    handles: list[BatchHandle] = []
    for index in range(200):
        async with th.batch(kind="empty", key=f"many:{index}") as root:
            pass
        handles.append(root.handle)

    # aclose дожидается задач финализации после commit; maintenance не запускался.
    await th.aclose()
    states = [(await handle.view()).state for handle in handles]

    assert states.count(BatchState.SUCCEEDED) == len(handles)


async def test_cancel_right_after_seal_never_finalizes_succeeded(th: Tallyho) -> None:
    """Fix-9: отмена сразу после выхода из builder гонится с финализацией после seal."""
    # Старт отложен: relay после commit не отправляет Items, и отмена закрывает их сразу.
    later = datetime.now(UTC) + timedelta(minutes=30)
    for index in range(8):
        async with th.batch(kind="cancelled-early", key=f"run:{index}", start_at=later) as root:
            for page in range(4):
                await root.add(parse_page, "url", page=page)
        await root.handle.cancel()

        view = await root.handle.wait(timeout=timedelta(seconds=10))
        assert view.state is BatchState.CANCELLED
        assert (view.progress.ok, view.progress.cancelled) == (0, 4)


async def test_cancel_during_finalization_hook_finalizes_cancelled(th: Tallyho) -> None:
    """Отмена, закоммиченная во время хука, определяет итог: хук вызывается заново."""
    entered = asyncio.Event()
    release = asyncio.Event()
    seen: list[BatchState] = []

    @th.on_finalized("cancelled-in-hook")
    async def save(_session: AsyncSession, summary: BatchSummary) -> None:
        seen.append(summary.state)
        if len(seen) == 1:
            entered.set()
            await release.wait()

    async with th.batch(kind="cancelled-in-hook", key="one") as root:
        pass
    await entered.wait()
    await root.handle.cancel()
    release.set()

    view = await root.handle.wait(timeout=timedelta(seconds=10))
    # Хук считается после закрытия: фоновые финализации к этому моменту завершены.
    await th.aclose()
    assert view.state is BatchState.CANCELLED
    assert seen[0] is BatchState.SUCCEEDED
    assert set(seen[1:]) == {BatchState.CANCELLED}
