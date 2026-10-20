"""Статические контракты ParamSpec; EXPECT-строки перепроверяет test_typing."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, ParamSpec, assert_type
from uuid import UUID

from tallyho import BatchBuilder, Call, Defer, Ok, Skip, WorkItem, callback, item
from tallyho.integrations.dishka import (  # ruff: ignore[typing-only-first-party-import]  # dishka читает аннотации в рантайме
    FromDishka,
    inject,
)
from tallyho.runtime import CallbackContext
from tallyho.runtime.context import CallbackFacade, ItemFacade, RuntimeSubBatch

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho import BatchHandle, Tallyho

__all__: list[str] = []


P = ParamSpec("P")


class Interactor:
    """Зависимость задачи, которую внедряет контейнер."""

    async def __call__(self, value: int) -> int:
        """Вернуть то же значение.

        Returns:
            Значение без изменений.
        """
        await asyncio.sleep(0)
        return value


@inject
async def injected(value: int, interactor: FromDishka[Interactor]) -> Ok:
    """Задача с внедряемой зависимостью: DI-параметр не виден вызывающему.

    Returns:
        Успешный итог.
    """
    return Ok("done", result=await interactor(value))


async def unary(value: int, /) -> None:
    """Задача с одним аргументом."""
    _ = value
    await asyncio.sleep(0)


async def configured(value: int, *, mode: str) -> str:
    """Задача с обязательным keyword-only аргументом."""
    await asyncio.sleep(0)
    return f"{value}:{mode}"


def requires_string_result(value: Call[P, str]) -> None:
    """Compile-time ограничение результата вызова."""
    _ = value


async def cases(th: Tallyho, batch: BatchBuilder) -> None:
    """Корректные и намеренно ошибочные compile-time вызовы."""
    assert_type(th.call(unary, 1), Call[[int], None])
    configured_call = th.call(configured, 1, mode="strict").opts(key="1")
    requires_string_result(configured_call)
    await batch.add(configured, 1, mode="strict")
    await batch.map(unary, [1, 2])
    item.spawn(unary, 1)
    item.spawn(unary, 1, into="next", key="one")

    _ = th.call(unary, "bad")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # EXPECTED_NEGATIVE
    _ = th.call(configured, 1)  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # EXPECTED_NEGATIVE
    await batch.add(unary, "bad")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # EXPECTED_NEGATIVE
    await batch.map(unary, ["bad"])  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # EXPECTED_NEGATIVE
    item.spawn(unary, "bad")  # type: ignore[arg-type]  # pyright: ignore[reportCallIssue, reportArgumentType]  # EXPECTED_NEGATIVE


async def work_item_cases(  # ruff: ignore[too-many-positional-arguments]  # кейсы берут весь публичный набор
    th: Tallyho, batch: BatchBuilder, handle: BatchHandle, session: AsyncSession
) -> None:
    """RM8: закладка, итог в транзакции пользователя, пробуждение, DI."""
    work = item.require()
    assert_type(work, WorkItem)

    # a) Закладка и итог значением пишутся в транзакции вызывающего.
    await work.checkpoint_in(session, 7)
    await work.complete_in(session, Ok("chunk"))
    await work.complete_in(session)
    await item.checkpoint_in(session, 7)
    await work.checkpoint_in(session, "7")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # EXPECTED_NEGATIVE

    # b) release_deferred возвращает число разбуженных записей.
    assert_type(await handle.release_deferred(), int)
    assert_type(await handle.release_deferred(["no_accounts"]), int)

    # c) Задача с @inject типизируется без DI-параметров.
    _ = th.call(injected, 1)
    await batch.add(injected, 1)


def facade_cases(th: Tallyho, handle: BatchHandle) -> None:
    """Fix-13: публичный API задачи совпадает с ARCHITECTURE §11.2."""
    # a) Фасады задачи — модульные, не атрибуты Tallyho.
    assert_type(item, ItemFacade)
    assert_type(callback, CallbackFacade)
    _ = th.item  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue, reportUnknownVariableType]  # EXPECTED_NEGATIVE
    _ = th.tracked  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue, reportUnknownVariableType]  # EXPECTED_NEGATIVE

    # b) Опции вызова из задачи — только через spawn_call(th.call(...).opts(...)).
    item.spawn_call(th.call(unary, 1).opts(key="k", weight=2, queue="bulk"), into="next")
    item.spawn(configured, 1, mode="strict")  # форма ParamSpec: аргументы задачи как есть
    item.spawn(unary, 1, opts={"weight": 2})  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # EXPECTED_NEGATIVE
    item.spawn(configured, 1, mode="strict", into="next")  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # EXPECTED_NEGATIVE

    # c) into= — ключ под-батча или UUID батча, не BatchHandle.
    item.spawn(unary, 1, into=handle.id)
    item.spawn_call(th.call(unary, 1), into=handle.id)
    item.expect(3, into=handle.id)
    item.spawn_call(th.call(unary, 1), into=handle)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # EXPECTED_NEGATIVE

    # d) Вес — опция вызова: Call.opts(weight=) сохраняет тип вызова.
    assert_type(th.call(unary, 1).opts(weight=4), Call[[int], None])

    # e) Колбэки динамического под-батча — on_...=, как у BatchBuilder.sub_batch.
    parts = item.sub_batch("parts", on_succeeded=th.call(unary, 1), on_finalized_task=None)
    assert_type(parts, RuntimeSubBatch)
    _ = item.sub_batch("parts", callbacks={})  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # EXPECTED_NEGATIVE

    # f) Контекст колбэка — callback_id и batch_id, без сводки.
    context = callback.current()
    assert_type(context, CallbackContext | None)
    if context is not None:
        assert_type(context.batch_id, UUID)
        _ = context.summary  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue, reportUnknownVariableType]  # EXPECTED_NEGATIVE

    # f2) Единица работы: current() мягкий, require() строгий (оба WorkItem).
    work = item.require()
    assert_type(work, WorkItem)
    assert_type(work.attempt, int)
    assert_type(item.current(), WorkItem | None)

    # f3) Итоги-значения: метка позиционна, остальное keyword-only.
    assert_type(Ok("chunk", result={"checked": 1}), Ok)
    assert_type(Skip("duplicate"), Skip)
    assert_type(Defer(reason="quota"), Defer)
    _ = Ok("chunk", {"checked": 1})  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # EXPECTED_NEGATIVE
    _ = Defer()  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # EXPECTED_NEGATIVE

    # f4) Закладка: снимок — целое или None, запись принимает position.
    assert_type(work.checkpoint(), int | None)
    assert_type(work.defers, int)

    # g) Потоковое добавление: seal=False у th.batch (UC-02).
    assert_type(th.batch("import", key="file:1", seal=False), BatchBuilder)
    _ = th.batch("import", key="file:1", seal="no")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # EXPECTED_NEGATIVE
