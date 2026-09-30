"""Статические контракты ParamSpec; EXPECT-строки перепроверяет test_typing."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, ParamSpec, assert_type

from tallyho import Call, item

if TYPE_CHECKING:
    from tallyho import BatchBuilder, Tallyho

__all__: list[str] = []


P = ParamSpec("P")


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
