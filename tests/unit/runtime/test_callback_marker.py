"""Маркер колбэк-джобы: новый формат и джобы, поставленные до удаления сводки."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest

from tallyho.runtime import CallbackContext, TaskRuntime, callback

if TYPE_CHECKING:
    from tallyho.engine.completer import Completer
    from tallyho.engine.spawn import TreeCache
    from tallyho.protocols.broker import Dispatcher, Runtime

__all__: list[str] = []


def _runtime() -> TaskRuntime:
    # Колбэк-задача не трогает ни Completer, ни брокер: им хватает заглушек.
    stub = object()
    return TaskRuntime(
        completer=cast("Completer", stub),
        broker=cast("Runtime", stub),
        dispatcher=cast("Dispatcher", stub),
        tree_cache=cast("TreeCache", stub),
        heartbeat_every=timedelta(seconds=20),
    )


@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"r": 3},
        # Джобы до Fix-14 несли "s": None, ещё раньше — сводку.
        {"r": 3, "s": None},
        {"s": {"ok": 1}},
    ],
)
async def test_callback_marker_with_and_without_legacy_summary(extra: dict[str, object]) -> None:
    callback_id, batch_id = uuid4(), uuid4()
    seen: list[tuple[CallbackContext | None, object]] = []

    async def task(**kwargs: object) -> str:
        await asyncio.sleep(0)
        seen.append((callback.current(), kwargs.get("_th")))
        return "done"

    marker = {"c": str(callback_id), "b": str(batch_id), **extra}
    assert await _runtime().wrap(task)(_th=marker) == "done"

    assert seen == [(CallbackContext(callback_id=callback_id, batch_id=batch_id), None)]
