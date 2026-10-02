"""Завершение Item в транзакции пользователя (ARCHITECTURE UC-08)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

    from tallyho.engine.completer import Completer, FinishResult, ItemRef

__all__ = ["complete_in"]


async def complete_in(
    target: AsyncSession | AsyncConnection,
    item: ItemRef,
    value: FinishResult,
    *,
    completer: Completer,
    attempt: int | None = None,
) -> bool:
    """Завершить ``item`` атомарно с доменными записями ``target``.

    Счётчики пишутся append-only дельтами и сворачиваются Completer после
    commit внешней транзакции. При ``False`` ничего не записано: вызывающий
    должен откатить доменные записи своей транзакции (I-04).

    Args:
        target: Пользовательская ``AsyncSession`` или ``AsyncConnection``.
        item: Завершаемый Item.
        value: Итог и накопленные spawn/expect/sub-batch операции.
        completer: Completer текущего runtime.
        attempt: Номер попытки из claim: завершить Item может только её
            владелец lease (UC-08). ``None`` — только CAS по ``state``.

    Returns:
        ``True`` после успешного CAS ``active -> terminal``; ``False`` для
        уже завершённого Item и для попытки, которая им не владеет.
    """
    return await completer.complete_in(target, item, value, attempt=attempt)
