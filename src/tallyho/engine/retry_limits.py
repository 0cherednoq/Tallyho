"""Эффективный лимит повторов Item (ARCHITECTURE UC-15, D-012)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tallyho.protocols.broker import RetryLimits

__all__ = ["effective_max_retries"]


def effective_max_retries(
    options: Mapping[str, object], task_name: str, limits: RetryLimits | None
) -> int:
    """Тот же лимит, с которым relay ставит задачу в брокер.

    Опция вызова ``max_retries`` из ``th_item.options``, иначе умолчание
    задачи, известное только адаптеру; без адаптера с :class:`RetryLimits` — 0.

    Returns:
        Неотрицательное число повторов; неверное значение опции даёт 0.
    """
    if "max_retries" in options or limits is None:
        value = options.get("max_retries", 0)
    else:
        value = limits.max_retries(task_name)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0
