"""Типизированный вызов задачи для публичного API."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Generic, ParamSpec, Self, TypeVar

from typing_extensions import override

from tallyho.model.calls import TaskCall

__all__ = ["Call"]

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True, slots=True, kw_only=True)
class Call(TaskCall, Generic[P, R]):
    """TaskCall, который фантомно сохраняет сигнатуру и результат функции."""

    @override
    def opts(
        self,
        *,
        key: str | None = None,
        weight: int | None = None,
        queue: str | None = None,
        **options: object,
    ) -> Self:
        """Вернуть вызов с опциями, не теряя его параметрический тип.

        Returns:
            Типизированная копия вызова.
        """
        return replace(
            self,
            key=self.key if key is None else key,
            weight=self.weight if weight is None else weight,
            queue=self.queue if queue is None else queue,
            options={**self.options, **options},
        )
