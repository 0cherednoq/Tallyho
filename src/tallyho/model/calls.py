"""Описание вызова задачи: ``th.call(fn, *args, **kwargs).opts(...)`` (ARCHITECTURE §11.2).

:class:`TaskCall` — нетипизированное ядро: имя задачи уже разрешено
адаптером (``Dispatcher.task_name``), аргументы сохранены как есть. Типизация
через ``ParamSpec`` — забота ``th.call`` (T6.3). Используется в ``add_calls``,
``spawn_call`` и колбэках батча (``on_succeeded=``, ``on_finalized_task=``).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import TYPE_CHECKING

from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["TaskCall"]


def _check_weight(weight: object) -> int:
    if isinstance(weight, bool) or not isinstance(weight, int) or weight < 1:
        message = f"weight должен быть целым >= 1, получено {weight!r}"
        raise ConfigurationError(message)
    return weight


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskCall:
    """Вызов задачи с опциями постановки.

    ``key`` — ключ дедупликации Item в целевом батче, ``weight`` — вес для
    ``ratio`` (§9.4), ``queue`` — очередь брокера. ``options`` — прочие опции
    брокера (``priority``, ``max_retries`` …); их проверяет адаптер.
    """

    task_name: str
    args: tuple[object, ...] = ()
    kwargs: Mapping[str, object] = field(default_factory=dict[str, object])
    key: str | None = None
    weight: int = 1
    queue: str | None = None
    options: Mapping[str, object] = field(default_factory=dict[str, object])

    def __post_init__(self) -> None:
        """Проверить поля и заморозить словари.

        Raises:
            ConfigurationError: пустое имя задачи или ``weight < 1``.
        """
        if not self.task_name:
            message = "task_name не может быть пустым"
            raise ConfigurationError(message)
        _check_weight(self.weight)
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", MappingProxyType(dict(self.kwargs)))
        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))

    def opts(
        self,
        *,
        key: str | None = None,
        weight: int | None = None,
        queue: str | None = None,
        **options: object,
    ) -> TaskCall:
        """Новый вызов с изменёнными опциями; ``None`` — оставить как было.

        Опции брокера из ``options`` добавляются к уже заданным (новые
        значения перекрывают старые).

        Returns:
            Копия вызова с новыми опциями.
        """
        return replace(
            self,
            key=self.key if key is None else key,
            weight=self.weight if weight is None else weight,
            queue=self.queue if queue is None else queue,
            options={**self.options, **options},
        )
