"""Сериализация аргументов задач: :class:`Serializer` и :class:`PayloadCodec`.

``th_item.payload`` и ``th_outbox.payload`` хранят байты «аргументов вызова».
Кодирует их адаптер брокера (DECISIONS D-006): у flexiq свой кодек задачи
(сериализатор + codecs), и tallyho хранит ровно те байты, что положил бы
``apply_async``. Для этого адаптер реализует :class:`PayloadCodec`.

Адаптеру без собственного кодека хватает :class:`Serializer`: его оборачивает
:class:`SerializerCodec`. По умолчанию — :class:`JsonSerializer`. JSON теряет
``bytes``/``datetime``/``Decimal``, превращает кортежи в списки, а int-ключи
в строки, поэтому для flexiq он не годится (FLEXIQ_SPIKE §5).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final, Protocol, TypeAlias, cast, runtime_checkable

from typing_extensions import override

from tallyho.model.errors import TallyhoError

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "CallArgs",
    "JsonSerializer",
    "PayloadCodec",
    "SerializationError",
    "Serializer",
    "SerializerCodec",
]

CallArgs: TypeAlias = tuple[tuple[object, ...], dict[str, object]]
"""Аргументы вызова задачи: ``(args, kwargs)``."""

_ARGS: Final = "args"
_KWARGS: Final = "kwargs"


class SerializationError(TallyhoError):
    """Значение нельзя закодировать или байты нельзя раскодировать."""


@runtime_checkable
class Serializer(Protocol):
    """Преобразование значения в байты и обратно."""

    def dumps(self, value: object) -> bytes:
        """Закодировать значение.

        Args:
            value: значение для кодирования.

        Returns:
            Байты для хранения.
        """
        ...

    def loads(self, data: bytes) -> object:
        """Раскодировать байты, полученные от :meth:`dumps`.

        Args:
            data: байты из хранилища.

        Returns:
            Исходное значение (с точностью до возможностей формата).
        """
        ...


@runtime_checkable
class PayloadCodec(Protocol):
    """Кодек аргументов вызова задачи; реализует адаптер брокера (D-006)."""

    def encode(
        self, task_name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> bytes:
        """Закодировать аргументы вызова задачи ``task_name``.

        Args:
            task_name: имя задачи у брокера (кодек может зависеть от задачи).
            args: позиционные аргументы.
            kwargs: именованные аргументы.

        Returns:
            Байты для ``th_item.payload``.
        """
        ...

    def decode(self, task_name: str, data: bytes) -> CallArgs:
        """Раскодировать аргументы, закодированные :meth:`encode`.

        Args:
            task_name: имя задачи у брокера.
            data: байты из ``th_item.payload``.

        Returns:
            ``(args, kwargs)``.
        """
        ...


class JsonSerializer(Serializer):
    """JSON в UTF-8: компактный, без ``NaN``/``Infinity`` (их не принимает ``jsonb``)."""

    @override
    def dumps(self, value: object) -> bytes:
        """Закодировать значение в JSON.

        Args:
            value: JSON-совместимое значение.

        Returns:
            JSON в UTF-8.

        Raises:
            SerializationError: значение не представимо в JSON.
        """
        try:
            text = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise SerializationError(str(exc)) from exc
        return text.encode()

    @override
    def loads(self, data: bytes) -> object:
        """Раскодировать JSON.

        Args:
            data: JSON в UTF-8.

        Returns:
            Значение из JSON.

        Raises:
            SerializationError: байты — не JSON в UTF-8.
        """
        try:
            value = cast("object", json.loads(data))
        except ValueError as exc:
            raise SerializationError(str(exc)) from exc
        return value


class SerializerCodec(PayloadCodec):
    """:class:`PayloadCodec` поверх :class:`Serializer` для адаптеров без своего кодека.

    Аргументы хранятся как ``{"args": [...], "kwargs": {...}}``. Позиционные
    аргументы при декодировании снова становятся кортежем; вложенные значения
    восстанавливаются настолько точно, насколько позволяет сериализатор.
    """

    def __init__(self, serializer: Serializer | None = None) -> None:
        """Создать кодек.

        Args:
            serializer: сериализатор; по умолчанию :class:`JsonSerializer`.
        """
        self.serializer: Final = serializer or JsonSerializer()

    @override
    def encode(
        self, task_name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> bytes:
        """Закодировать аргументы сериализатором (``task_name`` не влияет).

        Args:
            task_name: имя задачи у брокера.
            args: позиционные аргументы.
            kwargs: именованные аргументы.

        Returns:
            Байты сериализатора.
        """
        return self.serializer.dumps({_ARGS: list(args), _KWARGS: dict(kwargs)})

    @override
    def decode(self, task_name: str, data: bytes) -> CallArgs:
        """Раскодировать аргументы.

        Args:
            task_name: имя задачи у брокера.
            data: байты из :meth:`encode`.

        Returns:
            ``(args, kwargs)``.

        Raises:
            SerializationError: байты не похожи на результат :meth:`encode`.
        """
        value = self.serializer.loads(data)
        if isinstance(value, dict):
            envelope = cast("dict[object, object]", value)
            args = envelope.get(_ARGS)
            kwargs = envelope.get(_KWARGS)
            if isinstance(args, list | tuple) and isinstance(kwargs, dict):
                named = cast("dict[object, object]", kwargs)
                positional = tuple(cast("Iterable[object]", args))
                return positional, {str(key): item for key, item in named.items()}
        msg = "payload не содержит пары args/kwargs"
        raise SerializationError(msg)
