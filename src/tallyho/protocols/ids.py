"""Идентификаторы: протокол :class:`IdFactory` и генератор UUIDv7.

UUIDv7 (RFC 9562) сортируется по времени создания, поэтому новые строки
``th_batch`` / ``th_item`` / ``th_outbox`` ложатся в конец B-дерева индекса.
Генератор повторяет схему ``uuid.uuid7()`` из Python 3.14 (метод 2 RFC 9562):

* 48 бит — миллисекунды Unix;
* 42 бита — счётчик: случайный в начале каждой миллисекунды (старший бит
  обнулён, чтобы был запас на инкремент), ``+1`` внутри той же миллисекунды;
* 32 бита — случайный хвост.

В пределах процесса идентификаторы строго возрастают, даже если часы пошли
назад: тогда метка времени остаётся прежней, а растёт счётчик. На Python 3.14+
фабрика по умолчанию вызывает ``uuid.uuid7()``.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["IdFactory", "UuidV7Factory"]

_COUNTER_BITS: Final = 42
_COUNTER_MAX: Final = (1 << _COUNTER_BITS) - 1
_COUNTER_LOW_BITS: Final = 30
_TAIL_BITS: Final = 32
_TIMESTAMP_MASK: Final = (1 << 48) - 1
_VERSION_7: Final = 0x7 << 76
_VARIANT_RFC_4122: Final = 0b10 << 62


@runtime_checkable
class IdFactory(Protocol):
    """Источник новых идентификаторов батчей, Items и записей outbox."""

    def new_id(self) -> uuid.UUID:
        """Новый уникальный идентификатор.

        Returns:
            UUID, по которому строки сортируются в порядке создания.
        """
        ...


def _native_uuid7() -> Callable[[], uuid.UUID] | None:
    if sys.version_info >= (3, 14):
        return uuid.uuid7  # pyright: ignore[reportUnreachable]  # ветка Python 3.14+, анализ идёт для 3.11
    return None


class UuidV7Factory(IdFactory):
    """Генератор UUIDv7, монотонный в пределах процесса и потокобезопасный."""

    def __init__(
        self,
        *,
        time_ns: Callable[[], int] | None = None,
        urandom: Callable[[int], bytes] | None = None,
    ) -> None:
        """Создать фабрику.

        Если ни один источник не подменён и Python ≥ 3.14, используется
        ``uuid.uuid7()``.

        Args:
            time_ns: источник времени в наносекундах Unix (для тестов).
            urandom: источник случайных байт (для тестов).
        """
        self._native: Final = _native_uuid7() if time_ns is None and urandom is None else None
        self._time_ns: Final = time_ns or time.time_ns
        self._urandom: Final = urandom or os.urandom
        self._lock: Final = threading.Lock()
        self._last_ms: int = -1
        self._counter: int = 0

    @override
    def new_id(self) -> uuid.UUID:
        """Новый UUIDv7, строго больше предыдущего из этой фабрики.

        Returns:
            UUID версии 7 варианта RFC 4122.
        """
        if self._native is not None:
            return self._native()
        with self._lock:
            timestamp_ms = self._time_ns() // 1_000_000
            if timestamp_ms > self._last_ms:
                counter = self._fresh_counter()
            else:
                timestamp_ms = self._last_ms
                counter = self._counter + 1
                if counter > _COUNTER_MAX:
                    timestamp_ms += 1
                    counter = self._fresh_counter()
            self._last_ms = timestamp_ms
            self._counter = counter
            tail = int.from_bytes(self._urandom(4)) & ((1 << _TAIL_BITS) - 1)
        value = (
            (timestamp_ms & _TIMESTAMP_MASK) << 80
            | _VERSION_7
            | (counter >> _COUNTER_LOW_BITS) << 64
            | _VARIANT_RFC_4122
            | (counter & ((1 << _COUNTER_LOW_BITS) - 1)) << _TAIL_BITS
            | tail
        )
        return uuid.UUID(int=value)

    def _fresh_counter(self) -> int:
        # Старший бит обнулён: в миллисекунде остаётся не меньше 2**41 инкрементов.
        return int.from_bytes(self._urandom(6)) & (_COUNTER_MAX >> 1)
