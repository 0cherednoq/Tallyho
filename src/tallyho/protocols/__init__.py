"""Точки расширения: Dispatcher, Runtime, Serializer, Clock, Observer, IdFactory."""

from __future__ import annotations

from tallyho.protocols.clock import Clock, SystemClock
from tallyho.protocols.ids import IdFactory, UuidV7Factory

__all__ = [
    "Clock",
    "IdFactory",
    "SystemClock",
    "UuidV7Factory",
]
