"""Точки расширения: Dispatcher, Runtime, Serializer, Clock, Observer, IdFactory."""

from __future__ import annotations

from tallyho.protocols.clock import Clock, SystemClock
from tallyho.protocols.ids import IdFactory, UuidV7Factory
from tallyho.protocols.serialization import (
    CallArgs,
    JsonSerializer,
    PayloadCodec,
    SerializationError,
    Serializer,
    SerializerCodec,
)

__all__ = [
    "CallArgs",
    "Clock",
    "IdFactory",
    "JsonSerializer",
    "PayloadCodec",
    "SerializationError",
    "Serializer",
    "SerializerCodec",
    "SystemClock",
    "UuidV7Factory",
]
