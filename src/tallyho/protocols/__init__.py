"""Точки расширения: Dispatcher, Runtime, Serializer, Clock, Observer, IdFactory."""

from __future__ import annotations

from tallyho.protocols.broker import DeadLetters, Dispatcher, Message, Runtime, Verdict
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
    "DeadLetters",
    "Dispatcher",
    "IdFactory",
    "JsonSerializer",
    "Message",
    "PayloadCodec",
    "Runtime",
    "SerializationError",
    "Serializer",
    "SerializerCodec",
    "SystemClock",
    "UuidV7Factory",
    "Verdict",
]
