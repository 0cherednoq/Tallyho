"""Точки расширения: Dispatcher, Runtime, Serializer, Clock, Observer, IdFactory."""

from __future__ import annotations

from tallyho.protocols.clock import Clock, SystemClock

__all__ = [
    "Clock",
    "SystemClock",
]
