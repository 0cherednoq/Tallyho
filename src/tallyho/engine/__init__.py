"""Движок: Completer, Relay, Sweeper, Finalizer, Snapshotter, Counters."""

from __future__ import annotations

from tallyho.engine.completion import complete_in
from tallyho.engine.operations import Operations, OperationTriggers

__all__ = ["OperationTriggers", "Operations", "complete_in"]
