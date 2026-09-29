"""Модель: состояния, сводки, представления, ошибки. Нижний слой, без зависимостей."""

from __future__ import annotations

from tallyho.model.errors import (
    BatchPurged,
    ConcurrentModification,
    ConfigurationError,
    DownstreamFinalized,
    HookMissingError,
    HookTransactionError,
    InvalidStateError,
    NotFoundError,
    SealError,
    SpawnTargetError,
    TallyhoError,
    UnsupportedOption,
)
from tallyho.model.states import (
    BatchState,
    CancelReason,
    ItemState,
    OnFeederFailed,
    OutboxKind,
    ResultClass,
)

__all__ = [
    "BatchPurged",
    "BatchState",
    "CancelReason",
    "ConcurrentModification",
    "ConfigurationError",
    "DownstreamFinalized",
    "HookMissingError",
    "HookTransactionError",
    "InvalidStateError",
    "ItemState",
    "NotFoundError",
    "OnFeederFailed",
    "OutboxKind",
    "ResultClass",
    "SealError",
    "SpawnTargetError",
    "TallyhoError",
    "UnsupportedOption",
]
