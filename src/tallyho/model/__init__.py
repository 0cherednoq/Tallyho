"""Модель: состояния, сводки, представления, ошибки. Нижний слой, без зависимостей."""

from __future__ import annotations

from tallyho.model.calls import TaskCall
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
from tallyho.model.policy import (
    FailurePolicy,
    OutcomeCounts,
    PolicyAction,
    PolicyBreach,
    PolicyKind,
    PolicyVerdict,
)
from tallyho.model.progress import (
    NodeCounters,
    ProgressSettings,
    compute_progress,
    estimate_threshold,
)
from tallyho.model.states import (
    BatchState,
    CancelReason,
    ItemState,
    OnFeederFailed,
    OutboxKind,
    ResultClass,
)
from tallyho.model.views import (
    BatchSummary,
    BatchView,
    InFlightItem,
    ItemView,
    Progress,
)

__all__ = [
    "BatchPurged",
    "BatchState",
    "BatchSummary",
    "BatchView",
    "CancelReason",
    "ConcurrentModification",
    "ConfigurationError",
    "DownstreamFinalized",
    "FailurePolicy",
    "HookMissingError",
    "HookTransactionError",
    "InFlightItem",
    "InvalidStateError",
    "ItemState",
    "ItemView",
    "NodeCounters",
    "NotFoundError",
    "OnFeederFailed",
    "OutboxKind",
    "OutcomeCounts",
    "PolicyAction",
    "PolicyBreach",
    "PolicyKind",
    "PolicyVerdict",
    "Progress",
    "ProgressSettings",
    "ResultClass",
    "SealError",
    "SpawnTargetError",
    "TallyhoError",
    "TaskCall",
    "UnsupportedOption",
    "compute_progress",
    "estimate_threshold",
]
