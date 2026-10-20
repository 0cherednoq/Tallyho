"""tallyho — групповой учёт задач поверх любого брокера на PostgreSQL.

Публичный API реэкспортируется отсюда. Всё, чего нет в ``__all__``, — внутреннее.
"""

from __future__ import annotations

from importlib.metadata import version as _version

from tallyho.api import BatchBuilder, BatchHandle, Call, Settings, Tallyho
from tallyho.model.errors import TallyhoError
from tallyho.model.outcomes import Defer, Error, Ok, Outcome, Skip
from tallyho.runtime import ItemContext, ItemMiddleware, WorkItem, callback, item, tracked

__all__ = [
    "BatchBuilder",
    "BatchHandle",
    "Call",
    "Defer",
    "Error",
    "ItemContext",
    "ItemMiddleware",
    "Ok",
    "Outcome",
    "Settings",
    "Skip",
    "Tallyho",
    "TallyhoError",
    "WorkItem",
    "__version__",
    "callback",
    "item",
    "tracked",
]

__version__: str = _version("tallyho")
