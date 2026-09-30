"""tallyho — групповой учёт задач поверх любого брокера на PostgreSQL.

Публичный API реэкспортируется отсюда. Всё, чего нет в ``__all__``, — внутреннее.
"""

from __future__ import annotations

from importlib.metadata import version as _version

from tallyho.api import BatchBuilder, BatchHandle, Settings, Tallyho
from tallyho.model.errors import TallyhoError
from tallyho.runtime import callback, item, tracked

__all__ = [
    "BatchBuilder",
    "BatchHandle",
    "Settings",
    "Tallyho",
    "TallyhoError",
    "__version__",
    "callback",
    "item",
    "tracked",
]

__version__: str = _version("tallyho")
