"""tallyho — групповой учёт задач поверх любого брокера на PostgreSQL.

Публичный API реэкспортируется отсюда. Всё, чего нет в ``__all__``, — внутреннее.
"""

from __future__ import annotations

from importlib.metadata import version as _version

from tallyho.model.errors import TallyhoError

__all__ = ["TallyhoError", "__version__"]

__version__: str = _version("tallyho")
