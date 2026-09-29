"""Модель: состояния, сводки, представления, ошибки. Нижний слой, без зависимостей."""

from __future__ import annotations

from tallyho.model.errors import (
    ConfigurationError,
    InvalidStateError,
    NotFoundError,
    TallyhoError,
)

__all__ = ["ConfigurationError", "InvalidStateError", "NotFoundError", "TallyhoError"]
