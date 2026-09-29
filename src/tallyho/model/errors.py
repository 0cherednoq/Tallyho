"""Иерархия исключений tallyho.

Правило: всё, что библиотека бросает наружу, наследуется от :class:`TallyhoError`.
Проверяется тестом ``tests/architecture/test_conventions.py``.
"""

from __future__ import annotations

__all__ = ["ConfigurationError", "InvalidStateError", "NotFoundError", "TallyhoError"]


class TallyhoError(Exception):
    """Базовое исключение библиотеки."""


class ConfigurationError(TallyhoError):
    """Неверная конфигурация клиента, адаптера или батча."""


class NotFoundError(TallyhoError):
    """Батч или Item не найден."""


class InvalidStateError(TallyhoError):
    """Операция недопустима в текущем техническом состоянии батча."""
