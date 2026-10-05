"""Длинные строки без неявной конкатенации (basedpyright ``reportImplicitStringConcatenation``)."""

from __future__ import annotations

__all__ = ["prose"]


def prose(text: str) -> str:
    """Схлопнуть переводы строк и отступы многострочного литерала в одиночные пробелы.

    Returns:
        Текст одной строкой.
    """
    return " ".join(text.split())
