"""Профили прогона: объёмы и длительности задаются профилем, а не флагами каждого P-NN."""

from __future__ import annotations

from enum import StrEnum

__all__ = ["Profile"]


class Profile(StrEnum):
    """Масштаб прогона.

    * ``smoke`` — минуты на локальной машине, цели только информативны;
    * ``nightly`` — объёмы ACCEPTANCE §11 (P-01; P-04 — 1kx1k и история 5M) на runner CI;
    * ``full`` — объёмы ACCEPTANCE §9, запускается человеком на эталонном стенде.
    """

    SMOKE = "smoke"
    NIGHTLY = "nightly"
    FULL = "full"

    @property
    def enforced(self) -> bool:
        """Проваливает ли невыполненная цель прогон (на ``smoke`` — нет)."""
        return self is not Profile.SMOKE
