"""Реестр сценариев A-UC-01…22 и особенности их стендов."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests.acceptance.uc import export, extra, mandatory

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from tests.acceptance.uc.context import UcContext

__all__ = ["SCENARIOS", "STAND_VARIANTS"]

SCENARIOS: Mapping[str, Callable[[UcContext], Awaitable[None]]] = {
    "A-UC-01": mandatory.uc01,
    "A-UC-02": mandatory.uc02,
    "A-UC-03": mandatory.uc03,
    "A-UC-04": mandatory.uc04,
    "A-UC-05": mandatory.uc05,
    "A-UC-06": mandatory.uc06,
    "A-UC-07": extra.uc07,
    "A-UC-08": extra.uc08,
    "A-UC-09": extra.uc09,
    "A-UC-10": extra.uc10,
    "A-UC-11": extra.uc11,
    "A-UC-12": extra.uc12,
    "A-UC-13": extra.uc13,
    "A-UC-14": extra.uc14,
    "A-UC-15": extra.uc15,
    "A-UC-16": extra.uc16,
    "A-UC-17": extra.uc17,
    "A-UC-18": extra.uc18,
    "A-UC-19": extra.uc19,
    "A-UC-20": extra.uc20,
    "A-UC-21": export.uc21,
    "A-UC-22": export.uc22,
}

STAND_VARIANTS: Mapping[str, Mapping[str, bool]] = {
    # Каталог без единого PDF: пустой этап (ACCEPTANCE §3.2, A-UC-05).
    "A-UC-05": {"empty_pdfs": True},
}
