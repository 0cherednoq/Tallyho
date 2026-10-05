"""Параметры прогона A-UC и реестр сценариев - без Docker и БД."""

from __future__ import annotations

import pytest

from tests.acceptance.uc.context import UC_IDS, UcConfig
from tests.acceptance.uc.scenarios import SCENARIOS, STAND_VARIANTS

__all__: list[str] = []


def test_every_use_case_has_a_scenario() -> None:
    assert tuple(f"A-UC-{index:02}" for index in range(1, 23)) == UC_IDS
    assert tuple(SCENARIOS) == UC_IDS
    assert set(STAND_VARIANTS) <= set(UC_IDS)


@pytest.mark.parametrize(
    ("scale", "base", "expected"),
    [(0.1, 20_000, 2_000), (1.0, 20_000, 20_000), (0.001, 20_000, 20), (0.0001, 20_000, 10)],
)
def test_volume_scales_acceptance_table_with_floor(scale: float, base: int, expected: int) -> None:
    assert UcConfig(seed=1, uc="A-UC-01", scale=scale).volume(base) == expected


def test_pages_and_names() -> None:
    config = UcConfig(seed=7, uc="A-UC-04")

    assert config.pages == 5
    assert UcConfig(seed=7, uc="A-UC-04", scale=1.0).pages == 50
    assert config.name == "a-uc-04-seed7"
    assert config.project == "tallyho-uc04-seed7"
