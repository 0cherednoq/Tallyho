"""Реестр сценариев P-01…P-11 (ACCEPTANCE §9) в порядке прогона."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from benchmarks.scenarios import (
    p01_overhead,
    p02_throughput,
    p03_scaling,
    p04_scale,
    p05_realistic,
    p06_user_tx,
    p07_cascade,
    p08_snapshots,
    p09_recovery,
    p10_db_resources,
    p11_retention,
)

if TYPE_CHECKING:
    from benchmarks.context import Scenario

__all__ = ["SCENARIOS"]

SCENARIOS: Final[dict[str, Scenario]] = {
    scenario.id: scenario
    for scenario in (
        p01_overhead.SCENARIO,
        p02_throughput.SCENARIO,
        p03_scaling.SCENARIO,
        p04_scale.SCENARIO,
        p05_realistic.SCENARIO,
        p06_user_tx.SCENARIO,
        p07_cascade.SCENARIO,
        p08_snapshots.SCENARIO,
        p09_recovery.SCENARIO,
        p10_db_resources.SCENARIO,
        p11_retention.SCENARIO,
    )
}
