"""A-UC-01…22 на compose-стенде, затем оракул I-01…I-14 (ACCEPTANCE §7).

Запуск: ``uv run poe acceptance-uc --uc A-UC-01,A-UC-02 --seed 1 --scale 0.1 --jobs 4``.
Без ``-m usecase`` эти тесты не собираются (см. ``conftest.py``): каждому сценарию нужен свой
стенд и минуты работы.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from tests.acceptance.uc.context import UC_IDS, UcConfig
from tests.acceptance.uc.runner import run_usecase

if TYPE_CHECKING:
    from collections.abc import Mapping

    from _pytest.mark import ParameterSet

    from tests.acceptance.uc.runner import UcReport

__all__: list[str] = []


def _selected() -> tuple[str, ...]:
    raw = os.environ.get("UC", "all").strip()
    if raw.lower() in {"", "all"}:
        return UC_IDS
    values = tuple(value.strip().upper() for value in raw.split(","))
    unknown = [value for value in values if value not in UC_IDS]
    if unknown:
        message = f"UC={raw!r}: неизвестно {unknown}; допустимы A-UC-01 … A-UC-22 или all"
        raise pytest.UsageError(message)
    return values


SEED = int(os.environ.get("SEED", "1"))
SCALE = float(os.environ.get("SCALE", "0.1"))
THREADS = int(os.environ.get("THREADS", "32"))
KEEP_STAND = os.environ.get("KEEP_STAND", "") == "1"
TIMEOUT = 3600 + 6000 * SCALE


@dataclass(frozen=True, slots=True)
class _Defect:
    """Дефект библиотеки, вскрытый сценарием и ждущий своей задачи Fix-N (PLAN §0.3)."""

    task: str
    summary: str

    def mark(self) -> pytest.MarkDecorator:
        """Нестрогое ожидаемое падение проверки; ошибки стенда им не прикрываются."""
        return pytest.mark.xfail(
            reason=f"{self.task}: {self.summary}", strict=False, raises=AssertionError
        )


# Нарушения инвариантов и невыполненные ожидания A-UC, ждущие задачи Fix-N.
#
# A-UC-07: путь B (`item.complete_in`) задачи, создавшей `item.sub_batch` без spawn, падает
# KeyError в `_Tx._sub_batches`: `Completer.complete_in` блокирует (и кладёт в `tx.batches`)
# только батчи маршрутов spawn/expect и `fed_by`, но не батч самого Item. Попытки
# исчерпываются, Item - `error("exhausted")`, под-батч не создаётся. Путь A тот же
# под-батч создаёт; поэтому глубина 3 проверяется через путь A, а путь B - отдельной пробой.
_SUB_BATCH_PATH_B = _Defect(
    "Fix-NEW-complete-in-sub-batch",
    "complete_in задачи с sub_batch без spawn падает KeyError: батч Item не заблокирован",
)
# A-UC-19: `ProgressWatcher` читает `Reads.view` без скоростей, ETA в `watch()` всегда None,
# хотя ARCHITECTURE §9.4 обещает её «в Snapshotter и в watch()»; в снимках on_progress ETA есть.
_WATCH_ETA = _Defect("Fix-NEW-watch-eta", "watch() не считает ETA: скорости не передаются")
_INVARIANT_DEFECTS: Mapping[str, _Defect] = {}
_EXPECTATION_DEFECTS: Mapping[str, _Defect] = {
    "A-UC-07": _SUB_BATCH_PATH_B,
    "A-UC-19": _WATCH_ETA,
}


def _cases(defects: Mapping[str, _Defect]) -> list[ParameterSet]:
    cases: list[ParameterSet] = []
    for uc in _selected():
        marks = [pytest.mark.xdist_group(uc)]
        if (defect := defects.get(uc)) is not None:
            marks.append(defect.mark())
        cases.append(pytest.param(uc, id=uc, marks=marks))
    return cases


pytestmark = [pytest.mark.usecase, pytest.mark.timeout(TIMEOUT)]

_RUNS: dict[str, UcReport | BaseException] = {}


async def _run(uc: str) -> UcReport:
    """Один прогон на сценарий; оба теста ниже читают один отчёт."""
    if uc not in _RUNS:
        config = UcConfig(seed=SEED, uc=uc, scale=SCALE, threads=THREADS, keep_stand=KEEP_STAND)
        try:
            _RUNS[uc] = await run_usecase(config)
        except Exception as exc:
            _RUNS[uc] = exc
            raise
    outcome = _RUNS[uc]
    if isinstance(outcome, BaseException):
        raise outcome
    return outcome


@pytest.mark.parametrize("uc", _cases(_INVARIANT_DEFECTS))
async def test_invariants_hold_after_usecase(uc: str) -> None:
    """I-01…I-14 зелёные, стенд пришёл к затишью без остановок дольше ``T_rec``."""
    report = await _run(uc)

    assert report.oracle_ok, report.describe()
    assert [item.invariant for item in report.invariants] == [
        f"I-{index:02}" for index in range(1, 15)
    ]


@pytest.mark.parametrize("uc", _cases(_EXPECTATION_DEFECTS))
async def test_usecase_expectations(uc: str) -> None:
    """Колонка «Ожидаемый результат» ACCEPTANCE §7 для сценария."""
    report = await _run(uc)

    assert report.expectations, report.describe()
    assert not report.unmet, report.describe()
