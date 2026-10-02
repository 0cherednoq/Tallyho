"""A-CH-01…12 поверх S1/S2/S3 на compose-стенде (ACCEPTANCE §6).

Запуск: ``uv run poe acceptance --seed 1 --scenario S1 --chaos A-CH-01 --duration 120``.
Без ``-m chaos`` эти тесты не собираются (см. ``conftest.py``): им нужен Docker и
несколько минут на каждую комбинацию «сценарий и отказ».
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from tests.acceptance.chaos import CHAOS_IDS, SCENARIOS, RunConfig, run_chaos

if TYPE_CHECKING:
    from collections.abc import Mapping

    from _pytest.mark import ParameterSet

    from tests.acceptance.chaos import RunReport

__all__: list[str] = []


def _selected(variable: str, known: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(variable, "all").strip()
    if raw.lower() in {"", "all"}:
        return known
    values = tuple(value.strip().upper() for value in raw.split(","))
    unknown = [value for value in values if value not in known]
    if unknown:
        message = f"{variable}={raw!r}: неизвестно {unknown}; допустимы {', '.join(known)} или all"
        raise pytest.UsageError(message)
    return values


SEED = int(os.environ.get("SEED", "1"))
DURATION = float(os.environ.get("DURATION", "120"))
LEASE_TTL = float(os.environ.get("LEASE_TTL", "60"))
SWEEP_INTERVAL = float(os.environ.get("SWEEP_INTERVAL", "5"))
DRAIN_TIMEOUT = int(os.environ.get("DRAIN_TIMEOUT", "20"))
KEEP_STAND = os.environ.get("KEEP_STAND", "") == "1"
# Потолок pytest-timeout: подъём стенда, окно хаоса, доработка нагрузки и ожидание T_rec.
# Сам прогон ограничивает себя раньше (hard_cap в runner), потолок — страховка от зависания.
TIMEOUT = 900 + 8 * DURATION + 12 * (LEASE_TTL + 2 * SWEEP_INTERVAL + 30)


@dataclass(frozen=True, slots=True)
class _Defect:
    """Дефект библиотеки, вскрытый хаосом и ждущий своей задачи Fix-N (PLAN §0.3).

    Attributes:
        task: Предлагаемая задача-владелец.
        summary: Что ломается.
        strict: Дефект проявляется в каждом прогоне. ``False`` - проявление зависит от того,
            успела ли джоба стартовать в окно отказа: прогон то зелёный, то красный.
    """

    task: str
    summary: str
    strict: bool

    def mark(self) -> pytest.MarkDecorator:
        """Ожидаемое падение проверки; ошибки самого стенда им не прикрываются."""
        return pytest.mark.xfail(
            reason=f"{self.task}: {self.summary}", strict=self.strict, raises=AssertionError
        )


# Джоба упала на claim, пока PostgreSQL недоступен, и ушла в DLQ; обработчик JOB_DEAD тоже
# не смог записать итог, а `reconcile_dead` движок не вызывает. Item остаётся active без
# lease и outbox, батч не финализируется.
_DEAD_JOB_ORPHAN = _Defect(
    "Fix-6", "Item с джобой в DLQ остаётся active: сверка DLQ не вызывается", strict=False
)
# Повторная доставка получает DUPLICATE и закрывает джобу брокера как успешную, а исходное
# выполнение потом уходит в release по вердикту RETRY. Повторять уже нечего: Item остаётся
# active без lease, outbox и живой джобы.
_DUPLICATE_ORPHAN = _Defect(
    "Fix-7", "release после no-op дубля оставляет Item active без джобы", strict=False
)

# Нарушения инвариантов. Оба дефекта оставляют «осиротевшие» Items; Fix-6 - после отказов
# PostgreSQL, Fix-7 - после повторной доставки (requeue_job, реап «мёртвого» воркера flexiq
# при сдвиге часов или разрыве сети). A-CH-12 включает оба пути.
#
# В тех же прогонах бывает красным I-10 без зависших Items: Item завершён ok, а его
# последняя джоба лежит в DLQ. Так выходит, когда flexiq отбирает джобу у живого выполнения
# (сдвиг часов, разрыв сети, отказ БД после commit) и исчерпывает попытки дублями-no-op.
# Эффект один, данные согласованы, но DLQ брокера расходится с tallyho; что с этим делать
# (уточнить I-10 или чистить DLQ), решает владелец вместе с Fix-6/Fix-7.
#
# Fix-7 исправлен: release после подтверждённого дубля возвращает Item в outbox. С A-CH-10
# метка снята (S1/S2/S3 зелёные); A-CH-05 и A-CH-09 перепроверяет и снимает T11.3b.
#
# Fix-6 исправлен: сверка с DLQ завершает Items, чья джоба умерла без записанного итога
# (A-CH-04 на S1 и S3, seed 1: зависших Items нет, красный только I-10). Метки
# A-CH-02/03/04/12 остаются: I-10 без зависших Items, описанный выше, ждёт решения владельца;
# кроме того, в одном из двух прогонов A-CH-04 на S2 два Item остались active с джобой
# pending/running во flexiq (не в DLQ) — сверка такие не видит. Перепроверяет T11.3b.
_INVARIANT_DEFECTS: Mapping[str, _Defect] = {
    "A-CH-02": _DEAD_JOB_ORPHAN,
    "A-CH-03": _DEAD_JOB_ORPHAN,
    "A-CH-04": _DEAD_JOB_ORPHAN,
    "A-CH-05": _DUPLICATE_ORPHAN,
    "A-CH-09": _DUPLICATE_ORPHAN,
    "A-CH-12": _DEAD_JOB_ORPHAN,
}
# Невыполненные ожидания A-CH: известных дефектов нет.
_EXPECTATION_DEFECTS: Mapping[tuple[str, str], _Defect] = {}


def _cases(defect_of: Mapping[tuple[str, str], _Defect]) -> list[ParameterSet]:
    cases: list[ParameterSet] = []
    for chaos in _selected("CHAOS", CHAOS_IDS):
        for scenario in _selected("SCENARIO", SCENARIOS):
            marks = [pytest.mark.xdist_group(f"{chaos}-{scenario}")]
            defect = defect_of.get((chaos, scenario))
            if defect is not None:
                marks.append(defect.mark())
            cases.append(pytest.param(chaos, scenario, id=f"{chaos}-{scenario}", marks=marks))
    return cases


INVARIANT_CASES = _cases(
    {
        (chaos, scenario): defect
        for chaos, defect in _INVARIANT_DEFECTS.items()
        for scenario in SCENARIOS
    }
)
EXPECTATION_CASES = _cases(_EXPECTATION_DEFECTS)

pytestmark = [pytest.mark.chaos, pytest.mark.timeout(TIMEOUT)]

_RUNS: dict[tuple[str, str], RunReport | BaseException] = {}


async def _run(chaos: str, scenario: str) -> RunReport:
    """Выполнить прогон один раз на комбинацию; оба теста ниже читают один отчёт."""
    key = (chaos, scenario)
    if key not in _RUNS:
        config = RunConfig(
            seed=SEED,
            scenario=scenario,
            chaos=chaos,
            duration=DURATION,
            lease_ttl=LEASE_TTL,
            sweep_interval=SWEEP_INTERVAL,
            drain_timeout=DRAIN_TIMEOUT,
            keep_stand=KEEP_STAND,
        )
        try:
            _RUNS[key] = await run_chaos(config)
        except Exception as exc:
            _RUNS[key] = exc
            raise
    outcome = _RUNS[key]
    if isinstance(outcome, BaseException):
        raise outcome
    return outcome


@pytest.mark.parametrize(("chaos", "scenario"), INVARIANT_CASES)
async def test_invariants_hold_after_chaos(chaos: str, scenario: str) -> None:
    """Критерий §6: I-01…I-14 зелёные и восстановление уложилось в ``T_rec``."""
    report = await _run(chaos, scenario)

    assert report.oracle_ok, report.describe()
    assert [item.invariant for item in report.invariants] == [
        f"I-{index:02}" for index in range(1, 15)
    ]


@pytest.mark.parametrize(("chaos", "scenario"), EXPECTATION_CASES)
async def test_chaos_specific_expectations(chaos: str, scenario: str) -> None:
    """Колонка «Дополнительно ожидаем» §6 и проверка, что отказ действительно состоялся."""
    report = await _run(chaos, scenario)

    assert not report.unmet, report.describe()
