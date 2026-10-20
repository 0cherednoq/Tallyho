"""Оракул `rate_limit` на записанных выборках: почему A-FQ-12 флакал.

Выборка `FLAKY` — настоящие моменты событий из упавшего прогона
`test_a_fq_12_broker_and_tallyho_concurrency_limits_compose`. Лимитер «2/s» её
не нарушал: в каждом его окне ровно два события. Нарушала только проверка,
раскладывавшая события по целым секундам.
"""

from __future__ import annotations

from collections import Counter

from tests.helpers.rates import max_in_window, min_span, span

__all__: list[str] = []

LIMIT = 2
FLAKY = [512.4, 512.9, 514.0, 514.3, 514.5, 515.0]
"""Шесть событий «2/s»: окна лимитера начались в 512.4, 513.4 и 514.4."""

BURST = [512.40, 512.41, 512.42, 512.43, 512.44, 512.45]
"""Лимитера нет: все шесть событий пришли пачкой."""


def test_integer_second_bins_report_a_violation_that_did_not_happen() -> None:
    # Прежняя проверка: события по целым секундам, не больше limit в каждой.
    buckets = Counter(int(at) for at in FLAKY)
    assert max(buckets.values()) == 3  # отсюда и падало `3 <= 2`
    # Но окна лимитера начинаются с первого разрешения, и в них по два события.
    first = min(FLAKY)
    windows = Counter(int(at - first) for at in FLAKY)
    assert set(windows.values()) == {LIMIT}


def test_sliding_window_bound_accepts_a_correct_fixed_window_limiter() -> None:
    # Предел фиксированного окна на границе двух окон — 2 * limit.
    assert max_in_window(FLAKY) <= 2 * LIMIT
    assert span(FLAKY) >= min_span(len(FLAKY), LIMIT)


def test_oracle_rejects_a_missing_limiter() -> None:
    assert max_in_window(BURST) > 2 * LIMIT
    assert span(BURST) < min_span(len(BURST), LIMIT)


def test_min_span_counts_waits_not_events() -> None:
    # Значения целые по построению, поэтому сравниваем как целые.
    assert int(min_span(6, 2)) == 2  # три окна — два ожидания между ними
    assert int(min_span(2, 2)) == 0  # помещается в одно окно
    assert int(min_span(5, 2)) == 2  # 2 + 2 + 1


def test_max_in_window_takes_the_densest_window() -> None:
    assert max_in_window([]) == 0
    assert max_in_window([1.0]) == 1
    # Событие ровно на границе окна в него не входит.
    assert max_in_window([1.0, 2.0]) == 1
    assert max_in_window([1.0, 1.999]) == 2


def test_span_of_empty_and_single_sample() -> None:
    assert not span([])
    assert not span([7.5])
