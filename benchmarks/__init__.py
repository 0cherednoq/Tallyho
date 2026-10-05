"""Бенчмарк-харнесс A-PERF (ACCEPTANCE §9): генераторы нагрузки P-01…P-11 и отчёты.

Запуск: ``uv run poe bench --id P-01,P-04 --profile smoke|nightly|full``. Отчёт и сырые
данные — в ``.work-tmp/bench/<id>-<profile>/``. Методика и профили —
``docs/benchmarks/README.md``.
"""

from __future__ import annotations

__all__: list[str] = []
