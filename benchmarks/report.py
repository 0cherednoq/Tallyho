"""Результат одного P-NN и его запись: ``result.json``, ``report.md`` и графики SVG."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, TypeAlias

from benchmarks.charts import BarChart, LineChart, render_svg

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.profiles import Profile

__all__ = [
    "Check",
    "JsonValue",
    "ScenarioResult",
    "Table",
    "Verdict",
    "write_result",
    "write_summary",
]

JsonValue: TypeAlias = "str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None"


class Verdict(StrEnum):
    """Итог P-NN относительно цели ACCEPTANCE §9."""

    MET = "цель выполнена"
    NOT_MET = "цель не выполнена"
    INFO = "только информативно"
    ERROR = "ошибка прогона"


@dataclass(frozen=True, slots=True)
class Check:
    """Одна проверяемая цель: формулировка, измеренное значение и итог.

    ``passed=None`` — проверку на этом профиле выполнить нельзя (например, нет данных о CPU
    PostgreSQL при внешнем DSN); такая проверка не проваливает прогон.
    """

    name: str
    target: str
    measured: str
    passed: bool | None


@dataclass(frozen=True, slots=True)
class Table:
    """Таблица отчёта."""

    title: str
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


@dataclass(slots=True)
class ScenarioResult:
    """Всё, что P-NN отдаёт в отчёт."""

    id: str
    title: str
    measures: str
    target: str
    profile: Profile
    parameters: dict[str, JsonValue] = field(default_factory=dict[str, JsonValue])
    checks: list[Check] = field(default_factory=list[Check])
    metrics: dict[str, JsonValue] = field(default_factory=dict[str, JsonValue])
    tables: list[Table] = field(default_factory=list[Table])
    charts: list[LineChart | BarChart] = field(default_factory=list[LineChart | BarChart])
    notes: list[str] = field(default_factory=list[str])
    error: str | None = None
    duration_s: float = 0.0

    @property
    def verdict(self) -> Verdict:
        """Итог: ошибка; на ``smoke`` — информативно; иначе по проверкам."""
        if self.error is not None:
            return Verdict.ERROR
        if not self.profile.enforced:
            return Verdict.INFO
        if any(check.passed is False for check in self.checks):
            return Verdict.NOT_MET
        return Verdict.MET

    @property
    def met(self) -> bool | None:
        """Выполнены ли цели по существу (без учёта профиля); ``None`` — нечего проверять."""
        decided = [check.passed for check in self.checks if check.passed is not None]
        if self.error is not None or not decided:
            return None
        return all(decided)

    def headline(self) -> str:
        """Одна строка результата для сводной таблицы.

        Returns:
            Измеренные значения проверок через «; ».
        """
        return "; ".join(check.measured for check in self.checks) or "—"


def _clean(value: JsonValue) -> JsonValue:
    """NaN и бесконечности в JSON недопустимы — заменяются на ``None``.

    Returns:
        Значение, пригодное для JSON.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, list):
        return [_clean(item) for item in value]
    if isinstance(value, dict):
        return {key: _clean(item) for key, item in value.items()}
    return value


def _fallback(value: object) -> str | float:
    """Значения из БД, которых нет в JSON (``Decimal``, ``UUID``, время), — числом или строкой.

    Returns:
        Число для ``Decimal``, иначе ``str(value)``.
    """
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _met_text(*, met: bool | None) -> str:
    if met is None:
        return "нет данных"
    return "да" if met else "нет"


def _as_json(result: ScenarioResult) -> dict[str, JsonValue]:
    return {
        "id": result.id,
        "title": result.title,
        "measures": result.measures,
        "target": result.target,
        "profile": str(result.profile),
        "verdict": str(result.verdict),
        "met": result.met,
        "duration_s": round(result.duration_s, 1),
        "parameters": _clean(result.parameters),
        "checks": [
            {
                "name": check.name,
                "target": check.target,
                "measured": check.measured,
                "passed": check.passed,
            }
            for check in result.checks
        ],
        "metrics": _clean(result.metrics),
        "notes": list(result.notes),
        "error": result.error,
    }


def _markdown_table(headers: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines.extend("| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |" for row in rows)
    return lines


def _markdown(result: ScenarioResult) -> str:
    lines = [
        f"# {result.id} — {result.title}",
        "",
        f"* **Профиль:** `{result.profile}`; длительность {result.duration_s:.1f} с",
        f"* **Что меряем:** {result.measures}",
        f"* **Цель (ACCEPTANCE §9):** {result.target}",
        f"* **Итог:** {result.verdict}; цель по существу выполнена: {_met_text(met=result.met)}",
    ]
    if result.error is not None:
        lines.extend(("", "## Ошибка", "", "```text", result.error, "```"))
    if result.checks:
        lines.extend(("", "## Проверки", ""))
        lines.extend(
            _markdown_table(
                ("проверка", "цель", "измерено", "выполнена"),
                tuple(
                    (check.name, check.target, check.measured, _met_text(met=check.passed))
                    for check in result.checks
                ),
            )
        )
    for table in result.tables:
        lines.extend(("", f"## {table.title}", ""))
        lines.extend(_markdown_table(table.headers, table.rows))
    if result.charts:
        lines.extend(("", "## Графики", ""))
        lines.extend(f"![{chart.title}]({chart.name}.svg)" for chart in result.charts)
    if result.parameters:
        lines.extend(("", "## Параметры", "", "```json"))
        lines.extend(
            (
                json.dumps(
                    _clean(result.parameters), ensure_ascii=False, indent=2, default=_fallback
                ),
                "```",
            )
        )
    if result.notes:
        lines.extend(("", "## Заметки", ""))
        lines.extend(f"* {note}" for note in result.notes)
    return "\n".join(lines) + "\n"


def write_result(result: ScenarioResult, directory: Path) -> Path:
    """Записать JSON, markdown и SVG; вернуть путь к ``report.md``.

    Returns:
        Путь к ``report.md``.
    """
    directory.mkdir(parents=True, exist_ok=True)
    _ = (directory / "result.json").write_text(
        json.dumps(_as_json(result), ensure_ascii=False, indent=2, default=_fallback) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    for chart in result.charts:
        _ = (directory / f"{chart.name}.svg").write_text(
            render_svg(chart), encoding="utf-8", newline="\n"
        )
    report = directory / "report.md"
    _ = report.write_text(_markdown(result), encoding="utf-8", newline="\n")
    return report


def write_summary(results: list[ScenarioResult], path: Path) -> None:
    """Сводная таблица всех P-NN прогона."""
    rows = tuple(
        (
            f"[{result.id}]({result.id}-{result.profile}/report.md)",
            result.measures,
            result.headline() if result.error is None else "ошибка: " + result.error[:120],
            result.target,
            str(result.verdict),
            f"{result.duration_s:.0f} с",
        )
        for result in results
    )
    profile = results[0].profile if results else "—"
    lines = [f"# A-PERF — сводка прогона (`{profile}`)", ""]
    lines.extend(_markdown_table(("id", "что меряли", "результат", "цель", "итог", "время"), rows))
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
