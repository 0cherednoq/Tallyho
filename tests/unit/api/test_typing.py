"""Исполняемая проверка позитивных и негативных ParamSpec-контрактов."""

from __future__ import annotations

import re
import subprocess  # ruff: ignore[suspicious-subprocess-import]  # тест запускает локальные type checker'ы
import sys
from pathlib import Path

import pytest

__all__: list[str] = []

ROOT = Path(__file__).parents[3]
CASES = ROOT / "tests" / "typing" / "cases.py"
EXPECTED = "# EXPECTED_NEGATIVE"
TYPE_IGNORE = "  # type:" + " ignore"
ERROR_LINE = re.compile(r"negative_cases\.py:(?P<line>\d+)(?::\d+)?:.*error", re.IGNORECASE)


def negative_source() -> tuple[str, set[int]]:
    """Снять подавления и вернуть строки, на которых обязаны быть ошибки."""
    lines = CASES.read_text(encoding="utf-8").splitlines()
    expected = {number for number, line in enumerate(lines, start=1) if EXPECTED in line}
    stripped = [
        line.split(TYPE_IGNORE, maxsplit=1)[0] if EXPECTED in line else line for line in lines
    ]
    return "\n".join(stripped) + "\n", expected


@pytest.mark.parametrize("module", ["mypy", "basedpyright"])
def test_negative_calls_fail_on_exact_lines(tmp_path: Path, module: str) -> None:
    """Оба checker'а отвергают каждый намеренно неверный вызов и только его."""
    source, expected = negative_source()
    generated = tmp_path / "negative_cases.py"
    generated.write_text(source, encoding="utf-8", newline="\n")
    command = [sys.executable, "-m", module]
    if module == "mypy":
        command.extend(["--config-file", str(ROOT / "pyproject.toml")])
    else:
        command.extend(["--project", str(ROOT / "pyproject.toml")])
    command.append(str(generated))
    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]  # статическая команда локального checker'а
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    found = {int(match["line"]) for match in ERROR_LINE.finditer(output)}
    assert result.returncode == 1, output
    assert found == expected, output
