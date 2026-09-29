"""Проверки соглашений, которые не покрывают ruff/mypy/pyright/import-linter.

Эти тесты — страховка от типичных ошибок LLM-кода: исключения мимо иерархии,
подавление ошибок без причины, модули без явного публичного API.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import inspect
import pkgutil
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import tallyho
from tallyho.model.errors import TallyhoError

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

SRC = Path(tallyho.__file__).parent
ROOT = SRC.parent.parent
SOURCE_FILES = sorted(SRC.rglob("*.py"))
ALL_PY_FILES = sorted([*SOURCE_FILES, *(ROOT / "tests").rglob("*.py")])

# Встроенные исключения, которые библиотеке разрешено бросать напрямую.
ALLOWED_BUILTIN_RAISES = frozenset({"NotImplementedError", "TypeError"})
BUILTIN_EXCEPTIONS = frozenset(
    name
    for name, obj in vars(builtins).items()
    if isinstance(obj, type) and issubclass(obj, BaseException)
)
BROAD_EXCEPTIONS = frozenset({"Exception", "BaseException"})

# Допустимо только `<tool>: ignore[правило]  # причина` для ruff / type / pyright.
SUPPRESSION = re.compile(r"#\s*(?P<tool>noqa|(ruff|type|pyright):\s*ignore)\b(?P<rest>.*)$")
SUPPRESSION_OK = re.compile(r"^\[[\w\-, ]+\]\s+#\s*\S.{4,}")


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _iter_modules() -> Iterator[ModuleType]:
    yield tallyho
    for info in pkgutil.walk_packages(tallyho.__path__, prefix="tallyho."):
        if info.name.endswith("__main__") or info.name.startswith("tallyho.adapters.flexiq."):
            continue
        yield importlib.import_module(info.name)


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _raised_name(node: ast.Raise) -> str | None:
    exc = node.exc
    if isinstance(exc, ast.Call):
        exc = exc.func
    return exc.id if isinstance(exc, ast.Name) else None


def test_all_library_exceptions_inherit_tallyho_error() -> None:
    offenders = [
        f"{module.__name__}.{name}"
        for module in _iter_modules()
        for name, obj in vars(module).items()
        if inspect.isclass(obj)
        and issubclass(obj, BaseException)
        and obj.__module__ == module.__name__
        and not issubclass(obj, TallyhoError)
    ]
    assert not offenders, f"Исключения вне иерархии TallyhoError: {offenders}"


@pytest.mark.parametrize("path", SOURCE_FILES, ids=_rel)
def test_no_raw_builtin_raises(path: Path) -> None:
    offenders = [
        f"{_rel(path)}:{node.lineno} raise {name}"
        for node in ast.walk(_parse(path))
        if isinstance(node, ast.Raise)
        and (name := _raised_name(node)) is not None
        and name in BUILTIN_EXCEPTIONS - ALLOWED_BUILTIN_RAISES
    ]
    assert not offenders, f"Бросай подкласс TallyhoError, а не builtin: {offenders}"


@pytest.mark.parametrize("path", SOURCE_FILES, ids=_rel)
def test_no_broad_suppress(path: Path) -> None:
    offenders = [
        f"{_rel(path)}:{node.lineno}"
        for node in ast.walk(_parse(path))
        if isinstance(node, ast.Call)
        and isinstance(node.func, (ast.Name, ast.Attribute))
        and (node.func.id if isinstance(node.func, ast.Name) else node.func.attr) == "suppress"
        and any(isinstance(arg, ast.Name) and arg.id in BROAD_EXCEPTIONS for arg in node.args)
    ]
    assert not offenders, f"contextlib.suppress(Exception) глотает ошибки: {offenders}"


@pytest.mark.parametrize("path", SOURCE_FILES, ids=_rel)
def test_modules_declare_all(path: Path) -> None:
    if path.name == "__main__.py":
        return
    tree = _parse(path)
    declared = any(
        isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(
            isinstance(t, ast.Name) and t.id == "__all__"
            for t in (node.targets if isinstance(node, ast.Assign) else [node.target])
        )
        for node in tree.body
    )
    assert declared, f"{_rel(path)}: объяви __all__ (публичный API модуля)"


@pytest.mark.parametrize("path", ALL_PY_FILES, ids=_rel)
def test_suppressions_have_codes_and_reasons(path: Path) -> None:
    offenders = [
        f"{_rel(path)}:{lineno}: {line.strip()}"
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if (match := SUPPRESSION.search(line))
        and (match["tool"] == "noqa" or not SUPPRESSION_OK.match(match["rest"]))
    ]
    assert not offenders, (
        "Подавление без правила или причины. Формат: "
        "`# ruff: ignore[rule-name]  # причина`, `# type: ignore[code]  # причина`, "
        f"`# pyright: ignore[rule]  # причина`: {offenders}"
    )
