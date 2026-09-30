"""Extract and execute the documentation blocks marked for CI."""

from __future__ import annotations

import ast
import inspect
import re
from dataclasses import dataclass
from pathlib import Path
from types import FunctionType
from typing import TYPE_CHECKING, Protocol, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []

ROOT = Path(__file__).parents[2]
DOCUMENTS = (ROOT / "README.md", ROOT / "docs" / "ARCHITECTURE.md")
EXPECTED = {
    "readme-quickstart",
    "architecture-mailing",
    "architecture-catalog",
}
MARKER = re.compile(r"^\s*<!--\s*tallyho-example:\s*([a-z0-9-]+)\s*-->\s*$")


@dataclass(frozen=True, slots=True)
class Example:
    """One executable fenced Python block from a Markdown document."""

    name: str
    path: Path
    line: int
    source: str


class ExampleRunner(Protocol):
    """Callable shape of a code object compiled with top-level await."""

    def __call__(self) -> Awaitable[object]: ...


def extract_examples(paths: Iterable[Path]) -> list[Example]:
    """Return every Python fence immediately following a tallyho marker."""
    examples: list[Example] = []
    names: set[str] = set()
    for path in paths:
        lines = path.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            match = MARKER.fullmatch(line)
            if match is None:
                continue
            name = match.group(1)
            fence = index + 1
            while fence < len(lines) and not lines[fence].strip():
                fence += 1
            message = f"{path}:{index + 1}: marker must be followed by a Python fence"
            assert fence < len(lines), message
            assert lines[fence].strip() == "```python", message
            end = fence + 1
            while end < len(lines) and lines[end].strip() != "```":
                end += 1
            assert end < len(lines), f"{path}:{fence + 1}: unclosed Python fence"
            assert name not in names, f"duplicate documentation example: {name}"
            names.add(name)
            examples.append(
                Example(
                    name=name,
                    path=path,
                    line=fence + 2,
                    source="\n".join(lines[fence + 1 : end]) + "\n",
                )
            )
    return examples


EXAMPLES = extract_examples(DOCUMENTS)


def test_documentation_example_manifest_is_complete() -> None:
    assert {example.name for example in EXAMPLES} == EXPECTED
    assert {example.path for example in EXAMPLES} == set(DOCUMENTS)
    readme = DOCUMENTS[0]
    readme_python_fences = sum(
        line.strip() == "```python" for line in readme.read_text(encoding="utf-8").splitlines()
    )
    assert sum(example.path == readme for example in EXAMPLES) == readme_python_fences


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda value: value.name)
async def test_documentation_example_executes(
    example: Example,
    engine: AsyncEngine,
    schema: str,
) -> None:
    filename = f"{example.path.relative_to(ROOT).as_posix()}:{example.line}"
    namespace: dict[str, object] = {
        "__name__": f"tallyho_documentation_{example.name.replace('-', '_')}",
        "engine": engine,
        "schema": schema,
    }
    code = compile(example.source, filename, "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
    runner = cast("ExampleRunner", FunctionType(code, namespace))
    result = runner()
    assert inspect.isawaitable(result), f"{filename}: example must contain top-level await"
    _ = await result
