"""Extract and execute the documentation blocks marked for CI.

Two markers are recognised, each on the line(s) right above a Python fence:

* ``<!-- tallyho-example: name -->`` — the block is compiled with top-level
  ``await`` and executed on PostgreSQL with ``engine`` and ``schema`` in scope;
* ``<!-- tallyho-noexec: reason -->`` — the block is illustrative (it needs a
  broker worker, pgbouncer, a user project layout …) and is only accounted for.

README and every page of the documentation site (``docs/index.md``,
``docs/guide``, ``docs/reference``) are *strict*: a Python fence without one of
the markers fails the manifest tests. The pages show application code without
assertions, so most of their fences are ``tallyho-noexec``. The behaviour they
describe is executed elsewhere: ``tests/examples/guide_scenarios.md`` holds the
assert-based scenarios of the guide, ``tests/examples/tutorial`` runs the
tutorial applications. ARCHITECTURE keeps its many illustrative fragments
unmarked; only its marked blocks are executed.

Relative links of the strict documents are checked too: the target file must
exist and a ``#fragment`` must match a heading of that file.
"""

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
README = ROOT / "README.md"
ARCHITECTURE = ROOT / "docs" / "ARCHITECTURE.md"
DOCS = ROOT / "docs"
# Страницы сайта документации (docs/conf.py, include_patterns): главная, руководство, справочник.
GUIDE_PAGES = tuple(
    sorted(
        [DOCS / "index.md", *(DOCS / "guide").rglob("*.md"), *(DOCS / "reference").rglob("*.md")]
    )
)
# Учебный раздел показывает код приложения на flexiq без проверок: исполняемых блоков в нём нет,
# его сценарии выполняет tests/examples/tutorial/test_tutorial.py.
TUTORIAL = DOCS / "guide" / "tutorial"
TUTORIAL_TESTS = ROOT / "tests" / "examples" / "tutorial" / "test_tutorial.py"
# Сценарии руководства с проверками: страницы сайта показывают тот же код без assert.
SCENARIOS = ROOT / "tests" / "examples" / "guide_scenarios.md"
DOCUMENTS = (README, ARCHITECTURE, SCENARIOS, *GUIDE_PAGES)
STRICT_DOCUMENTS = (README, *GUIDE_PAGES)
EXPECTED = {
    "readme-quickstart",
    "architecture-mailing",
    "architecture-delivery",
    "architecture-catalog",
    "guide-start-first-batch",
    "guide-install-migrate",
    "guide-install-session",
    "guide-batches-basics",
    "guide-batches-streaming",
    "guide-batches-streaming-close",
    "guide-batches-pipeline",
    "guide-batches-policy",
    "guide-batches-operations",
    "guide-batches-listing",
    "guide-hooks-domain",
    "guide-hooks-retry",
    "guide-hooks-lease-lost",
    "guide-hooks-release",
    "guide-testing-broker",
    "guide-testing-clock",
    "guide-flexiq-call-options",
    "guide-operations-maintenance",
    "guide-operations-observer",
    "guide-postgres-storage",
}
# Guide pages that must exist; a renamed or deleted page fails the manifest.
EXPECTED_GUIDE_PAGES = {
    "index.md",
    "guide/getting-started.md",
    "guide/concepts.md",
    "guide/installation.md",
    "guide/batches.md",
    "guide/batches/tasks.md",
    "guide/batches/pipelines.md",
    "guide/batches/failure-policies.md",
    "guide/batches/operations.md",
    "guide/batches/progress.md",
    "guide/batches/attributes.md",
    "guide/hooks.md",
    "guide/hooks/retry.md",
    "guide/hooks/callbacks.md",
    "guide/hooks/complete-in.md",
    "guide/hooks/retention.md",
    "guide/hooks/recipe.md",
    "guide/testing.md",
    "guide/tutorial/overview.md",
    "guide/tutorial/checker.md",
    "guide/tutorial/export.md",
    "guide/tutorial/export-progress.md",
    "guide/tutorial/export-finalization.md",
    "guide/flexiq.md",
    "guide/operations.md",
    "guide/operations/shutdown.md",
    "guide/operations/postgres.md",
    "guide/operations/observability.md",
    "guide/limitations.md",
    "reference/settings.md",
    "reference/cli.md",
    "reference/errors.md",
    "reference/api.md",
    "reference/api/client.md",
    "reference/api/runtime.md",
    "reference/api/model.md",
    "reference/api/testing.md",
    "reference/api/extensions.md",
}
MARKER = re.compile(r"^\s*<!--\s*tallyho-example:\s*([a-z0-9-]+)\s*-->\s*$")
NOEXEC = re.compile(r"^\s*<!--\s*tallyho-noexec:\s*(\S.*?)\s*-->\s*$")
PYTHON_INFO = {"python", "py", "python3"}
EXECUTABLE_FENCE = "```python"
FENCE = "```"
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$")
NOT_SLUG = re.compile(r"[^\w\- ]")
EXTERNAL = ("http://", "https://", "mailto:")


@dataclass(frozen=True, slots=True)
class Example:
    """One executable fenced Python block from a Markdown document."""

    name: str
    path: Path
    line: int
    source: str


@dataclass(frozen=True, slots=True)
class PythonFence:
    """One Python fence of a document together with the marker above it."""

    path: Path
    line: int
    opening: str
    source: str
    example: str | None
    noexec: str | None


class ExampleRunner(Protocol):
    """Callable shape of a code object compiled with top-level await."""

    def __call__(self) -> Awaitable[object]: ...


def _is_python(opening: str) -> bool:
    info = opening.strip()[len(FENCE) :].split()
    return bool(info) and info[0].lower() in PYTHON_INFO


def _marker_above(lines: list[str], fence: int) -> tuple[int | None, str | None, str | None]:
    """Return ``(marker line index, example name, noexec reason)`` for a fence."""
    index = fence - 1
    while index >= 0 and not lines[index].strip():
        index -= 1
    if index < 0:
        return None, None, None
    if (match := MARKER.fullmatch(lines[index])) is not None:
        return index, match.group(1), None
    if (match := NOEXEC.fullmatch(lines[index])) is not None:
        return index, None, match.group(1)
    return None, None, None


def python_fences(path: Path) -> list[PythonFence]:
    """Return every Python fence of ``path``; a dangling marker is an error."""
    lines = path.read_text(encoding="utf-8").splitlines()
    found: list[PythonFence] = []
    attached: set[int] = set()
    index = 0
    while index < len(lines):
        opening = lines[index]
        if not opening.strip().startswith(FENCE):
            index += 1
            continue
        end = index + 1
        while end < len(lines) and lines[end].strip() != FENCE:
            end += 1
        assert end < len(lines), f"{path}:{index + 1}: unclosed fence"
        if _is_python(opening):
            marker, example, noexec = _marker_above(lines, index)
            if marker is not None:
                attached.add(marker)
            found.append(
                PythonFence(
                    path=path,
                    line=index + 2,
                    opening=opening.strip(),
                    source="\n".join(lines[index + 1 : end]) + "\n",
                    example=example,
                    noexec=noexec,
                )
            )
        index = end + 1
    for number, line in enumerate(lines):
        if MARKER.fullmatch(line) is not None or NOEXEC.fullmatch(line) is not None:
            message = f"{path}:{number + 1}: marker must be followed by a Python fence"
            assert number in attached, message
    return found


def prose_lines(path: Path) -> list[str]:
    """Return the lines of ``path`` that lie outside fenced code blocks."""
    prose: list[str] = []
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith(FENCE):
            fenced = not fenced
        elif not fenced:
            prose.append(line)
    return prose


def heading_slugs(path: Path) -> set[str]:
    """Return GitHub-style anchors of every heading in ``path``."""
    slugs: set[str] = set()
    for line in prose_lines(path):
        match = HEADING.fullmatch(line)
        if match is not None:
            slugs.add(NOT_SLUG.sub("", match.group(1).lower()).replace(" ", "-"))
    return slugs


def broken_links(path: Path) -> list[str]:
    """Return relative links of ``path`` whose file or heading does not exist."""
    broken: list[str] = []
    for line in prose_lines(path):
        for target in cast("list[str]", LINK.findall(line)):
            if target.startswith(EXTERNAL):
                continue
            location, _, fragment = target.partition("#")
            destination = (path.parent / location).resolve() if location else path
            missing = not destination.exists()
            checks_heading = bool(fragment) and destination.suffix == ".md"
            if missing or (checks_heading and fragment not in heading_slugs(destination)):
                broken.append(target)
    return broken


def extract_examples(paths: Iterable[Path]) -> list[Example]:
    """Return every Python fence immediately following a tallyho-example marker."""
    examples: list[Example] = []
    names: set[str] = set()
    for path in paths:
        for fence in python_fences(path):
            if fence.example is None:
                continue
            message = f"{path}:{fence.line - 1}: executable block must open with ```python"
            assert fence.opening == EXECUTABLE_FENCE, message
            assert fence.example not in names, f"duplicate documentation example: {fence.example}"
            names.add(fence.example)
            examples.append(
                Example(name=fence.example, path=path, line=fence.line, source=fence.source)
            )
    return examples


EXAMPLES = extract_examples(DOCUMENTS)


def test_documentation_example_manifest_is_complete() -> None:
    assert {example.name for example in EXAMPLES} == EXPECTED
    with_examples = {example.path for example in EXAMPLES}
    assert {ARCHITECTURE, SCENARIOS} <= with_examples
    assert python_fences(README)


def test_guide_pages_are_present() -> None:
    assert {page.relative_to(DOCS).as_posix() for page in GUIDE_PAGES} == EXPECTED_GUIDE_PAGES


@pytest.mark.parametrize(
    "path", STRICT_DOCUMENTS, ids=lambda value: cast("Path", value).relative_to(ROOT).as_posix()
)
def test_every_python_block_is_marked(path: Path) -> None:
    unmarked = [
        f"{path.relative_to(ROOT).as_posix()}:{fence.line - 1}"
        for fence in python_fences(path)
        if fence.example is None and fence.noexec is None
    ]
    assert not unmarked, f"Python blocks without tallyho-example/tallyho-noexec: {unmarked}"


def test_scenarios_name_the_page_they_support() -> None:
    # Каждый сценарий называет страницу, поведение которой он подтверждает, и она существует.
    text = SCENARIOS.read_text(encoding="utf-8")
    pages = re.findall(r"^Страница: `([^`]+)`", text, flags=re.MULTILINE)
    assert len(pages) == len(extract_examples([SCENARIOS]))
    assert all((ROOT / page).is_file() for page in pages)


def test_tutorial_scenarios_are_tested_outside_the_pages() -> None:
    assert any(python_fences(page) for page in GUIDE_PAGES if TUTORIAL in page.parents)
    source = TUTORIAL_TESTS.read_text(encoding="utf-8")
    assert "async def test_account_check" in source
    assert "async def test_mail_export" in source


@pytest.mark.parametrize(
    "path", STRICT_DOCUMENTS, ids=lambda value: cast("Path", value).relative_to(ROOT).as_posix()
)
def test_relative_links_resolve(path: Path) -> None:
    assert broken_links(path) == []


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda value: cast("Example", value).name)
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
