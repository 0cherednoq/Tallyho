"""import_hook_modules: импорт регистрирует хуки, ошибки импорта — ConfigurationError."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from tallyho.hooks.registry import HookRegistry, import_hook_modules
from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

HOOKS_MODULE = """\
from __future__ import annotations

from datetime import timedelta

from tallyho.hooks.registry import HookRegistry

REGISTRY = HookRegistry()


@REGISTRY.on_finalized("k")
async def save_result(session, s):
    pass


@REGISTRY.on_progress("k", every=timedelta(seconds=2))
async def save_progress(session, s):
    pass
"""


@pytest.fixture
def module_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.syspath_prepend(str(tmp_path))
    before = set(sys.modules)
    yield tmp_path
    for name in set(sys.modules) - before:
        if name.startswith("th_hooks_"):
            del sys.modules[name]


def _write(directory: Path, name: str, source: str) -> None:
    (directory / f"{name}.py").write_text(source, encoding="utf-8", newline="\n")


def test_import_registers_hooks_once(module_dir: Path) -> None:
    _write(module_dir, "th_hooks_ok", HOOKS_MODULE)
    import_hook_modules(["th_hooks_ok"])
    import_hook_modules(["th_hooks_ok"])  # повтор из sys.modules, без дубля
    registry = sys.modules["th_hooks_ok"].REGISTRY
    assert isinstance(registry, HookRegistry)
    assert registry.required_hooks("k") == ("finalized", "progress")


def test_empty_hook_modules_is_noop() -> None:
    import_hook_modules([])


def test_missing_module_is_configuration_error(module_dir: Path) -> None:
    del module_dir
    with pytest.raises(ConfigurationError, match="th_hooks_absent") as info:
        import_hook_modules(["th_hooks_absent"])
    assert isinstance(info.value.__cause__, ImportError)


def test_broken_dependency_is_configuration_error(module_dir: Path) -> None:
    _write(module_dir, "th_hooks_broken", "import th_hooks_no_such_dependency\n")
    with pytest.raises(ConfigurationError, match="th_hooks_broken"):
        import_hook_modules(["th_hooks_broken"])
