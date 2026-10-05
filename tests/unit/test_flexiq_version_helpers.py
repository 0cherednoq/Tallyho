"""Диапазон версий flexiq для A-FQ-17 (без БД и брокера)."""

from __future__ import annotations

import pytest

from tests.helpers import flexiq_version


@pytest.mark.parametrize(
    "raw",
    ["2.0.0", "2.0.7", "2.1.0", "2.1.0.dev0", "2.14.3rc1", "2.0.1+local"],
)
def test_supported_versions_pass(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    seen: list[str] = []

    def fake_version(name: str) -> str:
        seen.append(name)
        return raw

    monkeypatch.setattr(flexiq_version, "version", fake_version)
    assert flexiq_version.installed_flexiq_supported()
    assert seen == ["flexiq"]


@pytest.mark.parametrize("raw", ["3.0.0", "3.1.0.dev0", "1.9.9", "1.0", "0.2.0", "", "2", "dev"])
def test_unsupported_versions_fail(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    def fake_version(_name: str) -> str:
        return raw

    monkeypatch.setattr(flexiq_version, "version", fake_version)
    assert not flexiq_version.installed_flexiq_supported()
