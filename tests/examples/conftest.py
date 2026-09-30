"""Markers for executable end-to-end examples."""

from __future__ import annotations

from pathlib import Path

import pytest

__all__: list[str] = []

HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark all executable examples as PostgreSQL integration tests."""
    for item in items:
        if item.path.is_relative_to(HERE):
            item.add_marker(pytest.mark.integration)
