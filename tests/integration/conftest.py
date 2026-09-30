"""Маркеры для интеграционных тестов.

Общие PostgreSQL-фикстуры живут в ``tests/conftest.py``, чтобы ими могли
пользоваться и интеграционные тесты библиотеки, и исполняемые примеры.
"""

from __future__ import annotations

from pathlib import Path

import pytest

HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Хук видит все тесты сессии, поэтому помечаем только тесты из этой папки.
    for item in items:
        if item.path.is_relative_to(HERE):
            item.add_marker(pytest.mark.integration)
