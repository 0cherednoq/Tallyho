"""Поддерживаемый диапазон версий flexiq для контракта A-FQ-17 (без БД и брокера)."""

from __future__ import annotations

import re
from importlib.metadata import version

__all__ = ["SUPPORTED_MAJOR", "SUPPORTED_MIN_MINOR", "installed_flexiq_supported", "is_supported"]

# Диапазон из pyproject (`flexiq>=2.0,<3`): nightly гоняет контракты на 2.0.x и на master,
# а master может уже нести 2.1+ — это та же поддерживаемая линейка.
SUPPORTED_MAJOR = 2
SUPPORTED_MIN_MINOR = 0
_RELEASE = re.compile(r"^(\d+)\.(\d+)")


def is_supported(raw: str) -> bool:
    """Версия лежит в `>=2.0,<3`; суффиксы (`.dev0`, `rc1`, `+local`) не важны."""
    match = _RELEASE.match(raw)
    if match is None:
        return False
    major, minor = int(match[1]), int(match[2])
    return major == SUPPORTED_MAJOR and minor >= SUPPORTED_MIN_MINOR


def installed_flexiq_supported() -> bool:
    """Установленный flexiq входит в заявленный диапазон."""
    return is_supported(version("flexiq"))
