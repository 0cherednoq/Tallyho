"""Командная строка ``tallyho`` (maintenance, миграции)."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

from tallyho import __version__

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["build_parser", "main"]


def build_parser() -> argparse.ArgumentParser:
    """Собрать парсер аргументов CLI.

    Returns:
        Настроенный ``ArgumentParser``.
    """
    parser = argparse.ArgumentParser(prog="tallyho", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа CLI.

    Args:
        argv: Аргументы командной строки; ``None`` — взять из ``sys.argv``.

    Returns:
        Код возврата процесса.
    """
    build_parser().parse_args(argv)
    return 0
