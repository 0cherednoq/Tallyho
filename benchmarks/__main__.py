"""``python -m benchmarks`` — то же, что ``poe bench``."""

from __future__ import annotations

from benchmarks.cli import main

__all__: list[str] = []

if __name__ == "__main__":
    raise SystemExit(main())
