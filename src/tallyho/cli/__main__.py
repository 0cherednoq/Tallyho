"""``python -m tallyho.cli``."""

from __future__ import annotations

import sys

from tallyho.cli import main

sys.exit(main())  # ruff: ignore[banned-api]  # единственная точка выхода процесса
