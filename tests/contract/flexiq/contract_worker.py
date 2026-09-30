"""Subprocess entry point for the real Flexiq contract worker."""

from __future__ import annotations

import argparse
from pathlib import Path

from tests.contract.flexiq.contract_app import build_app

__all__ = ["main"]


def main() -> None:
    """Create the same app as the producer and run a thread-pool worker."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--tallyho-schema", required=True)
    parser.add_argument("--flexiq-schema", required=True)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--queues", nargs="+", default=["default", "contract"])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--ready-name", default="ready")
    values = parser.parse_args()
    app = build_app(
        dsn=values.dsn,
        tallyho_schema=values.tallyho_schema,
        flexiq_schema=values.flexiq_schema,
        root=values.root,
        worker_count=values.workers,
    )
    (values.root / values.ready_name).write_text("ready", encoding="utf-8")
    app.queue.run_worker(queues=values.queues, pool="thread")


if __name__ == "__main__":
    main()
