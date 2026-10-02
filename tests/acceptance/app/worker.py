"""Subprocess entry point for real FlexIQ acceptance workers."""

from __future__ import annotations

import argparse
from pathlib import Path

from tests.acceptance.app.application import build_app
from tests.acceptance.app.options import add_tuning_arguments, tuning_from

__all__ = ["main"]


def main() -> None:
    """Register S1/S2/S3 tasks and run a FlexIQ thread-pool worker."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--tallyho-schema", required=True)
    parser.add_argument("--flexiq-schema", required=True)
    parser.add_argument("--domain-schema", required=True)
    parser.add_argument("--site-url", required=True)
    parser.add_argument("--mail-url", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--network-scale", type=float, default=1.0)
    parser.add_argument("--transient-rate", type=float, default=0.05)
    parser.add_argument("--permanent-rate", type=float, default=0.01)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--ready", type=Path)
    add_tuning_arguments(parser)
    values = parser.parse_args()
    app = build_app(
        dsn=values.dsn,
        tallyho_schema=values.tallyho_schema,
        flexiq_schema=values.flexiq_schema,
        domain_schema=values.domain_schema,
        site_url=values.site_url,
        mail_url=values.mail_url,
        seed=values.seed,
        network_scale=values.network_scale,
        transient_rate=values.transient_rate,
        permanent_rate=values.permanent_rate,
        worker_count=values.workers,
        tuning=tuning_from(values),
    )
    if values.ready is not None:
        values.ready.write_text("ready", encoding="utf-8")
    app.queue.run_worker(queues=["default"], pool="thread")


if __name__ == "__main__":
    main()
