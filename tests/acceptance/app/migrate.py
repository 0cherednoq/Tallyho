"""One-shot database initializer for the docker-compose acceptance stand."""

from __future__ import annotations

import argparse
import asyncio

from tests.acceptance.app.application import build_app

__all__ = ["main"]


async def _run(values: argparse.Namespace) -> None:
    app = build_app(
        dsn=values.dsn,
        tallyho_schema=values.tallyho_schema,
        flexiq_schema=values.flexiq_schema,
        domain_schema=values.domain_schema,
        site_url=values.site_url,
        mail_url=values.mail_url,
        seed=values.seed,
        network_scale=values.network_scale,
    )
    try:
        await app.migrate()
    finally:
        await app.close()


def main() -> None:
    """Create the reference app's domain and Tallyho schema objects once."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--tallyho-schema", default="th")
    parser.add_argument("--flexiq-schema", default="flexiq")
    parser.add_argument("--domain-schema", default="app")
    parser.add_argument("--site-url", required=True)
    parser.add_argument("--mail-url", required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--network-scale", type=float, default=1.0)
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
