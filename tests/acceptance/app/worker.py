"""Subprocess entry point for real FlexIQ acceptance workers."""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from tests.acceptance.app.application import build_app
from tests.acceptance.app.options import add_tuning_arguments, tuning_from

if TYPE_CHECKING:
    from tests.acceptance.app.application import AcceptanceApp

__all__ = ["main"]

_CLOSE_RESERVE = 3.0
"""Seconds kept before FlexIQ's own drain deadline for closing the installation."""


def _serve(app: AcceptanceApp, *, drain_timeout: float, ready: Path | None) -> None:
    """Run the worker until SIGTERM/SIGINT, then close the installation in bounded time.

    FlexIQ 2.0 cannot be trusted with the deadline: if ``drain_timeout`` expires while
    every ``async_concurrency`` slot is still busy, the whole process stops running Python
    code - ``run_worker`` does not return, jobs make no progress, signal handlers are not
    called. So FlexIQ runs in a thread and the main thread owns the signals and an earlier
    deadline: jobs that are still running when it passes get their Items returned to the
    outbox by ``aclose`` (A-CH-08), and the process exits on its own.

    The ready file appears only after the signal handlers are installed: the process is
    PID 1 of its container, and the kernel drops a SIGTERM that PID 1 has no handler for,
    so the chaos controller must not send it earlier (A-CH-08).
    """
    stop = threading.Event()
    failures: list[BaseException] = []

    def request_stop(signum: int, frame: object) -> None:
        _ = signum, frame
        stop.set()
        app.queue.shutdown()  # what FlexIQ's own SIGTERM handler does

    def run() -> None:
        try:
            app.queue.run_worker(queues=["default"], pool="thread")
        except BaseException as exc:
            failures.append(exc)
            raise

    for candidate in (signal.SIGINT, signal.SIGTERM):
        _ = signal.signal(candidate, request_stop)
    if ready is not None:
        _ = ready.write_text("ready", encoding="utf-8", newline="\n")
    worker = threading.Thread(target=run, name="flexiq-worker", daemon=True)
    worker.start()
    while worker.is_alive() and not stop.wait(0.2):
        pass
    worker.join(timeout=max(drain_timeout - _CLOSE_RESERVE, drain_timeout / 2))
    drained = not worker.is_alive()
    # The Completer and relay live in FlexIQ's executor loop, still running or already
    # stopped: ``aclose`` flushes the buffer there and returns held Items to the outbox.
    asyncio.run(app.th.aclose())
    if failures:
        raise failures[0]
    if drained:
        app.queue.close()
        return
    # Some jobs will not make it: their Items are back in the outbox, leave them behind.
    _ = sys.stderr.write("worker: drain deadline reached, unfinished Items returned to outbox\n")
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)  # FlexIQ is still draining: a normal exit would wait for it


def main() -> None:
    """Register S1/S2/S3 tasks and run a FlexIQ thread-pool worker until SIGTERM or SIGINT."""
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
    _serve(app, drain_timeout=float(values.drain_timeout), ready=values.ready)


if __name__ == "__main__":
    main()
