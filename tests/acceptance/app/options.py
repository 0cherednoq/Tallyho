"""Command-line knobs shared by the acceptance worker and API entry points."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, cast

from tests.acceptance.app.application import StandTuning

if TYPE_CHECKING:
    import argparse

__all__ = ["add_tuning_arguments", "tuning_from"]

_DEFAULTS = StandTuning()


def add_tuning_arguments(parser: argparse.ArgumentParser) -> None:
    """Register the :class:`StandTuning` options with their smoke-harness defaults."""
    parser.add_argument("--application-name")
    parser.add_argument("--lease-ttl", type=float, default=_DEFAULTS.lease_ttl.total_seconds())
    parser.add_argument(
        "--heartbeat-every", type=float, default=_DEFAULTS.heartbeat_every.total_seconds()
    )
    parser.add_argument(
        "--sweep-interval", type=float, default=_DEFAULTS.sweep_interval.total_seconds()
    )
    parser.add_argument("--drain-timeout", type=int, default=_DEFAULTS.drain_timeout)
    parser.add_argument("--hook-delay", type=float, default=_DEFAULTS.hook_delay)


def tuning_from(values: argparse.Namespace) -> StandTuning:
    """Build the tuning record from parsed command-line values."""
    return StandTuning(
        application_name=cast("str | None", values.application_name),
        lease_ttl=timedelta(seconds=cast("float", values.lease_ttl)),
        heartbeat_every=timedelta(seconds=cast("float", values.heartbeat_every)),
        sweep_interval=timedelta(seconds=cast("float", values.sweep_interval)),
        drain_timeout=cast("int", values.drain_timeout),
        hook_delay=cast("float", values.hook_delay),
    )
