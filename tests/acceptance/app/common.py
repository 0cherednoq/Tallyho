"""Seeded network and failure rules shared by every acceptance task."""

from __future__ import annotations

import asyncio
import hashlib
import random
from dataclasses import dataclass
from enum import StrEnum

__all__ = [
    "FaultKind",
    "FaultPlan",
    "PermanentError",
    "TransientError",
    "hold_transaction",
    "network",
    "rng_for",
]


class TransientError(Exception):
    """Seeded retryable external failure."""


class PermanentError(Exception):
    """Seeded non-retryable external failure."""


class FaultKind(StrEnum):
    """Deterministic outcome selected for one logical task."""

    NONE = "none"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


def rng_for(seed: int, identity: object, *, namespace: str) -> random.Random:
    """Create a stable RNG independent of process order and hash randomization."""
    raw = f"{seed}:{namespace}:{identity}".encode()
    digest = hashlib.blake2b(raw, digest_size=16).digest()
    return random.Random(  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # deterministic test data, not security
        int.from_bytes(digest)
    )


async def network(
    seed: int,
    identity: object,
    *,
    namespace: str,
    scale: float = 1.0,
) -> float:
    """Sleep for the deterministic ACCEPTANCE interval of 1-5 seconds.

    ``scale`` only accelerates the small local smoke test. The compose stand uses
    the default ``1.0`` and therefore always performs the full network delay.
    """
    seconds = rng_for(seed, identity, namespace=namespace).uniform(1.0, 5.0)
    await asyncio.sleep(seconds * scale)
    return seconds


async def hold_transaction(
    seed: int,
    identity: object,
    *,
    namespace: str,
    scale: float = 1.0,
) -> bool:
    """Keep 20% of domain transactions open for another seeded network delay."""
    selected = rng_for(seed, identity, namespace=f"{namespace}:long-tx").random() < 0.2
    if selected:
        _ = await network(seed, identity, namespace=f"{namespace}:inside-tx", scale=scale)
    return selected


@dataclass(frozen=True, slots=True)
class FaultPlan:
    """Deterministic 5% transient / 1% permanent injection policy."""

    seed: int
    transient_rate: float = 0.05
    permanent_rate: float = 0.01

    def choose(self, identity: object, *, namespace: str) -> FaultKind:
        """Return one stable outcome for the logical task identity."""
        value = rng_for(self.seed, identity, namespace=namespace).random()
        if value < self.permanent_rate:
            return FaultKind.PERMANENT
        if value < self.permanent_rate + self.transient_rate:
            return FaultKind.TRANSIENT
        return FaultKind.NONE

    def raise_for(self, identity: object, *, namespace: str, attempt: int) -> None:
        """Raise the selected failure; a transient failure clears after one retry."""
        outcome = self.choose(identity, namespace=namespace)
        if outcome is FaultKind.PERMANENT:
            message = f"permanent acceptance fault: {namespace}:{identity}"
            raise PermanentError(message)
        if outcome is FaultKind.TRANSIENT and attempt == 0:
            message = f"transient acceptance fault: {namespace}:{identity}"
            raise TransientError(message)
