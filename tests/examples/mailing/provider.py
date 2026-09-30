"""Deterministic mail provider used by the section 12 scenarios."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

__all__ = [
    "FakeMailProvider",
    "HardBounceError",
    "RejectedError",
    "SentMessage",
    "TemporaryMailError",
]


class TemporaryMailError(Exception):
    """A retryable 4xx provider response."""


class HardBounceError(Exception):
    """A permanent missing-recipient response."""


class RejectedError(Exception):
    """A permanent policy or content rejection."""


@dataclass(frozen=True, slots=True)
class SentMessage:
    """One message accepted by the fake provider."""

    to: str
    from_: str
    id: str


class FakeMailProvider:
    """Choose deterministic behavior from the recipient domain."""

    def __init__(self) -> None:
        self.sent: list[SentMessage] = []
        self.attempts: Counter[str] = Counter()

    async def send(self, *, from_: str, to: str) -> str:
        """Accept a message or raise the domain-specific provider outcome."""
        self.attempts[to] += 1
        domain = to.rsplit("@", 1)[-1]
        if domain == "bounce.test":
            message = "550 5.1.1 user unknown"
            raise HardBounceError(message)
        if domain == "reject.test":
            message = "554 5.7.1 message content rejected"
            raise RejectedError(message)
        if domain == "flaky.test" and self.attempts[to] <= 2:
            message = "421 4.7.0 try again later"
            raise TemporaryMailError(message)
        if domain == "down.test":
            message = "451 4.3.0 local error"
            raise TemporaryMailError(message)
        message_id = f"msg-{len(self.sent) + 1}"
        self.sent.append(SentMessage(to=to, from_=from_, id=message_id))
        return message_id
