"""Datasets specified by ARCHITECTURE section 12.6."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["ContactSeed", "bounce_heavy_dataset", "compact_dataset", "standard_dataset"]


@dataclass(frozen=True, slots=True)
class ContactSeed:
    """One user-owned audience row plus optional pre-existing suppression."""

    email: str
    deleted: bool = False
    unsubscribed: bool = False
    suppressed: bool = False


def standard_dataset() -> list[ContactSeed]:
    """Return 10,000 unique addresses and 40 case-variant duplicates."""
    rows = [ContactSeed(f"user{index}@ok.test") for index in range(9_000)]
    rows.extend(ContactSeed(f"flaky{index}@flaky.test") for index in range(100))
    rows.extend(ContactSeed(f"deleted{index}@ok.test", deleted=True) for index in range(300))
    rows.extend(
        ContactSeed(f"unsubscribed{index}@ok.test", unsubscribed=True) for index in range(200)
    )
    rows.extend(ContactSeed(f"suppressed{index}@ok.test", suppressed=True) for index in range(100))
    rows.extend(ContactSeed(f"not-an-email-{index}") for index in range(50))
    rows.extend(ContactSeed(f"bounce{index}@bounce.test") for index in range(150))
    rows.extend(ContactSeed(f"reject{index}@reject.test") for index in range(50))
    rows.extend(ContactSeed(f"down{index}@down.test") for index in range(50))
    rows.extend(ContactSeed(f"User{index}@OK.test") for index in range(40))
    return rows


def compact_dataset(size: int = 640) -> list[ContactSeed]:
    """Return a fast all-success audience for behavioral scenarios."""
    return [ContactSeed(f"compact{index}@ok.test") for index in range(size)]


def bounce_heavy_dataset(size: int = 1_000) -> list[ContactSeed]:
    """Return an interleaved audience with exactly eight percent hard bounces."""
    bounce_positions = {index * size // 80 for index in range(80)}
    return [
        ContactSeed(f"heavy{index}@{'bounce.test' if index in bounce_positions else 'ok.test'}")
        for index in range(size)
    ]
