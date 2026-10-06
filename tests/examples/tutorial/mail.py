"""A dictionary-backed mail service for the tutorial scenarios."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import final

__all__ = [
    "AccountLockedError",
    "FakeMail",
    "InvalidCredentialsError",
    "MailTemporaryError",
    "MessageNotFoundError",
]


class InvalidCredentialsError(Exception):
    """The mail service rejected the credentials."""


class AccountLockedError(Exception):
    """The mail service locked the account."""


class MailTemporaryError(Exception):
    """A network failure: the only error worth a broker retry."""


class MessageNotFoundError(Exception):
    """The message was deleted between listing and fetching."""


@final
@dataclass(slots=True)
class FakeMail:
    """Answers of the mail service; tests edit the dictionaries to "repair" it."""

    logins: dict[int, str] = field(default_factory=dict)
    mailboxes: dict[int, list[list[str]]] = field(default_factory=dict)
    attachments: dict[str, list[str]] = field(default_factory=dict)
    broken_files: set[str] = field(default_factory=set)
    login_attempts: Counter[int] = field(default_factory=Counter)

    async def login(self, account_id: int) -> None:
        """Raise the error configured for the account, if any."""
        await asyncio.sleep(0)
        self.login_attempts[account_id] += 1
        status = self.logins[account_id]
        if status == "down" or (status == "flaky" and self.login_attempts[account_id] == 1):
            raise MailTemporaryError
        if status == "bad_password":
            raise InvalidCredentialsError
        if status == "locked":
            raise AccountLockedError

    async def list_page(self, mailbox_id: int, page: int) -> tuple[list[str], int | None]:
        """Return message ids of the page and the number of the next page."""
        await asyncio.sleep(0)
        pages = self.mailboxes[mailbox_id]
        return pages[page], page + 1 if page + 1 < len(pages) else None

    async def fetch(self, message_id: str) -> list[str]:
        """Return attachment ids of the message."""
        await asyncio.sleep(0)
        if message_id not in self.attachments:
            raise MessageNotFoundError
        return self.attachments[message_id]

    async def download(self, attachment_id: str) -> bytes:
        """Return the attachment body."""
        await asyncio.sleep(0)
        if attachment_id in self.broken_files:
            raise MailTemporaryError
        return b"x" * 1024
