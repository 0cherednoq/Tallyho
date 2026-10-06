"""The two tutorial applications (docs/guide/tutorial) on ``InlineBroker``.

Task bodies, batch trees and hooks follow the tutorial pages; only the broker and the mail
service differ.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, final

from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select, update

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker
from tests.examples.tutorial.mail import (
    AccountLockedError,
    InvalidCredentialsError,
    MessageNotFoundError,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from tallyho import BatchHandle
    from tallyho.model.views import BatchSummary
    from tests.examples.tutorial.mail import (
        FakeMail,
    )

__all__ = ["CheckerApp", "ExportApp"]

STATUS = {BatchState.SUCCEEDED: "done", BatchState.COMPLETED_WITH_ERRORS: "partial"}

metadata = MetaData()
accounts = Table(
    "accounts", metadata, Column("id", Integer, primary_key=True), Column("verdict", String)
)
check_runs = Table(
    "check_runs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("status", String, nullable=False),
    Column("valid", Integer),
    Column("unchecked", Integer),
)
export_runs = Table(
    "export_runs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("status", String, nullable=False),
    Column("messages", Integer),
    Column("files", Integer),
    Column("failed", Integer),
    Column("skipped_by_limit", Integer),
)


@final
class CheckerApp:
    """Example 1: one batch that checks a list of accounts."""

    def __init__(self, engine: AsyncEngine, schema: str, mail: FakeMail) -> None:
        self.engine = engine.execution_options(schema_translate_map={None: schema})
        self.mail = mail
        self.broker = InlineBroker(max_retries=2)
        self.th = Tallyho(self.engine, schema=schema)
        self.th.install(self.broker.adapter)

        async def check_account(account_id: int) -> None:
            await self._check_account(account_id)

        self.check_account: Callable[[int], Awaitable[None]] = check_account

        @self.th.on_finalized("account_check")
        async def save_check_result(session: AsyncSession, summary: BatchSummary) -> None:
            await session.execute(
                update(check_runs).values(
                    status=STATUS[summary.state],
                    valid=summary.labels.get("valid", 0),
                    unchecked=summary.progress.error,
                )
            )

        self.save_check_result = save_check_result

    async def setup(self) -> None:
        """Create the application tables and the tallyho schema."""
        async with self.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
            await connection.execute(
                insert(accounts), [{"id": account_id} for account_id in self.mail.logins]
            )
            await connection.execute(insert(check_runs).values(id=1, status="running"))
        _ = await self.th.migrate()

    async def _check_account(self, account_id: int) -> None:
        try:
            await self.mail.login(account_id)
        except InvalidCredentialsError:
            verdict = "bad_password"
        except AccountLockedError:
            verdict = "locked"
        else:
            verdict = "valid"

        async with self.engine.begin() as connection:
            await connection.execute(
                update(accounts).where(accounts.c.id == account_id).values(verdict=verdict)
            )
            item.ok(verdict)
            await item.complete_in(connection)

    async def start_check(self) -> BatchHandle:
        """Create the batch as the tutorial's ``start_check`` does."""
        policy = self.th.FailurePolicy.threshold(
            ratio=0.5, min_processed=20, labels=["exhausted"], action="pause"
        )
        async with self.th.batch(
            "account_check", key="list:42", max_in_flight=10, failure_policy=policy
        ) as batch:
            await batch.add_calls(
                self.th.call(self.check_account, account_id).opts(key=str(account_id))
                for account_id in self.mail.logins
            )
        return batch.handle

    async def verdicts(self) -> dict[int, str | None]:
        """Read the verdict written next to every account."""
        async with self.engine.connect() as connection:
            rows = await connection.execute(select(accounts.c.id, accounts.c.verdict))
            return {row.id: row.verdict for row in rows}

    async def run(self) -> tuple[str, int | None, int | None]:
        """Read the row the finalization hook maintains."""
        async with self.engine.connect() as connection:
            row = (await connection.execute(select(check_runs))).one()
            return (row.status, row.valid, row.unchecked)


@final
class ExportApp:
    """Example 2: the pages, messages and attachments pipeline."""

    def __init__(self, engine: AsyncEngine, schema: str, mail: FakeMail) -> None:
        self.engine = engine.execution_options(schema_translate_map={None: schema})
        self.mail = mail
        self.clock = FakeClock(datetime(2026, 10, 7, 9, tzinfo=UTC))
        self.broker = InlineBroker(max_retries=1)
        self.th = Tallyho(
            self.engine,
            schema=schema,
            clock=self.clock,
            lease_ttl=timedelta(seconds=60),
            relay_grace=timedelta(0),
        )
        self.th.install(self.broker.adapter)

        async def list_page(mailbox_id: int, page: int = 0) -> None:
            await self._list_page(mailbox_id, page)

        async def fetch_message(mailbox_id: int, message_id: str) -> None:
            await self._fetch_message(mailbox_id, message_id)

        async def download(mailbox_id: int, attachment_id: str) -> None:
            await self._download(mailbox_id, attachment_id)

        self.list_page: Callable[[int, int], Awaitable[None]] = list_page
        self.fetch_message: Callable[[int, str], Awaitable[None]] = fetch_message
        self.download: Callable[[int, str], Awaitable[None]] = download

        @self.th.on_finalized("mail_export")
        async def save_export_result(session: AsyncSession, summary: BatchSummary) -> None:
            stage = summary.children
            await session.execute(
                update(export_runs).values(
                    status=STATUS[summary.state],
                    messages=stage["messages"].progress.ok,
                    files=stage["attachments"].progress.ok,
                    failed=sum(child.progress.error for child in stage.values()),
                    skipped_by_limit=sum(
                        child.progress.skipped_by_limit for child in stage.values()
                    ),
                )
            )

        self.save_export_result = save_export_result

    async def setup(self) -> None:
        """Create the application tables and the tallyho schema."""
        async with self.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
            await connection.execute(insert(export_runs).values(id=1, status="running"))
        _ = await self.th.migrate()

    async def _list_page(self, mailbox_id: int, page: int) -> None:
        message_ids, next_page = await self.mail.list_page(mailbox_id, page)
        for message_id in message_ids:
            item.spawn(
                self.fetch_message,
                mailbox_id,
                message_id,
                into="messages",
                key=f"{mailbox_id}/{message_id}",
            )
        if next_page is not None:
            item.spawn(self.list_page, mailbox_id, next_page)

    async def _fetch_message(self, mailbox_id: int, message_id: str) -> None:
        try:
            attachment_ids = await self.mail.fetch(message_id)
        except MessageNotFoundError:
            item.skip("deleted")
            return
        for attachment_id in attachment_ids:
            item.spawn(
                self.download, mailbox_id, attachment_id, into="attachments", key=attachment_id
            )
        item.ok("with_files" if attachment_ids else "plain")

    async def _download(self, mailbox_id: int, attachment_id: str) -> None:
        del mailbox_id
        body = await self.mail.download(attachment_id)
        item.incr("bytes", len(body))
        item.ok("saved")

    async def start_export(self) -> BatchHandle:
        """Create the tree as the tutorial's ``start_export`` does."""
        async with self.th.batch("mail_export", key="run:1", max_items=2_000_000) as root:
            pages = root.sub_batch("pages", max_depth=5_000)
            messages = root.sub_batch("messages", fed_by=[pages])
            _ = root.sub_batch("attachments", fed_by=[messages], max_in_flight=20)
            await pages.add_calls(
                self.th.call(self.list_page, mailbox_id, 0).opts(key=f"{mailbox_id}:first")
                for mailbox_id in self.mail.mailboxes
            )
        return root.handle

    async def run(self) -> tuple[str, int | None, int | None, int | None, int | None]:
        """Read the row the finalization hook maintains."""
        async with self.engine.connect() as connection:
            row = (await connection.execute(select(export_runs))).one()
            return (row.status, row.messages, row.files, row.failed, row.skipped_by_limit)
