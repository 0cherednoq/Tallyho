"""The tutorial applications from docs/guide/tutorial, executed on PostgreSQL.

The tutorial pages show flexiq application code without assertions. The same tasks, batch trees
and hooks run here on ``InlineBroker`` with a dictionary instead of a mail service, so the
behaviour the pages describe (labels, stage closing, recovery, repeated finalization) is checked.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tallyho.model.states import BatchState
from tests.examples.tutorial.apps import CheckerApp, ExportApp
from tests.examples.tutorial.mail import FakeMail

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []

pytestmark = pytest.mark.timeout(300)


async def test_account_check_records_verdicts_and_retries_unchecked(
    engine: AsyncEngine, schema: str
) -> None:
    mail = FakeMail(
        logins={1: "ok", 2: "ok", 3: "bad_password", 4: "locked", 5: "flaky", 6: "down"}
    )
    app = CheckerApp(engine, schema, mail)
    await app.setup()
    try:
        handle = await app.start_check()
        await app.broker.drain()

        view = await handle.wait(timeout=30)
        assert view.state is BatchState.COMPLETED_WITH_ERRORS
        assert (view.progress.found, view.progress.ok, view.progress.error) == (6, 5, 1)
        assert dict(view.labels) == {"valid": 3, "bad_password": 1, "locked": 1, "exhausted": 1}
        assert (mail.login_attempts[5], mail.login_attempts[6]) == (2, 3)
        assert await app.run() == ("partial", 3, 1)
        verdicts = {1: "valid", 2: "valid", 3: "bad_password", 4: "locked", 5: "valid", 6: None}
        assert await app.verdicts() == verdicts
        assert [entry.key async for entry in handle.items(labels=["exhausted"])] == ["6"]
        assert (await app.th.find("account_check", "list:42")).id == handle.id

        mail.logins[6] = "ok"
        assert await handle.retry_failed(labels=["exhausted"]) == 1
        await app.broker.drain()

        view = await handle.wait(timeout=30)
        assert view.state is BatchState.SUCCEEDED
        assert view.labels["valid"] == 4
        assert await app.run() == ("done", 4, 0)
        assert (await app.verdicts())[6] == "valid"
    finally:
        await app.th.aclose()


async def test_mail_export_pipeline_survives_failures_and_reports_the_result(
    engine: AsyncEngine, schema: str
) -> None:
    mail = FakeMail(
        mailboxes={1: [["m1", "m2"], ["m3"]], 2: [["m4", "gone"]]},
        attachments={"m1": ["offer", "logo"], "m2": [], "m3": ["dump"], "m4": ["offer"]},
        broken_files={"dump"},
    )
    app = ExportApp(engine, schema, mail)
    await app.setup()
    try:
        handle = await app.start_export()

        # The worker dies on the third delivery, in the middle of message m1.
        app.broker.kill_worker_after(3)
        await app.broker.drain()
        stuck = await handle.view()
        assert stuck.state is BatchState.SEALED
        assert stuck.children["pages"].state is BatchState.SUCCEEDED
        assert stuck.children["messages"].state is BatchState.SEALED
        assert stuck.children["messages"].progress.in_flight == 1
        assert stuck.children["attachments"].state is BatchState.OPEN
        assert (await app.run())[0] == "running"

        # The lease expires, maintenance returns the message to the queue.
        _ = app.clock.advance(seconds=61)
        await app.th.run_maintenance_once()
        await app.broker.drain()

        view = await handle.wait(timeout=30)
        assert view.state is BatchState.COMPLETED_WITH_ERRORS
        stage = view.children
        assert stage["pages"].progress.found == 3
        assert dict(stage["messages"].labels) == {"with_files": 3, "plain": 1, "deleted": 1}
        files = stage["attachments"].progress
        assert (files.found, files.duplicates, files.ok, files.error) == (3, 1, 2, 1)
        assert await app.run() == ("partial", 4, 2, 1, 0)
        failed = await handle.child("attachments")
        assert [entry.key async for entry in failed.items(labels=["exhausted"])] == ["dump"]

        mail.broken_files.clear()
        assert await handle.retry_failed() == 1
        await app.broker.drain()

        view = await handle.wait(timeout=30)
        assert view.state is BatchState.SUCCEEDED
        assert (view.progress.found, view.progress.done, view.progress.ratio) == (3, 3, 1.0)
        assert view.children["attachments"].metrics["bytes"] == 3 * 1024
        assert await app.run() == ("done", 4, 3, 0, 0)
    finally:
        await app.th.aclose()
