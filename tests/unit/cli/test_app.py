"""CLI parsing and graceful maintenance shutdown."""

from __future__ import annotations

import argparse
import asyncio
import signal
from typing import TYPE_CHECKING, cast, final
from uuid import UUID

import pytest
from typing_extensions import override

from tallyho.cli import app
from tallyho.model.errors import ConfigurationError
from tallyho.model.states import BatchState, OutboxKind
from tallyho.model.views import BatchView, Progress
from tallyho.protocols.broker import Message

if TYPE_CHECKING:
    from collections.abc import Callable

    from tallyho import Tallyho

__all__: list[str] = []


class _Loop:
    def __init__(self) -> None:
        self.callbacks: dict[signal.Signals, Callable[[], object]] = {}
        self.removed: list[signal.Signals] = []

    def add_signal_handler(
        self, candidate: signal.Signals, callback: Callable[[], object], *args: object
    ) -> None:
        _ = args
        self.callbacks[candidate] = callback
        if candidate is signal.SIGTERM:
            _ = callback()

    def remove_signal_handler(self, candidate: signal.Signals) -> bool:
        self.removed.append(candidate)
        return True


class _Runner:
    def __init__(self) -> None:
        self.stopped: bool = False
        self.ran: bool = False

    async def run(self) -> None:
        self.ran = True
        assert self.stopped

    def stop(self) -> None:
        self.stopped = True

    async def run_once(self) -> object:
        return None


@final
class _UnsupportedLoop(_Loop):
    @override
    def add_signal_handler(
        self, candidate: signal.Signals, callback: Callable[[], object], *args: object
    ) -> None:
        _ = candidate, callback, args
        raise NotImplementedError


@final
class _PassiveRunner(_Runner):
    @override
    async def run(self) -> None:
        self.ran: bool = True


class _Client:
    def handle(self, batch_id: UUID) -> object:
        raise AssertionError(batch_id)


async def test_sigterm_requests_graceful_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _Loop()
    runner = _Runner()
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)

    await app.serve_maintenance(runner)

    assert runner.ran
    assert runner.stopped
    assert loop.removed == [signal.SIGINT, signal.SIGTERM]


def test_parser_exposes_required_commands() -> None:
    help_text = app.build_parser().format_help()
    assert "migrate" in help_text
    assert "maintenance" in help_text
    assert "inspect" in help_text


async def test_maintenance_without_signal_support_still_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _UnsupportedLoop()
    runner = _PassiveRunner()
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)

    await app.serve_maintenance(runner)

    assert runner.ran
    assert loop.removed == []


async def test_rejecting_dispatcher_never_acknowledges_messages() -> None:
    dispatcher = app._RejectingDispatcher()  # ruff: ignore[private-member-access]  # CLI safety boundary is tested directly

    async def task() -> None:
        await asyncio.sleep(0)

    assert dispatcher.task_name(task).endswith("task")
    await dispatcher.dispatch([])
    message = Message(
        id=UUID(int=1),
        batch_id=UUID(int=2),
        kind=OutboxKind.ITEM,
        task_name="task",
        payload=b"payload",
        options={},
    )
    with pytest.raises(ConfigurationError):
        await dispatcher.dispatch([message])


async def test_invalid_inspect_target_is_rejected() -> None:
    client = cast("Tallyho", cast("object", _Client()))
    with pytest.raises(ConfigurationError):
        _ = await app._resolve(  # ruff: ignore[private-member-access]  # invalid target is rejected before client access
            client, "not-a-target"
        )


def test_render_handles_unknown_progress_and_nested_keyless_batch() -> None:
    child = BatchView(
        id=UUID(int=2),
        kind="child",
        key=None,
        state=BatchState.OPEN,
        progress=Progress(),
        labels={},
        metrics={},
        children={},
    )
    root = BatchView(
        id=UUID(int=1),
        kind="root",
        key=None,
        state=BatchState.OPEN,
        progress=Progress(),
        labels={},
        metrics={},
        children={"child": child},
    )

    rendered = app._render(root)  # ruff: ignore[private-member-access]  # formatting helper is deterministic

    assert len(rendered) == 2
    assert "done=0/?" in rendered[0]
    assert "progress=?" in rendered[0]
    assert rendered[1].startswith("  child id=")


@pytest.mark.parametrize(
    "raw",
    [
        {"command": "maintenance", "dsn": "dsn", "schema": "schema", "hook_module": "bad"},
        {
            "command": "maintenance",
            "dsn": "dsn",
            "schema": "schema",
            "hook_module": [1],
            "once": False,
        },
        {
            "command": "maintenance",
            "dsn": "dsn",
            "schema": "schema",
            "hook_module": [],
            "once": "bad",
        },
        {"command": "unknown", "dsn": "dsn", "schema": "schema"},
    ],
)
def test_parse_defensively_rejects_malformed_namespaces(
    monkeypatch: pytest.MonkeyPatch,
    raw: dict[str, object],
) -> None:
    parser = app.build_parser()

    def parse_args(argv: object) -> argparse.Namespace:
        _ = argv
        return argparse.Namespace(**raw)

    monkeypatch.setattr(parser, "parse_args", parse_args)
    monkeypatch.setattr(app, "build_parser", lambda: parser)

    with pytest.raises(ConfigurationError):
        _ = app._parse([])  # ruff: ignore[private-member-access]  # parser postconditions are defended explicitly


def test_parse_accepts_hook_modules() -> None:
    command = app._parse(  # ruff: ignore[private-member-access]  # parsed value is verified through its representation
        [
            "maintenance",
            "--dsn",
            "dsn",
            "--schema",
            "schema",
            "--hook-module",
            "example.hooks",
        ]
    )
    assert "example.hooks" in repr(command)


def test_required_value_and_main_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigurationError):
        _ = app._required({}, "missing")  # ruff: ignore[private-member-access]  # defensive parser helper

    def run_sync(coroutine: object) -> int:
        close = getattr(coroutine, "close", None)
        assert callable(close)
        close()
        return 7

    monkeypatch.setattr(asyncio, "run", run_sync)
    assert app.main(["migrate", "--dsn", "dsn", "--schema", "schema"]) == 7
