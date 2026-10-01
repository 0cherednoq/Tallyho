"""Command line interface for migrations, maintenance, and batch inspection."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, ParamSpec, cast, final
from uuid import UUID

from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

from tallyho import Tallyho, __version__
from tallyho.model.errors import ConfigurationError
from tallyho.protocols.broker import Dispatcher

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from tallyho.api import BatchHandle
    from tallyho.engine.public import MaintenanceRunner
    from tallyho.model.views import BatchView
    from tallyho.protocols.broker import Message

__all__ = ["build_parser", "main", "run", "serve_maintenance"]

P = ParamSpec("P")
_BAD_COMMAND = "unknown CLI command"
_BAD_TARGET = "inspect target must be a batch UUID or kind:key"
_NO_BROKER = "CLI maintenance has no broker adapter; outbox was left untouched"


@dataclass(frozen=True, slots=True)
class _Migrate:
    dsn: str
    schema: str


@dataclass(frozen=True, slots=True)
class _Maintenance:
    dsn: str
    schema: str
    hook_modules: tuple[str, ...]
    once: bool


@dataclass(frozen=True, slots=True)
class _Inspect:
    dsn: str
    schema: str
    target: str


_Command = _Migrate | _Maintenance | _Inspect


@final
class _RejectingDispatcher(Dispatcher):
    """Keep outbox rows intact when CLI maintenance has no broker configuration."""

    @override
    def task_name(self, fn: Callable[P, object]) -> str:
        return fn.__qualname__

    @override
    async def dispatch(self, messages: Sequence[Message]) -> None:
        if messages:
            raise ConfigurationError(_NO_BROKER)


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dsn", required=True, help="SQLAlchemy async PostgreSQL DSN")
    parser.add_argument("--schema", required=True, help="installation schema")


def build_parser() -> argparse.ArgumentParser:
    """Build the public CLI argument parser.

    Returns:
        Parser with all supported subcommands.
    """
    parser = argparse.ArgumentParser(prog="tallyho", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    migrate = commands.add_parser("migrate", help="install or upgrade the schema")
    _common(migrate)

    maintenance = commands.add_parser("maintenance", help="run leader maintenance")
    _common(maintenance)
    maintenance.add_argument(
        "--hook-module",
        action="append",
        default=[],
        help="module containing tx-hook registrations; repeatable",
    )
    maintenance.add_argument("--once", action="store_true", help="run one pass and exit")

    inspect = commands.add_parser("inspect", help="print a batch tree and progress")
    inspect.add_argument("target", help="batch UUID or kind:key")
    _common(inspect)
    return parser


def _required(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigurationError(_BAD_COMMAND)
    return value


def _parse(argv: Sequence[str] | None) -> _Command:
    namespace = build_parser().parse_args(argv)
    raw = cast("dict[str, object]", vars(namespace))
    command = _required(raw, "command")
    dsn = _required(raw, "dsn")
    schema = _required(raw, "schema")
    if command == "migrate":
        return _Migrate(dsn, schema)
    if command == "inspect":
        return _Inspect(dsn, schema, _required(raw, "target"))
    if command == "maintenance":
        modules = raw.get("hook_module")
        if not isinstance(modules, list):
            raise ConfigurationError(_BAD_COMMAND)
        hook_modules: list[str] = []
        for item in cast("list[object]", modules):
            if not isinstance(item, str):
                raise ConfigurationError(_BAD_COMMAND)
            hook_modules.append(item)
        once = raw.get("once")
        if not isinstance(once, bool):
            raise ConfigurationError(_BAD_COMMAND)
        return _Maintenance(dsn, schema, tuple(hook_modules), once)
    raise ConfigurationError(_BAD_COMMAND)


async def _resolve(client: Tallyho, target: str) -> BatchHandle:
    try:
        return client.handle(UUID(target))
    except ValueError:
        kind, separator, key = target.partition(":")
        if not separator or not kind or not key:
            raise ConfigurationError(_BAD_TARGET) from None
        return await client.find(kind, key)


def _progress(view: BatchView) -> str:
    progress = view.progress
    ratio = "?" if progress.ratio is None else f"{progress.ratio:.1%}"
    expected = "?" if progress.expected is None else str(progress.expected)
    return (
        f"state={view.state.name.lower()} done={progress.done}/{expected} "
        f"found={progress.found} queued={progress.queued} in_flight={progress.in_flight} "
        f"errors={progress.error} cancelled={progress.cancelled} progress={ratio}"
    )


def _render(view: BatchView, *, indent: str = "") -> list[str]:
    key = "" if view.key is None else f" key={view.key}"
    lines = [f"{indent}{view.kind}{key} id={view.id} {_progress(view)}"]
    for child_key in sorted(view.children):
        lines.extend(_render(view.children[child_key], indent=f"{indent}  "))
    return lines


async def serve_maintenance(runner: MaintenanceRunner) -> None:
    """Run maintenance until stopped, wiring SIGINT and SIGTERM when supported."""
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []
    for candidate in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(candidate, runner.stop)
        except (NotImplementedError, RuntimeError):
            continue
        registered.append(candidate)
    try:
        await runner.run()
    finally:
        for candidate in registered:
            _ = loop.remove_signal_handler(candidate)


async def run(argv: Sequence[str] | None = None) -> int:
    """Execute a parsed CLI command without terminating the process.

    Returns:
        Zero after a successful command.
    """
    command = _parse(argv)
    engine = create_async_engine(command.dsn)
    try:
        client = Tallyho(
            engine,
            schema=command.schema,
            hook_modules=command.hook_modules if isinstance(command, _Maintenance) else (),
        )
        if isinstance(command, _Migrate):
            version = await client.migrate()
            _ = sys.stdout.write(f"schema={command.schema} version={version}\n")
            return 0
        client.install(_RejectingDispatcher())
        if isinstance(command, _Inspect):
            view = await (await _resolve(client, command.target)).view()
            _ = sys.stdout.write("\n".join(_render(view)) + "\n")
            return 0
        if command.once:
            _ = await client.run_maintenance_once()
            _ = sys.stdout.write("maintenance pass complete\n")
            return 0
        await serve_maintenance(client.maintenance())
        return 0
    finally:
        await engine.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    """Run the console script; process exit lives in ``__main__`` only.

    Returns:
        Zero after a successful command.
    """
    args = tuple(sys.argv[1:] if argv is None else argv)
    if not args:
        build_parser().print_help()
        return 0
    return asyncio.run(run(args))
