"""Fake HTTP mail provider with a durable-in-process call journal."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import cast

from aiohttp import web

__all__ = ["FakeMailProvider", "MailCall"]


@dataclass(frozen=True, slots=True)
class MailCall:
    """One provider request, including repeated delivery attempts."""

    item_id: str
    address: str
    status: int


class FakeMailProvider:
    """Local HTTP provider used by subprocess workers on the acceptance stand."""

    def __init__(self) -> None:
        self.calls: list[MailCall] = []
        self._attempts: dict[str, int] = {}
        self._runner: web.AppRunner | None = None
        self.base_url: str | None = None

    async def start(self, *, host: str = "127.0.0.1", port: int | None = None) -> str:
        """Start on loopback and return the provider URL."""
        app = web.Application()
        app.router.add_post("/send", self._send)
        app.router.add_get("/journal", self._journal)
        runner = web.AppRunner(app)
        await runner.setup()
        selected_port = _free_port() if port is None else port
        await web.TCPSite(runner, host, selected_port).start()
        self._runner = runner
        self.base_url = f"http://{host}:{selected_port}"
        return self.base_url

    async def close(self) -> None:
        """Stop the provider."""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _send(self, request: web.Request) -> web.Response:
        body = cast("dict[str, object]", await request.json())
        item_id = str(body["item_id"])
        address = str(body["address"])
        attempt = self._attempts.get(item_id, 0)
        self._attempts[item_id] = attempt + 1
        if address.startswith("retry-") and attempt == 0:
            status = 503
        elif address.startswith("reject-"):
            status = 422
        else:
            status = 202
        self.calls.append(MailCall(item_id=item_id, address=address, status=status))
        return web.json_response({"message_id": f"mail-{item_id}-{attempt}"}, status=status)

    async def _journal(self, _request: web.Request) -> web.Response:
        return web.json_response(
            [
                {"item_id": call.item_id, "address": call.address, "status": call.status}
                for call in self.calls
            ]
        )


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        address = cast("tuple[str, int]", listener.getsockname())
        return address[1]
