"""Управление доставкой ``after_commit`` соединения пользователя в тестах."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import tallyho.storage.tx as tx_module

if TYPE_CHECKING:
    import pytest

    from tallyho.storage.tx import _Pending

__all__ = ["pause_commit_polling"]


def pause_commit_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Опрос COMMIT «ушёл в паузу»: соединение зарегистрировано, но не проверяется.

    Так ведёт себя опрос после первых миллисекунд на медленной машине: колбэки
    закоммиченной транзакции доставит только следующее обращение к соединению
    или :func:`tallyho.storage.tx.deliver_committed`.
    """
    polling = tx_module._polling  # ruff: ignore[private-member-access]  # тот же реестр, что у опроса

    def register_only(pending: _Pending) -> None:
        polling.setdefault(asyncio.get_running_loop(), set()).add(pending)

    monkeypatch.setattr(tx_module, "_deliver_later", register_only)
