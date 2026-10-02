"""Журнал хаоса: что, когда и над чем сделал контроллер (артефакт прогона)."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

__all__ = ["ChaosJournal", "JournalEntry"]


@dataclass(frozen=True, slots=True)
class JournalEntry:
    """Одна запись журнала.

    Attributes:
        seq: Порядковый номер записи.
        t: Секунды от начала хаоса (отрицательные — подготовка стенда).
        wall: Время по часам хоста, UTC.
        event: Имя события (``kill_worker``, ``pg_started``, …).
        target: Сервис или прокси, над которым выполнено действие.
        detail: Числа и пояснения события.
    """

    seq: int
    t: float
    wall: str
    event: str
    target: str
    detail: Mapping[str, object]

    def to_json(self) -> str:
        """Вернуть запись одной строкой JSON."""
        return json.dumps(
            {
                "seq": self.seq,
                "t": self.t,
                "wall": self.wall,
                "event": self.event,
                "target": self.target,
                "detail": dict(self.detail),
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )


@dataclass(slots=True)
class ChaosJournal:
    """Журнал в памяти с дозаписью в JSONL-файл, чтобы он пережил падение прогона."""

    path: Path | None = None
    entries: list[JournalEntry] = field(default_factory=list[JournalEntry])
    _origin: float = field(default_factory=time.monotonic)

    def start(self) -> None:
        """Принять текущий момент за начало хаоса (``t = 0``)."""
        self._origin = time.monotonic()

    def elapsed(self) -> float:
        """Секунды от начала хаоса."""
        return time.monotonic() - self._origin

    def record(self, event: str, target: str = "", **detail: object) -> JournalEntry:
        """Добавить запись и сразу дописать её в файл."""
        entry = JournalEntry(
            seq=len(self.entries) + 1,
            t=round(self.elapsed(), 3),
            wall=datetime.now(UTC).isoformat(timespec="milliseconds"),
            event=event,
            target=target,
            detail=detail,
        )
        self.entries.append(entry)
        if self.path is not None:
            with self.path.open("a", encoding="utf-8", newline="\n") as stream:
                _ = stream.write(entry.to_json() + "\n")
        return entry

    def events(self, event: str) -> list[JournalEntry]:
        """Все записи с данным именем события."""
        return [entry for entry in self.entries if entry.event == event]
