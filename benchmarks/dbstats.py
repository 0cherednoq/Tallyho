"""Наблюдение за PostgreSQL во время нагрузки: ожидания блокировок, WAL, мёртвые строки."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from benchmarks.prose import prose

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = ["WATCHED_TABLES", "LockSampler", "TableSample", "TableSampler"]

WATCHED_TABLES: Final = ("th_counter", "th_lease", "th_outbox", "th_counter_delta", "th_item")
_PGSTATTUPLE_TABLES: Final = ("th_counter", "th_lease")

_LOCKS = text(
    """
    SELECT
        count(*) FILTER (WHERE wait_event_type = 'Lock' AND position(:schema IN query) > 0),
        count(*)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND backend_type = 'client backend'
      AND state NOT IN ('idle')
      AND pid <> pg_backend_pid()
    """
)


@dataclass(slots=True)
class LockSampler:
    """Доля времени активных backend'ов, проведённого в ожидании блокировок на таблицах tallyho.

    Раз в ``interval`` секунд считается, сколько не-idle backend'ов БД ждут ``Lock`` в запросе,
    который обращается к схеме tallyho, и сколько не-idle backend'ов всего.
    """

    engine: AsyncEngine
    schema: str
    interval: float = 0.1
    waiting: int = 0
    busy: int = 0
    samples: int = 0
    _task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Начать опрос."""
        self._task = asyncio.create_task(self._loop(), name="bench-lock-sampler")

    async def _loop(self) -> None:
        async with self.engine.connect() as connection:
            while True:
                row = (await connection.execute(_LOCKS, {"schema": self.schema})).one()
                await connection.commit()
                self.waiting += int(cast("int", row[0]))
                self.busy += int(cast("int", row[1]))
                self.samples += 1
                await asyncio.sleep(self.interval)

    async def stop(self) -> None:
        """Остановить опрос."""
        if self._task is not None:
            _ = self._task.cancel()
            _ = await asyncio.wait([self._task])
            self._task = None

    @property
    def share(self) -> float:
        """Доля backend-времени в ожидании блокировок tallyho."""
        return self.waiting / self.busy if self.busy else 0.0


@dataclass(frozen=True, slots=True)
class TableSample:
    """Срез одной таблицы tallyho."""

    at: float
    table: str
    live: int
    dead: int
    autovacuums: int
    size_bytes: int
    dead_tuple_percent: float | None
    free_percent: float | None


@dataclass(slots=True)
class TableSampler:
    """WAL и состояние таблиц tallyho раз в ``interval`` секунд (P-10).

    ``pgstattuple`` (расширение contrib) — для ``th_counter``/``th_lease``, если доступно;
    иначе только ``pg_stat_user_tables``.
    """

    engine: AsyncEngine
    schema: str
    interval: float
    pgstattuple: bool = True
    wal: list[tuple[float, int]] = field(default_factory=list[tuple[float, int]])
    tables: list[TableSample] = field(default_factory=list[TableSample])
    pgstattuple_error: str | None = None
    _task: asyncio.Task[None] | None = None

    async def start(self, origin: float) -> None:
        """Подключить ``pgstattuple`` (если можно) и начать опрос."""
        if self.pgstattuple:
            try:
                async with self.engine.begin() as connection:
                    _ = await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pgstattuple"))
            except DBAPIError as exc:
                self.pgstattuple = False
                self.pgstattuple_error = str(exc.orig)
        await self.sample(origin)
        self._task = asyncio.create_task(self._loop(origin), name="bench-table-sampler")

    async def _loop(self, origin: float) -> None:
        while True:
            await asyncio.sleep(self.interval)
            await self.sample(origin)

    async def sample(self, origin: float) -> None:
        """Один срез."""
        at = time.monotonic() - origin
        async with self.engine.connect() as connection:
            _ = await connection.execute(text("SELECT pg_stat_clear_snapshot()"))
            lsn = cast(
                "int",
                await connection.scalar(
                    text("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
                ),
            )
            self.wal.append((at, lsn))
            rows = await connection.execute(
                text(
                    prose(
                        """
                        SELECT relname, n_live_tup, n_dead_tup, autovacuum_count,
                        pg_total_relation_size(relid) FROM pg_stat_user_tables WHERE schemaname =
                        :schema AND relname = ANY(:tables)
                        """
                    )
                ),
                {"schema": self.schema, "tables": list(WATCHED_TABLES)},
            )
            for row in rows.all():
                name = cast("str", row[0])
                tuple_stats = await self._pgstattuple(connection, name)
                self.tables.append(
                    TableSample(
                        at=at,
                        table=name,
                        live=cast("int", row[1]),
                        dead=cast("int", row[2]),
                        autovacuums=cast("int", row[3]),
                        size_bytes=cast("int", row[4]),
                        dead_tuple_percent=tuple_stats[0],
                        free_percent=tuple_stats[1],
                    )
                )
            await connection.commit()

    async def _pgstattuple(
        self, connection: AsyncConnection, table: str
    ) -> tuple[float | None, float | None]:
        if not self.pgstattuple or table not in _PGSTATTUPLE_TABLES:
            return None, None
        row = (
            await connection.execute(
                text("SELECT dead_tuple_percent, free_percent FROM pgstattuple(:relation)"),
                {"relation": f'"{self.schema}".{table}'},
            )
        ).one()
        return float(cast("float", row[0])), float(cast("float", row[1]))

    async def stop(self, origin: float) -> None:
        """Остановить опрос и снять последний срез."""
        if self._task is not None:
            _ = self._task.cancel()
            _ = await asyncio.wait([self._task])
            self._task = None
        await self.sample(origin)

    def series(self, table: str) -> list[TableSample]:
        """Срезы одной таблицы по времени.

        Returns:
            Срезы в порядке снятия.
        """
        return [sample for sample in self.tables if sample.table == table]
