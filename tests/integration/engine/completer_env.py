"""Хелперы интеграционных тестов Completer: Items «у брокера» и Completer над схемой теста."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import delete, event, select, update
from typing_extensions import override

from tallyho.engine.completer import Completer, CompleterSettings, CompleterTriggers, ItemRef
from tallyho.engine.producer import RootSpec
from tallyho.model.calls import TaskCall
from tallyho.protocols.clock import SystemClock
from tallyho.storage.counters import CounterDelta, upsert_slots

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable
    from uuid import UUID

    from sqlalchemy import RowMapping
    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.protocols.clock import Clock
    from tests.integration.engine.conftest import Env

__all__ = [
    "COMPLETER_SLOT",
    "NOW",
    "RELAY_SLOT",
    "SETTINGS",
    "WORKER",
    "CommitCounter",
    "Finalized",
    "MovableClock",
    "RecordingRelay",
    "Seeded",
    "lease_row",
    "open_completer",
    "schema_engine",
    "seed",
    "set_batch",
]

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
WORKER = "worker-1"
COMPLETER_SLOT = 5
RELAY_SLOT = 6
SETTINGS = CompleterSettings(worker_id=WORKER, slot=COMPLETER_SLOT)


class MovableClock(SystemClock):
    """Часы теста: «сейчас» в SQL задаёт тест."""

    def __init__(self, now: datetime = NOW) -> None:
        self.value: datetime = now

    @override
    def now(self) -> datetime | None:
        return self.value


@dataclass
class Finalized:
    """Заглушка Finalizer: запоминает батчи, для которых звали ``try_finalize``."""

    calls: list[UUID] = field(default_factory=list["UUID"])

    async def try_finalize(self, batch_id: UUID) -> bool:
        self.calls.append(batch_id)
        return False


@dataclass
class RecordingRelay:
    """Заглушка Relay: запоминает батчи из fast-path ``kick``."""

    calls: list[list[UUID]] = field(default_factory=list[list["UUID"]])

    def kick(self, batch_ids: Iterable[UUID]) -> None:
        self.calls.append(list(batch_ids))


@dataclass(eq=False)
class CommitCounter:
    """Считает COMMIT на движке."""

    commits: int = 0

    def __call__(self, _conn: Connection) -> None:
        self.commits += 1


@dataclass(frozen=True, slots=True)
class Seeded:
    """Батч и его Items, «отправленные брокеру» (записей outbox нет)."""

    batch_id: UUID
    refs: list[ItemRef]


def schema_engine(env: Env) -> AsyncEngine:
    """Движок, у которого таблицы без схемы попадают в схему теста."""
    return env.engine.execution_options(schema_translate_map={None: env.schema})


async def seed(env: Env, n: int, *, kind: str = "mail") -> Seeded:
    """Корень с ``n`` Items; outbox очищен, ``dispatched += n``, как после relay."""
    async with env.transaction() as conn:
        root = await env.producer.create_root(conn, RootSpec(kind=kind))
        calls = [TaskCall(task_name="send", args=(i,), kwargs={}) for i in range(n)]
        _ = await env.producer.add_items(conn, root.id, calls)
        outbox = env.tables.outbox
        _ = await conn.execute(delete(outbox).where(outbox.c.batch_id == root.id))
        await upsert_slots(conn, env.tables, {(root.id, RELAY_SLOT): CounterDelta(dispatched=n)})
        item = env.tables.item
        ids = list(
            await conn.scalars(
                select(item.c.id).where(item.c.batch_id == root.id).order_by(item.c.id)
            )
        )
    return Seeded(batch_id=root.id, refs=[ItemRef(item_id, root.id) for item_id in ids])


async def set_batch(env: Env, batch_id: UUID, **values: object) -> None:
    """Изменить строку ``th_batch``."""
    batch = env.tables.batch
    async with env.transaction() as conn:
        _ = await conn.execute(update(batch).where(batch.c.id == batch_id).values(values))


async def lease_row(env: Env, item_id: UUID) -> RowMapping | None:
    """Строка ``th_lease`` Item или ``None``."""
    lease = env.tables.lease
    async with env.connection() as conn:
        result = await conn.execute(select(lease).where(lease.c.item_id == item_id))
        return result.mappings().one_or_none()


@contextlib.asynccontextmanager
async def open_completer(
    env: Env,
    *,
    clock: Clock | None = None,
    finalizer: Finalized | None = None,
    relay: RecordingRelay | None = None,
    counter: CommitCounter | None = None,
    settings: CompleterSettings = SETTINGS,
) -> AsyncGenerator[Completer]:
    """Completer над схемой теста; закрывается на выходе."""
    engine = schema_engine(env)
    if counter is not None:
        event.listen(engine.sync_engine, "commit", counter)
    completer = Completer(
        tables=env.tables,
        engine=engine,
        clock=clock or MovableClock(),
        settings=settings,
        triggers=CompleterTriggers(finalizer=finalizer, relay=relay),
    )
    try:
        yield completer
    finally:
        await completer.close()
        if counter is not None:
            event.remove(engine.sync_engine, "commit", counter)
