"""Параметры прогона A-UC и контекст, который получает каждый сценарий."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from sqlalchemy import func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho.model.errors import BatchPurged
from tests.acceptance.chaos.runner import ARTIFACTS
from tests.acceptance.chaos.verdict import Expectation

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
    from datetime import datetime
    from pathlib import Path
    from uuid import UUID

    from sqlalchemy.sql.base import Executable

    from tallyho.model.views import BatchView
    from tests.acceptance.app.application import AcceptanceApp
    from tests.acceptance.app.site import CatalogGenerator
    from tests.acceptance.chaos.journal import ChaosJournal
    from tests.acceptance.chaos.load import Root
    from tests.acceptance.chaos.stand import Stand
    from tests.acceptance.oracle import PurgedBatch

__all__ = ["UC_IDS", "UcConfig", "UcContext", "UcError"]

UC_IDS = tuple(f"A-UC-{index:02}" for index in range(1, 23))


class UcError(Exception):
    """Сценарий A-UC не смог выполнить свой шаг (ошибка стенда или сценария)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class UcConfig:
    """Параметры ``poe acceptance-uc``.

    Attributes:
        seed: Seed данных, сети и инъекции ошибок.
        uc: ``A-UC-01`` … ``A-UC-22``.
        scale: Доля объёмов таблицы ACCEPTANCE §3.2 и §7 (1.0 - 20 000 счетов,
            0.1 - функциональный объём 2 000).
        threads: Параллельность одного воркера (``async_concurrency``).
        lease_ttl: ``lease_ttl`` стенда, секунды.
        heartbeat_every: ``heartbeat_every`` стенда: короче умолчания, чтобы
            ``item.progress`` успевал попасть в ``in_flight`` (A-UC-04).
        sweep_interval: ``sweep_interval`` стенда, секунды.
        keep_stand: Не удалять стенд после прогона.
        artifacts: Каталог артефактов.
    """

    seed: int
    uc: str
    scale: float = 0.1
    threads: int = 32
    lease_ttl: float = 60.0
    heartbeat_every: float = 2.0
    sweep_interval: float = 5.0
    keep_stand: bool = False
    artifacts: Path = ARTIFACTS / "uc"

    @property
    def name(self) -> str:
        """Каталог артефактов и суффикс compose-проекта."""
        return f"{self.uc}-seed{self.seed}".lower()

    @property
    def project(self) -> str:
        """Уникальное имя compose-проекта."""
        return f"tallyho-{self.name.replace('a-uc-', 'uc')}"

    def volume(self, base: int, floor: int = 10) -> int:
        """Объём сценария: ``base`` из ACCEPTANCE, умноженный на ``scale``."""
        return max(floor, round(base * self.scale))

    @property
    def pages(self) -> int:
        """Страниц каталога S3 (ACCEPTANCE: 50 на функциональном стенде)."""
        return self.volume(50, floor=3)


@dataclass(slots=True)
class UcContext:
    """Всё, чем сценарий управляет стендом и что он оставляет оракулу."""

    config: UcConfig
    stand: Stand
    app: AcceptanceApp
    journal: ChaosJournal
    generated: CatalogGenerator
    roots: list[Root] = field(default_factory=list["Root"])
    retry_failed: Counter[UUID] = field(default_factory=Counter["UUID"])
    purged: list[PurgedBatch] = field(default_factory=list["PurgedBatch"])
    frozen: dict[UUID, BatchView] = field(default_factory=dict["UUID", "BatchView"])
    truth: dict[UUID, dict[str, tuple[int, int]]] = field(
        default_factory=dict["UUID", dict[str, tuple[int, int]]]
    )
    expectations: list[Expectation] = field(default_factory=list[Expectation])
    stats: dict[str, int] = field(default_factory=dict[str, int])
    _runs: int = 0

    # ------------------------------------------------------------------ вердикт

    def expect(self, name: str, ok: bool, detail: str | tuple[str, ...] = "") -> bool:  # ruff: ignore[boolean-type-hint-positional-argument]  # результат проверки - позиционный по смыслу
        """Записать дополнительное ожидание сценария и вернуть его результат.

        ``detail`` - строка или её части (склеиваются без разделителя).
        """
        text = detail if isinstance(detail, str) else "".join(detail)
        self.expectations.append(Expectation(name, ok, text))
        self.journal.record("expectation", name=name, ok=ok, detail=text)
        return ok

    # ------------------------------------------------------------------ домен

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncSession]:
        """Транзакция пользователя на движке процесса нагрузки."""
        async with (
            AsyncSession(self.app.engine, expire_on_commit=False) as session,
            session.begin(),
        ):
            yield session

    def upcoming_run(self) -> int:
        """Номер, который получит следующий :meth:`new_run` (план задаётся заранее)."""
        return self._runs + 1

    async def new_run(self, session: AsyncSession, kind: str) -> int:
        """Строка ``uc_runs`` для нового корня; номер прогона внутри сценария."""
        self._runs += 1
        _ = await session.execute(insert(self.app.domain.uc_runs).values(id=self._runs, kind=kind))
        return self._runs

    async def bind_run(self, session: AsyncSession, run_id: int, batch_id: UUID) -> None:
        """Связать строку ``uc_runs`` с корнем в той же транзакции, что его создание."""
        runs = self.app.domain.uc_runs
        _ = await session.execute(update(runs).where(runs.c.id == run_id).values(batch_id=batch_id))

    async def plan(self, run_id: int, modes: Mapping[int, str]) -> None:
        """Назначить (или заменить) режим логическим Items прогона."""
        if not modes:
            return
        table = self.app.domain.uc_plan
        rows = [{"run_id": run_id, "n": n, "mode": mode} for n, mode in modes.items()]
        async with self.app.engine.begin() as connection:
            statement = pg_insert(table).values(rows)
            _ = await connection.execute(
                statement.on_conflict_do_update(
                    index_elements=[table.c.run_id, table.c.n],
                    set_={"mode": statement.excluded.mode},
                )
            )

    async def set_flag(self, name: str, value: int) -> None:
        """Переключатель домена, который читают задачи (``uc_flags``)."""
        table = self.app.domain.uc_flags
        statement = pg_insert(table).values(name=name, value=value)
        async with self.app.engine.begin() as connection:
            _ = await connection.execute(
                statement.on_conflict_do_update(
                    index_elements=[table.c.name], set_={"value": value}
                )
            )

    async def scalar(self, statement: Executable) -> object:
        """Одно значение запроса через соединение процесса нагрузки."""
        async with self.app.engine.connect() as connection:
            return cast("object", await connection.scalar(statement))

    async def count(self, statement: Executable) -> int:
        """Целое значение запроса (``count(*)``, ``sum``); ``NULL`` - ноль."""
        return int(cast("int", await self.scalar(statement)) or 0)

    async def db_now(self) -> datetime:
        """Часы PostgreSQL: start_at, дедлайны и lease считаются по ним."""
        return cast("datetime", await self.scalar(select(func.now())))

    # ------------------------------------------------------------------ ожидания

    async def wait_terminal(self, batch_id: UUID, timeout_seconds: float = 600) -> BatchView:
        """Дождаться терминального состояния батча.

        Raises:
            UcError: Батч не стал терминальным за ``timeout_seconds``.
        """
        handle = self.app.th.handle(batch_id)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            view = await handle.view()
            if view.state.is_terminal:
                return view
            await asyncio.sleep(1)
        message = f"батч {batch_id} не стал терминальным за {timeout_seconds} с"
        raise UcError(message)

    async def wait_for(
        self,
        predicate: Callable[[], Awaitable[bool]],
        timeout_seconds: float,
        *,
        what: str,
        interval: float = 0.5,
    ) -> float:
        """Ждать, пока асинхронный предикат не вернёт истину; секунды ожидания.

        Raises:
            UcError: Условие не наступило за ``timeout_seconds``.
        """
        started = time.monotonic()
        while time.monotonic() - started < timeout_seconds:
            if await predicate():
                return round(time.monotonic() - started, 3)
            await asyncio.sleep(interval)
        message = f"не дождались за {timeout_seconds} с: {what}"
        raise UcError(message)

    async def is_purged(self, batch_id: UUID) -> bool:
        """Удалён ли батч retention (``view`` бросает ``BatchPurged``)."""
        try:
            _ = await self.app.th.handle(batch_id).view()
        except BatchPurged:
            return True
        return False

    async def root_items_sql(
        self, sql: str, root_id: UUID | int, **params: object
    ) -> list[tuple[object, ...]]:
        """Сырые строки запроса со связанным ``:root`` (таблицы ``th.*``, ``app.*``)."""
        async with self.app.engine.connect() as connection:
            rows = (await connection.execute(text(sql), {"root": root_id, **params})).all()
        return [tuple(row) for row in rows]
