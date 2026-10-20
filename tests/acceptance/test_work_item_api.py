"""A-WI: API единицы работы на сквозных сценариях (RM8.10, ACCEPTANCE §7.5).

Два сценария:

* **A-WI-01** — тот же domain-checker, что в A-RM-01 (20 доменов, ёмкость волнами
  5 + 3 + 0 и финальное пополнение), но на новом API: итог значением, судьба
  исключения при регистрации, ``WorkItem`` из контейнера Dishka и пробуждение
  отложенного вместо короткого ``delay``. Числовой оракул тот же: 0 сгоревших
  ретраев, 20 платных вызовов, 20 из 20.
* **A-WI-02** — пачка из 50 элементов с закладкой на каждом: попытка обрывается
  после 30-го, новая продолжает с 31-го, в доменной таблице ровно 50 строк.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, final

from dishka import (  # ruff: ignore[typing-only-third-party-import]  # dishka читает аннотации в рантайме
    BaseScope,
    Provider,
    Scope,
    make_async_container,
    provide,  # pyright: ignore[reportUnknownVariableType]  # decorator без аннотаций в dishka
)
from sqlalchemy import Column, Integer, MetaData, String, Table, TypedColumns, func, insert, select

from tallyho import Defer, Ok, Tallyho, item
from tallyho.integrations.dishka import (  # ruff: ignore[typing-only-first-party-import]  # dishka читает аннотации в рантайме
    DishkaItemScope,
    FromDishka,
    TallyhoProvider,
    inject,
)
from tallyho.model.states import BatchState
from tallyho.storage.tables import build_metadata
from tallyho.testing import FakeClock, InlineBroker
from tests.helpers.db import schema_transaction

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__: list[str] = []

DOMAINS = 20
CHUNK = 50
KILL_AFTER = 30
DEFER_DELAY = timedelta(minutes=5)
"""Срок отложения заведомо больше прогона: работу будит release_deferred, а не время."""


class NoCheckerAccountError(RuntimeError):
    """Доменное исключение приложения: свободного аккаунта нет."""

    def __init__(self, reason: str = "no_accounts") -> None:
        super().__init__(reason)
        self.reason: str = reason


class ChunkFailedError(RuntimeError):
    """Обрыв попытки посреди пачки: воркер «умер»."""


@dataclass
class ResourcePool:
    """Пул кредитов дефицитного ресурса с учётом платных вызовов и параллелизма."""

    available: int = 0
    paid: int = 0
    refused: int = 0
    concurrent: int = 0
    max_concurrent: int = 0

    def reset(self) -> None:
        """Обнулить пул перед сценарием."""
        self.available = self.paid = self.refused = 0
        self.concurrent = self.max_concurrent = 0

    def lease(self) -> None:
        """Снять кредит или отказать доменным исключением.

        Raises:
            NoCheckerAccountError: свободных кредитов нет.
        """
        if self.available <= 0:
            self.refused += 1
            raise NoCheckerAccountError
        self.available -= 1


@dataclass
class CheckDomain:
    """Интерактор приложения: tallyho он не импортирует."""

    pool: ResourcePool

    async def __call__(self, domain: int) -> int:
        """Проверить один домен за кредит ресурса.

        Returns:
            Проверенный домен.
        """
        await asyncio.sleep(0)
        self.pool.lease()
        self.pool.concurrent += 1
        self.pool.max_concurrent = max(self.pool.max_concurrent, self.pool.concurrent)
        self.pool.paid += 1
        self.pool.concurrent -= 1
        return domain


@final
class AppProvider(Provider):
    """Интерактор приложения в REQUEST-scope."""

    scope: BaseScope | None = Scope.REQUEST

    def __init__(self, interactor: CheckDomain) -> None:
        """Запомнить готовый интерактор сценария."""
        super().__init__()
        self._interactor: CheckDomain = interactor

    @provide
    def checker(self) -> CheckDomain:
        """Отдать интерактор задаче.

        Returns:
            Интерактор сценария.
        """
        return self._interactor


@final
class _CheckColumns(TypedColumns):
    """Колонки доменной таблицы: один элемент — одна строка."""

    position = Column(Integer(), primary_key=True)
    verdict = Column(String(), nullable=False)


@dataclass
class _Stand:
    """Стенд сценария, который видит задача брокера."""

    engine: AsyncEngine
    schema: str
    table: Table[_CheckColumns] | None = None
    attempts: list[int] = field(default_factory=list[int])
    killed: bool = False


_POOL = ResourcePool()
_INTERACTOR = CheckDomain(_POOL)
_stand: _Stand | None = None


@inject
async def check_domain(domain: int, checker: FromDishka[CheckDomain]) -> Ok:
    """Задача сценария A-WI-01: итог значением, ресурсное исключение — наружу.

    Returns:
        Успешный итог с проверенным доменом.
    """
    return Ok("checked", result={"domain": await checker(domain)})


async def check_chunk(run: int) -> Ok:
    """Задача сценария A-WI-02: пачка элементов с закладкой на каждом.

    Returns:
        Успешный итог пачки.

    Raises:
        ChunkFailedError: имитация обрыва попытки после ``KILL_AFTER`` элементов.
    """
    del run
    stand = _stand
    assert stand is not None
    assert stand.table is not None
    work = item.require()
    start = work.checkpoint() or 0
    stand.attempts.append(start)
    for position in range(start, CHUNK):
        if position == KILL_AFTER and not stand.killed:
            stand.killed = True
            raise ChunkFailedError
        async with schema_transaction(stand.engine, stand.schema) as conn:
            _ = await conn.execute(insert(stand.table).values(position=position, verdict="ok"))
            await work.checkpoint_in(conn, position + 1)
    return Ok("chunk")


async def _max_attempt(engine: AsyncEngine, schema: str) -> int:
    tables = build_metadata()
    translated = engine.execution_options(schema_translate_map={None: schema})
    async with translated.connect() as conn:
        return int(await conn.scalar(select(func.max(tables.item.c.attempt))) or 0)


async def _count(engine: AsyncEngine, schema: str, table: Table[TypedColumns]) -> int:
    translated = engine.execution_options(schema_translate_map={None: schema})
    async with translated.connect() as conn:
        return int(await conn.scalar(select(func.count()).select_from(table)) or 0)


async def _deferred(engine: AsyncEngine, schema: str) -> int:
    tables = build_metadata()
    translated = engine.execution_options(schema_translate_map={None: schema})
    async with translated.connect() as conn:
        value = await conn.scalar(
            select(func.count())
            .select_from(tables.outbox)
            .where(tables.outbox.c.defer_reason.is_not(None))
        )
        return int(value or 0)


async def test_a_wi_01_resource_shortage_on_the_new_api(engine: AsyncEngine, schema: str) -> None:
    _POOL.reset()
    clock = FakeClock(datetime(2026, 11, 1, 9, tzinfo=UTC))
    broker = InlineBroker(seed=7)
    broker.on_exception(
        check_domain,
        defer_on={NoCheckerAccountError: _to_defer},
    )
    container = make_async_container(AppProvider(_INTERACTOR), TallyhoProvider())
    th = Tallyho(engine, schema=schema, clock=clock, middleware=[DishkaItemScope(container)])
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        # Окно 1: не больше одного запроса с дефицитного ресурса одновременно.
        async with th.batch("domain-check", key="batch:1", max_in_flight=1) as root:
            await root.map(check_domain, range(DOMAINS))
        handle = root.handle

        for grant in (5, 3, 0, DOMAINS):
            _POOL.available += grant
            _ = await broker.drain()
            # Ресурс появился — будим отложенное, не дожидаясь его срока: без
            # этого волна простояла бы до истечения DEFER_DELAY.
            _ = await handle.release_deferred(reasons=["no_accounts"])
            _ = await broker.drain()

        view = await handle.view()
        max_attempt = await _max_attempt(engine, schema)
        deferred = await _deferred(engine, schema)
    finally:
        await th.aclose()
        await container.close()

    # Тот же числовой оракул, что у A-RM-01.
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == DOMAINS
    assert view.progress.error == 0
    assert view.progress.deferred == 0
    assert deferred == 0
    assert _POOL.paid == DOMAINS
    assert _POOL.max_concurrent == 1
    assert max_attempt == 0  # ни одного сгоревшего ретрая
    assert _POOL.refused > 0  # работа действительно ждала ресурса


def _to_defer(exc: BaseException) -> Defer:
    reason = exc.reason if isinstance(exc, NoCheckerAccountError) else "no_accounts"
    return Defer(reason=reason, delay=DEFER_DELAY)


async def test_a_wi_02_chunk_resumes_from_its_checkpoint(engine: AsyncEngine, schema: str) -> None:
    global _stand  # ruff: ignore[global-statement]  # задача брокера видит стенд через модуль
    table = Table("domain_checks", MetaData(schema=schema), _CheckColumns)
    async with engine.begin() as conn:
        await conn.run_sync(table.create)
    _stand = _Stand(engine=engine, schema=schema, table=table)

    clock = FakeClock(datetime(2026, 11, 2, 9, tzinfo=UTC))
    broker = InlineBroker(seed=11, max_retries=1)
    th = Tallyho(engine, schema=schema, clock=clock)
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        async with th.batch("chunked", key="chunk:1") as root:
            await root.add(check_chunk, 1)
        _ = await broker.drain()
        view = await root.handle.view()
    finally:
        await th.aclose()

    tables = build_metadata()
    translated = engine.execution_options(schema_translate_map={None: schema})
    async with translated.connect() as conn:
        positions = sorted(await conn.scalars(select(table.c.position)))
    marks = await _count(engine, schema, tables.checkpoint)

    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == 1
    # Новая попытка продолжила с 31-го элемента, а не начала заново.
    assert _stand.attempts == [0, KILL_AFTER]
    # Ровно 50 строк и ни одной повторной: результат и закладка коммитились вместе.
    assert positions == list(range(CHUNK))
    # Итог ok убрал закладку.
    assert marks == 0
