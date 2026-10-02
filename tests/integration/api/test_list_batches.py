"""Листинг корневых батчей ``th.list_batches`` (A-AT-06, A-AT-07)."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest

from tallyho import Tallyho
from tallyho.model.errors import ConfigurationError, InvalidAttributesError
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Collection, Mapping

    from sqlalchemy.ext.asyncio import AsyncEngine

    from tallyho.model.views import BatchInfo

__all__: list[str] = []

START = datetime(2026, 10, 1, 9, tzinfo=UTC)
TENANT = UUID("11111111-2222-3333-4444-555555555555")


@asynccontextmanager
async def make_client(
    engine: AsyncEngine, schema: str
) -> AsyncGenerator[tuple[Tallyho, InlineBroker, FakeClock]]:
    clock = FakeClock(START)
    broker = InlineBroker(seed=3)
    th = Tallyho(engine, schema=schema, clock=clock)
    th.install(broker.adapter)
    _ = await th.migrate()
    try:
        yield th, broker, clock
    finally:
        await th.aclose()


async def noop(value: int) -> None:
    _ = value
    await asyncio.sleep(0)


async def create(  # ruff: ignore[too-many-positional-arguments]  # тестовый хелпер: kind, key, атрибуты
    th: Tallyho,
    kind: str,
    key: str,
    attributes: Mapping[str, object] | None = None,
    *,
    items: int = 0,
) -> UUID:
    async with th.batch(kind, key=key, attributes=attributes) as root:
        for index in range(items):
            await root.add(noop, index)
    return root.handle.id


async def everything(
    th: Tallyho, *, limit: int, kinds: Collection[str] | None = None
) -> list[UUID]:
    """Обойти все страницы и вернуть id в порядке выдачи."""
    found: list[UUID] = []
    cursor: str | None = None
    while True:
        page = await th.list_batches(kinds=kinds, limit=limit, cursor=cursor)
        found.extend(info.id for info in page.items)
        assert len(page.items) <= limit
        if page.next_cursor is None:
            return found
        cursor = page.next_cursor


def keys(items: Collection[BatchInfo]) -> list[str | None]:
    return [info.key for info in items]


async def test_only_roots_newest_first_with_light_dto(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, clock):
        first = await create(th, "mail", "a", {"tenant": TENANT, "n": 1})
        clock.advance(seconds=10)
        async with th.batch("import", key="b") as root:
            _ = root.sub_batch("pages")
        second = root.handle.id

        page = await th.list_batches()

    assert [info.id for info in page.items] == [second, first]
    assert page.next_cursor is None
    newest, oldest = page.items
    assert (newest.kind, newest.key, dict(newest.attributes)) == ("import", "b", {})
    assert (oldest.kind, oldest.key) == ("mail", "a")
    assert dict(oldest.attributes) == {"tenant": str(TENANT), "n": 1}
    assert oldest.created_at == START
    assert newest.created_at == START + timedelta(seconds=10)
    assert oldest.state is BatchState.SEALED
    assert oldest.finished_at is None


async def test_filters_combine(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, broker, clock):
        _ = await create(th, "mail", "m1", {"tenant": "acme", "dry_run": False}, items=1)
        clock.advance(seconds=10)
        _ = await create(th, "mail", "m2", {"tenant": "acme", "dry_run": True})
        clock.advance(seconds=10)
        _ = await create(th, "import", "i1", {"tenant": "acme"})
        clock.advance(seconds=10)
        _ = await create(th, "import", "i2", {"tenant": "other"}, items=1)
        _ = await broker.drain()
        _ = await th.run_maintenance_once()

        by_kind = await th.list_batches(kinds=["mail"])
        two_kinds = await th.list_batches(kinds=["mail", "import", "mail"])
        acme = await th.list_batches(attributes={"tenant": "acme"})
        acme_dry = await th.list_batches(attributes={"tenant": "acme", "dry_run": True})
        acme_import = await th.list_batches(kinds=["import"], attributes={"tenant": "acme"})
        succeeded = await th.list_batches(states={BatchState.SUCCEEDED})
        window = await th.list_batches(
            created_after=START + timedelta(seconds=10),
            created_before=START + timedelta(seconds=30),
        )
        nothing = await th.list_batches(kinds=["mail"], attributes={"tenant": "other"})

    assert keys(by_kind.items) == ["m2", "m1"]
    assert keys(two_kinds.items) == ["i2", "i1", "m2", "m1"]
    assert keys(acme.items) == ["i1", "m2", "m1"]
    assert keys(acme_dry.items) == ["m2"]
    assert keys(acme_import.items) == ["i1"]
    # Все четыре батча финализированы: пустые — сразу, с Item — после выполнения.
    assert keys(succeeded.items) == ["i2", "i1", "m2", "m1"]
    # Полуинтервал [after, before): m2 (t+10) и i1 (t+20), без i2 (t+30).
    assert keys(window.items) == ["i1", "m2"]
    assert nothing.items == ()


async def test_attribute_filter_is_strict_about_json_type(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock):
        _ = await create(th, "typed", "int", {"n": 1})
        _ = await create(th, "typed", "str", {"n": "1"})
        _ = await create(th, "typed", "bool", {"n": True})
        _ = await create(th, "typed", "uuid", {"n": TENANT})

        as_int = await th.list_batches(attributes={"n": 1})
        as_str = await th.list_batches(attributes={"n": "1"})
        as_bool = await th.list_batches(attributes={"n": True})
        as_uuid = await th.list_batches(attributes={"n": TENANT})
        as_uuid_text = await th.list_batches(attributes={"n": str(TENANT)})

    assert keys(as_int.items) == ["int"]
    assert keys(as_str.items) == ["str"]
    assert keys(as_bool.items) == ["bool"]
    assert keys(as_uuid.items) == ["uuid"]
    assert keys(as_uuid_text.items) == ["uuid"]


async def test_pagination_has_no_gaps_or_duplicates(engine: AsyncEngine, schema: str) -> None:
    async with make_client(engine, schema) as (th, _broker, clock):
        created: list[UUID] = []
        for index in range(7):
            created.append(await create(th, "page", f"k{index}"))
            clock.advance(seconds=1)
        _ = await create(th, "other", "x")

        assert await everything(th, limit=3, kinds=["page"]) == created[::-1]
        assert await everything(th, limit=7, kinds=["page"]) == created[::-1]
        assert await everything(th, limit=1, kinds=["page"]) == created[::-1]
        assert len(await everything(th, limit=1000)) == 8


async def test_batches_created_during_traversal_do_not_shift_pages(
    engine: AsyncEngine, schema: str
) -> None:
    async with make_client(engine, schema) as (th, _broker, clock):
        existing: list[UUID] = []
        for index in range(9):
            existing.append(await create(th, "live", f"old{index}"))
            clock.advance(seconds=1)

        seen: list[UUID] = []
        cursor: str | None = None
        counter = 0
        while True:
            page = await th.list_batches(kinds=["live"], limit=2, cursor=cursor)
            seen.extend(info.id for info in page.items)
            # Между страницами параллельно появляются новые батчи того же kind.
            fresh = await asyncio.gather(
                create(th, "live", f"new{counter}a"), create(th, "live", f"new{counter}b")
            )
            counter += 1
            clock.advance(seconds=1)
            assert not set(fresh) & set(seen)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

    # Каждый батч, существовавший на момент первого запроса, выдан ровно один раз.
    assert seen == existing[::-1]


@pytest.mark.parametrize(
    "arguments",
    [
        {"limit": 0},
        {"limit": 1001},
        {"limit": True},
        {"kinds": "mail"},
        {"kinds": []},
        {"kinds": [1]},
        {"states": [10]},
        {"states": BatchState.OPEN},
        {"cursor": "not-a-cursor"},
        {"cursor": "AAAA"},
        {"cursor": 5},
        {"created_after": datetime(2026, 1, 1)},  # ruff: ignore[call-datetime-without-tzinfo]  # проверяется отказ naive datetime
    ],
    ids=repr,
)
async def test_malformed_arguments_are_rejected(
    engine: AsyncEngine, schema: str, arguments: dict[str, object]
) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock):
        _ = await create(th, "mail", "a")
        with pytest.raises(ConfigurationError):
            _ = await th.list_batches(
                kinds=cast("Collection[str] | None", arguments.get("kinds")),
                states=cast("Collection[BatchState] | None", arguments.get("states")),
                created_after=cast("datetime | None", arguments.get("created_after")),
                limit=cast("int", arguments.get("limit", 100)),
                cursor=cast("str | None", arguments.get("cursor")),
            )


async def test_attribute_filter_uses_the_same_normalization(
    engine: AsyncEngine, schema: str
) -> None:
    async with make_client(engine, schema) as (th, _broker, _clock):
        with pytest.raises(InvalidAttributesError):
            _ = await th.list_batches(attributes={"price": 1.5})
        with pytest.raises(InvalidAttributesError):
            _ = await th.list_batches(attributes={"tallyho.kind": "x"})
        # Пустой фильтр — это отсутствие фильтра.
        assert (await th.list_batches(attributes={})).items == ()
