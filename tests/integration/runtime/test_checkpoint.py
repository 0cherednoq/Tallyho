"""Закладка составной единицы работы: claim, запись, итог (RM8.5, UC-19)."""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import delete, event, func, insert, select, update
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.model.errors import ConfigurationError, LeaseLostError
from tallyho.model.outcomes import Error, Ok
from tallyho.model.states import ItemState
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import lease_row, open_completer, seed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from sqlalchemy import RowMapping

    from tallyho.engine.completer import Completer, ItemRef
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class Broker(Runtime):
    verdict: Verdict = Verdict.FINAL

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        return self.verdict

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)


def _runtime(completer: Completer, broker: Broker | None = None) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=broker or Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=timedelta(seconds=20),
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


async def _checkpoint(env: Env, item_id: UUID) -> RowMapping | None:
    table = env.tables.checkpoint
    async with env.connection() as conn:
        result = await conn.execute(select(table).where(table.c.item_id == item_id))
        return result.mappings().one_or_none()


async def _item_row(env: Env, item_id: UUID) -> RowMapping:
    table = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(table).where(table.c.id == item_id))).mappings().one()


async def test_checkpoint_is_written_with_domain_data_and_read_by_next_claim(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    seen: list[int | None] = []
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Error:
            work = item.require()
            seen.append(work.checkpoint())
            start = work.checkpoint() or 0
            for index in range(start, start + 2):
                async with env.transaction() as conn:
                    await insert_id(conn, probe, index)
                    await work.checkpoint_in(conn, index + 1)
            # Ошибка оставляет закладку: следующая попытка продолжит (D-085).
            return Error("частично")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

        row = await _checkpoint(env, ref.id)
        assert row is not None
        assert (row["position"], row["batch_id"]) == (2, ref.batch_id)

        # Повтор упавшего Item: closed-loop проверка, что claim отдаёт закладку.
        async with env.transaction() as conn:
            _ = await conn.execute(
                update(env.tables.item)
                .where(env.tables.item.c.id == ref.id)
                .values(state=int(ItemState.ACTIVE), finished_at=None)
            )
            _ = await conn.execute(
                delete(env.tables.outbox).where(env.tables.outbox.c.item_id == ref.id)
            )
        claim = await completer.claim(ref)
        assert claim.run
        assert (claim.checkpoint, claim.defers) == (2, 0)
        assert await completer.release(ref, attempt=claim.attempt)

    assert seen == [None]
    assert await committed_ids(env.engine, probe) == [0, 1]


async def test_checkpoint_moves_lease_progress(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 30)
            lease = await lease_row(env, ref.id)
            assert lease is not None
            # handle.in_flight() показывает «30 из N» без отдельного item.progress.
            assert lease["progress_done"] == 30
            return Ok("chunk")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))


async def test_rollback_keeps_checkpoint_and_snapshot(env: Env) -> None:
    # A-WI-03: откат транзакции приложения оставляет закладку прежней.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    probe = await create_probe(env.engine, env.schema)
    snapshots: list[int | None] = []
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 5)
            snapshots.append(work.checkpoint())
            async with env.connection() as conn:
                trans = await conn.begin()
                await insert_id(conn, probe, 1)
                await work.checkpoint_in(conn, 9)
                await trans.rollback()
            snapshots.append(work.checkpoint())
            return Ok("chunk")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    assert snapshots == [5, 5]
    assert await committed_ids(env.engine, probe) == []


async def test_checkpoint_is_monotonic(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 7)
            async with env.transaction() as conn:
                # Повтор с меньшим значением безвреден: закладка не откатывается.
                await work.checkpoint_in(conn, 3)
            return Ok("chunk")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    # Итог ok удалил строку, но монотонность видна по прогрессу lease до итога.
    assert await _checkpoint(env, ref.id) is None


@pytest.mark.parametrize("position", [-1, True])
async def test_checkpoint_rejects_invalid_position(env: Env, position: int) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> None:
            work = item.require()
            async with env.transaction() as conn:
                with pytest.raises(ConfigurationError, match="position"):
                    await work.checkpoint_in(conn, position)

        await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    assert await _checkpoint(env, ref.id) is None


async def test_foreign_attempt_gets_lease_lost(env: Env) -> None:
    # A-WI-04: чужая попытка получает LeaseLostError, закладка не меняется.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> None:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 4)
            # Lease перехвачен другим исполнителем: попытка больше не владеет Item.
            async with env.transaction() as conn:
                _ = await conn.execute(
                    update(env.tables.lease)
                    .where(env.tables.lease.c.item_id == ref.id)
                    .values(worker_id="другой", attempt=work.attempt + 1)
                )
            async with env.connection() as conn:
                trans = await conn.begin()
                with pytest.raises(LeaseLostError):
                    await work.checkpoint_in(conn, 8)
                await trans.rollback()

        await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    row = await _checkpoint(env, ref.id)
    assert row is not None
    assert row["position"] == 4
    # Чужую строку lease попытка тоже не тронула.
    lease = await lease_row(env, ref.id)
    assert lease is not None
    assert lease["worker_id"] == "другой"


async def test_terminal_outcome_removes_checkpoint(env: Env) -> None:
    # A-WI-11: после итога ok строки закладки нет.
    seeded = await seed(env, 2)
    finished, skipped = seeded.refs
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 50)
            return Ok("chunk")

        async def skipping(**_kwargs: object) -> None:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 3)
            item.skip("duplicate")

        runtime = _runtime(completer)
        _ = await runtime.wrap(chunk)(_th=_marker(finished))
        await runtime.wrap(skipping)(_th=_marker(skipped))

    assert await _checkpoint(env, finished.id) is None
    assert await _checkpoint(env, skipped.id) is None
    assert await env.count(env.tables.checkpoint) == 0


async def test_error_outcome_keeps_checkpoint(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Error:
            work = item.require()
            async with env.transaction() as conn:
                await work.checkpoint_in(conn, 30)
            return Error("частично")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    row = await _checkpoint(env, ref.id)
    assert row is not None
    assert row["position"] == 30


async def test_item_without_checkpoint_is_updated_once(env: Env) -> None:
    # A-WI-10: у Item без закладки число UPDATE th_item за жизнь не меняется.
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            assert item.checkpoint() is None
            return Ok("chunk")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    assert await env.count(env.tables.checkpoint) == 0
    row = await _item_row(env, ref.id)
    assert ItemState(row["state"]) is ItemState.OK


async def test_checkpoint_does_not_update_the_item_row(env: Env) -> None:
    # A-WI-11: пачка из 50 элементов — th_item обновлён один раз (finish, HOT).
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    item_table = env.tables.item
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            work = item.require()
            for index in range(50):
                async with env.transaction() as conn:
                    await work.checkpoint_in(conn, index + 1)
            async with env.connection() as conn:
                tuples = await conn.scalar(
                    select(func.count()).select_from(item_table).where(item_table.c.id == ref.id)
                )
            assert tuples == 1
            return Ok("chunk")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    # Закладка писалась 50 раз, но итоговая строка Item обновлена один раз.
    row = await _item_row(env, ref.id)
    assert (ItemState(row["state"]), row["attempt"]) == (ItemState.OK, 0)
    assert await env.count(env.tables.checkpoint) == 0


async def test_claim_reads_defers_into_work_item(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with env.transaction() as conn:
        _ = await conn.execute(
            update(env.tables.item).where(env.tables.item.c.id == ref.id).values(defers=3)
        )
    seen: list[int] = []
    async with open_completer(env) as completer:

        async def chunk(**_kwargs: object) -> Ok:
            await asyncio.sleep(0)
            seen.append(item.require().defers)
            return Ok("chunk")

        _ = await _runtime(completer).wrap(chunk)(_th=_marker(ref))

    assert seen == [3]


async def test_claim_statement_count_does_not_grow_with_checkpoint(env: Env) -> None:
    # A-WI-09: закладка читается тем же запросом claim, второго обращения нет.
    seeded = await seed(env, 2)
    plain, with_mark = seeded.refs
    async with env.transaction() as conn:
        _ = await conn.execute(
            insert(env.tables.checkpoint).values(
                item_id=with_mark.id,
                batch_id=with_mark.batch_id,
                position=11,
                updated_at=func.now(),
            )
        )

    async with open_completer(env) as completer:
        statements: list[str] = []
        engine = completer.engine.sync_engine

        def record(
            _conn: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", record)
        try:
            first = await completer.claim(plain)
            without = len(statements)
            statements.clear()
            second = await completer.claim(with_mark)
            withal = len(statements)
        finally:
            event.remove(engine, "before_cursor_execute", record)

        assert (first.checkpoint, second.checkpoint) == (None, 11)
        assert withal == without
        assert any("th_checkpoint" in statement for statement in statements)
        assert await completer.release(plain, attempt=first.attempt)
        assert await completer.release(with_mark, attempt=second.attempt)
