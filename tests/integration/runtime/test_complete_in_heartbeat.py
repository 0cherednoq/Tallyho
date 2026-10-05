"""Путь B с настоящим claim и heartbeat; удержанные Items Completer (Fix-20).

A-DB-06/07 вызывают ``complete_in`` без claim, поэтому не видят ни строки
``th_lease``, которую продлевает heartbeat, ни словаря ``Completer.held``.
Здесь задача идёт через обёртку ``tracked``: claim, heartbeat с малым
интервалом, ``item.complete_in`` в транзакции пользователя.
"""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import starmap
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

from tallyho.engine.completer import CompleterSettings, FinishResult
from tallyho.engine.spawn import TreeCache
from tallyho.engine.sweeper import Sweeper, SweeperSettings
from tallyho.model.states import ItemState, ResultClass
from tallyho.protocols.broker import DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tests.helpers.db import backend_pid, blocked_by
from tests.helpers.probe import committed_ids, create_probe, insert_id
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import (
    COMPLETER_SLOT,
    NOW,
    SETTINGS,
    Finalized,
    MovableClock,
    lease_row,
    open_completer,
    schema_engine,
    seed,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from uuid import UUID

    from sqlalchemy import RowMapping, Table

    from tallyho.engine.completer import Completer, ItemRef
    from tests.helpers.probe import ProbeColumns
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

LATER = NOW + SETTINGS.lease_ttl + timedelta(seconds=1)
"""Момент, когда lease, взятый в ``NOW``, уже истёк."""
OTHER = CompleterSettings(worker_id="worker-2", slot=COMPLETER_SLOT + 1)
BEAT = timedelta(milliseconds=40)
"""Малый ``heartbeat_every``: за секунду задача успевает продлить lease десятки раз."""
STUCK = 30.0
"""Страховка от зависшего теста, а не порог скорости: столько ждать события не нужно."""


@dataclass
class Broker(Runtime):
    """Брокер теста: ошибка задачи — финальная."""

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        return Verdict.FINAL

    @override
    async def reconcile_dead(self, since: str | None) -> DeadLetters:
        return DeadLetters((), since)


def _runtime(completer: Completer, *, beat: timedelta = BEAT) -> TaskRuntime:
    return TaskRuntime(
        completer=completer,
        broker=Broker(),
        dispatcher=RecordingDispatcher(),
        tree_cache=TreeCache(),
        heartbeat_every=beat,
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


@pytest.fixture
async def probe(env: Env) -> Table[ProbeColumns]:
    """Доменная таблица пользователя."""
    return await create_probe(env.engine, env.schema)


@pytest.fixture
async def thief(env: Env) -> AsyncGenerator[Completer]:
    """Completer другого воркера, для которого lease из ``NOW`` уже истёк."""
    async with open_completer(env, clock=MovableClock(LATER), settings=OTHER) as completer:
        yield completer


async def _state(env: Env, item_id: UUID) -> ItemState:
    async with env.connection() as conn:
        value = await conn.scalar(
            env.tables.item.select()
            .with_only_columns(env.tables.item.c.state)
            .where(env.tables.item.c.id == item_id)
        )
    assert value is not None
    return ItemState(value)


# --- (1) Completer.held -----------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.timeout(600)
async def test_held_is_empty_after_many_items_completed_in_user_transactions(env: Env) -> None:
    total = 10_000
    seeded = await seed(env, total)
    parallel = 32
    gate = asyncio.Semaphore(parallel)
    users = create_async_engine(
        env.engine.url, pool_size=parallel, max_overflow=0
    ).execution_options(schema_translate_map={None: env.schema})
    async with open_completer(env) as completer:
        runtime = _runtime(completer, beat=timedelta(seconds=30))

        async def task(**_kwargs: object) -> None:
            async with users.begin() as conn:
                item.ok()
                await item.complete_in(conn)

        async def run(ref: ItemRef) -> None:
            async with gate:
                await runtime.wrap(task)(_th=_marker(ref))

        try:
            _ = await asyncio.gather(*(run(ref) for ref in seeded.refs))
        finally:
            await users.dispose()
        await completer.settled()
        assert completer.held == frozenset()

    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (total, 0)
    assert await env.count(env.tables.lease) == 0


async def test_held_forgets_item_lost_in_user_transaction(env: Env, thief: Completer) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    held: list[bool] = []
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            held.append(ref.id in completer.held)
            held.append((await thief.claim(ref)).run)
            async with env.transaction() as conn:
                await item.complete_in(conn)

        # LeaseLostError — не ошибка задачи: обёртка завершает попытку тихо.
        await _runtime(completer, beat=timedelta(seconds=30)).wrap(task)(_th=_marker(ref))
        assert held == [True, True]
        assert completer.held == frozenset()


async def test_held_forgets_item_whose_path_a_attempt_lost_lease(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> str:
            sweeper = Sweeper(
                tables=env.tables,
                engine=schema_engine(env),
                clock=MovableClock(LATER),
                finalizer=Finalized(),
                settings=SweeperSettings(finalize_grace=timedelta(0)),
            )
            assert await sweeper.expire_leases() == 1
            return "done"

        assert (
            await _runtime(completer, beat=timedelta(seconds=30)).wrap(task)(_th=_marker(ref))
            == "done"
        )
        assert completer.held == frozenset()
    assert await _state(env, ref.id) is ItemState.ERROR


# --- (2) heartbeat против долгой транзакции пути B --------------------------------------


@dataclass(eq=False)
class SlowBeats:
    """Подтверждённые heartbeat медленного Item после ``complete_in``.

    Обёртка над ``Completer.heartbeat``: считает только вызовы, начатые после
    ``complete_in`` и завершённые ``True``, то есть закоммиченные групповой
    транзакцией, пока транзакция пользователя открыта.
    """

    completer: Completer
    item_id: UUID
    completed: asyncio.Event
    need: int = 2
    count: int = 0
    enough: asyncio.Event = field(default_factory=asyncio.Event)

    async def __call__(
        self,
        ref: ItemRef,
        *,
        attempt: int | None = None,
        progress_done: int | None = None,
        progress_total: int | None = None,
    ) -> bool:
        counted = ref.id == self.item_id and self.completed.is_set()
        alive = await type(self.completer).heartbeat(
            self.completer,
            ref,
            attempt=attempt,
            progress_done=progress_done,
            progress_total=progress_total,
        )
        if counted and alive:
            self.count += 1
            if self.count >= self.need:
                self.enough.set()
        return alive


@dataclass(frozen=True, slots=True)
class Observed:
    """Что видно, пока транзакция пути B открыта."""

    waiting: int
    """Сколько блокировок ждут транзакцию пользователя (``pg_locks``)."""
    finished: bool
    """Пачка завершилась раньше, чем кто-то встал в очередь за транзакцией."""
    open_tx: bool
    """Транзакция пользователя в этот момент ещё открыта (``pg_stat_activity``)."""


async def _watch_blocked(env: Env, pid: int, stop: asyncio.Event) -> int:
    """Опрашивать ``pg_locks``, пока кто-нибудь не встанет в очередь за ``pid``.

    Возвращает число ожидающих блокировок (``0`` — остановлен через ``stop``).
    """
    async with env.connection() as conn:
        while not stop.is_set():
            if waiting := await blocked_by(conn, pid):
                return waiting
            await asyncio.sleep(BEAT.total_seconds() / 4)
    return 0


async def _in_transaction(env: Env, pid: int) -> bool:
    """Backend ``pid`` всё ещё внутри транзакции (``pg_stat_activity.xact_start``)."""
    async with env.connection() as conn:
        _ = await conn.execute(text("SELECT pg_stat_clear_snapshot()"))
        started = await conn.scalar(
            text("SELECT xact_start IS NOT NULL FROM pg_stat_activity WHERE pid = :pid"),
            {"pid": pid},
        )
    return started is True


async def _observe(env: Env, pid: int, done_run: asyncio.Task[object]) -> Observed:
    """Ждать первого из событий: пачка завершилась или кто-то ждёт блокировку ``pid``.

    ``STUCK`` — страховка от зависания, а не порог: при исправном пути B первое
    событие наступает, как только групповые транзакции закоммичены.
    """
    stop = asyncio.Event()
    watch_run = asyncio.create_task(_watch_blocked(env, pid, stop))
    try:
        _ = await asyncio.wait(
            {done_run, watch_run}, timeout=STUCK, return_when=asyncio.FIRST_COMPLETED
        )
        waiting = watch_run.result() if watch_run.done() else 0
        finished = done_run.done() and not waiting
        open_tx = await _in_transaction(env, pid)
    finally:
        stop.set()
        _ = await watch_run
    return Observed(waiting=waiting, finished=finished, open_tx=open_tx)


async def _outcome(fast_run: Awaitable[list[None]], done_run: asyncio.Task[R]) -> R | None:
    """Дождаться быстрых задач после release и забрать итог ``done_run``.

    Если групповая транзакция ждала транзакцию пользователя, быстрые Items
    завершаются после её commit, а heartbeat медленного Item уже не будет:
    ``done_run`` тогда отменяется, итога нет.
    """
    _ = await fast_run
    if not done_run.done():
        _ = done_run.cancel()
    _ = await asyncio.wait({done_run})
    return None if done_run.cancelled() else done_run.result()


async def _assert_settled(
    env: Env, probe: Table[ProbeColumns], *, batch_id: UUID, slow_id: UUID, total: int
) -> None:
    """Итог после commit пути B и ``settled()``.

    Lease медленного Item удалён после commit, метрики перенесены в слот
    процесса, домен и счётчики закоммичены.
    """
    assert await lease_row(env, slow_id) is None
    metric = env.tables.metric
    async with env.connection() as conn:
        slots = set(await conn.scalars(select(metric.c.slot).distinct()))
    assert slots == {COMPLETER_SLOT}
    assert await committed_ids(env.engine, probe) == [0]
    counters = await env.counters(batch_id)
    assert (counters.ok, counters.pending) == (total, 0)


@pytest.mark.parametrize("label", ["shared", "distinct"])
async def test_long_user_transaction_does_not_delay_heartbeat_and_finish_of_batch(
    env: Env, probe: Table[ProbeColumns], monkeypatch: pytest.MonkeyPatch, *, label: str
) -> None:
    """Транзакция пути B открыта после ``complete_in``, пока остальная пачка не завершится.

    Остальные Items пачки в это время продлевают lease и завершаются путём A
    (с тем же label, что у медленного Item, и с разными), а медленный Item
    продлевает свой lease. Секунды не меряются: проверяется порядок событий —
    всё это закоммичено, пока транзакция пользователя ещё открыта, — и то, что
    ни один backend не встал в очередь за её блокировками (``pg_locks``). Если
    путь B снова возьмёт ``th_lease`` или горячий слот ``th_metric`` (label
    ``shared``), групповая транзакция Completer будет ждать транзакцию
    пользователя, а та — завершения пачки: наблюдатель увидит ожидание, и тест
    упадёт сразу.
    """
    others = 4
    seeded = await seed(env, 1 + others)
    slow, fast = seeded.refs[0], seeded.refs[1:]
    completed = asyncio.Event()
    released = asyncio.Event()
    slow_pid: list[int] = []
    async with open_completer(env) as completer:
        runtime = _runtime(completer)
        beats = SlowBeats(completer, slow.id, completed)
        monkeypatch.setattr(completer, "heartbeat", beats)

        async def slow_task(**_kwargs: object) -> None:
            async with env.transaction() as conn:
                slow_pid.append(await backend_pid(conn))
                await insert_id(conn, probe, 0)
                item.ok("ok" if label == "shared" else "slow")
                await item.complete_in(conn)
                completed.set()
                await released.wait()

        async def fast_task(index: int, **_kwargs: object) -> None:
            # Задача живёт несколько интервалов heartbeat, затем завершается путём A.
            await asyncio.sleep(BEAT.total_seconds() * 5)
            item.ok("ok" if label == "shared" else f"fast-{index}")

        async def run_fast(index: int, ref: ItemRef) -> None:
            await runtime.wrap(fast_task)(index, _th=_marker(ref))

        async def batch_done(fast_run: Awaitable[list[None]]) -> list[RowMapping | None]:
            _ = await fast_run
            _ = await beats.enough.wait()
            # Читается до release: транзакция пользователя ещё открыта.
            return [await lease_row(env, ref.id) for ref in fast]

        async def run_slow() -> None:
            await runtime.wrap(slow_task)(_th=_marker(slow))

        slow_run = asyncio.create_task(run_slow())
        try:
            async with asyncio.timeout(STUCK):
                _ = await completed.wait()
            fast_run = asyncio.gather(*starmap(run_fast, enumerate(fast)))
            done_run = asyncio.create_task(batch_done(fast_run))
            observed = await _observe(env, slow_pid[0], done_run)
        finally:
            released.set()
            await slow_run
        leases_left = await _outcome(fast_run, done_run)
        assert not observed.waiting, f"за транзакцией пути B ждут {observed.waiting} блокировок"
        assert observed.finished, "пачка не завершилась, пока транзакция пользователя открыта"
        assert observed.open_tx
        await completer.settled()
    assert beats.count >= beats.need
    assert leases_left == [None] * others
    await _assert_settled(env, probe, batch_id=seeded.batch_id, slow_id=slow.id, total=1 + others)


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_heartbeat_before_complete_in_gives_no_serialization_failure(
    env: Env, probe: Table[ProbeColumns], *, isolation: str
) -> None:
    """Heartbeat продлевает lease между снимком транзакции и ``complete_in``."""
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    engine = schema_engine(env).execution_options(isolation_level=isolation)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            async with engine.begin() as conn:
                await insert_id(conn, probe, 1)  # снимок взят
                await asyncio.sleep(BEAT.total_seconds() * 8)
                item.ok()
                await item.complete_in(conn)
                await asyncio.sleep(BEAT.total_seconds() * 8)

        try:
            await _runtime(completer).wrap(task)(_th=_marker(ref))
        except DBAPIError as exc:  # pragma: no cover - падение теста с понятной причиной
            pytest.fail(f"транзакция пользователя упала: {exc.orig!r}")
    assert await committed_ids(env.engine, probe) == [1]
    assert await _state(env, ref.id) is ItemState.OK
    assert await lease_row(env, ref.id) is None


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_path_a_finish_of_same_label_gives_no_serialization_failure(
    env: Env, probe: Table[ProbeColumns], *, isolation: str
) -> None:
    """Completer того же процесса завершает Item с тем же label после снимка пользователя."""
    seeded = await seed(env, 2)
    slow, fast = seeded.refs
    engine = schema_engine(env).execution_options(isolation_level=isolation)
    snapshot = asyncio.Event()
    async with open_completer(env) as completer:
        runtime = _runtime(completer, beat=timedelta(seconds=30))

        async def slow_task(**_kwargs: object) -> None:
            async with engine.begin() as conn:
                await insert_id(conn, probe, 1)
                snapshot.set()
                await asyncio.sleep(0.3)
                item.ok()
                await item.complete_in(conn)

        async def fast_task(**_kwargs: object) -> None:
            await snapshot.wait()
            item.ok()

        try:
            _ = await asyncio.gather(
                runtime.wrap(slow_task)(_th=_marker(slow)),
                runtime.wrap(fast_task)(_th=_marker(fast)),
            )
        except DBAPIError as exc:  # pragma: no cover - падение теста с понятной причиной
            pytest.fail(f"транзакция пользователя упала: {exc.orig!r}")
    assert await committed_ids(env.engine, probe) == [1]
    counters = await env.counters(seeded.batch_id)
    assert (counters.ok, counters.pending) == (2, 0)


# --- (3) heartbeat устаревшей попытки -------------------------------------------------------


async def test_stale_attempt_heartbeat_does_not_extend_lease_of_new_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    seen: list[tuple[object, ...]] = []
    async with open_completer(env, clock=clock) as completer:

        async def task(**_kwargs: object) -> None:
            # Lease попытки 0 истёк, тот же процесс получает Item заново.
            clock.value = LATER
            again = await completer.claim(ref)
            taken = await lease_row(env, ref.id)
            clock.value = LATER + timedelta(seconds=10)
            # Несколько интервалов heartbeat устаревшей попытки 0.
            await asyncio.sleep(BEAT.total_seconds() * 6)
            lease = await lease_row(env, ref.id)
            seen.append((again.run, again.attempt))
            seen.append((None if taken is None else taken["lease_until"],))
            seen.append((None if lease is None else (lease["attempt"], lease["lease_until"]),))
            seen.append((ref.id in completer.held,))

        # Ассерты — снаружи: исключение задачи обёртка обработала бы как её ошибку.
        await _runtime(completer).wrap(task)(_th=_marker(ref))
        assert seen[0] == (True, 1)
        assert seen[2] == ((1, seen[1][0]),), "heartbeat попытки 0 продлил lease попытки 1"
        assert seen[3] == (True,)

        # Попытка 0 не завершила Item, который теперь у попытки 1.
        assert await _state(env, ref.id) is ItemState.ACTIVE
        assert ref.id in completer.held
        value = FinishResult(result_class=ResultClass.OK)
        assert await completer.finish(ref, value, attempt=1)
        assert completer.held == frozenset()


async def test_heartbeat_with_attempt_checks_owner(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    clock = MovableClock()
    async with open_completer(env, clock=clock) as completer:
        assert (await completer.claim(ref)).attempt == 0
        assert await completer.heartbeat(ref, attempt=0, progress_done=1)
        clock.value = LATER
        assert (await completer.claim(ref)).attempt == 1
        assert not await completer.heartbeat(ref, attempt=0, progress_done=2)
        assert ref.id in completer.held
        assert await completer.heartbeat(ref, attempt=1, progress_done=3)
        # Без attempt — прежняя проверка только по worker_id.
        assert await completer.heartbeat(ref)
        lease = await lease_row(env, ref.id)
        assert lease is not None
        assert (lease["attempt"], lease["progress_done"]) == (1, 3)
