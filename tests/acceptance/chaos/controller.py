"""Хаос-контроллер: исполняет расписание A-CH над compose-стендом и ведёт журнал."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from sqlalchemy import text

from tests.acceptance.app.common import rng_for
from tests.acceptance.chaos.plan import API_REPLICAS, PROCESSES, WORKERS, ActionKind
from tests.acceptance.chaos.stand import CONNECTION_ERRORS, StandError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from flexiq import Queue
    from sqlalchemy.ext.asyncio import AsyncConnection
    from sqlalchemy.sql.elements import TextClause

    from tests.acceptance.chaos.journal import ChaosJournal
    from tests.acceptance.chaos.plan import Action, ChaosPlan
    from tests.acceptance.chaos.stand import Stand

__all__ = ["ChaosController"]

# Запрос хука on_finalized эталонного приложения: только он пишет batch_status в домен.
_HOOK_SQL = text("""
    SELECT pid, application_name, state, left(query, 160)
      FROM pg_stat_activity
     WHERE pid <> pg_backend_pid()
       AND state IN ('active', 'idle in transaction')
       AND query LIKE 'UPDATE %'
       AND query LIKE '%batch_status%'
     LIMIT 1
""")
# Сессионный advisory lock лидера maintenance держит один из API-процессов.
_LEADER_SQL = text("""
    SELECT a.application_name, l.classid, l.objid
      FROM pg_locks l
      JOIN pg_stat_activity a USING (pid)
     WHERE l.locktype = 'advisory' AND l.granted AND a.application_name LIKE 'api-%'
""")
_TERMINAL_ITEMS_SQL = text("SELECT count(*) FROM th.th_item WHERE state >= 10")
_LEASES_SQL = text("SELECT count(*) FROM th.th_lease")
_XMIN_SQL = text(
    "SELECT pg_backend_pid(), backend_xmin::text FROM pg_stat_activity WHERE pid = pg_backend_pid()"
)
# Наблюдатель работает внутри контейнера PostgreSQL: между «увидел транзакцию Completer»
# и SIGQUIT postmaster (то же, что pg_ctl stop -m immediate) проходят миллисекунды.
_FLUSH_WATCH = """
DO $watch$
DECLARE
    hit text;
    deadline timestamptz := clock_timestamp() + interval '{wait} seconds';
BEGIN
    LOOP
        PERFORM pg_stat_clear_snapshot();
        SELECT application_name || ' pid=' || pid || ' ' || state || ' ' || left(query, 120)
          INTO hit
          FROM pg_stat_activity
         WHERE application_name LIKE 'worker-%'
           AND xact_start IS NOT NULL
           AND state IN ('active', 'idle in transaction')
           AND query ~ 'th\\.th_(lease|counter|expiry)'
         LIMIT 1;
        IF hit IS NOT NULL THEN
            RAISE NOTICE 'flush in progress: %', hit;
            RETURN;
        END IF;
        IF clock_timestamp() > deadline THEN
            RAISE EXCEPTION 'flush not seen';
        END IF;
        PERFORM pg_sleep(0.002);
    END LOOP;
END
$watch$;
"""
_FLUSH_SCRIPT = 'psql -U tallyho -d tallyho -v ON_ERROR_STOP=1 -qAt -c "$1" && kill -QUIT 1'
_FLUSH_MARK = "flush in progress: "
_FLEXIQ_ERRORS = (RuntimeError, OSError, ValueError, KeyError)
_PAGE = 200


@dataclass(slots=True)
class _Redelivery:
    seen: set[str] = field(default_factory=set[str])
    requeued: int = 0
    replayed: int = 0
    retried_dead: int = 0


@dataclass(slots=True)
class ChaosController:
    """Исполнитель расписания хаоса.

    Действия выполняются по одному в порядке расписания. Действие, не успевшее к своему
    времени (предыдущее ещё ждало PostgreSQL), выполняется сразу; фактическое время — в журнале.
    """

    stand: Stand
    plan: ChaosPlan
    journal: ChaosJournal
    queue: Queue
    _long_tx: AsyncConnection | None = None
    _long_tx_started: float = 0.0
    _long_tx_done: int = 0
    _redelivery: _Redelivery = field(default_factory=_Redelivery)

    async def run(self) -> None:
        """Выполнить расписание и продержать окно хаоса до ``plan.duration``."""
        self.journal.start()
        self.journal.record(
            "chaos_started",
            chaos=self.plan.chaos,
            seed=self.plan.seed,
            duration=self.plan.duration,
            environment=dict(self.plan.environment),
        )
        for action in self.plan.actions:
            await self._sleep_until(action.at)
            await self._execute(action)
        await self._sleep_until(self.plan.duration)

    async def heal(self) -> None:
        """Остановить хаос: снять toxics, закрыть транзакцию, поднять PostgreSQL и процессы."""
        await self._end_long_tx()
        await self.stand.reset_toxics()
        _ = await self.stand.start("postgres")
        ready = await self.stand.wait_postgres()
        for service in PROCESSES:
            _ = await self.stand.start(service)
        self.journal.record("chaos_healed", postgres_ready_after=round(ready, 3))

    async def _sleep_until(self, at: float) -> None:
        delay = at - self.journal.elapsed()
        if delay > 0:
            await asyncio.sleep(delay)

    async def _execute(self, action: Action) -> None:
        handlers: Mapping[ActionKind, Callable[[Action], Awaitable[None]]] = {
            ActionKind.KILL_WORKER: self._kill_worker,
            ActionKind.START_SERVICE: self._start_service,
            ActionKind.TERM_WORKERS: self._term_workers,
            ActionKind.KILL_PG: self._kill_pg,
            ActionKind.STOP_PG_IMMEDIATE: self._stop_pg_immediate,
            ActionKind.START_PG: self._start_pg,
            ActionKind.KILL_PG_ON_HOOK: self._kill_pg_on_hook,
            ActionKind.STOP_PG_ON_FLUSH: self._stop_pg_on_flush,
            ActionKind.CUT_NETWORK: self._cut_network,
            ActionKind.HEAL_NETWORK: self._heal_network,
            ActionKind.ADD_LATENCY: self._add_latency,
            ActionKind.REMOVE_LATENCY: self._remove_latency,
            ActionKind.KILL_LEADER: self._kill_leader,
            ActionKind.REDELIVER: self._redeliver,
            ActionKind.BEGIN_LONG_TX: self._begin_long_tx,
            ActionKind.END_LONG_TX: self._finish_long_tx,
        }
        await handlers[action.kind](action)

    # ------------------------------------------------------------------ процессы

    async def _kill_worker(self, action: Action) -> None:
        result = await self.stand.kill(action.target)
        self.journal.record(
            "kill_worker", action.target, signal="KILL", delivered=result.ok, planned_at=action.at
        )

    async def _start_service(self, action: Action) -> None:
        result = await self.stand.start(action.target)
        self.journal.record("start_service", action.target, started=result.ok)

    async def _term_workers(self, action: Action) -> None:
        budget = self.stand.settings.drain_timeout + 15
        started = time.monotonic()

        async def stop(worker: str) -> float | None:
            sent = await self.stand.kill(worker, "TERM")
            if not sent.ok or not await self.stand.wait_exit(worker, budget):
                _ = await self.stand.kill(worker)
                return None
            return round(time.monotonic() - started, 3)

        exits = await asyncio.gather(*(stop(worker) for worker in WORKERS))
        leases = await self._scalar(_LEASES_SQL)
        self.journal.record(
            "term_workers",
            signal="TERM",
            exit_seconds=dict(zip(WORKERS, exits, strict=True)),
            leases_left=leases,
            planned_at=action.at,
        )
        await asyncio.sleep(action.params["restart"])
        for worker in WORKERS:
            _ = await self.stand.start(worker)
        self.journal.record("workers_started")

    # ------------------------------------------------------------------ PostgreSQL

    async def _kill_pg(self, action: Action) -> None:
        result = await self.stand.kill("postgres")
        self.journal.record(
            "kill_pg", "postgres", how="docker kill", delivered=result.ok, planned_at=action.at
        )

    async def _stop_pg_immediate(self, action: Action) -> None:
        result = await self.stand.postgres_exec("pg_ctl", "stop", "-m", "immediate")
        self.journal.record(
            "stop_pg",
            "postgres",
            how="pg_ctl stop -m immediate",
            code=result.code,
            planned_at=action.at,
        )

    async def _start_pg(self, _action: Action) -> None:
        await self._restart_pg()

    async def _restart_pg(self) -> None:
        _ = await self.stand.wait_exit("postgres", 30)
        started = await self.stand.start("postgres")
        ready = await self.stand.wait_postgres()
        self.journal.record(
            "pg_started", "postgres", started=started.ok, ready_after=round(ready, 3)
        )

    async def _kill_pg_on_hook(self, action: Action) -> None:
        deadline = time.monotonic() + action.params["wait"]
        armed = time.monotonic()
        hit: tuple[object, ...] | None = None
        while hit is None and time.monotonic() < deadline:
            try:
                hit = await self._watch_hook(deadline)
            except CONNECTION_ERRORS:
                await asyncio.sleep(0.25)
        if hit is None:
            self.journal.record("hook_not_seen", "postgres", waited=action.params["wait"])
            return
        result = await self.stand.kill("postgres")
        self.journal.record(
            "kill_pg_on_hook",
            "postgres",
            how="docker kill",
            delivered=result.ok,
            backend_pid=hit[0],
            process=hit[1],
            state=hit[2],
            query=hit[3],
            armed_for=round(time.monotonic() - armed, 3),
        )
        await asyncio.sleep(action.params["down"])
        await self._restart_pg()

    async def _watch_hook(self, deadline: float) -> tuple[object, ...] | None:
        engine = self.stand.engine.execution_options(isolation_level="AUTOCOMMIT")
        async with engine.connect() as connection:
            while time.monotonic() < deadline:
                row = (await connection.execute(_HOOK_SQL)).first()
                if row is not None:
                    return tuple(row)
                await asyncio.sleep(0.05)
        return None

    async def _stop_pg_on_flush(self, action: Action) -> None:
        wait = action.params["wait"]
        try:
            result = await self.stand.postgres_exec(
                "sh",
                "-c",
                _FLUSH_SCRIPT,
                "sh",
                _FLUSH_WATCH.format(wait=wait),
                timeout_seconds=wait + 30,
            )
        except StandError as exc:
            self.journal.record("flush_watch_failed", "postgres", error=str(exc))
            return
        if _FLUSH_MARK not in result.output:
            self.journal.record(
                "flush_not_seen", "postgres", waited=wait, output=result.output[-300:]
            )
            return
        seen = result.output.split(_FLUSH_MARK, 1)[1].splitlines()[0]
        self.journal.record(
            "stop_pg_on_flush", "postgres", how="SIGQUIT postmaster (immediate)", session=seen
        )
        await asyncio.sleep(action.params["down"])
        await self._restart_pg()

    # ------------------------------------------------------------------ сеть

    async def _cut_network(self, action: Action) -> None:
        for stream in ("upstream", "downstream"):
            await self.stand.add_toxic(
                action.target,
                f"cut-{stream}",
                kind="timeout",
                stream=stream,
                attributes={"timeout": 0},
            )
        self.journal.record("cut_network", action.target, toxic="timeout", planned_at=action.at)

    async def _heal_network(self, action: Action) -> None:
        removed = [
            await self.stand.remove_toxic(action.target, f"cut-{stream}")
            for stream in ("upstream", "downstream")
        ]
        self.journal.record("heal_network", action.target, removed=all(removed))

    async def _add_latency(self, action: Action) -> None:
        # toxiproxy принимает только целые миллисекунды.
        attributes = {
            "latency": int(action.params["latency_ms"]),
            "jitter": int(action.params["jitter_ms"]),
        }
        await self.stand.add_toxic(
            action.target, "latency", kind="latency", stream="downstream", attributes=attributes
        )
        self.journal.record("add_latency", action.target, attributes=attributes)

    async def _remove_latency(self, action: Action) -> None:
        removed = await self.stand.remove_toxic(action.target, "latency")
        self.journal.record("remove_latency", action.target, removed=removed)

    # ------------------------------------------------------------------ лидер maintenance

    async def _kill_leader(self, action: Action) -> None:
        try:
            outcome = await self._depose_leader()
        except CONNECTION_ERRORS as exc:
            self.journal.record("leader_watch_failed", error=type(exc).__name__)
            return
        if outcome is None:
            self.journal.record("leader_not_found")
            return
        leader, delivered, successor, takeover = outcome
        self.journal.record(
            "kill_leader",
            leader,
            signal="KILL",
            delivered=delivered,
            successor=successor,
            takeover_seconds=takeover if successor is not None else None,
            planned_at=action.at,
        )
        await asyncio.sleep(action.params["restart"])
        started = await self.stand.start(leader)
        self.journal.record("start_service", leader, started=started.ok)

    async def _depose_leader(self) -> tuple[str, bool, str | None, float] | None:
        """Убить лидера и измерить, за сколько лидерство взял другой API-процесс."""
        engine = self.stand.engine.execution_options(isolation_level="AUTOCOMMIT")
        async with engine.connect() as connection:
            leader = await self._leader(connection)
            if leader is None:
                return None
            result = await self.stand.kill(leader)
            killed = time.monotonic()
            successor = await self._successor(connection, leader)
            return leader, result.ok, successor, round(time.monotonic() - killed, 3)

    @staticmethod
    async def _leader(connection: AsyncConnection) -> str | None:
        """Процесс, который держит один и тот же advisory lock в двух замерах подряд."""
        first = {tuple(row) for row in (await connection.execute(_LEADER_SQL)).all()}
        await asyncio.sleep(0.3)
        second = {tuple(row) for row in (await connection.execute(_LEADER_SQL)).all()}
        stable = sorted(str(row[0]) for row in first & second)
        return stable[0] if stable else None

    @staticmethod
    async def _successor(connection: AsyncConnection, killed: str) -> str | None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = (await connection.execute(_LEADER_SQL)).all()
            others = sorted(str(row[0]) for row in rows if row[0] != killed)
            if others and others[0] in API_REPLICAS:
                return others[0]
            await asyncio.sleep(0.05)
        return None

    # ------------------------------------------------------------------ повторная доставка

    async def _redeliver(self, action: Action) -> None:
        try:
            counts = await asyncio.to_thread(self._redeliver_sync, action.params["fraction"])
        except _FLEXIQ_ERRORS as exc:
            self.journal.record("redeliver_failed", error=f"{type(exc).__name__}: {exc}"[:200])
            return
        self.journal.record("redeliver", planned_at=action.at, counts=counts)

    def _redeliver_sync(self, fraction: float) -> dict[str, int]:
        state = self._redelivery
        counts = {"requeue_job": 0, "replay": 0, "retry_dead": 0}

        def chosen(job_id: str, done: int) -> bool:
            if job_id in state.seen:
                return False
            state.seen.add(job_id)
            # Каждая операция должна выполниться хотя бы раз за прогон, даже если
            # кандидатов мало; остальные джобы выбираются с вероятностью fraction.
            roll = rng_for(self.plan.seed, job_id, namespace="redeliver").random()
            return done == 0 or roll < fraction

        for job in self.queue.list_jobs(status="running", limit=_PAGE):
            if chosen(job.id, state.requeued) and self.queue.requeue_job(job.id):
                state.requeued += 1
                counts["requeue_job"] += 1
        for job in self.queue.list_jobs(status="complete", limit=_PAGE):
            if chosen(job.id, state.replayed):
                _ = self.queue.replay(job.id)
                state.replayed += 1
                counts["replay"] += 1
        dead = cast("list[Mapping[str, object]]", self.queue.dead_letters(limit=_PAGE))
        for raw in dead:
            dead_id = str(raw["id"])
            if chosen(dead_id, state.retried_dead):
                _ = self.queue.retry_dead(dead_id)
                state.retried_dead += 1
                counts["retry_dead"] += 1
        return counts

    # ------------------------------------------------------------------ долгая транзакция

    async def _begin_long_tx(self, action: Action) -> None:
        engine = self.stand.engine.execution_options(isolation_level="REPEATABLE READ")
        connection = await engine.connect()
        self._long_tx = connection
        self._long_tx_done = int(await connection.scalar(_TERMINAL_ITEMS_SQL) or 0)
        _ = await connection.scalar(text("SELECT txid_current()"))
        row = (await connection.execute(_XMIN_SQL)).one()
        self._long_tx_started = time.monotonic()
        self.journal.record(
            "begin_long_tx",
            "postgres",
            backend_pid=row[0],
            backend_xmin=row[1],
            terminal_items=self._long_tx_done,
            rate_before=round(self._long_tx_done / max(self.journal.elapsed(), 1.0), 3),
            planned_at=action.at,
        )

    async def _finish_long_tx(self, _action: Action) -> None:
        await self._end_long_tx()

    async def _end_long_tx(self) -> None:
        connection = self._long_tx
        if connection is None:
            return
        self._long_tx = None
        held = time.monotonic() - self._long_tx_started
        done = await self._scalar(_TERMINAL_ITEMS_SQL)
        try:
            row = (await connection.execute(_XMIN_SQL)).one()
            await connection.commit()
            xmin: object = row[1]
        except CONNECTION_ERRORS as exc:
            xmin = f"lost: {type(exc).__name__}"
        finally:
            await connection.close()
        self.journal.record(
            "end_long_tx",
            "postgres",
            held_seconds=round(held, 3),
            backend_xmin=xmin,
            terminal_items=done,
            rate_during=round((done - self._long_tx_done) / max(held, 1.0), 3),
        )

    async def _scalar(self, statement: TextClause) -> int:
        try:
            async with self.stand.connect() as connection:
                return int(await connection.scalar(statement) or 0)
        except CONNECTION_ERRORS:
            return -1
