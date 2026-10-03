"""Управление compose-стендом с хоста: docker CLI, HTTP API toxiproxy, SQL через прокси control.

Стенд работает в Linux-контейнерах; этот модуль запускается на хосте (в том числе Windows)
и не публикует фиксированных портов: каждый стенд — отдельный compose-проект.
"""

from __future__ import annotations

import asyncio
import os
import time
import zlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import aiohttp
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from tests.acceptance.chaos.plan import PROCESSES, WORKERS
from tests.acceptance.oracle import recovery_timeout

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

__all__ = [
    "CONNECTION_ERRORS",
    "CommandResult",
    "Stand",
    "StandError",
    "StandSettings",
    "run_command",
]

HERE = Path(__file__).parent
COMPOSE_FILE = HERE.parent / "docker-compose.yml"
ROOT = HERE.parents[2]
# Тег зависит от рабочего дерева: параллельные прогоны из разных worktree собирают разный код
# и не должны подменять образ друг другу между build и up.
DEFAULT_IMAGE = f"tallyho-acceptance-app:chaos-{zlib.crc32(str(ROOT).encode()):08x}"

# Ошибки, которыми отвечает PostgreSQL за toxiproxy, пока он выключен или поднимается.
CONNECTION_ERRORS = (DBAPIError, OSError, TimeoutError)

# Готовность API: один из них уже держит advisory lock лидера maintenance.
_LEADER_SQL = text("""
    SELECT count(*)
      FROM pg_locks l
      JOIN pg_stat_activity a USING (pid)
     WHERE l.locktype = 'advisory' AND l.granted AND a.application_name LIKE 'api-%'
""")
_WORKER_READY = "/tmp/worker.ready"  # ruff: ignore[hardcoded-temp-file]  # путь внутри контейнера воркера


class StandError(Exception):
    """Стенд не поднялся или команда docker завершилась неожиданно."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Код возврата и объединённый вывод внешней команды."""

    code: int
    output: str

    @property
    def ok(self) -> bool:
        """Команда завершилась успешно."""
        return self.code == 0


async def run_command(
    *args: str,
    env: Mapping[str, str] | None = None,
    timeout_seconds: float = 120,
) -> CommandResult:
    """Выполнить внешнюю команду и вернуть код и вывод.

    Raises:
        StandError: Команда не уложилась в ``timeout_seconds``.
    """
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=None if env is None else {**os.environ, **env},
    )
    try:
        async with asyncio.timeout(timeout_seconds):
            output, _ = await process.communicate()
    except TimeoutError as exc:
        process.kill()
        _ = await process.wait()
        message = f"команда не уложилась в {timeout_seconds} с: {' '.join(args)}"
        raise StandError(message) from exc
    return CommandResult(process.returncode or 0, output.decode("utf-8", errors="replace").strip())


@dataclass(frozen=True, slots=True, kw_only=True)
class StandSettings:
    """Параметры стенда, одинаковые для всех процессов прогона.

    ``lease_ttl``, ``heartbeat_every`` и ``sweep_interval`` - умолчания библиотеки
    (ARCHITECTURE §15), поэтому ``T_rec`` = 100 с, как в ACCEPTANCE §4. Короткий
    ``lease_ttl`` стенду больше не мешает: ключ идемпотентности несёт поколение отправки
    (D-060), и повторная отправка Item не сливается с ещё «выполняющейся» джобой убитого
    воркера. ``LEASE_TTL=15 SWEEP_INTERVAL=1`` (A-CH-02 и A-CH-05 на S1, seed 1) проходят
    оракул; задачи flexiq при этом получают ``timeout`` 15 с (``StandTuning.job_timeout``).
    """

    seed: int
    pages: int = 1
    threads: int = 8
    lease_ttl: float = 60.0
    heartbeat_every: float = 20.0
    sweep_interval: float = 5.0
    drain_timeout: int = 20
    transient_rate: float = 0.05
    permanent_rate: float = 0.01
    image: str = DEFAULT_IMAGE

    @property
    def recovery(self) -> timedelta:
        """``T_rec = lease_ttl + 2 * sweep_interval + 30 с`` (ACCEPTANCE §4)."""
        return recovery_timeout(
            timedelta(seconds=self.lease_ttl), timedelta(seconds=self.sweep_interval)
        )

    @property
    def concurrency(self) -> int:
        """Число одновременно выполняемых задач на всём стенде."""
        return len(WORKERS) * self.threads

    def environment(self) -> dict[str, str]:
        """Переменные для ``docker compose``."""
        return {
            "APP_IMAGE": self.image,
            "SEED": str(self.seed),
            "PAGES": str(self.pages),
            "THREADS_PER_WORKER": str(self.threads),
            "LEASE_TTL": str(self.lease_ttl),
            "HEARTBEAT_EVERY": str(self.heartbeat_every),
            "SWEEP_INTERVAL": str(self.sweep_interval),
            "DRAIN_TIMEOUT": str(self.drain_timeout),
            "TRANSIENT_RATE": str(self.transient_rate),
            "PERMANENT_RATE": str(self.permanent_rate),
        }


@dataclass(slots=True)
class Stand:
    """Один compose-проект приёмочного стенда."""

    project: str
    settings: StandSettings
    extra_environment: Mapping[str, str] = field(default_factory=dict[str, str])
    toxiproxy_url: str = ""
    control_dsn: str = ""
    site_url: str = ""
    mail_url: str = ""
    _engine: AsyncEngine | None = None
    _http: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ жизненный цикл

    async def up(self) -> None:
        """Собрать образ, поднять стенд и дождаться готовности всех процессов.

        Raises:
            StandError: Образ не собрался, compose не поднялся или воркеры не вышли на связь.
        """
        await self.down()
        build = await run_command(
            "docker",
            "build",
            "-q",
            "-t",
            self.settings.image,
            "-f",
            str(HERE.parent / "Dockerfile"),
            str(ROOT),
            timeout_seconds=1200,
        )
        if not build.ok:
            message = f"образ {self.settings.image} не собрался:\n{build.output}"
            raise StandError(message)
        started = await self._compose("up", "-d", "--no-build", "--wait", timeout_seconds=600)
        if not started.ok:
            message = f"стенд {self.project} не поднялся:\n{started.output}"
            raise StandError(message)
        toxiproxy = await self._published("toxiproxy", 8474)
        control = await self._published("toxiproxy", 8660)
        self.toxiproxy_url = f"http://{toxiproxy}"
        self.control_dsn = f"postgresql+asyncpg://tallyho:tallyho@{control}/tallyho"
        self.site_url = f"http://{await self._published('external-services', 8081)}"
        self.mail_url = f"http://{await self._published('external-services', 8082)}"
        self._engine = create_async_engine(
            self.control_dsn, poolclass=NullPool, connect_args={"timeout": 5}
        )
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        await self._wait_workers()

    async def close(self) -> None:
        """Закрыть соединения хоста со стендом, не трогая контейнеры."""
        if self._http is not None:
            await self._http.close()
            self._http = None
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    async def down(self) -> None:
        """Удалить контейнеры, сети и тома проекта."""
        await self.close()
        _ = await self._compose("down", "-v", "--remove-orphans", timeout_seconds=300)

    async def save_logs(self, path: Path) -> None:
        """Сохранить логи всех контейнеров стенда."""
        logs = await self._compose("logs", "--no-color", "--timestamps", timeout_seconds=300)
        _ = await asyncio.to_thread(
            path.write_text, logs.output + "\n", encoding="utf-8", newline="\n"
        )

    async def _compose(self, *args: str, timeout_seconds: float) -> CommandResult:
        environment = {**self.settings.environment(), **self.extra_environment}
        return await run_command(
            "docker",
            "compose",
            "-p",
            self.project,
            "-f",
            str(COMPOSE_FILE),
            *args,
            env=environment,
            timeout_seconds=timeout_seconds,
        )

    async def _published(self, service: str, port: int) -> str:
        result = await self._compose("port", service, str(port), timeout_seconds=60)
        if not result.ok or ":" not in result.output:
            message = f"порт {service}:{port} не опубликован: {result.output}"
            raise StandError(message)
        return result.output.splitlines()[-1].strip()

    async def _wait_workers(self) -> None:
        """Дождаться, пока все воркеры зарегистрировали задачи, а maintenance выбрал лидера.

        Таблица ``flexiq.workers`` для этого не годится: при сдвиге часов (A-CH-09) flexiq
        сам удаляет из неё строки живых воркеров.
        """
        deadline = time.monotonic() + 180
        pending = list(WORKERS)
        leader = 0
        while time.monotonic() < deadline:
            pending = [worker for worker in pending if not await self._worker_ready(worker)]
            try:
                async with self.connect() as connection:
                    leader = int(await connection.scalar(_LEADER_SQL) or 0)
            except CONNECTION_ERRORS:
                leader = 0
            if not pending and leader:
                return
            await asyncio.sleep(0.5)
        message = f"стенд не готов за 180 с: воркеры без ready-файла {pending}, лидер={leader}"
        raise StandError(message)

    # ------------------------------------------------------------------ контейнеры

    def container(self, service: str) -> str:
        """Имя контейнера сервиса."""
        return f"{self.project}-{service}-1"

    async def kill(self, service: str, signal: str = "KILL") -> CommandResult:
        """Послать сигнал PID 1 контейнера (``kill -9`` по умолчанию)."""
        return await run_command("docker", "kill", "-s", signal, self.container(service))

    async def start(self, service: str) -> CommandResult:
        """Запустить остановленный контейнер; для работающего — ничего не делает."""
        return await run_command("docker", "start", self.container(service))

    async def _worker_ready(self, worker: str) -> bool:
        """Воркер зарегистрировал задачи и поставил обработчики SIGTERM/SIGINT."""
        result = await run_command(
            "docker", "exec", self.container(worker), "test", "-f", _WORKER_READY
        )
        return result.ok

    async def wait_worker_ready(self, worker: str, timeout_seconds: float) -> float | None:
        """Дождаться ready-файла воркера; секунды ожидания или ``None`` - не дождались."""
        started = time.monotonic()
        while time.monotonic() - started < timeout_seconds:
            if await self._worker_ready(worker):
                return round(time.monotonic() - started, 3)
            await asyncio.sleep(0.2)
        return None

    async def wait_exit(self, service: str, timeout_seconds: float) -> bool:
        """Дождаться остановки контейнера; ``False`` — не остановился за отведённое время."""
        try:
            result = await run_command(
                "docker", "wait", self.container(service), timeout_seconds=timeout_seconds
            )
        except StandError:
            return False
        return result.ok

    async def inspect(self, service: str, template: str) -> str:
        """Вернуть поле ``docker inspect`` контейнера."""
        result = await run_command("docker", "inspect", "-f", template, self.container(service))
        return result.output

    async def is_running(self, service: str) -> bool:
        """Работает ли контейнер."""
        return await self.inspect(service, "{{.State.Running}}") == "true"

    async def restart_counts(self) -> dict[str, int]:
        """Сколько раз Docker сам перезапускал процессы приложения (падения без хаоса)."""
        counts: dict[str, int] = {}
        for service in PROCESSES:
            raw = await self.inspect(service, "{{.RestartCount}}")
            counts[service] = int(raw) if raw.isdigit() else -1
        return counts

    async def postgres_exec(self, *command: str, timeout_seconds: float = 60) -> CommandResult:
        """Выполнить команду в контейнере PostgreSQL от пользователя ``postgres``."""
        return await run_command(
            "docker",
            "exec",
            "-u",
            "postgres",
            self.container("postgres"),
            *command,
            timeout_seconds=timeout_seconds,
        )

    async def wait_postgres(self, timeout_seconds: float = 120) -> float:
        """Дождаться, пока PostgreSQL принимает запросы через toxiproxy.

        Returns:
            Сколько секунд заняло ожидание.

        Raises:
            StandError: PostgreSQL не поднялся за ``timeout_seconds``.
        """
        started = time.monotonic()
        while time.monotonic() - started < timeout_seconds:
            try:
                async with self.connect() as connection:
                    _ = await connection.scalar(text("SELECT 1"))
            except CONNECTION_ERRORS:
                await asyncio.sleep(0.25)
                continue
            return time.monotonic() - started
        message = f"PostgreSQL не поднялся за {timeout_seconds} с"
        raise StandError(message)

    # ------------------------------------------------------------------ SQL

    @property
    def engine(self) -> AsyncEngine:
        """Engine хоста через прокси ``control`` (toxics на него не вешаются).

        Raises:
            StandError: Стенд ещё не поднят.
        """
        if self._engine is None:
            message = "стенд не поднят"
            raise StandError(message)
        return self._engine

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncConnection]:
        """Новое соединение хоста с PostgreSQL; ``th_*`` разрешаются в схему ``th``."""
        engine = self.engine.execution_options(schema_translate_map={None: "th"})
        async with engine.connect() as connection:
            yield connection

    # ------------------------------------------------------------------ toxiproxy

    async def add_toxic(
        self, proxy: str, name: str, *, kind: str, stream: str, attributes: Mapping[str, int]
    ) -> None:
        """Повесить toxic на прокси процесса.

        Raises:
            StandError: toxiproxy отклонил запрос.
        """
        body = {"name": name, "type": kind, "stream": stream, "attributes": dict(attributes)}
        async with self._session().post(
            f"{self.toxiproxy_url}/proxies/{proxy}/toxics", json=body
        ) as response:
            if response.status not in {200, 409}:
                message = f"toxiproxy {proxy}/{name}: {response.status} {await response.text()}"
                raise StandError(message)

    async def remove_toxic(self, proxy: str, name: str) -> bool:
        """Снять toxic; ``False`` — его уже не было."""
        async with self._session().delete(
            f"{self.toxiproxy_url}/proxies/{proxy}/toxics/{name}"
        ) as response:
            return response.status == 204

    async def reset_toxics(self) -> None:
        """Снять все toxics и включить все прокси.

        Raises:
            StandError: toxiproxy отклонил запрос.
        """
        async with self._session().post(f"{self.toxiproxy_url}/reset") as response:
            if response.status != 204:
                message = f"toxiproxy reset: {response.status}"
                raise StandError(message)

    async def fetch_json(self, url: str) -> object:
        """GET JSON с фейкового внешнего сервиса (журналы вызовов)."""
        async with self._session().get(url) as response:
            return cast("object", await response.json())

    def _session(self) -> aiohttp.ClientSession:
        if self._http is None:
            message = "стенд не поднят"
            raise StandError(message)
        return self._http
