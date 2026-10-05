"""Стенд бенчмарка: PostgreSQL 16 в Docker (или внешний DSN), toxiproxy, схемы, CPU.

Контейнеры называются ``bench-<роль>-<run>`` и удаляются по выходе из :func:`provision`.
Внешний DSN (``--dsn`` или ``TALLYHO_BENCH_DSN``) используется как есть: харнесс создаёт в
нём свои схемы и удаляет их, но не может перезапускать БД (P-09) или мерить её CPU (P-03).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from http.client import HTTPResponse

    from sqlalchemy import TextClause
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = [
    "POSTGRES_IMAGE",
    "TOXIPROXY_IMAGE",
    "BenchError",
    "CpuSampler",
    "Schemas",
    "Stand",
    "Toxiproxy",
    "ident",
    "provision",
    "schemas",
    "sql",
    "wait_postgres",
]

POSTGRES_IMAGE: Final = "postgres:16-alpine"
TOXIPROXY_IMAGE: Final = "ghcr.io/shopify/toxiproxy:2.12.0"
_USER: Final = "bench"
_READY_TIMEOUT: Final = 120.0
_READY_STREAK: Final = 2
"""Успешных подключений подряд: образ поднимает PostgreSQL дважды (initdb, затем рабочий)."""
_POSTGRES_SETTINGS: Final = (
    "fsync=on",
    "synchronous_commit=on",
    "full_page_writes=on",
    "max_connections=600",
    "shared_buffers=512MB",
    "work_mem=16MB",
    "maintenance_work_mem=256MB",
    "max_wal_size=4GB",
    "checkpoint_timeout=15min",
    "track_io_timing=on",
)


class BenchError(Exception):
    """Стенд или нагрузка не смогли выполнить прогон."""


async def _run(*args: str, check: bool = True) -> str:
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await process.communicate()
    if check and process.returncode != 0:
        message = f"{' '.join(args)} -> {process.returncode}: {err.decode(errors='replace')}"
        raise BenchError(message)
    return out.decode(errors="replace").strip()


async def _published_port(container: str, port: int) -> int:
    raw = await _run("docker", "port", container, f"{port}/tcp")
    # «127.0.0.1:49153»; при нескольких адресах — первая строка.
    return int(raw.splitlines()[0].rsplit(":", 1)[1])


async def wait_postgres(dsn: str, *, within: float = _READY_TIMEOUT) -> None:
    """Дождаться, пока PostgreSQL дважды подряд примет соединение (образ перезапускается).

    Raises:
        BenchError: не дождались за ``within`` секунд.
    """
    deadline = time.monotonic() + within
    successes = 0
    while True:
        engine = create_async_engine(dsn, pool_pre_ping=False)
        try:
            async with engine.connect() as connection:
                _ = await connection.execute(text("SELECT 1"))
            successes += 1
        except (OSError, OperationalError, DBAPIError):
            successes = 0
        finally:
            await engine.dispose()
        if successes >= _READY_STREAK:
            return
        if time.monotonic() > deadline:
            message = f"PostgreSQL не поднялся за {within:.0f} с"
            raise BenchError(message)
        await asyncio.sleep(0.5)


@dataclass(frozen=True, slots=True)
class Toxiproxy:
    """Прокси toxiproxy к PostgreSQL стенда."""

    api: str
    dsn: str

    async def latency(self, *, latency_ms: int, jitter_ms: int) -> None:
        """Задержка в обе стороны для всех новых и открытых соединений через прокси."""
        for stream in ("downstream", "upstream"):
            body = {
                "name": f"latency_{stream}",
                "type": "latency",
                "stream": stream,
                "attributes": {"latency": latency_ms, "jitter": jitter_ms},
            }
            await asyncio.to_thread(self.post, "/proxies/postgres/toxics", body)

    def post(self, path: str, body: object) -> None:
        """POST в HTTP API toxiproxy (синхронно; вызывать через ``asyncio.to_thread``)."""
        request = urllib.request.Request(  # ruff: ignore[suspicious-url-open-usage]  # http-адрес локального toxiproxy стенда
            self.api + path,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        opened = cast(
            "HTTPResponse",
            urllib.request.urlopen(request, timeout=10),  # ruff: ignore[suspicious-url-open-usage]  # http-адрес локального toxiproxy стенда
        )
        with opened as response:
            _ = response.read()


@dataclass(slots=True)
class Stand:
    """PostgreSQL прогона.

    Attributes:
        dsn: DSN SQLAlchemy (``postgresql+asyncpg://``) прямого подключения.
        container: имя своего контейнера PostgreSQL; ``None`` — внешний DSN.
        network: docker-сеть своего стенда (для toxiproxy).
        run_id: суффикс имён контейнеров и схем.
    """

    dsn: str
    container: str | None
    network: str | None
    run_id: str
    _proxies: list[str] = field(default_factory=list[str])

    @property
    def owned(self) -> bool:
        """Стенд поднят харнессом: можно перезапускать БД и мерить её CPU."""
        return self.container is not None

    async def restart_postgres(self, *, down_seconds: float) -> None:
        """``docker kill`` PostgreSQL и подъём через ``down_seconds`` (A-CH-02).

        Raises:
            BenchError: стенд на внешнем DSN.
        """
        if self.container is None:
            message = "перезапуск PostgreSQL возможен только на своём стенде"
            raise BenchError(message)
        _ = await _run("docker", "kill", self.container)
        await asyncio.sleep(down_seconds)
        _ = await _run("docker", "start", self.container)
        await wait_postgres(self.dsn)

    async def toxiproxy(self) -> Toxiproxy:
        """Поднять toxiproxy перед PostgreSQL своего стенда.

        Returns:
            Прокси с DSN через него.

        Raises:
            OSError: API toxiproxy не ответил за 30 с.
            BenchError: стенд на внешнем DSN.
        """
        if self.container is None or self.network is None:
            message = "toxiproxy доступен только на своём стенде"
            raise BenchError(message)
        name = f"bench-toxiproxy-{self.run_id}"
        _ = await _run(
            "docker", "run", "-d", "--name", name, "--network", self.network,
            "-p", "127.0.0.1::8474", "-p", "127.0.0.1::8666", TOXIPROXY_IMAGE,
        )  # fmt: skip
        self._proxies.append(name)
        api = f"http://127.0.0.1:{await _published_port(name, 8474)}"
        proxy = Toxiproxy(api, _dsn("127.0.0.1", await _published_port(name, 8666)))
        deadline = time.monotonic() + 30
        while True:
            try:
                await asyncio.to_thread(
                    proxy.post,
                    "/proxies",
                    {"name": "postgres", "listen": "0.0.0.0:8666", "upstream": "postgres:5432"},
                )
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.5)
        await wait_postgres(proxy.dsn)
        return proxy

    async def remove_proxies(self) -> None:
        """Удалить поднятые toxiproxy."""
        while self._proxies:
            _ = await _run("docker", "rm", "-f", self._proxies.pop(), check=False)


def _free_port() -> int:
    # Порт фиксируется при создании: после ``docker kill``/``start`` (P-09) он не меняется,
    # и воркеры переподключаются по тому же DSN.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        address = cast("tuple[str, int]", probe.getsockname())
        return address[1]


def _dsn(host: str, port: int) -> str:
    return f"postgresql+asyncpg://{_USER}:{_USER}@{host}:{port}/{_USER}"


@contextlib.asynccontextmanager
async def provision(dsn: str | None) -> AsyncGenerator[Stand]:
    """Внешний PostgreSQL по ``dsn`` или свой контейнер ``bench-pg-<run>``.

    Yields:
        Стенд.
    """
    run_id = uuid.uuid4().hex[:8]
    if dsn is not None:
        await wait_postgres(dsn, within=30)
        yield Stand(dsn, None, None, run_id)
        return
    network = f"bench-net-{run_id}"
    container = f"bench-pg-{run_id}"
    port = _free_port()
    _ = await _run("docker", "network", "create", network)
    try:
        settings = [part for setting in _POSTGRES_SETTINGS for part in ("-c", setting)]
        _ = await _run(
            "docker", "run", "-d", "--name", container, "--network", network,
            "--network-alias", "postgres", "--shm-size", "1g",
            "-e", f"POSTGRES_USER={_USER}", "-e", f"POSTGRES_PASSWORD={_USER}",
            "-e", f"POSTGRES_DB={_USER}", "-p", f"127.0.0.1:{port}:5432",
            POSTGRES_IMAGE, "postgres", *settings,
        )  # fmt: skip
        stand = Stand(_dsn("127.0.0.1", port), container, network, run_id)
        try:
            await wait_postgres(stand.dsn)
            yield stand
        finally:
            await stand.remove_proxies()
    finally:
        _ = await _run("docker", "rm", "-f", "-v", container, check=False)
        _ = await _run("docker", "network", "rm", network, check=False)


@dataclass(frozen=True, slots=True)
class Schemas:
    """Схемы одного P-NN: tallyho, flexiq и доменная."""

    tallyho: str
    flexiq: str
    domain: str


def ident(*parts: str) -> str:
    """Идентификатор SQL в кавычках: ``ident("s", "t")`` → ``"s"."t"``.

    Returns:
        Экранированное имя.
    """
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def sql(template: str, /, **identifiers: str) -> TextClause:
    """``text()`` с подставленными идентификаторами из :func:`ident`.

    Значения — только имена схем и таблиц, которые создаёт сам харнесс, уже экранированные
    :func:`ident`; данные передаются параметрами запроса.

    Returns:
        Запрос SQLAlchemy.
    """
    return text(template.format(**identifiers))


@contextlib.asynccontextmanager
async def schemas(engine: AsyncEngine, scenario: str) -> AsyncGenerator[Schemas]:
    """Создать три пустые схемы и удалить их по выходе.

    Yields:
        Имена схем.
    """
    suffix = uuid.uuid4().hex[:6]
    base = scenario.lower().replace("-", "")
    names = Schemas(f"b_{base}_{suffix}_th", f"b_{base}_{suffix}_fq", f"b_{base}_{suffix}_app")
    all_names = (names.tallyho, names.flexiq, names.domain)
    async with engine.begin() as connection:
        for name in all_names:
            _ = await connection.execute(text(f"CREATE SCHEMA {ident(name)}"))
    try:
        yield names
    finally:
        # Соединения пула могли умереть вместе с PostgreSQL (P-09): берём новые.
        await engine.dispose()
        async with engine.begin() as connection:
            for name in all_names:
                _ = await connection.execute(text(f"DROP SCHEMA IF EXISTS {ident(name)} CASCADE"))


@dataclass(slots=True)
class CpuSampler:
    """CPU контейнера PostgreSQL по ``docker stats``: доля всех ядер Docker, 0…1."""

    container: str
    samples: list[tuple[float, float]] = field(default_factory=list[tuple[float, float]])
    _cpus: int = 1
    _task: asyncio.Task[None] | None = None

    async def start(self, origin: float) -> None:
        """Начать опрос; моменты — секунды от ``origin`` (``time.monotonic``)."""
        self._cpus = max(1, int(await _run("docker", "info", "--format", "{{.NCPU}}")))
        self._task = asyncio.create_task(self._loop(origin), name="bench-cpu-sampler")

    async def _loop(self, origin: float) -> None:
        while True:
            raw = await _run(
                "docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", self.container,
                check=False,
            )  # fmt: skip
            with contextlib.suppress(ValueError):
                share = float(raw.strip().rstrip("%")) / 100 / self._cpus
                self.samples.append((time.monotonic() - origin, share))

    async def stop(self) -> None:
        """Остановить опрос."""
        if self._task is not None:
            _ = self._task.cancel()
            _ = await asyncio.wait([self._task])
            self._task = None

    def mean_between(self, start: float, end: float) -> float | None:
        """Средняя загрузка в окне ``[start, end]``; ``None`` — замеров нет.

        Returns:
            Доля ядер 0…1 или ``None``.
        """
        values = [share for at, share in self.samples if start <= at <= end]
        return sum(values) / len(values) if values else None
