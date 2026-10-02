"""Расписание хаоса A-CH-01…12, детерминированное от seed.

Модуль чистый: по ``(chaos, seed, duration)`` строится один и тот же список
действий со временем от начала хаоса. Исполняет его
:class:`tests.acceptance.chaos.controller.ChaosController`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from tests.acceptance.app.common import rng_for

if TYPE_CHECKING:
    import random
    from collections.abc import Callable, Mapping

__all__ = [
    "API_REPLICAS",
    "CHAOS_IDS",
    "CHAOS_TITLES",
    "FAKETIME_LIBRARY",
    "PROCESSES",
    "SCENARIOS",
    "WORKERS",
    "Action",
    "ActionKind",
    "ChaosPlan",
    "PlanError",
    "build_plan",
]

WORKERS = ("worker-1", "worker-2", "worker-3", "worker-4")
API_REPLICAS = ("api-1", "api-2")
PROCESSES = (*WORKERS, *API_REPLICAS)
SCENARIOS = ("S1", "S2", "S3")
FAKETIME_LIBRARY = "/usr/local/lib/libfaketime.so.1"

CHAOS_TITLES: Mapping[str, str] = {
    "A-CH-01": "kill -9 случайного воркера каждые 5-30 с",
    "A-CH-02": "docker kill и pg_ctl stop -m immediate PostgreSQL, подъём через 5-60 с",
    "A-CH-03": "kill PostgreSQL, пока виден запрос хука on_finalized",
    "A-CH-04": "остановка PostgreSQL во время flush Completer",
    "A-CH-05": "toxiproxy timeout: разрыв сети одного воркера на 10-60 с",
    "A-CH-06": "toxiproxy latency 50-500 мс, jitter 100 мс весь прогон",
    "A-CH-07": "kill -9 API-процесса — лидера maintenance",
    "A-CH-08": "SIGTERM воркеров под нагрузкой",
    "A-CH-09": "libfaketime ±5 мин на части воркеров",
    "A-CH-10": "requeue_job / replay / retry_dead для 10% джоб",
    "A-CH-11": "долгая транзакция, держащая backend_xmin",
    "A-CH-12": "A-CH-01 + 02 + 05 + 06 + 10 со случайным расписанием",
}
CHAOS_IDS = tuple(CHAOS_TITLES)

_MAX_PG_DOWNTIME = 60.0
_MAX_NETWORK_CUT = 60.0
_MAX_LONG_TX = 600.0


class PlanError(Exception):
    """Запрошен неизвестный хаос-сценарий или недопустимая длительность."""


class ActionKind(StrEnum):
    """Примитивы, из которых собирается любой A-CH."""

    KILL_WORKER = "kill_worker"
    START_SERVICE = "start_service"
    TERM_WORKERS = "term_workers"
    KILL_PG = "kill_pg"
    STOP_PG_IMMEDIATE = "stop_pg_immediate"
    START_PG = "start_pg"
    KILL_PG_ON_HOOK = "kill_pg_on_hook"
    STOP_PG_ON_FLUSH = "stop_pg_on_flush"
    CUT_NETWORK = "cut_network"
    HEAL_NETWORK = "heal_network"
    ADD_LATENCY = "add_latency"
    REMOVE_LATENCY = "remove_latency"
    KILL_LEADER = "kill_leader"
    REDELIVER = "redeliver"
    BEGIN_LONG_TX = "begin_long_tx"
    END_LONG_TX = "end_long_tx"


@dataclass(frozen=True, slots=True)
class Action:
    """Одно действие контроллера.

    Attributes:
        at: Секунды от начала хаоса.
        kind: Что сделать.
        target: Сервис compose или прокси toxiproxy; пусто, если цель выбирает действие.
        params: Числовые параметры действия (секунды, миллисекунды, доли).
    """

    at: float
    kind: ActionKind
    target: str = ""
    params: Mapping[str, float] = field(default_factory=dict[str, float])


@dataclass(frozen=True, slots=True)
class ChaosPlan:
    """Полное расписание одного прогона.

    Attributes:
        chaos: Идентификатор A-CH.
        seed: Seed прогона.
        duration: Длительность окна хаоса, секунды.
        actions: Действия по возрастанию времени.
        environment: Переменные compose, которые нужно задать до запуска стенда
            (сдвиг часов воркеров, задержка хука).
    """

    chaos: str
    seed: int
    duration: float
    actions: tuple[Action, ...]
    environment: Mapping[str, str] = field(default_factory=dict[str, str])


def _round(value: float) -> float:
    return round(value, 3)


def _worker_kills(rng: random.Random, duration: float) -> list[Action]:
    actions: list[Action] = []
    at = rng.uniform(5, 30)
    while at < duration:
        worker = rng.choice(WORKERS)
        actions.extend(
            (
                Action(_round(at), ActionKind.KILL_WORKER, worker),
                Action(_round(at + rng.uniform(1, 3)), ActionKind.START_SERVICE, worker),
            )
        )
        at += rng.uniform(5, 30)
    return actions


def _pg_outages(rng: random.Random, duration: float) -> list[Action]:
    # Простой 5-60 с; на коротких прогонах верхняя граница сжимается до duration / 6,
    # чтобы за окно уместились оба способа остановки.
    longest = min(_MAX_PG_DOWNTIME, max(8.0, duration / 6))
    modes = (ActionKind.KILL_PG, ActionKind.STOP_PG_IMMEDIATE)
    actions: list[Action] = []
    at = rng.uniform(8, 25)
    index = 0
    while at < duration - 5:
        down = rng.uniform(5, longest)
        actions.extend(
            (
                Action(_round(at), modes[index % 2], "postgres"),
                Action(_round(at + down), ActionKind.START_PG, "postgres"),
            )
        )
        at += down + rng.uniform(20, 45)
        index += 1
    return actions


def _triggered_pg_stops(
    rng: random.Random, duration: float, kind: ActionKind, *, wait: tuple[float, float]
) -> list[Action]:
    actions: list[Action] = []
    at = rng.uniform(3, 8)
    while at < duration - 5:
        watch = min(rng.uniform(*wait), duration - at)
        down = rng.uniform(5, 15)
        actions.append(
            Action(_round(at), kind, "postgres", {"wait": _round(watch), "down": _round(down)})
        )
        # Окна наблюдения идут вплотную: событие между окнами нельзя пропустить.
        at += watch
    return actions


def _network_cuts(rng: random.Random, duration: float) -> list[Action]:
    longest = min(_MAX_NETWORK_CUT, max(15.0, duration / 4))
    actions: list[Action] = []
    at = rng.uniform(5, 15)
    while at < duration - 5:
        worker = rng.choice(WORKERS)
        cut = rng.uniform(10, longest)
        actions.extend(
            (
                Action(_round(at), ActionKind.CUT_NETWORK, worker),
                Action(_round(at + cut), ActionKind.HEAL_NETWORK, worker),
            )
        )
        at += cut + rng.uniform(5, 20)
    return actions


def _latency(rng: random.Random, duration: float) -> list[Action]:
    latency = float(rng.randint(50, 500))
    params = {"latency_ms": latency, "jitter_ms": 100.0}
    actions = [Action(0.0, ActionKind.ADD_LATENCY, proxy, params) for proxy in PROCESSES]
    actions.extend(
        Action(_round(duration), ActionKind.REMOVE_LATENCY, proxy) for proxy in PROCESSES
    )
    return actions


def _leader_kills(rng: random.Random, duration: float) -> list[Action]:
    actions: list[Action] = []
    at = rng.uniform(10, 25)
    while at < duration - 5:
        restart = rng.uniform(3, 8)
        actions.append(Action(_round(at), ActionKind.KILL_LEADER, "", {"restart": _round(restart)}))
        at += restart + rng.uniform(20, 40)
    return actions


def _soft_stops(rng: random.Random, duration: float) -> list[Action]:
    actions: list[Action] = []
    at = rng.uniform(10, 25)
    while at < duration - 5:
        restart = rng.uniform(1, 3)
        actions.append(
            Action(_round(at), ActionKind.TERM_WORKERS, "", {"restart": _round(restart)})
        )
        at += rng.uniform(30, 45)
    return actions


def _redeliveries(rng: random.Random, duration: float) -> list[Action]:
    actions: list[Action] = []
    at = rng.uniform(5, 10)
    while at < duration:
        actions.append(Action(_round(at), ActionKind.REDELIVER, "", {"fraction": 0.1}))
        at += rng.uniform(5, 10)
    return actions


def _long_transaction(rng: random.Random, duration: float) -> list[Action]:
    begin = rng.uniform(5, 10)
    hold = min(_MAX_LONG_TX, max(10.0, duration * 0.4))
    return [
        Action(_round(begin), ActionKind.BEGIN_LONG_TX),
        Action(_round(begin + hold), ActionKind.END_LONG_TX),
    ]


def _clock_skew(rng: random.Random) -> dict[str, str]:
    ahead, behind = rng.sample(range(1, len(WORKERS) + 1), 2)
    return {
        f"FAKETIME_{ahead}": "+5m",
        f"FAKETIME_LIB_{ahead}": FAKETIME_LIBRARY,
        f"FAKETIME_{behind}": "-5m",
        f"FAKETIME_LIB_{behind}": FAKETIME_LIBRARY,
    }


def _hook_stops(rng: random.Random, duration: float) -> list[Action]:
    return _triggered_pg_stops(rng, duration, ActionKind.KILL_PG_ON_HOOK, wait=(20, 40))


def _flush_stops(rng: random.Random, duration: float) -> list[Action]:
    # Небольшая задержка сети воркеров растягивает групповую транзакцию Completer,
    # иначе её не успеть застать: без задержки она длится единицы миллисекунд.
    params = {"latency_ms": 20.0, "jitter_ms": 0.0}
    actions = [Action(0.0, ActionKind.ADD_LATENCY, proxy, params) for proxy in WORKERS]
    actions.extend(_triggered_pg_stops(rng, duration, ActionKind.STOP_PG_ON_FLUSH, wait=(10, 20)))
    actions.extend(Action(_round(duration), ActionKind.REMOVE_LATENCY, proxy) for proxy in WORKERS)
    return actions


def _nothing(_rng: random.Random, _duration: float) -> list[Action]:
    return []


_GENERATORS: Mapping[str, Callable[[random.Random, float], list[Action]]] = {
    "A-CH-01": _worker_kills,
    "A-CH-02": _pg_outages,
    "A-CH-03": _hook_stops,
    "A-CH-04": _flush_stops,
    "A-CH-05": _network_cuts,
    "A-CH-06": _latency,
    "A-CH-07": _leader_kills,
    "A-CH-08": _soft_stops,
    "A-CH-09": _nothing,
    "A-CH-10": _redeliveries,
    "A-CH-11": _long_transaction,
}
_EVERYTHING = ("A-CH-01", "A-CH-02", "A-CH-05", "A-CH-06", "A-CH-10")
_HOOK_DELAY = "3"


def build_plan(chaos: str, seed: int, duration: float) -> ChaosPlan:
    """Построить расписание хаоса; одинаковые аргументы дают одинаковый план.

    Args:
        chaos: Идентификатор ``A-CH-01`` … ``A-CH-12``.
        seed: Seed прогона.
        duration: Длительность окна хаоса в секундах.

    Returns:
        Расписание и переменные окружения стенда.

    Raises:
        PlanError: Неизвестный сценарий или слишком короткое окно.
    """
    if chaos not in CHAOS_TITLES:
        message = f"неизвестный хаос-сценарий {chaos!r}; допустимы {', '.join(CHAOS_IDS)}"
        raise PlanError(message)
    if duration < 30:
        message = f"длительность хаоса {duration} с меньше 30 с"
        raise PlanError(message)
    parts = _EVERYTHING if chaos == "A-CH-12" else (chaos,)
    actions: list[Action] = []
    for part in parts:
        rng = rng_for(seed, part, namespace=f"chaos-plan:{chaos}")
        actions.extend(_GENERATORS[part](rng, duration))
    environment: dict[str, str] = {}
    if chaos == "A-CH-09":
        environment.update(_clock_skew(rng_for(seed, chaos, namespace="chaos-plan:skew")))
    if chaos == "A-CH-03":
        environment["HOOK_DELAY"] = _HOOK_DELAY
    ordered = sorted(enumerate(actions), key=lambda pair: (pair[1].at, pair[0]))
    return ChaosPlan(chaos, seed, duration, tuple(action for _, action in ordered), environment)
