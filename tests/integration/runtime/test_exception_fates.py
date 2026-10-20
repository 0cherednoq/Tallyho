"""Судьба исключения задачи: ``defer_on`` / ``error_on`` / ``skip_on`` (RM8.3, UC-18)."""

from __future__ import annotations

import asyncio
import functools
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, ParamSpec, TypeVar

import pytest
from sqlalchemy import select
from typing_extensions import override

from tallyho.engine.spawn import TreeCache
from tallyho.model.errors import ConfigurationError, LeaseLostError
from tallyho.model.outcomes import Defer, Error, Ok, Skip
from tallyho.model.states import ItemState
from tallyho.protocols.broker import CancellationClassifier, DeadLetters, Runtime, Verdict
from tallyho.runtime import TaskRuntime, item
from tallyho.runtime.exceptions import build_exception_policy
from tests.helpers.relay import RecordingDispatcher
from tests.integration.engine.completer_env import NOW, lease_row, open_completer, seed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from uuid import UUID

    from sqlalchemy import RowMapping

    from tallyho.engine.completer import Completer, ItemRef
    from tallyho.runtime.exceptions import ExceptionPolicy
    from tests.integration.engine.conftest import Env

__all__: list[str] = []

P = ParamSpec("P")
R = TypeVar("R")

DELAY = timedelta(minutes=5)


class NoResourceError(RuntimeError):
    """Ресурса нет: судьба — отложить."""

    def __init__(self, reason: str = "no_accounts") -> None:
        super().__init__(reason)
        self.reason: str = reason


class QuotaError(NoResourceError):
    """Частный случай нехватки ресурса: проверка выбора по MRO."""


class InvalidInputError(ValueError):
    """Неисправимо: ошибочный итог без ретраев."""


class AlreadyDoneError(RuntimeError):
    """Работа уже сделана: пропуск."""


class ProviderDownError(RuntimeError):
    """Временный сбой: судьба не объявлена, идёт обычным путём."""


@dataclass
class Broker(Runtime, CancellationClassifier):
    verdict: Verdict = Verdict.FINAL
    cancels: bool = False
    asked: int = 0

    @override
    def wrap(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await fn(*args, **kwargs)

        return functools.update_wrapper(wrapper, fn)

    @override
    def retry_verdict(self, exc: BaseException) -> Verdict:
        self.asked += 1
        return self.verdict

    @override
    def is_cancelled(self, exc: BaseException) -> bool:
        return self.cancels

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
        defer_default_delay=DELAY,
    )


def _defer_with_reason(exc: BaseException) -> Defer:
    reason = exc.reason if isinstance(exc, NoResourceError) else "no_accounts"
    return Defer(reason=reason)


def _policy() -> ExceptionPolicy:
    return build_exception_policy(
        defer_on={
            NoResourceError: _defer_with_reason,
            QuotaError: Defer(reason="quota", delay=timedelta(minutes=1)),
        },
        error_on={InvalidInputError: "invalid_input"},
        skip_on={AlreadyDoneError: Skip("duplicate")},
    )


def _marker(ref: ItemRef) -> dict[str, UUID]:
    return {"i": ref.id, "b": ref.batch_id}


async def _item(env: Env, item_id: UUID) -> RowMapping:
    table = env.tables.item
    async with env.connection() as conn:
        return (await conn.execute(select(table).where(table.c.id == item_id))).mappings().one()


async def _outbox(env: Env, item_id: UUID) -> RowMapping | None:
    outbox = env.tables.outbox
    async with env.connection() as conn:
        result = await conn.execute(select(outbox).where(outbox.c.item_id == item_id))
        return result.mappings().one_or_none()


async def test_declared_exception_becomes_defer_without_spending_attempt(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker()
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise NoResourceError

        # Исключение не выходит наружу: это итог попытки, а не её провал.
        await _runtime(completer, broker).wrap(task, policy=_policy())(_th=_marker(ref))

    assert broker.asked == 0
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["attempt"], row["defers"]) == (ItemState.ACTIVE, 0, 1)
    assert await lease_row(env, ref.id) is None
    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == "no_accounts"
    assert entry["available_at"] == NOW + DELAY


async def test_most_specific_rule_in_mro_wins(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise QuotaError

        await _runtime(completer).wrap(task, policy=_policy())(_th=_marker(ref))

    entry = await _outbox(env, ref.id)
    assert entry is not None
    assert entry["defer_reason"] == "quota"
    assert entry["available_at"] == NOW + timedelta(minutes=1)


async def test_declared_exception_becomes_error_without_retry(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker(verdict=Verdict.RETRY)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise InvalidInputError

        await _runtime(completer, broker).wrap(task, policy=_policy())(_th=_marker(ref))

    # Вердикт о ретрае даже не спрашивают: судьба объявлена.
    assert broker.asked == 0
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.ERROR, "invalid_input")
    assert (await env.counters(ref.batch_id)).error == 1


async def test_declared_exception_becomes_skip(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise AlreadyDoneError

        await _runtime(completer).wrap(task, policy=_policy())(_th=_marker(ref))

    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.SKIP, "duplicate")


async def test_undeclared_exception_keeps_broker_verdict(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker(verdict=Verdict.RETRY)
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise ProviderDownError

        with pytest.raises(ProviderDownError):
            await _runtime(completer, broker).wrap(task, policy=_policy())(_th=_marker(ref))

    assert broker.asked == 1
    # Вердикт RETRY: lease отпущен, попытка потрачена, итога нет.
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["attempt"]) == (ItemState.ACTIVE, 1)


async def test_lease_lost_is_never_translated(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    policy = build_exception_policy(defer_on={LeaseLostError: Defer(reason="нельзя")})
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise LeaseLostError(ref.id)

        # Путь владения принадлежит учёту работы, а не задаче: перевода нет.
        with pytest.raises(LeaseLostError):
            await _runtime(completer).wrap(task, policy=policy)(_th=_marker(ref))

    assert await _outbox(env, ref.id) is None


async def test_cancellation_is_never_translated(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    policy = build_exception_policy(defer_on={asyncio.CancelledError: Defer(reason="нельзя")})
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await _runtime(completer).wrap(task, policy=policy)(_th=_marker(ref))

    # Отмена отпускает lease и ничего не записывает (UC-03).
    row = await _item(env, ref.id)
    assert ItemState(row["state"]) is ItemState.ACTIVE
    assert await _outbox(env, ref.id) is None


async def test_failing_rule_falls_back_to_broker_verdict(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    broker = Broker()

    def broken(_exc: BaseException) -> Defer:
        message = "правило сломано"
        raise RuntimeError(message)

    policy = build_exception_policy(defer_on={NoResourceError: broken})
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise NoResourceError

        with pytest.raises(NoResourceError):
            await _runtime(completer, broker).wrap(task, policy=policy)(_th=_marker(ref))

    # Исходное исключение идёт обычным путём: вердикт брокера, итог exhausted.
    assert broker.asked == 1
    row = await _item(env, ref.id)
    assert (ItemState(row["state"]), row["label"]) == (ItemState.ERROR, "exhausted")


async def test_rule_returning_other_outcome_class_is_a_configuration_error(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]

    def wrong(_exc: BaseException) -> Ok:
        return Ok("нет")

    policy = build_exception_policy(defer_on={NoResourceError: wrong})
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            raise NoResourceError

        # ConfigurationError правила — тоже падение правила: обычный путь.
        with pytest.raises(NoResourceError):
            await _runtime(completer).wrap(task, policy=policy)(_th=_marker(ref))


async def test_translated_outcome_conflicts_with_imperative_one(env: Env) -> None:
    seeded = await seed(env, 1)
    ref = seeded.refs[0]
    async with open_completer(env) as completer:

        async def task(**_kwargs: object) -> None:
            await asyncio.sleep(0)
            item.ok("первый")
            raise NoResourceError

        with pytest.raises(ConfigurationError, match="дважды"):
            await _runtime(completer).wrap(task, policy=_policy())(_th=_marker(ref))


def test_policy_must_be_a_policy() -> None:
    with pytest.raises(ConfigurationError, match="ExceptionPolicy"):
        _ = TaskRuntime(
            completer=None,  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # до claim не доходим
            broker=Broker(),
            dispatcher=RecordingDispatcher(),
            tree_cache=TreeCache(),
            heartbeat_every=timedelta(seconds=20),
        ).wrap(_noop, policy=Error("не политика"))


async def _noop() -> None:
    await asyncio.sleep(0)
