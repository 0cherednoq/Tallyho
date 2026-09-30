"""Транзакции storage без БД: классификация ошибок, backoff, приём сессий."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from tallyho.model.errors import HookTransactionError
from tallyho.storage.tx import (
    RETRYABLE_SQLSTATES,
    HookSession,
    RetryPolicy,
    after_commit,
    is_retryable,
    sqlstate_of,
)
from tests.helpers.probe import seeded

if TYPE_CHECKING:
    from sqlalchemy.exc import DBAPIError


class DriverError(Exception):
    """Ошибка драйвера с кодом SQLSTATE, как у asyncpg/psycopg."""

    def __init__(self, sqlstate: object) -> None:
        super().__init__("driver error")
        self.sqlstate: object = sqlstate


def _wrapped(sqlstate: object) -> DBAPIError:
    return OperationalError("SELECT 1", None, DriverError(sqlstate))


@pytest.mark.parametrize("code", sorted(RETRYABLE_SQLSTATES))
def test_conflicts_are_retryable(code: str) -> None:
    exc = _wrapped(code)

    assert sqlstate_of(exc) == code
    assert is_retryable(exc)


def test_retryable_codes_match_architecture() -> None:
    assert {"40001", "40P01", "55P03"} == RETRYABLE_SQLSTATES


def test_other_database_errors_are_not_retryable() -> None:
    exc = _wrapped("23505")

    assert sqlstate_of(exc) == "23505"
    assert not is_retryable(exc)


def test_missing_or_odd_sqlstate_is_none() -> None:
    assert sqlstate_of(_wrapped(None)) is None
    assert sqlstate_of(_wrapped(40001)) is None
    assert sqlstate_of(OperationalError("SELECT 1", None, Exception("no code"))) is None


def test_non_database_errors_are_not_retryable() -> None:
    assert sqlstate_of(RuntimeError("40P01")) is None
    assert not is_retryable(RuntimeError("40P01"))


def test_backoff_grows_exponentially_up_to_cap() -> None:
    policy = RetryPolicy(base_delay=0.1, max_delay=1.0)
    rng = seeded(7)

    delays = [policy.delay(retry, rng) for retry in range(8)]

    caps = [min(1.0, 0.1 * 2**retry) for retry in range(8)]
    for delay, cap in zip(delays, caps, strict=True):
        assert cap / 2 <= delay <= cap
    assert max(delays) <= 1.0


def test_jitter_is_deterministic_for_seed() -> None:
    policy = RetryPolicy()

    first = [policy.delay(retry, seeded(42)) for retry in range(5)]
    second = [policy.delay(retry, seeded(42)) for retry in range(5)]

    assert first == second


def test_jitter_spreads_colliding_transactions() -> None:
    policy = RetryPolicy()
    rng = seeded(1)

    delays = {policy.delay(3, rng) for _ in range(20)}

    assert len(delays) > 1


def test_default_policy() -> None:
    policy = RetryPolicy()

    assert policy.attempts == 5
    assert policy.base_delay == pytest.approx(0.05)
    assert policy.max_delay == pytest.approx(2.0)


async def test_after_commit_needs_open_connection() -> None:
    engine = create_async_engine("postgresql+asyncpg://")
    conn = AsyncConnection(engine)

    with pytest.raises(TypeError, match="AsyncConnection"):
        await after_commit(conn, lambda: None)


async def test_hook_session_forbids_transaction_control() -> None:
    session = HookSession()

    assert isinstance(session, AsyncSession)
    with pytest.raises(HookTransactionError, match="commit"):
        await session.commit()
    with pytest.raises(HookTransactionError, match="rollback"):
        await session.rollback()
    with pytest.raises(HookTransactionError, match="close"):
        await session.close()
