"""Разбор и применение `defer_on` / `error_on` / `skip_on` (RM8.3, UC-18)."""

from __future__ import annotations

from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest

from tallyho.model.errors import ConfigurationError
from tallyho.model.outcomes import Defer, Error, Ok, Skip
from tallyho.runtime.exceptions import build_exception_policy

if TYPE_CHECKING:
    from collections.abc import Callable

    from tallyho.runtime.exceptions import ExceptionPolicy

__all__: list[str] = []


class ProviderUnavailableError(RuntimeError):
    """Временная недоступность поставщика: ретрай брокера."""


class NoCheckerAccountError(RuntimeError):
    """Ресурса нет: отложить."""

    def __init__(self, reason: str = "no_accounts") -> None:
        super().__init__(reason)
        self.reason: str = reason


class NoCheckerAccountAtAllError(NoCheckerAccountError):
    """Более конкретный случай того же: проверяем выбор по MRO."""


class InvalidDomainNameError(ValueError):
    """Неисправимо: ошибочный итог без ретраев."""


def test_empty_policy_is_falsy_and_translates_nothing() -> None:
    policy = build_exception_policy()
    assert not policy
    assert policy.outcome_for(ProviderUnavailableError()) is None


def test_constant_rules_for_every_option() -> None:
    policy = build_exception_policy(
        defer_on={NoCheckerAccountError: Defer(reason="no_accounts", delay=timedelta(seconds=10))},
        error_on={InvalidDomainNameError: "invalid_domain"},
        skip_on={KeyError: Skip("duplicate")},
    )
    assert policy
    assert policy.outcome_for(NoCheckerAccountError()) == Defer(
        reason="no_accounts", delay=timedelta(seconds=10)
    )
    assert policy.outcome_for(InvalidDomainNameError()) == Error("invalid_domain")
    assert policy.outcome_for(KeyError()) == Skip("duplicate")
    assert policy.outcome_for(ProviderUnavailableError()) is None


def _defer_with_reason(exc: BaseException) -> Defer:
    return Defer(reason=exc.args[0] if exc.args else "no_accounts")


def _error_with_text(exc: BaseException) -> Error:
    return Error("invalid", detail={"text": str(exc)})


def _wrong_kind(_exc: BaseException) -> Ok:
    return Ok("нет")


def test_callable_rule_reads_the_exception() -> None:
    policy = build_exception_policy(
        defer_on={NoCheckerAccountError: _defer_with_reason},
        error_on={InvalidDomainNameError: _error_with_text},
    )
    assert policy.outcome_for(NoCheckerAccountError("quota")) == Defer(reason="quota")
    assert policy.outcome_for(InvalidDomainNameError("плохое имя")) == Error(
        "invalid", detail={"text": "плохое имя"}
    )


def test_most_specific_class_in_mro_wins() -> None:
    policy = build_exception_policy(
        defer_on={
            NoCheckerAccountError: Defer(reason="база"),
            NoCheckerAccountAtAllError: Defer(reason="конкретный"),
        }
    )
    assert policy.outcome_for(NoCheckerAccountAtAllError()) == Defer(reason="конкретный")
    assert policy.outcome_for(NoCheckerAccountError()) == Defer(reason="база")


def test_declaration_order_does_not_matter() -> None:
    first = build_exception_policy(
        defer_on={
            NoCheckerAccountAtAllError: Defer(reason="конкретный"),
            NoCheckerAccountError: Defer(reason="база"),
        }
    )
    assert first.outcome_for(NoCheckerAccountAtAllError()) == Defer(reason="конкретный")


def test_same_class_in_two_options_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="одна судьба"):
        _ = build_exception_policy(
            defer_on={InvalidDomainNameError: Defer(reason="x")},
            error_on={InvalidDomainNameError: "invalid"},
        )


def test_class_in_retry_on_and_rules_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="retry_on"):
        _ = build_exception_policy(
            defer_on={ProviderUnavailableError: Defer(reason="x")},
            retry_on=(ProviderUnavailableError,),
        )


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (lambda: build_exception_policy(defer_on=[NoCheckerAccountError]), "словарь"),
        (
            lambda: build_exception_policy(defer_on={"NoCheckerAccount": Defer(reason="x")}),
            "класс исключения",
        ),
        (lambda: build_exception_policy(defer_on={NoCheckerAccountError: "no_accounts"}), "Defer"),
        (lambda: build_exception_policy(defer_on={NoCheckerAccountError: Ok("нет")}), "Defer"),
        (lambda: build_exception_policy(error_on={InvalidDomainNameError: 7}), "error_on"),
    ],
)
def test_bad_rule_shape_is_rejected(build: Callable[[], ExceptionPolicy], match: str) -> None:
    with pytest.raises(ConfigurationError, match=match):
        _ = build()


def test_mapper_returning_other_outcome_class_is_an_error() -> None:
    policy = build_exception_policy(defer_on={NoCheckerAccountError: _wrong_kind})
    with pytest.raises(ConfigurationError, match="другого класса"):
        _ = policy.outcome_for(NoCheckerAccountError())


def test_rules_are_read_only() -> None:
    policy = build_exception_policy(defer_on={NoCheckerAccountError: Defer(reason="x")})
    assert isinstance(policy.rules, MappingProxyType)
