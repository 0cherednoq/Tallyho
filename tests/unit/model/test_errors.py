"""Иерархия ошибок: все в TallyhoError, родители стабильны, данные доступны."""

from __future__ import annotations

from uuid import UUID

import pytest

import tallyho
import tallyho.model
from tallyho.model import attributes, calls, errors, policy, progress, states, views

# Снимок родителей: пользователи ловят ошибки по базовым классам.
PARENTS: dict[type[errors.TallyhoError], type[errors.TallyhoError]] = {
    errors.ConfigurationError: errors.TallyhoError,
    errors.NotFoundError: errors.TallyhoError,
    errors.InvalidStateError: errors.TallyhoError,
    errors.ConcurrentModification: errors.TallyhoError,
    errors.CompleterError: errors.TallyhoError,
    errors.LeaseLostError: errors.TallyhoError,
    errors.HookTransactionError: errors.TallyhoError,
    errors.ClosedError: errors.InvalidStateError,
    errors.SealError: errors.InvalidStateError,
    errors.SpawnTargetError: errors.InvalidStateError,
    errors.DownstreamFinalized: errors.InvalidStateError,
    errors.BatchPurged: errors.NotFoundError,
    errors.HookMissingError: errors.ConfigurationError,
    errors.InvalidAttributesError: errors.ConfigurationError,
    errors.UnsupportedOption: errors.ConfigurationError,
}


def test_snapshot_covers_every_exported_error() -> None:
    exported = {getattr(errors, name) for name in errors.__all__} - {errors.TallyhoError}
    assert exported == set(PARENTS)


@pytest.mark.parametrize(("exc_type", "parent"), PARENTS.items(), ids=lambda t: t.__name__)
def test_error_parent(exc_type: type[errors.TallyhoError], parent: type[Exception]) -> None:
    assert exc_type.__bases__ == (parent,)
    assert issubclass(exc_type, tallyho.TallyhoError)


def test_batch_purged_carries_batch_id() -> None:
    batch_id = UUID(int=42)
    exc = errors.BatchPurged(batch_id)
    assert exc.batch_id == batch_id
    assert str(batch_id) in str(exc)


def test_lease_lost_carries_item_id() -> None:
    item_id = UUID(int=7)
    exc = errors.LeaseLostError(item_id)
    assert exc.item_id == item_id
    assert str(item_id) in str(exc)


def test_hook_missing_names_kind_and_hook() -> None:
    exc = errors.HookMissingError("campaign_deliveries", "on_finalized")
    assert (exc.kind, exc.hook) == ("campaign_deliveries", "on_finalized")
    assert "campaign_deliveries" in str(exc)
    assert "on_finalized" in str(exc)


def test_unsupported_option_with_hint() -> None:
    exc = errors.UnsupportedOption("depends_on", hint="используйте этапы fed_by")
    assert exc.option == "depends_on"
    assert exc.hint == "используйте этапы fed_by"
    assert str(exc).endswith(": используйте этапы fed_by")


def test_model_package_reexports_public_names() -> None:
    sources = {name: getattr(errors, name) for name in errors.__all__}
    sources |= {name: getattr(states, name) for name in states.__all__}
    sources |= {name: getattr(views, name) for name in views.__all__}
    sources |= {name: getattr(policy, name) for name in policy.__all__}
    sources |= {name: getattr(calls, name) for name in calls.__all__}
    sources |= {name: getattr(progress, name) for name in progress.__all__}
    sources |= {name: getattr(attributes, name) for name in attributes.__all__}
    for constant in (
        "TERMINAL_THRESHOLD",
        "DEFAULT_ESTIMATE_MIN_BASIS",
        "DEFAULT_ESTIMATE_MIN_SHARE",
        "DEFAULT_ETA_WINDOW",
    ):
        del sources[constant]
    assert set(tallyho.model.__all__) == set(sources)
    for name, obj in sources.items():
        assert getattr(tallyho.model, name) is obj


def test_closed_error_explains_itself_and_accepts_detail() -> None:
    assert "aclose" in str(errors.ClosedError())
    assert str(errors.ClosedError("Completer закрыт")) == "Completer закрыт"


def test_unsupported_option_without_hint() -> None:
    exc = errors.UnsupportedOption("debounce")
    assert exc.hint is None
    assert str(exc) == "опция 'debounce' не поддерживается для отслеживаемых задач"
