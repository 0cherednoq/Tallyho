"""Иерархия ошибок: все в TallyhoError, родители стабильны, данные доступны."""

from __future__ import annotations

from uuid import UUID

import pytest

import tallyho
from tallyho.model import errors

# Снимок родителей: пользователи ловят ошибки по базовым классам.
PARENTS: dict[type[errors.TallyhoError], type[errors.TallyhoError]] = {
    errors.ConfigurationError: errors.TallyhoError,
    errors.NotFoundError: errors.TallyhoError,
    errors.InvalidStateError: errors.TallyhoError,
    errors.ConcurrentModification: errors.TallyhoError,
    errors.HookTransactionError: errors.TallyhoError,
    errors.SealError: errors.InvalidStateError,
    errors.SpawnTargetError: errors.InvalidStateError,
    errors.DownstreamFinalized: errors.InvalidStateError,
    errors.BatchPurged: errors.NotFoundError,
    errors.HookMissingError: errors.ConfigurationError,
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


def test_unsupported_option_without_hint() -> None:
    exc = errors.UnsupportedOption("debounce")
    assert exc.hint is None
    assert str(exc) == "опция 'debounce' не поддерживается для отслеживаемых задач"
