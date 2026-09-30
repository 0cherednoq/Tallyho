"""HookRegistry: регистрация, дубли, required_hooks, fallback breach-хука на корень."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from tallyho.hooks.registry import (
    HookName,
    HookRegistry,
    ProgressRegistration,
)
from tallyho.model.errors import ConfigurationError, HookMissingError
from tallyho.storage.tables import PROGRESS_HOOK

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.model.policy import PolicyBreach
    from tallyho.model.views import BatchSummary

KIND = "campaign_deliveries"
EVERY = timedelta(seconds=2)


async def save_result(session: AsyncSession, s: BatchSummary) -> None:
    del session, s
    await asyncio.sleep(0)


async def save_progress(session: AsyncSession, s: BatchSummary) -> None:
    del session, s
    await asyncio.sleep(0)


async def auto_pause(session: AsyncSession, s: BatchSummary, breach: PolicyBreach) -> None:
    del session, s, breach
    await asyncio.sleep(0)


def _full_registry() -> HookRegistry:
    registry = HookRegistry()
    registry.on_finalized(KIND)(save_result)
    registry.on_progress(KIND, every=EVERY)(save_progress)
    registry.on_policy_breach(KIND)(auto_pause)
    return registry


def test_decorators_return_hook_unchanged() -> None:
    registry = HookRegistry()
    finalized = registry.on_finalized(KIND)(save_result)
    progress = registry.on_progress(KIND, EVERY)(save_progress)
    breach = registry.on_policy_breach(KIND)(auto_pause)
    assert finalized is save_result
    assert progress is save_progress
    assert breach is auto_pause


def test_lookup_returns_registered_hooks() -> None:
    registry = _full_registry()
    assert registry.finalized(KIND) is save_result
    assert registry.progress(KIND) == ProgressRegistration(save_progress, EVERY)
    assert registry.policy_breach(KIND) is auto_pause


def test_lookup_of_unknown_kind_is_none() -> None:
    registry = _full_registry()
    assert registry.finalized("other") is None
    assert registry.progress("other") is None
    assert registry.policy_breach("other") is None


def test_required_hooks_lists_registered_hooks_in_fixed_order() -> None:
    registry = HookRegistry()
    assert registry.required_hooks(KIND) == ()
    registry.on_policy_breach(KIND)(auto_pause)
    registry.on_finalized(KIND)(save_result)
    assert registry.required_hooks(KIND) == ("finalized", "policy_breach")
    registry.on_progress(KIND, EVERY)(save_progress)
    assert registry.required_hooks(KIND) == ("finalized", "progress", "policy_breach")
    assert registry.required_hooks("other") == ()


def test_progress_hook_name_matches_snapshotter_index() -> None:
    assert HookName.PROGRESS.value == PROGRESS_HOOK


def test_hooks_are_independent_per_kind() -> None:
    registry = HookRegistry()
    registry.on_finalized("a")(save_result)
    registry.on_finalized("b")(save_result)
    assert registry.required_hooks("a") == ("finalized",)
    assert registry.required_hooks("b") == ("finalized",)


@pytest.mark.parametrize("name", list(HookName))
def test_duplicate_registration_is_configuration_error(name: HookName) -> None:
    registry = _full_registry()
    register: dict[HookName, Callable[[], object]] = {
        HookName.FINALIZED: lambda: registry.on_finalized(KIND),
        HookName.PROGRESS: lambda: registry.on_progress(KIND, EVERY),
        HookName.POLICY_BREACH: lambda: registry.on_policy_breach(KIND),
    }
    with pytest.raises(ConfigurationError, match=f"on_{name.value}.*{KIND}"):
        register[name]()


def test_duplicate_detected_when_decorators_prepared_in_advance() -> None:
    registry = HookRegistry()
    finalized = (registry.on_finalized(KIND), registry.on_finalized(KIND))
    progress = (registry.on_progress(KIND, EVERY), registry.on_progress(KIND, EVERY))
    breach = (registry.on_policy_breach(KIND), registry.on_policy_breach(KIND))
    finalized[0](save_result)
    progress[0](save_progress)
    breach[0](auto_pause)
    with pytest.raises(ConfigurationError, match="on_finalized"):
        finalized[1](save_result)
    with pytest.raises(ConfigurationError, match="on_progress"):
        progress[1](save_progress)
    with pytest.raises(ConfigurationError, match="on_policy_breach"):
        breach[1](auto_pause)


@pytest.mark.parametrize("every", [timedelta(0), timedelta(seconds=-1)])
def test_progress_every_must_be_positive(every: timedelta) -> None:
    with pytest.raises(ConfigurationError, match="every"):
        HookRegistry().on_progress(KIND, every)


def test_empty_kind_is_configuration_error() -> None:
    with pytest.raises(ConfigurationError, match="kind"):
        HookRegistry().on_finalized("")


def test_policy_breach_falls_back_to_root_kind() -> None:
    registry = HookRegistry()
    registry.on_policy_breach(KIND)(auto_pause)
    assert registry.policy_breach("send_stage", root_kind=KIND) is auto_pause
    assert registry.policy_breach("send_stage") is None
    assert registry.policy_breach("send_stage", root_kind="other") is None


def test_policy_breach_prefers_own_kind_over_root() -> None:
    async def own(session: AsyncSession, s: BatchSummary, breach: PolicyBreach) -> None:
        del session, s, breach
        await asyncio.sleep(0)

    registry = HookRegistry()
    registry.on_policy_breach(KIND)(auto_pause)
    registry.on_policy_breach("send_stage")(own)
    assert registry.policy_breach("send_stage", root_kind=KIND) is own


def test_ensure_passes_when_all_required_hooks_present() -> None:
    registry = _full_registry()
    registry.ensure(KIND, ("finalized", "progress", "policy_breach"))
    registry.ensure(KIND, ())
    registry.ensure("other", [])


def test_ensure_raises_for_missing_hook() -> None:
    registry = HookRegistry()
    registry.on_finalized(KIND)(save_result)
    with pytest.raises(HookMissingError) as info:
        registry.ensure(KIND, ["finalized", "progress"])
    assert (info.value.kind, info.value.hook) == (KIND, "on_progress")


def test_ensure_treats_unknown_hook_name_as_missing() -> None:
    with pytest.raises(HookMissingError) as info:
        _full_registry().ensure(KIND, ["from_the_future"])
    assert info.value.hook == "from_the_future"
