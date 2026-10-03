"""Лидер maintenance — у каждой установки, а не у схемы (ARCHITECTURE §3.2)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from tallyho.engine.installation import create_installation
from tests.integration.engine.test_maintenance import eventually, maintenance, services, stop_all

if TYPE_CHECKING:
    from tests.integration.engine.conftest import Env

__all__: list[str] = []


async def test_installations_with_different_prefixes_have_own_leaders(env: Env) -> None:
    default = create_installation(env.engine, env.schema, "th_")
    jobs = create_installation(env.engine, env.schema, "jobs_")
    first = maintenance(env, services(), identity=default.maintenance_identity)
    second = maintenance(env, services(), identity=jobs.maintenance_identity)
    pairs = (
        (first, asyncio.create_task(first.run())),
        (second, asyncio.create_task(second.run())),
    )
    try:
        await eventually(lambda: first.is_leader and second.is_leader)
    finally:
        await stop_all(*pairs)


async def test_same_installation_in_two_processes_has_one_leader(env: Env) -> None:
    identity = create_installation(env.engine, env.schema, "jobs_").maintenance_identity
    first = maintenance(env, services(), identity=identity)
    second = maintenance(env, services(), identity=identity)
    pairs = (
        (first, asyncio.create_task(first.run())),
        (second, asyncio.create_task(second.run())),
    )
    try:
        await eventually(lambda: first.is_leader or second.is_leader)
        await asyncio.sleep(0.2)
        assert first.is_leader != second.is_leader
    finally:
        await stop_all(*pairs)
