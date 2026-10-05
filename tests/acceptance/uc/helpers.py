"""Общие шаги сценариев A-UC: создание дерева, чтение строк стенда, опрос снимков."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from tallyho.model.states import BatchState
from tests.acceptance.app.usecases import UC_KIND
from tests.acceptance.chaos.load import Root

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from datetime import datetime
    from uuid import UUID

    from tallyho import BatchBuilder
    from tallyho.model.calls import TaskCall
    from tallyho.model.policy import FailurePolicy
    from tallyho.model.views import BatchView
    from tests.acceptance.uc.context import UcContext

__all__ = [
    "TreeNode",
    "TreeOptions",
    "domain_status",
    "hooks",
    "item_bounds",
    "start_tree",
    "tree",
    "work_calls",
]


@dataclass(frozen=True, slots=True, kw_only=True)
class TreeOptions:
    """Параметры корня, которые сценарии меняют (остальные - умолчания библиотеки)."""

    kind: str = UC_KIND
    start_at: datetime | None = None
    deadline: datetime | timedelta | None = None
    retention: timedelta | None = timedelta(days=14)
    release_required: bool = False
    max_items: int | None = None
    max_in_flight: int | None = None
    failure_policy: FailurePolicy | None = None
    settle: bool = False
    """``on_finalized_task`` - колбэк экспорта ``uc_settle`` (§12.9)."""
    attributes: dict[str, object] = field(default_factory=dict[str, object])


def domain_status(view: BatchView) -> str:
    """Статус, который хук ``on_finalized`` вида A-UC пишет в ``uc_runs.status``."""
    name = view.state.name.lower()
    if view.state is BatchState.FAILED and view.reason is not None:
        return f"{name}:{view.reason.value}"
    return name


async def start_tree(
    ctx: UcContext,
    build: Callable[[BatchBuilder, int], Awaitable[None]],
    options: TreeOptions | None = None,
) -> tuple[int, UUID]:
    """Создать корень, его строку ``uc_runs`` и содержимое в одной транзакции пользователя.

    Returns:
        Номер прогона в ``uc_runs`` и id корня.
    """
    chosen = options or TreeOptions()
    th = ctx.app.th
    async with ctx.transaction() as session:
        run = await ctx.new_run(session, chosen.kind)
        settle = th.call(ctx.app.uc.settle, run) if chosen.settle else None
        async with th.batch(
            chosen.kind,
            key=f"uc:{run}",
            start_at=chosen.start_at,
            deadline=chosen.deadline,
            retention=chosen.retention,
            release_required=chosen.release_required,
            max_items=chosen.max_items,
            max_in_flight=chosen.max_in_flight,
            failure_policy=chosen.failure_policy,
            on_finalized_task=settle,
            attributes=chosen.attributes or None,
            session=session,
        ) as root:
            await build(root, run)
        await ctx.bind_run(session, run, root.handle.id)
    ctx.roots.append(Root(root.handle.id, "UC", run))
    ctx.journal.record("batch_started", "UC", batch_id=str(root.handle.id), run=run)
    return run, root.handle.id


def work_calls(ctx: UcContext, run: int, numbers: Sequence[int]) -> list[TaskCall]:
    """Вызовы ``uc_work`` с ключами ``w:<n>``."""
    th = ctx.app.th
    return [th.call(ctx.app.uc.work, run, n).opts(key=f"w:{n}") for n in numbers]


@dataclass(frozen=True, slots=True)
class TreeNode:
    """Строка ``th_batch`` дерева."""

    id: UUID
    parent_id: UUID | None
    key: str | None
    state: int
    finished_at: datetime | None
    released_at: datetime | None


async def tree(ctx: UcContext, root_id: UUID) -> list[TreeNode]:
    """Все батчи дерева корня."""
    rows = await ctx.root_items_sql(
        """
        SELECT id, parent_id, key, state, finished_at, released_at
          FROM th.th_batch WHERE root_id = :root
        """,
        root_id,
    )
    return [
        TreeNode(
            cast("UUID", row[0]),
            cast("UUID | None", row[1]),
            cast("str | None", row[2]),
            int(cast("int", row[3])),
            cast("datetime | None", row[4]),
            cast("datetime | None", row[5]),
        )
        for row in rows
    ]


async def item_bounds(ctx: UcContext, batch_id: UUID) -> tuple[datetime | None, datetime | None]:
    """Самое раннее и самое позднее завершение Items батча."""
    rows = await ctx.root_items_sql(
        "SELECT min(finished_at), max(finished_at) FROM th.th_item WHERE batch_id = :root",
        batch_id,
    )
    return cast("datetime | None", rows[0][0]), cast("datetime | None", rows[0][1])


async def hooks(ctx: UcContext, batch_id: UUID, hook: str) -> list[tuple[int, str]]:
    """Записи ``hook_log`` батча: состояние и процесс, выполнивший хук."""
    rows = await ctx.root_items_sql(
        """
        SELECT state, app_name FROM app.hook_log
         WHERE batch_id = :root AND hook = :hook ORDER BY id
        """,
        batch_id,
        hook=hook,
    )
    return [(int(cast("int", state)), str(app_name)) for state, app_name in rows]
