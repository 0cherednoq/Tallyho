"""Встраивание миграций tallyho в Alembic пользователя (ARCHITECTURE §11.1).

В ревизии Alembic::

    from tallyho.storage.alembic import upgrade as tallyho_upgrade


    def upgrade() -> None:
        tallyho_upgrade(op, version=1, schema="app")  # первая ревизия


    # В следующей ревизии:
    def upgrade() -> None:
        tallyho_upgrade(op, version=2, schema="app")

Версия указывается в ревизии явно: ревизия не должна менять смысл, когда
обновляется библиотека. Для новой версии схемы tallyho пишется новая ревизия
с ``version=2``. Выполняются те же операции, что у
:func:`tallyho.storage.migrations.migrate`, включая ``SET LOCAL lock_timeout``
и запись версии в ``th_meta``, поэтому ``migrate()`` после ревизии ничего не
делает. Транзакцией и очерёдностью управляет Alembic: advisory lock не берётся.

Alembic — необязательная зависимость (extra ``alembic``): модуль импортирует
его только для аннотаций, поэтому импорт модуля без alembic не падает, а
``op`` приносит сам Alembic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tallyho.storage.migrations import DEFAULT_LOCK_TIMEOUT, migration_statements
from tallyho.storage.tables import DEFAULT_PREFIX

if TYPE_CHECKING:
    from datetime import timedelta

    from alembic.operations import Operations

__all__ = ["upgrade"]


def upgrade(
    op: Operations,
    *,
    version: int,
    schema: str | None,
    prefix: str = DEFAULT_PREFIX,
    lock_timeout: timedelta = DEFAULT_LOCK_TIMEOUT,
) -> None:
    """Применить миграцию версии ``version`` через ``op`` ревизии Alembic.

    Работает и в offline-режиме (``alembic upgrade --sql``).

    Args:
        op: объект ``alembic.op`` текущей ревизии.
        version: версия схемы tallyho, которую создаёт ревизия.
        schema: схема установки (создаётся, если её нет) или ``None``.
        prefix: префикс имён таблиц.
        lock_timeout: сколько DDL ждёт чужие блокировки.

    Неизвестная версия, неверные схема, префикс или ``lock_timeout`` —
    ``ConfigurationError``, как у :func:`~tallyho.storage.migrations.migration_statements`.
    """
    for statement in migration_statements(
        version, schema=schema, prefix=prefix, lock_timeout=lock_timeout
    ):
        op.execute(statement)
