"""Продюсер: создание батчей и под-батчей, этапы, добавление Items, seal, expect.

ARCHITECTURE UC-01, UC-02, UC-06, §6.1, §8.1 п.1-2, §11.2.

Все методы :class:`Producer` работают на переданном ``AsyncConnection`` в
открытой транзакции (D-004): это транзакция пользователя (UC-01, откат не
оставляет ни строки) или своя. Commit и ``after_commit`` — забота вызывающего.
«Сейчас» в SQL — только ``sql_now(clock)`` (D-002). Счётчики продюсер пишет
сразу в слот ``th_counter`` (:func:`~tallyho.storage.counters.upsert_slots`),
слот передаёт вызывающий.

Порядок блокировок тот же, что у Completer (§9.2): ``th_batch`` → ``th_item``
→ ``th_counter``.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy import BigInteger, DateTime, Interval, func, literal, select, update
from sqlalchemy.dialects.postgresql import insert

from tallyho.model.errors import ConfigurationError, NotFoundError
from tallyho.model.states import BatchState
from tallyho.storage.now import sql_now

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.hooks.registry import HookRegistry
    from tallyho.model.calls import TaskCall
    from tallyho.model.policy import FailurePolicy
    from tallyho.protocols.clock import Clock
    from tallyho.protocols.ids import IdFactory
    from tallyho.protocols.serialization import PayloadCodec
    from tallyho.storage.tables import Tables

__all__ = [
    "MAX_PAYLOAD_BYTES",
    "BatchRef",
    "CallbackName",
    "Producer",
    "RootSpec",
    "StoredCallback",
]

MAX_PAYLOAD_BYTES: Final = 1024 * 1024
"""Предел закодированного payload по умолчанию: ``max_payload_bytes`` flexiq (1 MiB)."""


class CallbackName(StrEnum):
    """Колбэк-задачи батча (ARCHITECTURE §11.2); ключи ``options["callbacks"]``."""

    ON_SUCCEEDED = "on_succeeded"
    ON_COMPLETED_WITH_ERRORS = "on_completed_with_errors"
    ON_FAILED = "on_failed"
    ON_CANCELLED = "on_cancelled"
    ON_FINALIZED_TASK = "on_finalized_task"


_CALLBACK_BROKEN = "options.callbacks: повреждённая запись колбэка"
_OPTIONS_NOT_JSON = "опции колбэка должны сериализоваться в JSON"


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredCallback:
    """Колбэк-задача в ``th_batch.options``: аргументы уже закодированы кодеком.

    Finalizer (T4.4) ставит его в ``th_outbox`` как есть.
    """

    task_name: str
    payload: bytes
    queue: str | None = None
    options: Mapping[str, object] = field(default_factory=dict[str, object])

    def to_json(self) -> dict[str, object]:
        """Представление для jsonb.

        Returns:
            Словарь; payload — base64.

        Raises:
            ConfigurationError: опции брокера не сериализуются в JSON.
        """
        options = dict(self.options)
        try:
            _ = json.dumps(options)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(_OPTIONS_NOT_JSON) from exc
        return {
            "task_name": self.task_name,
            "payload": base64.b64encode(self.payload).decode("ascii"),
            "queue": self.queue,
            "options": options,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, object]) -> StoredCallback:
        """Восстановить колбэк из :meth:`to_json`.

        Returns:
            Колбэк, равный исходному.

        Raises:
            ConfigurationError: данные повреждены.
        """
        task_name = data.get("task_name")
        payload = data.get("payload")
        queue = data.get("queue")
        options = data.get("options")
        if not (
            isinstance(task_name, str)
            and isinstance(payload, str)
            and (queue is None or isinstance(queue, str))
            and isinstance(options, dict)
        ):
            raise ConfigurationError(_CALLBACK_BROKEN)
        try:
            raw = base64.b64decode(payload, validate=True)
        except ValueError as exc:  # binascii.Error и не-ASCII строка
            raise ConfigurationError(_CALLBACK_BROKEN) from exc
        return cls(
            task_name=task_name,
            payload=raw,
            queue=queue,
            options=cast("dict[str, object]", options),
        )


def _check_optional(name: str, value: int | None, minimum: int) -> None:
    if value is None:
        return
    if isinstance(value, bool) or value < minimum:
        message = f"{name} должен быть целым >= {minimum}, получено {value!r}"
        raise ConfigurationError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class _BatchSpec:
    """Параметры, общие для корня и под-батча (ARCHITECTURE §11.2)."""

    start_at: datetime | None = None
    deadline: datetime | timedelta | None = None
    callbacks: Mapping[CallbackName, TaskCall] = field(
        default_factory=dict["CallbackName", "TaskCall"]
    )
    failure_policy: FailurePolicy | None = None
    max_in_flight: int | None = None
    expected_total: int | None = None

    def _check(self) -> None:
        _check_optional("max_in_flight", self.max_in_flight, 1)
        _check_optional("expected_total", self.expected_total, 0)


@dataclass(frozen=True, slots=True, kw_only=True)
class RootSpec(_BatchSpec):
    """Параметры корня: ``th.batch(kind, key=, ...)``.

    ``retention``, ``release_required`` и ``max_items`` задаются только на
    корне и наследуются под-батчами.
    """

    kind: str
    key: str | None = None
    max_items: int | None = None
    retention: timedelta | None = None
    release_required: bool = False

    def __post_init__(self) -> None:
        """Проверить параметры.

        Raises:
            ConfigurationError: пустой ``kind`` или недопустимый лимит.
        """
        if not self.kind:
            message = "kind не может быть пустым"
            raise ConfigurationError(message)
        self._check()
        _check_optional("max_items", self.max_items, 1)


@dataclass(frozen=True, slots=True, kw_only=True)
class BatchRef:
    """Созданный или найденный батч.

    Attributes:
        id: Идентификатор батча.
        root_id: Корень дерева (у корня — он сам).
        created: ``False`` — батч с таким ключом уже был (идемпотентный повтор).
    """

    id: UUID
    root_id: UUID
    created: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class Producer:
    """Операции продюсера над батчами; без состояния, кроме зависимостей.

    Attributes:
        tables: Таблицы установки.
        clock: Часы: «сейчас» в SQL.
        ids: Фабрика идентификаторов батчей и Items.
        codec: Кодек payload адаптера.
        hooks: Реестр tx-хуков: ``th_batch.hooks`` заполняется при создании.
        slot: Слот ``th_counter`` этого процесса.
        max_payload_bytes: Предел закодированного payload.
    """

    tables: Tables
    clock: Clock
    ids: IdFactory
    codec: PayloadCodec
    hooks: HookRegistry
    slot: int = 0
    max_payload_bytes: int = MAX_PAYLOAD_BYTES

    # --- корень ------------------------------------------------------------

    async def create_root(self, conn: AsyncConnection, spec: RootSpec) -> BatchRef:
        """Создать корень или вернуть существующий с тем же ``(kind, key)`` (UC-01).

        ``INSERT … ON CONFLICT (kind, key) DO NOTHING RETURNING``: при
        конфликте параллельная транзакция ждёт первую и после её commit
        находит тот же батч.

        Args:
            conn: Соединение в открытой транзакции.
            spec: Параметры корня.

        Returns:
            Ссылка на батч; ``created=False`` для существующего.
        """
        batch = self.tables.batch
        batch_id = self.ids.new_id()
        now = sql_now(self.clock)
        values: dict[str, object] = {
            **self._common_values(spec),
            "id": batch_id,
            "root_id": batch_id,
            "kind": spec.kind,
            "key": spec.key,
            "hooks": list(self.hooks.required_hooks(spec.kind)),
            "max_items": spec.max_items,
            "retention": spec.retention,
            "release_required": spec.release_required,
            "created_at": now,
            "updated_at": now,
            "deadline_at": self._deadline(spec.deadline),
        }
        stmt = (
            insert(batch)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[batch.c.kind, batch.c.key],
                index_where=batch.c.parent_id.is_(None) & batch.c.key.is_not(None),
            )
            .returning(batch.c.id)
        )
        inserted = await conn.scalar(stmt)
        if inserted is not None:
            return BatchRef(id=inserted, root_id=inserted, created=True)
        # Конфликт возможен только при заданном key: партиальный индекс его требует.
        found = await conn.execute(
            select(batch.c.id).where(
                batch.c.kind == spec.kind, batch.c.key == spec.key, batch.c.parent_id.is_(None)
            )
        )
        existing = found.scalar_one()
        return BatchRef(id=existing, root_id=existing, created=False)

    # --- expect ------------------------------------------------------------

    async def expect(self, conn: AsyncConnection, batch_id: UUID, n: int) -> None:
        """Сообщить ожидаемое число Items: ``expected_total = GREATEST(expected_total, n)``.

        Значение только растёт; ``NULL`` в ``GREATEST`` PostgreSQL игнорирует.
        ``n < 0`` — ``ConfigurationError``.

        Args:
            conn: Соединение в открытой транзакции.
            batch_id: Батч.
            n: Ожидаемое число Items, ``>= 0``.

        Raises:
            NotFoundError: батча нет.
        """
        _check_optional("n", n, 0)
        batch = self.tables.batch
        stmt = (
            update(batch)
            .where(batch.c.id == batch_id)
            .values(expected_total=_greatest(batch.c.expected_total, n))
            .returning(batch.c.id)
        )
        if await conn.scalar(stmt) is None:
            raise NotFoundError(str(batch_id))

    # --- общее -------------------------------------------------------------

    def _common_values(self, spec: _BatchSpec) -> dict[str, object]:
        return {
            "state": int(BatchState.OPEN),
            "start_at": spec.start_at,
            "options": self._options(spec),
            "expected_total": spec.expected_total,
            "max_in_flight": spec.max_in_flight,
        }

    def _options(self, spec: _BatchSpec) -> dict[str, object]:
        options: dict[str, object] = {}
        if spec.failure_policy is not None:
            options["failure_policy"] = spec.failure_policy.to_json()
        if spec.callbacks:
            options["callbacks"] = {
                CallbackName(name).value: self._callback(call).to_json()
                for name, call in spec.callbacks.items()
            }
        return options

    def _callback(self, call: TaskCall) -> StoredCallback:
        return StoredCallback(
            task_name=call.task_name,
            payload=self._encode(call),
            queue=call.queue,
            options=call.options,
        )

    def _encode(self, call: TaskCall) -> bytes:
        payload = self.codec.encode(call.task_name, call.args, call.kwargs)
        if len(payload) > self.max_payload_bytes:
            message = (
                f"payload задачи {call.task_name!r} занимает {len(payload)} байт,"
                f" предел {self.max_payload_bytes}"
            )
            raise ConfigurationError(message)
        return payload

    def _deadline(self, deadline: datetime | timedelta | None) -> ColumnElement[datetime] | None:
        if deadline is None:
            return None
        if isinstance(deadline, timedelta):
            return sql_now(self.clock) + literal(deadline, Interval())
        return literal(deadline, DateTime(timezone=True))


def _greatest(column: ColumnElement[int], value: int) -> ColumnElement[int]:
    return func.greatest(column, literal(value, BigInteger()))
