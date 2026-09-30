"""Счётчики батчей: дельты, суммы и запросы к ``th_counter`` (ARCHITECTURE §9).

Счётчики хранятся двумя путями (§9.1, COUNTERS §3.3):

* путь A — Completer прибавляет дельту к строке ``th_counter`` своего слота;
* путь B — транзакция пользователя только вставляет строку в
  ``th_counter_delta``, а Completer потом сворачивает её в слот.

Здесь описаны значения, которыми обмениваются эти пути:
:class:`CounterDelta` (приращение) и :class:`CounterTotals` (точная сумма
слотов и несвёрнутых дельт), и запросы к таблицам счётчиков.

Функции принимают ``AsyncConnection`` в открытой транзакции (D-004) и
:class:`~tallyho.storage.tables.Tables` установки; схему подставляет
``schema_translate_map`` соединения. Строки ``th_counter`` и ``th_metric``
блокируются в порядке первичного ключа (ARCHITECTURE §9.2).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeVar

from sqlalchemy import (
    BigInteger,
    Uuid,
    any_,
    cast,
    delete,
    func,
    literal,
    literal_column,
    select,
    true,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from uuid import UUID

    from sqlalchemy import ColumnElement
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.storage.tables import Tables

__all__ = [
    "COUNTER_FIELDS",
    "DELTA_FIELDS",
    "CounterDelta",
    "CounterTotals",
    "SlotKey",
    "fold_deltas",
    "insert_delta",
    "read_counters",
    "upsert_slots",
]

COUNTER_FIELDS: Final = (
    "total",
    "ok",
    "skip",
    "error",
    "cancelled",
    "dispatched",
    "w_total",
    "w_done",
    "duplicates",
    "skipped_by_limit",
    "tree_total",
)
"""Счётчики строки ``th_counter`` в порядке колонок (ARCHITECTURE §5.1)."""

DELTA_FIELDS: Final = ("total", "ok", "skip", "error", "cancelled", "w_done")
"""Счётчики, у которых есть колонка ``d_<имя>`` в ``th_counter_delta``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class CounterDelta:
    """Приращение счётчиков одного батча.

    Поля совпадают с колонками ``th_counter``. Путь B (``th_counter_delta``)
    хранит только поля из :data:`DELTA_FIELDS`.
    """

    total: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    dispatched: int = 0
    w_total: int = 0
    w_done: int = 0
    duplicates: int = 0
    skipped_by_limit: int = 0
    tree_total: int = 0

    def as_dict(self) -> dict[str, int]:
        """Значения по именам.

        Returns:
            Словарь в порядке :data:`COUNTER_FIELDS`.
        """
        return {
            "total": self.total,
            "ok": self.ok,
            "skip": self.skip,
            "error": self.error,
            "cancelled": self.cancelled,
            "dispatched": self.dispatched,
            "w_total": self.w_total,
            "w_done": self.w_done,
            "duplicates": self.duplicates,
            "skipped_by_limit": self.skipped_by_limit,
            "tree_total": self.tree_total,
        }

    @property
    def is_zero(self) -> bool:
        """Все поля равны нулю: записывать нечего."""
        return not any(self.as_dict().values())

    @property
    def fits_delta_table(self) -> bool:
        """Дельту можно записать в ``th_counter_delta`` без потерь."""
        values = self.as_dict()
        return not any(values[name] for name in COUNTER_FIELDS if name not in DELTA_FIELDS)

    def __add__(self, other: CounterDelta) -> CounterDelta:
        """Сумма двух приращений.

        Returns:
            Поэлементная сумма.
        """
        return CounterDelta(
            total=self.total + other.total,
            ok=self.ok + other.ok,
            skip=self.skip + other.skip,
            error=self.error + other.error,
            cancelled=self.cancelled + other.cancelled,
            dispatched=self.dispatched + other.dispatched,
            w_total=self.w_total + other.w_total,
            w_done=self.w_done + other.w_done,
            duplicates=self.duplicates + other.duplicates,
            skipped_by_limit=self.skipped_by_limit + other.skipped_by_limit,
            tree_total=self.tree_total + other.tree_total,
        )

    def __neg__(self) -> CounterDelta:
        """Обратное приращение.

        Returns:
            Приращение с противоположными знаками.
        """
        return CounterDelta(**{name: -value for name, value in self.as_dict().items()})

    def __sub__(self, other: CounterDelta) -> CounterDelta:
        """Разность двух приращений.

        Returns:
            Поэлементная разность.
        """
        return self + -other


@dataclass(frozen=True, slots=True, kw_only=True)
class CounterTotals:
    """Точные счётчики батча: сумма слотов ``th_counter`` и несвёрнутых дельт (§9.3).

    Attributes:
        total: Уникальные Items батча (``found``).
        ok: Завершены с классом ``ok``.
        skip: Завершены с классом ``skip``.
        error: Завершены с классом ``error``.
        cancelled: Отменены.
        dispatched: Отправлены брокеру.
        w_total: Сумма весов всех Items.
        w_done: Сумма весов завершённых Items.
        duplicates: Отсечённые дубли spawn/add.
        skipped_by_limit: Не созданы из-за лимитов дерева.
        tree_total: Items всего дерева (только у корня).
    """

    total: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    dispatched: int = 0
    w_total: int = 0
    w_done: int = 0
    duplicates: int = 0
    skipped_by_limit: int = 0
    tree_total: int = 0

    @property
    def done(self) -> int:
        """Завершённые Items: ``ok + skip + error + cancelled``."""
        return self.ok + self.skip + self.error + self.cancelled

    @property
    def pending(self) -> int:
        """Незавершённые Items: ``total - done``."""
        return self.total - self.done

    def as_delta(self) -> CounterDelta:
        """Те же значения как приращение от нуля.

        Returns:
            Приращение с теми же полями.
        """
        return CounterDelta(
            total=self.total,
            ok=self.ok,
            skip=self.skip,
            error=self.error,
            cancelled=self.cancelled,
            dispatched=self.dispatched,
            w_total=self.w_total,
            w_done=self.w_done,
            duplicates=self.duplicates,
            skipped_by_limit=self.skipped_by_limit,
            tree_total=self.tree_total,
        )

    def __add__(self, delta: CounterDelta) -> CounterTotals:
        """Счётчики после приращения ``delta``.

        Returns:
            Новые счётчики.
        """
        return CounterTotals(**(self.as_delta() + delta).as_dict())


SlotKey = tuple["UUID", int]
"""Ключ строки ``th_counter``: ``(batch_id, slot)``."""

_CHUNK: Final = 1000
"""Строк в одном многострочном INSERT: до 13 параметров на строку при лимите 32 767."""

_DELTA_OVERFLOW = "th_counter_delta хранит только поля DELTA_FIELDS: остальные потерялись бы"

_ZERO: Final = literal_column("0", BigInteger())

_Row = TypeVar("_Row")


def _chunks(rows: list[_Row]) -> Iterator[list[_Row]]:
    return (rows[start : start + _CHUNK] for start in range(0, len(rows), _CHUNK))


def _sum(column: ColumnElement[int]) -> ColumnElement[int]:
    # sum(bigint) в PostgreSQL — numeric; возвращаем bigint, пустая сумма — 0.
    return cast(func.coalesce(func.sum(column), _ZERO), BigInteger)


async def read_counters(
    conn: AsyncConnection, tables: Tables, batch_ids: Iterable[UUID]
) -> dict[UUID, CounterTotals]:
    """Точные счётчики батчей: ``sum(th_counter) + sum(th_counter_delta)`` (§9.3).

    Один statement — один снимок: свёртка дельт переносит их в слот атомарно,
    поэтому сумма не «мигает». Та же формула годится для проверки финализации.

    Args:
        conn: Соединение (в транзакции или autocommit).
        tables: Таблицы установки.
        batch_ids: Батчи; повторы допускаются.

    Returns:
        Счётчики по каждому запрошенному батчу; у батча без строк — нули.
    """
    ids = sorted(set(batch_ids))
    if not ids:
        return {}
    counter = tables.counter
    delta = tables.counter_delta
    b = func.unnest(literal(ids, ARRAY(Uuid()))).table_valued("batch_id").render_derived("b")
    c = (
        select(*(_sum(counter.c[name]).label(name) for name in COUNTER_FIELDS))
        .where(counter.c.batch_id == b.c.batch_id)
        .lateral("c")
    )
    d = (
        select(*(_sum(delta.c[f"d_{name}"]).label(name) for name in DELTA_FIELDS))
        .where(delta.c.batch_id == b.c.batch_id)
        .lateral("d")
    )
    columns: list[ColumnElement[object]] = [b.c.batch_id]
    columns.extend(
        (c.c[name] + d.c[name] if name in DELTA_FIELDS else c.c[name]).label(name)
        for name in COUNTER_FIELDS
    )
    stmt = select(*columns).select_from(b.join(c, true()).join(d, true()))
    result = await conn.execute(stmt)
    totals: dict[UUID, CounterTotals] = {}
    for row in result.mappings():
        values: dict[str, int] = {name: row[name] for name in COUNTER_FIELDS}
        totals[row["batch_id"]] = CounterTotals(**values)
    return totals


async def upsert_slots(
    conn: AsyncConnection, tables: Tables, deltas: Mapping[SlotKey, CounterDelta]
) -> None:
    """Прибавить дельты к строкам ``th_counter`` (путь A, §9.2 шаг 8).

    Строка слота создаётся при первом обращении (``INSERT … ON CONFLICT DO
    UPDATE``). Строки блокируются в порядке ``(batch_id, slot)``: это
    глобальный порядок блокировок счётчиков. Нулевые дельты пропускаются.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        deltas: Приращения по ключу ``(batch_id, slot)``.
    """
    counter = tables.counter
    rows = [
        {"batch_id": key[0], "slot": key[1], **deltas[key].as_dict()}
        for key in sorted(deltas)
        if not deltas[key].is_zero
    ]
    for chunk in _chunks(rows):
        stmt = insert(counter).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[counter.c.batch_id, counter.c.slot],
            set_={name: counter.c[name] + stmt.excluded[name] for name in COUNTER_FIELDS},
        )
        _ = await conn.execute(stmt)


async def insert_delta(
    conn: AsyncConnection, tables: Tables, deltas: Mapping[UUID, CounterDelta]
) -> None:
    """Записать дельты в ``th_counter_delta`` (путь B, транзакция пользователя).

    Только ``INSERT``: горячие строки ``th_counter`` не блокируются, поэтому
    транзакция пользователя не ждёт Completer и не ловит ``40001``. Нулевые
    дельты пропускаются.

    Args:
        conn: Соединение транзакции пользователя.
        tables: Таблицы установки.
        deltas: Приращения по батчам.

    Raises:
        TypeError: Дельта содержит ненулевое поле вне :data:`DELTA_FIELDS`.
    """
    rows: list[dict[str, object]] = []
    for batch_id in sorted(deltas):
        value = deltas[batch_id]
        if not value.fits_delta_table:
            raise TypeError(_DELTA_OVERFLOW)
        if value.is_zero:
            continue
        values = value.as_dict()
        row: dict[str, object] = {f"d_{n}": values[n] for n in DELTA_FIELDS}
        row["batch_id"] = batch_id
        rows.append(row)
    for chunk in _chunks(rows):
        _ = await conn.execute(insert(tables.counter_delta).values(chunk))


async def fold_deltas(
    conn: AsyncConnection, tables: Tables, batch_ids: Iterable[UUID]
) -> dict[UUID, CounterDelta]:
    """Забрать закоммиченные дельты батчей: ``DELETE … RETURNING`` с суммой по батчу.

    Удаляются только строки, видимые снимку запроса, то есть закоммиченные:
    дельты незавершённой транзакции пользователя остаются до следующего
    прохода. Параллельная свёртка тех же строк ждёт блокировку и после commit
    первой их уже не находит, поэтому дельта сворачивается ровно один раз.

    Вызывающий обязан в **той же** транзакции прибавить результат к слоту
    (:func:`upsert_slots`, вместе с остальными дельтами — одним вызовом, чтобы
    не нарушить порядок блокировок): тогда перенос атомарен и
    :func:`read_counters` не «мигает».

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        batch_ids: Батчи, дельты которых сворачиваются.

    Returns:
        Сумма удалённых дельт по батчам; батчи без дельт не попадают.
    """
    ids = sorted(set(batch_ids))
    if not ids:
        return {}
    delta = tables.counter_delta
    gone = (
        delete(delta)
        .where(delta.c.batch_id == any_(literal(ids, ARRAY(Uuid()))))
        .returning(
            delta.c.batch_id,
            delta.c.d_total,
            delta.c.d_ok,
            delta.c.d_skip,
            delta.c.d_error,
            delta.c.d_cancelled,
            delta.c.d_w_done,
        )
        .cte("gone")
    )
    sums = [_sum(gone.c[f"d_{name}"]).label(name) for name in DELTA_FIELDS]
    columns: list[ColumnElement[UUID] | ColumnElement[int]] = [gone.c.batch_id, *sums]
    stmt = select(*columns).group_by(gone.c.batch_id)
    folded: dict[UUID, CounterDelta] = {}
    for row in (await conn.execute(stmt)).mappings():
        values: dict[str, int] = {name: row[name] for name in DELTA_FIELDS}
        folded[row["batch_id"]] = CounterDelta(**values)
    return folded
