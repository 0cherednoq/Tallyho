"""Счётчики батчей: дельты, суммы и запросы к ``th_counter`` (ARCHITECTURE §9).

Счётчики хранятся двумя путями (§9.1, COUNTERS §3.3):

* путь A — Completer прибавляет дельту к строке ``th_counter`` своего слота;
* путь B — транзакция пользователя только вставляет строку в
  ``th_counter_delta``, а Completer потом сворачивает её в слот.

Здесь описаны значения, которыми обмениваются эти пути:
:class:`CounterDelta` (приращение) и :class:`CounterTotals` (точная сумма
слотов и несвёрнутых дельт), и запросы к таблицам счётчиков.

Функции принимают ``AsyncConnection`` в открытой транзакции (D-004) и
:class:`~tallyho.storage.tables.Tables` установки; схема записана в самих
таблицах. Строки ``th_counter`` и ``th_metric``
блокируются в порядке первичного ключа (ARCHITECTURE §9.2).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeVar

from sqlalchemy import (
    BigInteger,
    SmallInteger,
    Uuid,
    cast,
    delete,
    func,
    literal,
    literal_column,
    select,
    true,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert

from tallyho.model.states import TERMINAL_THRESHOLD, ItemState

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import ColumnElement, Select
    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.storage.tables import Tables

__all__ = [
    "COUNTER_FIELDS",
    "DELTA_FIELDS",
    "USER_METRIC_SLOTS",
    "CounterDelta",
    "CounterTotals",
    "MetricKey",
    "SlotKey",
    "fold_delta_ids",
    "insert_delta",
    "read_counters",
    "reconcile",
    "stale_metric_slots_statement",
    "take_metric_slot",
    "take_stale_metric_slots",
    "upsert_metrics",
    "upsert_slots",
    "user_metric_slot",
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

DELTA_FIELDS: Final = COUNTER_FIELDS
"""Счётчики с колонкой ``d_<имя>`` в ``th_counter_delta``: все (D-029)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class CounterDelta:
    """Приращение счётчиков одного батча.

    Поля совпадают с колонками ``th_counter`` и ``d_*``-колонками
    ``th_counter_delta``: оба пути записывают любое поле.
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
"""Ключ строки ``th_counter`` или слота ``th_metric`` батча: ``(batch_id, slot)``."""

MetricKey = tuple["UUID", str, int]
"""Ключ строки ``th_metric``: ``(batch_id, name, slot)``."""

_CHUNK: Final = 1000
"""Строк в одном многострочном INSERT: до 13 параметров на строку при лимите 32 767."""

_ZERO: Final = literal_column("0", BigInteger())

USER_METRIC_SLOTS: Final = 32_767
"""Сколько отрицательных слотов ``th_metric`` у транзакций пути B."""

_DERIVED_FIELDS: Final = ("total", "ok", "skip", "error", "cancelled", "w_total", "w_done")
"""Счётчики, которые :func:`reconcile` выводит из строк ``th_item``."""

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
    result = await conn.execute(_totals_select(tables, ids))
    totals: dict[UUID, CounterTotals] = {}
    for row in result.mappings():
        values: dict[str, int] = {name: row[name] for name in COUNTER_FIELDS}
        totals[row["batch_id"]] = CounterTotals(**values)
    return totals


def _totals_select(tables: Tables, ids: list[UUID]) -> Select[*tuple[object, ...]]:
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
    columns.extend((c.c[name] + d.c[name]).label(name) for name in COUNTER_FIELDS)
    return select(*columns).select_from(b.join(c, true()).join(d, true()))


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
    rows: list[dict[str, object]] = [
        {"batch_id": key[0], "slot": key[1], **deltas[key].as_dict()}
        for key in sorted(deltas)
        if not deltas[key].is_zero
    ]
    for chunk in _chunks(rows):
        await _upsert_rows(conn, tables, chunk)


async def _upsert_rows(
    conn: AsyncConnection, tables: Tables, rows: list[dict[str, object]]
) -> None:
    counter = tables.counter
    stmt = insert(counter).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[counter.c.batch_id, counter.c.slot],
        set_={name: counter.c[name] + stmt.excluded[name] for name in COUNTER_FIELDS},
    )
    _ = await conn.execute(stmt)


async def insert_delta(
    conn: AsyncConnection,
    tables: Tables,
    deltas: Mapping[UUID, CounterDelta],
    *,
    created_at: datetime | ColumnElement[datetime],
) -> dict[UUID, list[int]]:
    """Записать дельты в ``th_counter_delta`` (путь B, транзакция пользователя).

    Только ``INSERT``: горячие строки ``th_counter`` не блокируются, поэтому
    транзакция пользователя не ждёт Completer и не ловит ``40001``. Нулевые
    дельты пропускаются.

    Args:
        conn: Соединение транзакции пользователя.
        tables: Таблицы установки.
        deltas: Приращения по батчам.
        created_at: Время создания дельт из внедрённого Clock/DB-выражения.

    Returns:
        Идентификаторы вставленных строк по батчам.
    """
    rows: list[dict[str, object]] = []
    for batch_id in sorted(deltas):
        value = deltas[batch_id]
        if value.is_zero:
            continue
        values = value.as_dict()
        row: dict[str, object] = {f"d_{n}": values[n] for n in DELTA_FIELDS}
        row["batch_id"] = batch_id
        row["created_at"] = created_at
        rows.append(row)
    inserted: defaultdict[UUID, list[int]] = defaultdict(list)
    for chunk in _chunks(rows):
        result = await conn.execute(
            insert(tables.counter_delta)
            .values(chunk)
            .returning(tables.counter_delta.c.id, tables.counter_delta.c.batch_id)
        )
        for delta_id, batch_id in result:
            inserted[batch_id].append(delta_id)
    return dict(inserted)


async def upsert_metrics(
    conn: AsyncConnection, tables: Tables, increments: Mapping[MetricKey, int]
) -> None:
    """Прибавить значения к строкам ``th_metric`` (labels и метрики, §9.2 шаг 9).

    Как и :func:`upsert_slots`: строка создаётся при первом обращении, строки
    блокируются в порядке ``(batch_id, name, slot)`` — после ``th_counter``
    в порядке блокировок транзакции. Нулевые приращения пропускаются.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        increments: Приращения по ключу ``(batch_id, name, slot)``.
    """
    metric = tables.metric
    rows: list[dict[str, object]] = [
        {"batch_id": key[0], "name": key[1], "slot": key[2], "value": increments[key]}
        for key in sorted(increments)
        if increments[key]
    ]
    for chunk in _chunks(rows):
        stmt = insert(metric).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[metric.c.batch_id, metric.c.name, metric.c.slot],
            set_={"value": metric.c.value + stmt.excluded.value},
        )
        _ = await conn.execute(stmt)


def user_metric_slot(delta_id: int) -> int:
    """Отрицательный слот ``th_metric`` транзакции пути B для батча (UC-08).

    Считается по id дельты этого батча, вставленной той же транзакцией: id
    уникальны и растут, поэтому одновременные транзакции получают разные
    слоты и не блокируют строки друг друга и групповой транзакции Completer
    (слоты процессов неотрицательны). По дельте строку слота находит и
    sweeper, если перенос после commit не состоялся.

    Args:
        delta_id: Id строки ``th_counter_delta`` батча.

    Returns:
        ``-1 - delta_id mod 32767``: от ``-32767`` до ``-1``.
    """
    return -1 - delta_id % USER_METRIC_SLOTS


def _user_metric_slot_sql(delta_id: ColumnElement[int]) -> ColumnElement[int]:
    # То же, что user_metric_slot, в SQL; константы — литералами (D-020).
    modulo = literal_column(str(USER_METRIC_SLOTS), BigInteger())
    return literal_column("-1", BigInteger()) - delta_id % modulo


def _slot_arrays(
    keys: Iterable[SlotKey],
) -> tuple[ColumnElement[Sequence[UUID]], ColumnElement[Sequence[int]]]:
    # Два параметра-массива для unnest: план не зависит от числа слотов. Явный
    # CAST сохраняет типы колонок и в SQL с литералами (EXPLAIN-гард).
    ordered = sorted(set(keys))
    uuids: list[UUID] = [key[0] for key in ordered]
    slots: list[int] = [key[1] for key in ordered]
    return (
        cast(literal(uuids, ARRAY(Uuid())), ARRAY(Uuid())),
        cast(literal(slots, ARRAY(SmallInteger())), ARRAY(SmallInteger())),
    )


async def take_metric_slot(
    conn: AsyncConnection, tables: Tables, keys: Iterable[SlotKey]
) -> dict[tuple[UUID, str], int]:
    """Удалить строки ``th_metric`` слотов транзакции пути B и вернуть их значения.

    Путь B пишет метрики в собственный слот транзакции пользователя, чтобы не
    ждать строку слота процесса (ARCHITECTURE UC-08); после commit Completer
    забирает их и в той же транзакции прибавляет к своему слоту
    (:func:`upsert_metrics`), поэтому сумма по слотам не меняется.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        keys: Пары ``(batch_id, slot)`` — слоты транзакции по батчам.

    Returns:
        Значения по ``(batch_id, name)``.
    """
    pairs = func.unnest(*_slot_arrays(keys)).table_valued("batch_id", "slot").render_derived("k")
    metric = tables.metric
    taken = await conn.execute(
        delete(metric)
        .where(metric.c.batch_id == pairs.c.batch_id, metric.c.slot == pairs.c.slot)
        .returning(metric.c.batch_id, metric.c.name, metric.c.value)
    )
    return _sum_metric_rows(taken)


def stale_metric_slots_statement(tables: Tables, keys: Iterable[SlotKey]) -> Select[UUID, str, int]:
    """Строки отрицательных слотов, которые можно перенести без их транзакции.

    Строка ``(batch_id, name, slot)`` берётся, если слот отрицательный, в
    батче не осталось несвёрнутой дельты с тем же слотом (её свёртка сама
    заберёт строку) и строку не держит другая транзакция (``SKIP LOCKED``:
    живая транзакция пути B с совпавшим слотом или свёртка Completer).
    Строки блокируются в порядке первичного ключа.

    Args:
        tables: Таблицы установки.
        keys: Пары ``(batch_id, slot)`` свёрнутых дельт.

    Returns:
        ``SELECT … FOR UPDATE SKIP LOCKED`` ключей строк ``th_metric``.
    """
    pairs = func.unnest(*_slot_arrays(keys)).table_valued("batch_id", "slot").render_derived("k")
    metric = tables.metric
    delta = tables.counter_delta
    pending = (
        select(delta.c.id)
        .where(
            delta.c.batch_id == metric.c.batch_id,
            _user_metric_slot_sql(delta.c.id) == metric.c.slot,
        )
        .exists()
    )
    return (
        select(metric.c.batch_id, metric.c.name, metric.c.slot)
        .join(pairs, (metric.c.batch_id == pairs.c.batch_id) & (metric.c.slot == pairs.c.slot))
        .where(metric.c.slot < literal_column("0", SmallInteger()), ~pending)
        .order_by(metric.c.batch_id, metric.c.name, metric.c.slot)
        .with_for_update(of=metric, skip_locked=True)
    )


async def take_stale_metric_slots(
    conn: AsyncConnection, tables: Tables, keys: Iterable[SlotKey]
) -> dict[tuple[UUID, str], int]:
    """Забрать строки слотов транзакций пути B, перенос которых не состоялся.

    Sweeper вызывает функцию в транзакции, которая свернула устаревшие дельты
    этих слотов (:func:`fold_delta_ids`), и прибавляет результат к своему
    слоту (:func:`upsert_metrics`), поэтому сумма по слотам не меняется.
    Какие строки берутся — :func:`stale_metric_slots_statement`; остальные
    перенесёт свёртка их транзакции.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        keys: Пары ``(batch_id, slot)`` свёрнутых дельт.

    Returns:
        Значения по ``(batch_id, name)``.
    """
    ordered = sorted(set(keys))
    if not ordered:
        return {}
    metric = tables.metric
    locked = stale_metric_slots_statement(tables, ordered).cte("locked")
    taken = await conn.execute(
        delete(metric)
        .where(
            metric.c.batch_id == locked.c.batch_id,
            metric.c.name == locked.c.name,
            metric.c.slot == locked.c.slot,
        )
        .returning(metric.c.batch_id, metric.c.name, metric.c.value)
    )
    return _sum_metric_rows(taken)


def _sum_metric_rows(rows: Iterable[tuple[UUID, str, int]]) -> dict[tuple[UUID, str], int]:
    values: defaultdict[tuple[UUID, str], int] = defaultdict(int)
    for batch_id, name, value in rows:
        values[batch_id, name] += value
    return dict(values)


async def fold_delta_ids(
    conn: AsyncConnection, tables: Tables, delta_ids: Iterable[int]
) -> dict[UUID, CounterDelta]:
    """Забрать закоммиченные дельты по id: ``DELETE … RETURNING`` с суммой по батчу.

    Удаляются только строки, видимые снимку запроса, то есть закоммиченные:
    дельта незавершённой транзакции пользователя остаётся до следующего
    прохода. Параллельная свёртка тех же строк ждёт блокировку и после commit
    первой их уже не находит, поэтому дельта сворачивается ровно один раз.

    Условие по первичному ключу не ставит predicate-lock на диапазон
    ``batch_id`` и поэтому не конфликтует с последующими append-only INSERT
    транзакций ``SERIALIZABLE`` того же батча. Свёртка по батчу целиком не
    нужна: дельта указывает на свой отрицательный слот ``th_metric``, и
    вызывающий забирает его вместе с ней (D-067).

    Вызывающий обязан в **той же** транзакции прибавить результат к слоту
    (:func:`upsert_slots`, вместе с остальными дельтами — одним вызовом, чтобы
    не нарушить порядок блокировок): тогда перенос атомарен и
    :func:`read_counters` не «мигает».

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        delta_ids: Id строк ``th_counter_delta``.

    Returns:
        Суммы удалённых дельт по батчам; батчи без дельт не попадают.
    """
    ids = sorted(set(delta_ids))
    if not ids:
        return {}
    delta = tables.counter_delta
    returned: list[ColumnElement[UUID] | ColumnElement[int]] = [delta.c.batch_id]
    returned.extend(delta.c[f"d_{name}"] for name in DELTA_FIELDS)
    gone = delete(delta).where(delta.c.id.in_(ids)).returning(*returned).cte("gone")
    sums = [_sum(gone.c[f"d_{name}"]).label(name) for name in DELTA_FIELDS]
    columns: list[ColumnElement[UUID] | ColumnElement[int]] = [gone.c.batch_id, *sums]
    stmt = select(*columns).group_by(gone.c.batch_id)
    folded: dict[UUID, CounterDelta] = {}
    for row in (await conn.execute(stmt)).mappings():
        values: dict[str, int] = {name: row[name] for name in DELTA_FIELDS}
        folded[row["batch_id"]] = CounterDelta(**values)
    return folded


def _state_is(value: int) -> ColumnElement[int]:
    # Литерал, а не bind-параметр (D-020).
    return literal_column(str(value), SmallInteger())


async def reconcile(conn: AsyncConnection, tables: Tables, batch_id: UUID) -> CounterDelta | None:
    """Исправить дрейф счётчиков батча по фактическим строкам ``th_item`` (COUNTERS §3.4).

    Под ``FOR UPDATE`` строки батча одним запросом (один снимок) считаются
    ``count(*)``/``sum(weight)`` Items по состояниям и текущая сумма
    ``th_counter + th_counter_delta``. Любая наша транзакция меняет Items и
    счётчики атомарно, поэтому в одном снимке они согласованы, а разница —
    это дрейф. Он прибавляется к слоту 0, затем остальные слоты переносятся в
    слот 0 и обнуляются. Всё делается приращениями: параллельные завершения не
    теряются, а несвёрнутые дельты остаются в ``th_counter_delta``.

    Исправляются ``total, ok, skip, error, cancelled, w_total, w_done``;
    остальные счётчики из Items не выводятся и только переносятся в слот 0.

    Args:
        conn: Соединение в открытой транзакции.
        tables: Таблицы установки.
        batch_id: Батч.

    Returns:
        Найденный дрейф (факт минус счётчики) или ``None``, если батча нет.
    """
    batch = tables.batch
    found = await conn.scalar(select(batch.c.id).where(batch.c.id == batch_id).with_for_update())
    if found is None:
        return None
    drift = await _drift(conn, tables, batch_id)
    await _upsert_rows(conn, tables, [{"batch_id": batch_id, "slot": 0, **drift.as_dict()}])
    await _collapse_into_slot_zero(conn, tables, batch_id)
    return drift


async def _drift(conn: AsyncConnection, tables: Tables, batch_id: UUID) -> CounterDelta:
    item = tables.item
    state = item.c.state
    done_weight = func.sum(item.c.weight).filter(state >= _state_is(TERMINAL_THRESHOLD))
    truth = (
        select(
            func.count().label("t_total"),
            func.count().filter(state == _state_is(ItemState.OK)).label("t_ok"),
            func.count().filter(state == _state_is(ItemState.SKIP)).label("t_skip"),
            func.count().filter(state == _state_is(ItemState.ERROR)).label("t_error"),
            func.count().filter(state == _state_is(ItemState.CANCELLED)).label("t_cancelled"),
            _sum(item.c.weight).label("t_w_total"),
            cast(func.coalesce(done_weight, _ZERO), BigInteger).label("t_w_done"),
        )
        .where(item.c.batch_id == batch_id)
        .subquery("t")
    )
    observed = _totals_select(tables, [batch_id]).subquery("o")
    stmt = select(truth, observed).select_from(truth.join(observed, true()))
    row = (await conn.execute(stmt)).mappings().one()
    fact: dict[str, int] = {name: row[f"t_{name}"] for name in _DERIVED_FIELDS}
    seen: dict[str, int] = {name: row[name] for name in _DERIVED_FIELDS}
    return CounterDelta(**{name: fact[name] - seen[name] for name in _DERIVED_FIELDS})


async def _collapse_into_slot_zero(conn: AsyncConnection, tables: Tables, batch_id: UUID) -> None:
    # Слот 0 уже заблокирован upsert'ом; остальные — по возрастанию slot.
    counter = tables.counter
    others = (
        await conn.execute(
            select(counter)
            .where(counter.c.batch_id == batch_id, counter.c.slot != 0)
            .order_by(counter.c.slot)
            .with_for_update()
        )
    ).mappings()
    moved = CounterDelta()
    slots: list[int] = []
    for row in others:
        values: dict[str, int] = {name: row[name] for name in ("slot", *COUNTER_FIELDS)}
        slots.append(values.pop("slot"))
        moved += CounterDelta(**values)
    if not slots:
        return
    zeroed = dict.fromkeys(COUNTER_FIELDS, 0)
    _ = await conn.execute(
        update(counter)
        .where(counter.c.batch_id == batch_id, counter.c.slot.in_(slots))
        .values(zeroed)
    )
    amounts = moved.as_dict()
    _ = await conn.execute(
        update(counter)
        .where(counter.c.batch_id == batch_id, counter.c.slot == 0)
        .values({name: counter.c[name] + amounts[name] for name in COUNTER_FIELDS})
    )
