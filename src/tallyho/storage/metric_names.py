"""Имена строк ``th_metric``: метки итога и метрики ``item.incr`` (ARCHITECTURE §11.2).

Обе величины лежат в одной таблице ``th_metric`` с ключом ``(batch_id, name,
slot)``. Метка итога записывается под своим именем, метрика — под именем с
зарезервированным первым символом U+001F. Поэтому метка и метрика с одним
именем — разные строки, а чтение раскладывает их в ``labels`` и ``metrics``.
Имя метки или метрики, начинающееся с U+001F, отклоняется заранее
(:func:`check_counter_name`): иначе чтение приняло бы метку за метрику.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from sqlalchemy import func, select

from tallyho.model.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.storage.tables import Tables

__all__ = [
    "METRIC_PREFIX",
    "LabelsAndMetrics",
    "check_counter_name",
    "metric_rows",
    "read_labels_and_metrics",
    "split_metric_rows",
]

METRIC_PREFIX: Final = "\x1f"
"""Первый символ имени строки ``th_metric`` пользовательской метрики."""


def check_counter_name(name: str, *, what: str) -> None:
    """Отклонить имя метки или метрики с зарезервированным первым символом.

    Args:
        name: Имя метки итога или метрики ``item.incr``.
        what: ``"метки"`` или ``"метрики"`` — для сообщения.

    Raises:
        ConfigurationError: имя начинается с U+001F.
    """
    if name.startswith(METRIC_PREFIX):
        message = f"имя {what} не может начинаться с символа U+001F: он зарезервирован"
        raise ConfigurationError(message)


def metric_rows(metrics: Mapping[str, int]) -> dict[str, int]:
    """Перевести метрики ``item.incr`` в имена строк ``th_metric``.

    Уже переведённое имя не меняется, поэтому повторный перевод безопасен.

    Returns:
        Приращения по именам строк ``th_metric``.
    """
    rows: dict[str, int] = {}
    for name, value in metrics.items():
        row = name if name.startswith(METRIC_PREFIX) else METRIC_PREFIX + name
        rows[row] = rows.get(row, 0) + value
    return rows


def split_metric_rows(values: Mapping[str, int]) -> tuple[dict[str, int], dict[str, int]]:
    """Разложить суммы строк ``th_metric`` батча на метки итога и метрики.

    Returns:
        ``(labels, metrics)`` — имена метрик без зарезервированного символа.
    """
    labels: dict[str, int] = {}
    metrics: dict[str, int] = {}
    for name, value in values.items():
        if name.startswith(METRIC_PREFIX):
            metrics[name.removeprefix(METRIC_PREFIX)] = value
        else:
            labels[name] = value
    return labels, metrics


@dataclass(frozen=True, slots=True)
class LabelsAndMetrics:
    """Суммы строк ``th_metric`` одного батча: метки итога и метрики ``item.incr``."""

    labels: Mapping[str, int] = field(default_factory=dict[str, int])
    metrics: Mapping[str, int] = field(default_factory=dict[str, int])


async def read_labels_and_metrics(
    conn: AsyncConnection, tables: Tables, batch_ids: Iterable[UUID]
) -> dict[UUID, LabelsAndMetrics]:
    """Прочитать суммы ``th_metric`` по слотам для батчей.

    Returns:
        Метки и метрики по id батча; батча без строк в словаре нет.
    """
    metric = tables.metric
    result = await conn.execute(
        select(metric.c.batch_id, metric.c.name, func.sum(metric.c.value))
        .where(metric.c.batch_id.in_(list(batch_ids)))
        .group_by(metric.c.batch_id, metric.c.name)
    )
    values: dict[UUID, dict[str, int]] = {}
    for batch_id, name, value in result:
        values.setdefault(batch_id, {})[name] = int(value)
    return {
        batch_id: LabelsAndMetrics(*split_metric_rows(rows)) for batch_id, rows in values.items()
    }
