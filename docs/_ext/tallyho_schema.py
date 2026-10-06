# ruff: ignore[implicit-namespace-package]  # расширение Sphinx из каталога docs, а не модуль пакета
"""Директива ``tallyho-schema``: таблицы и индексы tallyho, собранные из кода схемы.

Колонки, типы, первичные ключи, индексы и параметры хранения читаются из
``tallyho.storage.tables.build_metadata``, поэтому страница не расходится со схемой. Описания
колонок и индексов лежат в этом файле. Если в схеме появилась колонка или индекс без описания,
сборка сайта выдаёт предупреждение, а с ``-W`` падает: описание нужно дописать здесь.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from sphinx.util import logging
from sphinx.util.docutils import SphinxDirective
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from tallyho.storage.tables import build_metadata

if TYPE_CHECKING:
    from docutils import nodes
    from sphinx.application import Sphinx
    from sqlalchemy import Column, Index, Table

__all__ = ["setup"]

_log = logging.getLogger(__name__)
_DIALECT: Final = postgresql.dialect()
_SCHEMA: Final = "app"

# Порядок вкладок: от главных таблиц к служебным.
_ORDER: Final = (
    "batch",
    "item",
    "outbox",
    "lease",
    "counter",
    "counter_delta",
    "metric",
    "item_mark",
    "feed",
    "window",
    "expiry",
    "batch_attr",
    "meta",
)

_TABLES: Final[dict[str, str]] = {
    "batch": (
        "Батчи и под-батчи: состояние, флаги паузы и отмены, лимиты, сроки. "
        "Одна строка на узел дерева."
    ),
    "item": (
        "Задачи. Строка вставляется при добавлении задачи и обновляется один "
        "раз, когда записан итог."
    ),
    "outbox": (
        "Очередь отправки в брокер: задачи и колбэки, которые записаны, но ещё не отправлены. "
        "Размер таблицы равен объёму неотправленного."
    ),
    "lease": (
        "Аренды выполняющихся задач. Строка появляется, когда воркер взял задачу, и удаляется "
        "вместе с записью итога."
    ),
    "counter": (
        "Счётчики батча. Каждый процесс пишет в свой слот, поэтому воркеры не ждут друг друга на "
        "одной строке; прогресс равен сумме слотов."
    ),
    "counter_delta": (
        "Изменения счётчиков от задач, завершённых в вашей транзакции (`item.complete_in`). Строки "
        "только вставляются, а позже сворачиваются в `th_counter`."
    ),
    "metric": "Счётчики по меткам итога и суммы `item.incr`, тоже по слотам процессов.",
    "item_mark": (
        "Помеченные задачи, по умолчанию ошибочные. По этой таблице работает "
        "`handle.items(labels=...)`."
    ),
    "feed": "Связи конвейера: какой под-батч какой этап наполняет (`fed_by`).",
    "window": "Отправленные и ещё не завершённые задачи батчей с `max_in_flight`.",
    "expiry": "Сроки задач, поставленных с опцией брокера `expires`.",
    "batch_attr": (
        "Атрибуты и `memo` корневого батча. Строка есть только у корня, которому они заданы."
    ),
    "meta": "Служебные значения установки: версия схемы и место, до которого разобран DLQ брокера.",
}

_COLUMNS: Final[dict[str, dict[str, str]]] = {
    "batch": {
        "id": "идентификатор батча, UUIDv7",
        "root_id": "корень дерева; у корня равен `id`",
        "parent_id": "родительский батч; у корня пусто",
        "parent_item_id": "задача родителя, которой этот под-батч представлен в его счётчиках",
        "kind": "тип батча; по нему находятся хуки",
        "key": "ключ идемпотентного создания",
        "state": "состояние: `OPEN`, `SEALED` или одно из терминальных",
        "paused_at": "когда поставлен на паузу",
        "cancel_requested_at": "когда запрошена отмена",
        "cancel_reason": "причина запроса отмены: `cancel`, `deadline`, `fail_fast`, `policy`",
        "start_at": "отложенный старт",
        "options": "колбэк-задачи и политика ошибок",
        "hooks": "хуки, зарегистрированные для `kind` при создании батча",
        "expected_total": "ожидаемый объём из `expected_total` и `expect()`",
        "max_in_flight": "сколько задач батча одновременно в брокере и в работе",
        "max_items": "лимит задач на дерево; только у корня",
        "max_depth": "предел цепочки «задача заказала задачу в своём батче»",
        "on_feeder_failed": "что делать этапу, когда источник завершился неуспешно",
        "deadline_at": "срок, после которого батч отменяется с итогом `FAILED`",
        "snap_seq": "номер последнего снимка прогресса; он же `summary.seq`",
        "hook_attempts": "сколько раз подряд упал хук финализации",
        "hook_error": "текст последней ошибки хука",
        "retention": "через сколько после завершения удалять дерево; пусто означает хранить вечно",
        "release_required": "удалять только после `release()`",
        "released_at": "когда вызван `release()`",
        "created_at": "когда создан",
        "updated_at": "последнее изменение строки; по нему фоновые проверки находят зависшее",
        "finished_at": "когда финализирован",
    },
    "item": {
        "id": "идентификатор задачи, UUIDv7",
        "batch_id": "батч задачи",
        "state": "`active` или класс итога: `ok`, `skip`, `error`, `cancelled`",
        "label": "метка итога",
        "attempt": "номер попытки",
        "depth": "глубина в цепочке задач, заказанных в своём батче",
        "task_name": "имя задачи у брокера",
        "payload": "аргументы задачи в кодировке адаптера",
        "options": "очередь и опции брокера для этого вызова",
        "key": "ключ дедупликации внутри батча",
        "child_batch_id": "под-батч, если строка представляет его в счётчиках родителя",
        "weight": "вес задачи в `ratio`",
        "result": "значение из `item.ok(result=...)`",
        "error": "значение из `item.error(detail=...)` или описание сбоя",
        "created_at": "когда задача добавлена",
        "finished_at": "когда записан итог",
        "generation": "сколько раз задача возвращалась в очередь отправки",
    },
    "outbox": {
        "id": "идентификатор записи",
        "kind": "что отправлять: задачу или колбэк",
        "batch_id": "батч",
        "item_id": "задача; у колбэка пусто",
        "task_name": "имя задачи колбэка",
        "payload": "аргументы колбэка",
        "options": "опции постановки колбэка",
        "available_at": (
            "не раньше какого момента отправлять; так устроены отложенный старт и пауза"
        ),
        "attempts": "сколько раз запись пытались отправить",
    },
    "lease": {
        "item_id": "задача",
        "batch_id": "батч задачи",
        "lease_until": "до какого момента аренда действует без продления",
        "worker_id": "процесс, который выполняет задачу",
        "attempt": "попытка, которой принадлежит аренда",
        "progress_done": "собственный прогресс задачи из `item.progress`",
        "progress_total": "его знаменатель",
        "redelivered": "брокер доставил задачу повторно, пока она выполнялась",
    },
    "counter": {
        "batch_id": "батч",
        "slot": "слот процесса",
        "total": "найдено задач; в сумме по слотам это `progress.found`",
        "ok": "завершено с итогом `ok`",
        "skip": "завершено с итогом `skip`",
        "error": "завершено с итогом `error`",
        "cancelled": "отменено",
        "dispatched": "отправлено в брокер",
        "w_total": "сумма весов найденных задач",
        "w_done": "сумма весов завершённых задач",
        "duplicates": "отсечено дедупликацией по ключу",
        "skipped_by_limit": "не создано из-за `max_items` или `max_depth`",
        "tree_total": (
            "задач во всём дереве; ведётся только у корня, по нему проверяется `max_items`"
        ),
    },
    "counter_delta": {
        "id": "порядковый номер записи",
        "batch_id": "батч",
        "d_total": "изменение `total`",
        "d_ok": "изменение `ok`",
        "d_skip": "изменение `skip`",
        "d_error": "изменение `error`",
        "d_cancelled": "изменение `cancelled`",
        "d_dispatched": "изменение `dispatched`",
        "d_w_total": "изменение `w_total`",
        "d_w_done": "изменение `w_done`",
        "d_duplicates": "изменение `duplicates`",
        "d_skipped_by_limit": "изменение `skipped_by_limit`",
        "d_tree_total": "изменение `tree_total`",
        "created_at": "когда записано; по нему фоновые проверки сворачивают залежавшиеся записи",
    },
    "metric": {
        "batch_id": "батч",
        "name": "метка итога или имя метрики `item.incr`",
        "slot": "слот процесса",
        "value": "число задач с этой меткой или сумма метрики",
    },
    "item_mark": {
        "batch_id": "батч",
        "label": "метка итога",
        "item_id": "помеченная задача",
    },
    "feed": {
        "feeder_id": "источник: батч, чьи задачи добавляют работу",
        "fed_id": "этап, который он наполняет",
    },
    "window": {
        "item_id": "отправленная задача",
        "batch_id": "батч с `max_in_flight`",
    },
    "expiry": {
        "item_id": "задача",
        "expires_at": (
            "срок, после которого невзятая задача получает итог `error` с меткой `expired`"
        ),
    },
    "batch_attr": {
        "batch_id": "корневой батч",
        "attributes": "атрибуты для поиска и корреляции",
        "memo": "произвольный JSON для диагностики; в поиске не участвует",
    },
    "meta": {
        "key": "имя значения",
        "value": "значение",
    },
}

_INDEXES: Final[dict[str, str]] = {
    "batch_active_updated_idx": "фоновые проверки: незавершённые батчи, которые давно не менялись",
    "batch_deadline_idx": "фоновые проверки: батчи с истёкшим дедлайном",
    "batch_kind_idx": "`th.list_batches(kinds=...)`: корни одного типа, от новых к старым",
    "batch_kind_key_uq": "идемпотентное создание корня и `th.find(kind, key)`",
    "batch_parent_idx": "обход дерева: пауза, отмена, `view()`",
    "batch_progress_idx": "снимки прогресса: активные батчи с хуком `on_progress`",
    "batch_retention_idx": "удаление по retention: завершённые корни, которые уже можно удалять",
    "batch_root_key_uq": 'под-батч по ключу внутри дерева: `into="cards"`, `handle.child(...)`',
    "batch_attr_attributes_idx": "`th.list_batches(attributes=...)`",
    "counter_delta_batch_idx": "чтение прогресса и свёртка записей батча",
    "counter_delta_created_idx": "фоновые проверки: свёртка залежавшихся записей",
    "expiry_expires_idx": "фоновые проверки: задачи с истёкшим сроком",
    "feed_fed_idx": "все ли источники этапа финализированы",
    "item_batch_idx": "задачи батча: `handle.items(states=...)`, отмена, сверка счётчиков",
    "item_batch_key_uq": "дедупликация задач по ключу",
    "lease_batch_idx": "`handle.in_flight()` и `progress.in_flight`",
    "lease_until_idx": "фоновые проверки: истёкшие аренды",
    "outbox_available_idx": "отправка в брокер: что пора отправлять",
    "outbox_batch_idx": (
        "пауза, продолжение, отмена, перенос старта и окно `max_in_flight` одного батча"
    ),
    "window_batch_idx": "сколько задач батча отправлено и не завершено",
}


def _column_row(key: str, column: Column[object]) -> str:
    description = _COLUMNS.get(key, {}).get(column.name)
    if description is None:
        _log.warning("tallyho-schema: у колонки %s.%s нет описания в docs/_ext", key, column.name)
        description = ""
    marks = "PK" if column.primary_key else ("NULL" if column.nullable else "")
    kind = column.type.compile(dialect=_DIALECT).lower()
    return f"| `{column.name}` | `{kind}` | {marks} | {description} |"


def _index_row(prefix: str, index: Index) -> str:
    name = str(index.name)
    description = _INDEXES.get(name.removeprefix(prefix))
    if description is None:
        _log.warning("tallyho-schema: у индекса %s нет описания в docs/_ext", name)
        description = ""
    ddl = str(CreateIndex(index).compile(dialect=_DIALECT, compile_kwargs={"literal_binds": True}))
    definition = " ".join(ddl.split(" ON ", 1)[1].split()).split(" ", 1)[1]
    unique = "да" if index.unique else ""
    return f"| `{name}` | `{definition}` | {unique} | {description} |"


def _tab(key: str, prefix: str, table: Table) -> list[str]:
    if key not in _TABLES:
        _log.warning("tallyho-schema: у таблицы %s нет описания в docs/_ext", table.name)
    lines = [
        f":::{{tab-item}} {table.name}",
        "",
        _TABLES.get(key, ""),
        "",
        "| Колонка | Тип | | Смысл |",
        "|---|---|---|---|",
        *(_column_row(key, column) for column in table.columns),
        "",
    ]
    indexes = sorted(table.indexes, key=lambda index: str(index.name))
    if indexes:
        lines += [
            "| Индекс | Определение | Уникальный | Для чего |",
            "|---|---|---|---|",
            *(_index_row(prefix, index) for index in indexes),
            "",
        ]
    storage = table.dialect_options["postgresql"]["with"]
    if storage:
        options = ", ".join(f"`{name}={value}`" for name, value in storage.items())
        lines += [f"Параметры хранения: {options}.", ""]
    return [*lines, ":::", ""]


class SchemaDirective(SphinxDirective):
    """Вкладка на каждую таблицу: колонки, индексы, параметры хранения."""

    has_content: ClassVar[bool] = False

    def run(self) -> list[nodes.Node]:
        """Собрать вкладки из метаданных схемы.

        Returns:
            Узлы набора вкладок.
        """
        tables = build_metadata(schema=_SCHEMA)
        prefix = "th_"
        known = {name.removeprefix(prefix): table for name, table in _named(tables.metadata.tables)}
        missing = sorted(set(known) - set(_ORDER))
        if missing:
            _log.warning("tallyho-schema: таблицы %s не перечислены в _ORDER", ", ".join(missing))
        lines = ["::::{tab-set}", ""]
        for key in (*_ORDER, *missing):
            if key in known:
                lines += _tab(key, prefix, known[key])
        lines.append("::::")
        return self.parse_text_to_nodes("\n".join(lines))


def _named(tables: dict[str, Table]) -> list[tuple[str, Table]]:
    return [(table.name, table) for table in tables.values()]


def setup(app: Sphinx) -> dict[str, bool]:
    """Зарегистрировать директиву.

    Returns:
        Метаданные расширения для Sphinx.
    """
    app.add_directive("tallyho-schema", SchemaDirective)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
