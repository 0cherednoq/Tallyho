# Атрибуты, memo и листинг

**Атрибуты** - неизменяемые пары из ключа и значения типа `str | int | bool` у корневого батча.
Они нужны для корреляции и поиска. **`memo`** - неизменяемый JSON-объект для диагностики; он не индексируется и в фильтрах не
участвует. Оба задаются только при создании корня и видны в `view.attributes` / `view.memo` любого
узла дерева, а атрибуты - ещё и в сводке, которую получают хуки.

<!-- tallyho-noexec: фрагмент приложения: send и список получателей принадлежат вашему проекту -->
```python
# app/api.py
async def start_issue(tenant: str, issue: int, recipients: list[str], requested_by: str) -> None:
    async with th.batch(
        "newsletter",
        key=f"issue:{issue}",
        attributes={"tenant": tenant, "issue": issue},  # по ним батч ищут
        memo={"requested_by": requested_by},  # только для диагностики
    ) as batch:
        await batch.add_calls(th.call(send, address).opts(key=address) for address in recipients)


async def issues_with_errors(tenant: str) -> list[BatchInfo]:
    # Листинг корневых батчей: от новых к старым, постранично, без чтения счётчиков.
    page = await th.list_batches(
        kinds=["newsletter"],
        states=[BatchState.COMPLETED_WITH_ERRORS],
        attributes={"tenant": tenant},
        limit=50,
    )
    return list(page.items)  # page.next_cursor is None: страниц больше нет


async def bounced(issue: int) -> list[tuple[str | None, object]]:
    handle = await th.find("newsletter", f"issue:{issue}")
    # По меткам находятся только помеченные задачи; по умолчанию помечаются ошибки.
    return [(entry.key, entry.error) async for entry in handle.items(labels=["hard_bounce"])]


async def delivered(issue: int) -> list[str | None]:
    handle = await th.find("newsletter", f"issue:{issue}")
    # По состояниям находятся любые задачи, включая успешные и отменённые.
    return [entry.key async for entry in handle.items(states={ItemState.OK})]


# issues_with_errors("acme")   [BatchInfo(key="issue:42", state=COMPLETED_WITH_ERRORS, ...)]
# bounced(42)                  [("gone@bounce.test", "550 user unknown")]
# delivered(42)                ["ada@ok.test", "grace@ok.test"]
# view.attributes              {"tenant": "acme", "issue": 42}
# view.memo                    {"requested_by": "ops@example.test"}
```

## Правила атрибутов

* Значения - только `str`, `int` и `bool`; `UUID` превращается в строку и при записи, и в фильтре.
  `float`, `None`, `datetime` и коллекции отклоняются с `InvalidAttributesError`.
* Типы не приводятся: `{"issue": 42}` и `{"issue": "42"}` - разные значения, и фильтр по одному не
  найдёт другое.
* Ключ - непустая строка; префикс `tallyho.` зарезервирован.
* Лимиты по умолчанию: 32 атрибута, ключ до 128 байт, строковое значение до 512 байт, все атрибуты
  до 8 КиБ, `memo` до 16 КиБ ([настройки](../../reference/settings.md)).
* Атрибуты и `memo` не попадают в логи и телеметрию, но хранятся в базе открытым текстом: не кладите
  в них секреты.
* Тенант - обычный атрибут. Фильтровать по нему в листинге обязано ваше приложение: tallyho не
  знает, кто вызывает.

## `th.list_batches`

| Параметр | Значение |
|---|---|
| `kinds` | коллекция `kind`; `None` - любые |
| `states` | коллекция `BatchState`; `None` - любые |
| `attributes` | пары, которые все должны совпасть с атрибутами корня |
| `created_after` / `created_before` | границы по времени создания, полуинтервал `[after, before)` |
| `limit` | размер страницы: по умолчанию 100, не больше 1 000 |
| `cursor` | значение `next_cursor` предыдущей страницы; чужой или испорченный курсор - `ConfigurationError` |

Результат - `BatchPage(items, next_cursor)`. Каждый элемент - `BatchInfo` с полями `id`, `kind`,
`key`, `state`, `attributes`, `created_at`, `finished_at`. Листинг возвращает только корни и не
читает счётчики. Батч, созданный во время обхода, на уже пройденные страницы не попадает и не
сдвигает их.

## `handle.items`

`handle.items(*, states=None, labels=None)` - асинхронный итератор `ItemView` одного батча, не
поддерева. Задачи этапа читайте у этапа: `await root.child("send")`.

* Хотя бы один фильтр обязателен; вызов без фильтров - `ConfigurationError`.
* `labels=` находит только помеченные задачи (по умолчанию - ошибки) и работает быстро при любом
  размере батча.
* `states=` находит задачи в любом состоянии, включая `CANCELLED`, которые не помечаются. Батч
  читается окнами, и число запросов пропорционально размеру батча, а не числу совпадений.
* Оба фильтра вместе - пересечение.
* Порядок выдачи не гарантируется. Обход не изолирован снимком: задача, изменившаяся во время
  обхода, может попасть в выдачу в любом из двух состояний. Для точного результата читайте
  финализированный батч.
* Под-батчи выдаются как задачи с заполненным `child_batch_id`.
* Поля `ItemView`: `id`, `batch_id`, `state`, `task_name`, `label`, `attempt`, `depth`, `key`,
  `weight`, `child_batch_id`, `result`, `error`, `created_at`, `finished_at`.
