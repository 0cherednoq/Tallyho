# Под-батчи и конвейеры

`batch.sub_batch(key, ...)` создаёт под-батч. Для родителя он одна задача: родитель завершится
только после всех своих под-батчей. Под-батч, которому указали источники `fed_by=[...]`, становится
**этапом конвейера**: его наполняют задачи источников, а закрывает сама библиотека.

<!-- tallyho-noexec: фрагмент приложения: site и catalog принадлежат вашему проекту -->
```python
# app/tasks.py
@fq.task(max_retries=3, retry_on=[SiteTimeout], queue="parse")
async def parse_page(catalog_id: int, page: int) -> None:
    listing = await site.page(catalog_id, page)
    if page == 1:  # число страниц становится известно после первой
        item.expect(listing.total_pages)
        for other in range(2, listing.total_pages + 1):
            item.spawn(parse_page, catalog_id, other)  # в свой этап
    for url in listing.card_urls:
        item.spawn(parse_card, url, into="cards", key=url)  # в следующий этап, с дедупликацией


@fq.task(max_retries=3, retry_on=[SiteTimeout], queue="parse")
async def parse_card(url: str) -> None:
    await catalog.save(await site.card(url))
    item.ok("parsed")


# app/api.py
async def start_parse(catalog_id: int) -> None:
    async with th.batch("catalog_parse", key=f"catalog:{catalog_id}", max_items=10_000) as root:
        pages = root.sub_batch("pages", max_depth=1)
        root.sub_batch("cards", fed_by=[pages], max_in_flight=2)
        await pages.add(parse_page, catalog_id, 1)
    # при выходе закрыты корень и pages; cards закроется сам, когда закончится pages


# каталог из трёх страниц, на них пять ссылок на четыре карточки:
# view.state                                 SUCCEEDED
# view.progress.found, .ok, .ratio           2 2 1.0     у корня каждый этап - одна задача
# view.children["pages"].progress.found      3
# view.children["cards"].progress            found=4, ok=4, duplicates=1
```

## Правила конвейера

* Этап не ждёт конца предыдущего. `cards` начинает работать с первой же найденной карточки,
  пока `pages` ещё разбирает страницы.
* Этап с `fed_by` закрывает библиотека - когда все его источники финализированы. Вызывать
  `seal()` для такого этапа нельзя (`SealError`). Этапы без `fed_by` и корень закрываются при
  выходе из `async with`.
* В этап с `fed_by` добавляют только его собственные задачи и задачи
  его источников (`into=`). Продюсер добавлять в него не может: `add` бросит `SpawnTargetError`.
  То же исключение получит внутри себя задача, которая указала в `into=` этап, для которого её батч
  не источник; как и любое исключение, оно ведёт к ретраям и итогу `exhausted`.
* `fed_by` - только под-батчи того же дерева, без циклов. «Страница порождает страницу» - это
  `spawn` в свой этап, а не `fed_by` на себя.
* Пустой этап - не зависание. Этап, в который не пришло ни одной задачи, закрывается и
  финализируется сразу, каскадом закрывая следующие.
* Если источник завершился с ошибками, провалом или отменой, этап по умолчанию всё равно закрывается
  и доделывает полученное (`on_feeder_failed="seal"`). `on_feeder_failed="cancel"` отменяет этап.
* Родитель финализируется только после всех детей; их хуки `on_finalized`
  коммитятся раньше родительского. Если хотя бы один прямой ребёнок завершился с ошибками, провалом
  или отменой, родитель без собственной более сильной причины станет `COMPLETED_WITH_ERRORS`.
* `kind` под-батча по умолчанию - `<kind родителя>.<key>` (`catalog_parse.cards`). Поэтому хук,
  зарегистрированный на `kind` корня, не срабатывает на каждом этапе. Задайте `kind=` явно, если
  этапу нужен собственный хук.

Параметры `sub_batch`: `kind`, `fed_by`, `on_feeder_failed`, `start_at`, `deadline`, `failure_policy`,
`max_in_flight`, `expected_total`, `max_depth` и колбэки `on_...=`. Параметры `retention`,
`release_required`, `max_items`, `attributes` и `memo` задаются только у корня.

## `spawn`: задачи порождают задачи

| Вызов в задаче | Что делает |
|---|---|
| `item.spawn(fn, *args, **kwargs)` | добавить задачу в свой батч |
| `item.spawn(fn, a1, into="cards", key="…")` | добавить задачу в этап `cards`, для которого свой батч - источник; до трёх позиционных аргументов задачи |
| `item.spawn_call(th.call(fn, ...).opts(...), into=...)` | то же с именованными аргументами задачи и опциями вызова (ключ, вес, очередь, опции брокера) |
| `item.expect(n)`, `item.expect(n, into="cards")` | сообщить ожидаемый объём своего или целевого батча |

* `spawn` только складывает вызовы в буфер. Они записываются одной транзакцией с
  завершением самой задачи: либо задача завершена и все её дети созданы, либо ничего. Если задача
  упала, её дети не появятся; при повторе они будут порождены заново.
* `into=` - ключ под-батча внутри дерева или идентификатор батча (`UUID`, например
  `handle.id`; сам `BatchHandle` не принимается).
* Формы `spawn` и типизация. Аргументы задачи проверяются type checker'ом по её сигнатуре.
  Поэтому `item.spawn` либо передаёт задаче всё как есть (`spawn(fn, *args, **kwargs)`), либо
  принимает маршрут `into=`/`key=` вместе с позиционными аргументами (не больше трёх). Именованные
  аргументы задачи вместе с маршрутом, вес и опции брокера задаёт подготовленный вызов:
  `item.spawn_call(th.call(fn, x, mode="fast").opts(key=..., weight=2), into="cards")`. Параметра
  `opts=` у `spawn` нет.
* `key=` - ключ дедупликации в целевом батче. Повторно найденная ссылка не создаёт задачу, а
  увеличивает счётчик `duplicates`. Для URL берите нормализованный адрес без фрагмента. Дедупликация
  действует всё время жизни батча.
* `max_depth` (у под-батча) ограничивает глубину самоподпитки: задача, добавленная продюсером
  или через `into=`, имеет глубину 0, порождённая ею в том же батче - 1 и так далее.
* `max_items` (у корня) ограничивает число задач во всём дереве. Задачи сверх любого из двух
  лимитов не создаются и учитываются в `skipped_by_limit` - это не ошибка. Лимит `max_items`
  мягкий - см. [ограничения](../limitations.md#мягкий-max_items).
* Имена `into` и `key` зарезервированы: `item.spawn` забирает их себе и в функцию не передаёт.

Задача может создать и целый под-батч - он тоже появится атомарно с её завершением:

<!-- tallyho-noexec: фрагмент тела задачи; render_part и publish_video - задачи вашего приложения -->
```python
async def split_video(video_id: int, parts: int) -> None:
    async with item.sub_batch(
        f"video:{video_id}",
        max_in_flight=4,
        on_succeeded=th.call(publish_video, video_id),  # колбэки - как у batch.sub_batch
    ) as sub:
        for part in range(parts):
            sub.add(render_part, video_id, part)  # без await: вызовы копятся в буфере
```

Параметры `item.sub_batch`: `kind`, `start_at`, `deadline`, `failure_policy`, `max_in_flight`,
`expected_total`, `max_depth` и колбэки `on_...=`. Вне задачи `item.sub_batch` бросает
`ConfigurationError`.
