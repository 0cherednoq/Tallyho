# Батчи и конвейеры

[← Оглавление](README.md) · назад: [Установка и миграции](installation.md) · далее: [Хуки](hooks.md)

## Понятия

| Термин | Значение |
|---|---|
| **Батч** | группа задач с общим учётом: счётчики, состояние, прогресс, финализация |
| **`kind`** | строковый тип батча (`"campaign_deliveries"`). По нему находятся [хуки](hooks.md) |
| **`key`** | ключ идемпотентного создания и связи с вашей сущностью. У корня уникален в пределах `kind` (`"campaign:42"`), у под-батча — в пределах дерева (`"send"`) |
| **Item** | одна задача брокера внутри батча |
| **Под-батч** | батч внутри батча; для родителя выглядит одной задачей |
| **Этап конвейера** | под-батч, который наполняют задачи других под-батчей (`fed_by`) |
| **Seal** | «новых задач не будет». Без этого батч не завершится |
| **Класс итога** | технический итог задачи: `ok`, `skip`, `error`, `cancelled` |
| **Метка (label)** | свободная строка-категория итога (`"sent"`, `"hard_bounce"`); по меткам ведутся счётчики |

Состояния батча (`BatchState` из `tallyho.model.states`):

| Состояние | Когда |
|---|---|
| `OPEN` | создан, задачи ещё могут добавляться |
| `SEALED` | закрыт для добавления, задачи выполняются |
| `SUCCEEDED` | все задачи завершены, ошибок нет |
| `COMPLETED_WITH_ERRORS` | все задачи завершены, есть ошибки, политика ошибок не сработала |
| `FAILED` | сработала политика ошибок с действием «провалить» или истёк дедлайн |
| `CANCELLED` | батч отменён |

Четыре последних состояния терминальные (`state.is_terminal`). Переход в любое из них —
**финализация**: она происходит ровно один раз и атомарно с вашим хуком `on_finalized`.

## Создание батча

Батч создаётся конструктором `th.batch(...)`, который используют как `async with`:

<!-- tallyho-example: guide-batches-basics -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне — адаптер вашего брокера
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def resize(image_id: int) -> None:
    if image_id % 5 == 0:
        item.skip("already_resized")  # задача не понадобилась
        return
    if image_id == 7:
        item.error("corrupt_file", detail={"image_id": image_id})  # ошибка без исключения
        return
    item.incr("bytes_saved", 1024)  # своя метрика батча
    item.ok("resized")


try:
    async with th.batch("thumbnails", key="album:1") as batch:
        await batch.add(resize, 1)  # один вызов
        await batch.map(resize, range(2, 9))  # по вызову на каждый элемент
        await batch.add_calls([th.call(resize, 9).opts(weight=3), th.call(resize, 10)])
    # выход из блока: батч закрыт (seal), транзакция закоммичена, задачи ждут отправки в брокер

    await broker.drain()  # в тестах задачи выполняет InlineBroker

    view = await batch.handle.view()
    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    progress = view.progress
    assert (progress.found, progress.ok, progress.skip, progress.error) == (10, 7, 2, 1)
    labels = view.labels  # счётчики по меткам итога
    assert (labels["resized"], labels["already_resized"], labels["corrupt_file"]) == (7, 2, 1)
    assert view.metrics["bytes_saved"] == 7 * 1024  # сумма item.incr

    # Корневой батч всегда можно найти по (kind, key).
    again = await th.find("thumbnails", "album:1")
    assert again.id == batch.handle.id
finally:
    await th.aclose()
```

Что важно знать:

* **Одна транзакция.** Всё, что добавлено внутри блока, записывается одной транзакцией. При
  исключении в блоке транзакция откатывается, и в брокер не уходит ничего. После коммита задачи
  сразу [отправляет в брокер](operations.md#отправка-в-брокер) тот же процесс; в тестах это
  делает `InlineBroker` по командам `step()` и `drain()`.
* **Seal при выходе.** При успешном выходе из блока батч закрывается сам. `await batch.seal()`
  можно вызвать и раньше; после него `add` бросает `ConfigurationError`. Если объём работы заранее
  неизвестен, не держите батч открытым, а порождайте задачи из задач — см. [`spawn`](#spawn-задачи-порождают-задачи).
* **Идемпотентность по `(kind, key)`.** Повторный `th.batch(kind, key=...)` с тем же ключом не
  создаёт второй батч, а возвращает существующий; его параметры, атрибуты и `memo` не меняются.
  Двойной клик по кнопке «запустить» не запустит рассылку дважды. Добавить задачи в уже закрытый
  батч при этом нельзя: `add` бросит `SealError`.
* **Пустой батч** финализируется сразу со статусом `SUCCEEDED`.
* **`batch.handle`** — ссылка на батч: по ней читают прогресс и управляют деревом. Сохраните
  `batch.handle.id` в своей таблице, чтобы потом получить ссылку через `th.handle(batch_id)`.

### В вашей транзакции

Передайте `session=` (`AsyncSession` или `AsyncConnection`), и батч будет создан в вашей
транзакции — атомарно с доменной записью. `commit` делаете вы; при откате в брокер не уходит ничего.

<!-- tallyho-noexec: фрагмент использует доменную модель Campaign вашего приложения -->
```python
async def schedule(session: AsyncSession, campaign_id: int) -> None:
    campaign = await session.get(Campaign, campaign_id, with_for_update=True)
    async with th.batch("campaign_deliveries", key=f"campaign:{campaign.id}", session=session) as batch:
        await batch.map(send_email, campaign.contact_ids)
    campaign.status = "running"
    campaign.batch_id = batch.handle.id
    # commit делает вызывающий: доменная запись и батч появятся вместе
```

Ваше соединение должно находить таблицы tallyho без имени схемы — как это настроить, описано в
разделе [«Схема в ваших сессиях»](installation.md#схема-в-ваших-сессиях).

Тот же параметр `session=` есть у всех управляющих операций (`pause`, `resume`, `cancel`,
`reschedule`, `retry_failed`, `release`). Порядок работы везде один: **сначала ваша строка, потом
вызов tallyho**. Хуки используют тот же порядок блокировок, поэтому дедлоков между вашим API и
хуками не возникает.

### Параметры `th.batch`

| Параметр | Значение |
|---|---|
| `kind` | тип батча; обязательный |
| `key` | ключ идемпотентности; без него каждый вызов создаёт новый батч |
| `start_at` | отложенный старт: задачи уйдут в брокер не раньше этого момента |
| `deadline` | `datetime` или `timedelta`: если батч не завершился к сроку, он отменяется с итогом `FAILED` |
| `failure_policy` | [политика ошибок](#политики-ошибок) |
| `max_in_flight` | сколько задач **этого** батча одновременно находится в брокере и в работе |
| `expected_total` | заранее известный объём — для прогресса до закрытия батча |
| `max_items` | лимит задач на всё дерево; только у корня |
| `retention`, `release_required` | хранение завершённого дерева — см. [retention](hooks.md#retention-и-release); только у корня |
| `attributes`, `memo` | [контекст корреляции](#атрибуты-memo-и-листинг); только у корня |
| `on_succeeded`, `on_completed_with_errors`, `on_failed`, `on_cancelled`, `on_finalized_task` | [колбэк-задачи](hooks.md#колбэк-задачи) на завершение |
| `session` | ваша сессия или соединение |

`max_in_flight` ограничивает один экземпляр батча. Общий лимит на тип задачи для всех батчей сразу —
настройка брокера.

## Задача и её итог

Задача — обычная `async def`. Внутри неё доступен объект `item` (`from tallyho import item`):

| Вызов | Что делает |
|---|---|
| `item.ok(label=None, *, result=None)` | успешный итог; без метки считается как `"ok"` |
| `item.skip(label=None)` | задача пропущена — не ошибка |
| `item.error(label=None, *, detail=None)` | ошибочный итог без исключения и без ретраев |
| `item.incr(name, value=1)` | прибавить к метрике батча |
| `item.progress(done, total=None)` | собственный прогресс долгой задачи; виден в `handle.in_flight()` |
| `item.cancelled()` | `True`, если батч отменяют: долгой задаче пора выйти |
| `item.spawn(...)`, `item.spawn_call(...)`, `item.expect(...)`, `item.sub_batch(...)` | [динамический fan-out](#spawn-задачи-порождают-задачи) |
| `await item.complete_in(session)` | завершить задачу [в вашей транзакции](hooks.md#итог-задачи-в-вашей-транзакции) |
| `item.id()`, `item.current()` | идентификатор и контекст текущей задачи; `None` вне задачи |

Правила итога:

* Задача вернулась без вызова `ok/skip/error` — итог `ok` с меткой `"ok"`.
* Последний вызов `ok/skip/error` перед возвратом выигрывает. Итог записывается после возврата из
  функции, вместе со всеми `spawn`, `incr` и `expect` — одной транзакцией.
* **Исключение — это ретрай брокера**, а не итог. Когда брокер исчерпал попытки, задача получает
  `error` с меткой `"exhausted"`. Если воркер умер и аренда истекла на последней попытке — метка
  `"lease_expired"`.
* После завершения задача неизменяема. Повторная доставка того же сообщения брокером задачу не
  вызовет.
* Ошибочные задачи помечаются для быстрого поиска по метке; `ok` и `skip` — нет. Поведение меняет
  параметр `mark=True/False` у `ok`, `skip` и `error`.
* Вне отслеживаемой задачи вызовы `item.*` ничего не делают, поэтому функцию можно вызывать напрямую
  в юнит-тестах.
* **Метки и метрики делят одно пространство имён.** `view.labels` и `view.metrics` (и те же поля
  сводки в хуках) возвращают один общий набор счётчиков: и число задач по меткам итога, и суммы
  `item.incr`. Не называйте метрику так же, как метку, и читайте нужные ключи по имени, а не весь
  словарь целиком.

### Вызовы и опции

`th.call(fn, *args, **kwargs)` готовит вызов задачи, `.opts(...)` задаёт его опции:

| Опция | Значение |
|---|---|
| `key` | ключ дедупликации задачи внутри батча |
| `weight` | вес задачи в доле прогресса `ratio`, целое ≥ 1 (по умолчанию 1) |
| `queue` | очередь брокера |
| прочие именованные | опции брокера (`max_retries`, `priority`, `timeout` …); их проверяет адаптер — см. [опции flexiq](flexiq.md#опции-постановки) |

Подготовленные вызовы принимают `batch.add_calls([...])`, `item.spawn_call(call, into=...)` и
параметры колбэков `on_...=`.

## Под-батчи и конвейеры этапов

`batch.sub_batch(key, ...)` создаёт под-батч. Для родителя он одна задача: родитель завершится
только после всех своих под-батчей. Под-батч, которому указали источники `fed_by=[...]`, становится
**этапом конвейера**: его наполняют задачи источников, а закрывает сама библиотека.

<!-- tallyho-example: guide-batches-pipeline -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()

CATALOG = {1: ["a", "b"], 2: ["b", "c"], 3: ["d"]}  # страница → карточки на ней


async def parse_page(page: int) -> None:
    if page == 1:  # число страниц становится известно после первой
        item.expect(len(CATALOG))
        for other in range(2, len(CATALOG) + 1):
            item.spawn(parse_page, other)  # в свой этап
    for card in CATALOG[page]:
        item.spawn(parse_card, card, into="cards", key=card)  # в следующий этап, с дедупликацией


async def parse_card(card: str) -> None:
    item.ok("parsed")


try:
    async with th.batch("catalog_parse", key="catalog:7", max_items=10_000) as root:
        pages = root.sub_batch("pages", max_depth=1)
        root.sub_batch("cards", fed_by=[pages], max_in_flight=2)
        await pages.add(parse_page, 1)
    # при выходе закрыты корень и pages; cards закроется сам, когда закончится pages

    await broker.drain()

    view = await root.handle.view()
    assert view.state is BatchState.SUCCEEDED
    assert view.children["pages"].progress.found == 3
    cards = view.children["cards"].progress
    assert (cards.found, cards.ok, cards.duplicates) == (4, 4, 1)  # карточка "b" встретилась дважды
finally:
    await th.aclose()
```

### Правила конвейера

* **Этап не ждёт конца предыдущего.** `cards` начинает работать с первой же найденной карточки,
  пока `pages` ещё разбирает страницы.
* **Этап с `fed_by` закрывает библиотека** — когда все его источники финализированы. Вызывать
  `seal()` для такого этапа нельзя (`SealError`). Этапы без `fed_by` и корень закрываются при
  выходе из `async with`.
* **Кто может писать в этап.** В этап с `fed_by` добавляют только его собственные задачи и задачи
  его источников (`into=`). Продюсер добавлять в него не может: `add` бросит `SpawnTargetError`.
  То же исключение получит внутри себя задача, которая указала в `into=` этап, для которого её батч
  не источник; как и любое исключение, оно ведёт к ретраям и итогу `exhausted`.
* **`fed_by` — только под-батчи того же дерева**, без циклов. «Страница порождает страницу» — это
  `spawn` в свой этап, а не `fed_by` на себя.
* **Пустой этап — не зависание.** Этап, в который не пришло ни одной задачи, закрывается и
  финализируется сразу, каскадом закрывая следующие.
* **Источник завершился с ошибками, провалом или отменой.** По умолчанию этап всё равно закрывается
  и доделывает полученное (`on_feeder_failed="seal"`). `on_feeder_failed="cancel"` отменяет этап.
* **Порядок финализации.** Родитель финализируется только после всех детей; их хуки `on_finalized`
  коммитятся раньше родительского. Если хотя бы один прямой ребёнок завершился с ошибками, провалом
  или отменой, родитель без собственной более сильной причины станет `COMPLETED_WITH_ERRORS`.
* **`kind` под-батча** по умолчанию — `<kind родителя>.<key>` (`catalog_parse.cards`). Поэтому хук,
  зарегистрированный на `kind` корня, не срабатывает на каждом этапе. Задайте `kind=` явно, если
  этапу нужен собственный хук.

Параметры `sub_batch`: `kind`, `fed_by`, `on_feeder_failed`, `start_at`, `deadline`, `failure_policy`,
`max_in_flight`, `expected_total`, `max_depth` и колбэки `on_...=`. Параметры `retention`,
`release_required`, `max_items`, `attributes` и `memo` задаются только у корня.

### `spawn`: задачи порождают задачи

| Вызов в задаче | Что делает |
|---|---|
| `item.spawn(fn, *args, **kwargs)` | добавить задачу в свой батч |
| `item.spawn(fn, *args, into="cards", key="…")` | добавить задачу в этап `cards`, для которого свой батч — источник |
| `item.spawn_call(th.call(fn, ...).opts(...), into=...)` | то же с опциями вызова (вес, очередь, опции брокера) |
| `item.expect(n)`, `item.expect(n, into="cards")` | сообщить ожидаемый объём своего или целевого батча |

* **Атомарность.** `spawn` только складывает вызовы в буфер. Они записываются одной транзакцией с
  завершением самой задачи: либо задача завершена и все её дети созданы, либо ничего. Если задача
  упала, её дети не появятся; при повторе они будут порождены заново.
* **`into=`** — ключ под-батча внутри дерева или идентификатор батча.
* **`key=`** — ключ дедупликации в целевом батче. Повторно найденная ссылка не создаёт задачу, а
  увеличивает счётчик `duplicates`. Для URL берите нормализованный адрес без фрагмента. Дедупликация
  действует всё время жизни батча.
* **`max_depth`** (у под-батча) ограничивает глубину самоподпитки: задача, добавленная продюсером
  или через `into=`, имеет глубину 0, порождённая ею в том же батче — 1 и так далее.
* **`max_items`** (у корня) ограничивает число задач во всём дереве. Задачи сверх любого из двух
  лимитов не создаются и учитываются в `skipped_by_limit` — это не ошибка. Лимит `max_items`
  мягкий — см. [ограничения](limitations.md#мягкий-max_items).
* Имена `into` и `key` зарезервированы: `item.spawn` забирает их себе и в функцию не передаёт.

Задача может создать и целый под-батч — он тоже появится атомарно с её завершением:

<!-- tallyho-noexec: фрагмент тела задачи; render_part — задача вашего приложения -->
```python
async def split_video(video_id: int, parts: int) -> None:
    async with item.sub_batch(f"video:{video_id}", max_in_flight=4) as sub:
        for part in range(parts):
            sub.add(render_part, video_id, part)  # без await: вызовы копятся в буфере
```

## Политики ошибок

Политика решает, что делать с батчем, когда задачи завершаются с ошибкой. Фабрики доступны как
`th.FailurePolicy`:

| Политика | Поведение |
|---|---|
| `th.FailurePolicy.continue_()` | по умолчанию: ошибки не останавливают батч, итог — `COMPLETED_WITH_ERRORS` |
| `th.FailurePolicy.fail_fast()` | первая ошибка отменяет оставшиеся задачи, итог — `FAILED` |
| `th.FailurePolicy.threshold(ratio=, min_processed=0, labels=None, action="fail")` | доля ошибок превысила порог → `action` |

Как считается порог `threshold`:

* обработанные задачи — `ok + skip + error`; отменённые не учитываются;
* числитель — все ошибки или, если задан `labels=[...]`, только задачи с этими метками;
* политика срабатывает, когда обработано не меньше `min_processed` задач и доля **строго больше**
  `ratio`;
* `action="fail"` отменяет оставшиеся задачи и даёт итог `FAILED`; `action="pause"` ставит **всё
  дерево** на паузу и ждёт вашего решения. Оба действия атомарно вызывают хук
  [`on_policy_breach`](hooks.md#on_policy_breach).

<!-- tallyho-example: guide-batches-policy -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState, CancelReason
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def charge(order_id: int) -> None:
    if order_id % 2:
        item.error("declined")


try:
    policy = th.FailurePolicy.threshold(ratio=0.2, min_processed=4, labels=["declined"])
    async with th.batch("billing", key="run:1", failure_policy=policy) as batch:
        await batch.map(charge, range(10))

    await broker.drain()

    view = await batch.handle.view()
    assert view.state is BatchState.FAILED
    assert view.reason is CancelReason.POLICY  # почему батч провален
    assert view.progress.error >= 2
    assert view.progress.cancelled >= 1  # оставшиеся задачи не выполнялись
    assert view.progress.done == view.progress.found == 10
finally:
    await th.aclose()
```

Провал по политике, дедлайну и `fail_fast` — не мгновенный переход, а запрос отмены: новые задачи
не принимаются, неотправленные сразу становятся `cancelled`, выполняющиеся доделываются, после чего
батч финализируется обычным путём, через хук `on_finalized`. Причину показывает `view.reason`:
`policy`, `fail_fast`, `deadline` или `cancel`.

## Операции над деревом

Все операции вызываются на `BatchHandle` и действуют на батч вместе с его под-батчами.

| Операция | Что делает |
|---|---|
| `await handle.reschedule(start_at)` | переносит старт ещё не отправленных задач; возвращает число уже отправленных, которых перенос не коснулся |
| `await handle.pause()` | пауза: новые задачи не отправляются, пришедшие из брокера откладываются без выполнения, выполняющиеся доделываются |
| `await handle.resume()` | снимает паузу |
| `await handle.cancel()` | запрос отмены: неотправленные задачи сразу `cancelled`, отправленные отменяются при получении воркером, выполняющиеся доделываются; итог — `CANCELLED` |
| `await handle.retry_failed(labels=None)` | возвращает ошибочные задачи в работу; возвращает их число |
| `await handle.retry_finalize()` | немедленно повторяет финализацию, если [хук упал](hooks.md#повтор-упавшего-хука) |
| `await handle.release()` | разрешает удалить дерево по [retention](hooks.md#retention-и-release) |

Каждая операция принимает `session=` и тогда выполняется в вашей транзакции.

<!-- tallyho-example: guide-batches-operations -->
```python
from datetime import UTC, datetime, timedelta

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))  # управляемое время для примера
broker = InlineBroker()
th = Tallyho(engine, schema=schema, clock=clock, relay_grace=timedelta(0))
th.install(broker.adapter)
await th.migrate()

delivered: list[int] = []
glitches = {3}  # посылка 3 в первый раз не доставится


async def deliver(parcel: int) -> None:
    if parcel in glitches:
        glitches.discard(parcel)
        item.error("courier_unavailable")
        return
    delivered.append(parcel)


try:
    # Отложенный старт и перенос.
    start = clock.now() + timedelta(hours=1)
    async with th.batch("deliveries", key="route:1", start_at=start) as batch:
        await batch.map(deliver, range(6))
    handle = batch.handle
    assert await broker.drain() == 0  # до start_at в брокер ничего не уходит
    assert await handle.reschedule(start + timedelta(hours=1)) == 0
    clock.advance(hours=1)
    assert await broker.drain() == 0  # старт перенесён
    clock.advance(hours=1)
    await th.run_maintenance_once()  # срок наступил: фоновый проход отправил задачи в брокер

    # Пауза и продолжение.
    assert await broker.step(2) == 2  # выполнить две задачи
    await handle.pause()
    await broker.drain()  # сообщения из брокера откладываются, задачи не выполняются
    paused = await handle.view()
    assert paused.paused
    assert paused.progress.done == 2
    await handle.resume()
    await broker.drain()
    assert (await handle.view()).state is BatchState.COMPLETED_WITH_ERRORS

    # Повтор упавших.
    assert await handle.retry_failed(labels=["courier_unavailable"]) == 1
    await broker.drain()
    assert (await handle.view()).state is BatchState.SUCCEEDED
    assert sorted(delivered) == [0, 1, 2, 3, 4, 5]

    # Отмена посреди работы.
    async with th.batch("deliveries", key="route:2") as other:
        await other.map(deliver, range(10, 14))
    assert await broker.step(1) == 1  # одна посылка успела уехать
    await other.handle.cancel()
    await broker.drain()  # уже отправленные задачи отменяются, когда их получает воркер
    cancelled = await other.handle.view()
    assert cancelled.state is BatchState.CANCELLED
    assert (cancelled.progress.ok, cancelled.progress.cancelled) == (1, 3)
finally:
    await th.aclose()
```

Замечания:

* **Пауза и отложенный старт — флаги, а не состояния.** Батч на паузе остаётся `OPEN` или `SEALED`
  (`view.paused`, `view.paused_at`). Если на паузу нажали, когда оставались только выполняющиеся
  задачи, батч может финализироваться во время паузы — ваш хук должен это допускать.
* **Отмена кооперативна.** Уже выполняющаяся задача не прерывается: проверяйте `item.cancelled()` в
  долгих задачах и выходите сами.
* **`retry_failed`** возможен из `COMPLETED_WITH_ERRORS` и `FAILED`. Он переоткрывает батч, после
  выполнения задач батч финализируется заново, и `on_finalized` вызывается ещё раз с новым итогом.
  На корне он переоткрывает весь конвейер от источников к получателям. Для отдельного этапа,
  получатели которого уже финализированы, он бросает `DownstreamFinalized`: повторяйте с корня.
* **Дедлайн** (`deadline=`) проверяют фоновые проверки: просроченный батч получает запрос отмены
  с причиной `deadline` и итог `FAILED`.

## Чтение прогресса

`await handle.view()` возвращает `BatchView` — согласованный снимок батча и всего его поддерева.

| Поле `BatchView` | Значение |
|---|---|
| `id`, `kind`, `key`, `state` | идентификация и состояние |
| `progress` | счётчики и оценки — таблица ниже |
| `labels`, `metrics` | счётчики батча по именам: число задач по меткам итога и суммы `item.incr` ([общее пространство имён](#задача-и-её-итог)) |
| `children` | под-батчи по ключу: `view.children["send"]` |
| `reason` | причина запроса отмены: `cancel`, `deadline`, `fail_fast`, `policy` |
| `paused`, `paused_at`, `cancel_requested`, `cancel_requested_at` | флаги паузы и отмены |
| `start_at`, `deadline_at`, `created_at`, `finished_at` | времена |
| `hook_attempts`, `hook_error` | состояние [упавшего хука](hooks.md#повтор-упавшего-хука) |
| `attributes`, `memo` | контекст корня |

| Поле `Progress` | Значение |
|---|---|
| `found` | сколько уникальных задач добавлено (растёт по ходу работы) |
| `ok`, `skip`, `error`, `cancelled` | завершённые по классам итога |
| `done` | их сумма |
| `pending` | `found - done` |
| `in_flight` | выполняются прямо сейчас |
| `queued` | ждут выполнения |
| `duplicates` | отсечено дедупликацией по `key` |
| `skipped_by_limit` | не создано из-за `max_items` или `max_depth` |
| `final` | батч финализирован, числа окончательные |
| `expected` | ожидаемый итог или `None`, если оценить нечем |
| `expected_is_estimate` | `expected` — оценка, а не точное число |
| `estimate_basis` | на скольких завершённых задачах источников построена оценка |
| `ratio` | доля выполненного по весам задач, от 0 до 1, или `None` |
| `eta` | оценка времени до завершения или `None` |

Как получается `expected`:

1. Батч закрыт или завершён — точное число, равное `found`.
2. Задан `expected_total` или вызван `expect(n)` — `max(found, n)`; до закрытия батча это оценка.
3. У этапа есть источники и накопилась достаточная выборка — оценка по среднему числу порождённых
   задач на одну завершённую задачу источника. Оценка появляется после 20 завершённых задач
   источника или 5% его объёма (настройки `estimate_min_basis`, `estimate_min_share`) и уточняется
   по ходу работы.
4. Иначе `None`: показывайте только «найдено».

В счётчиках родителя каждый под-батч учитывается как одна задача: у корня конвейера из двух этапов
`found == 2`. Числа по настоящим задачам смотрите в `view.children[...]`.

`ratio` может немного уменьшиться, если оценка объёма выросла. Чтобы полоска прогресса не ехала
назад, храните у себя максимум — так сделано в [примере хука](hooks.md#on_progress).

Остальные способы чтения:

| Вызов | Для чего |
|---|---|
| `await handle.in_flight(limit=100)` | выполняющиеся задачи: воркер, попытка, возраст аренды, `progress_done/progress_total` из `item.progress` |
| `handle.watch()` | асинхронный поток `BatchView` при изменениях, до терминального снимка |
| `await handle.wait(timeout=None)` | дождаться терминального состояния; `timeout` — секунды или `timedelta` |
| `await handle.child("send")` | ссылка на прямой под-батч по ключу |

Если прогресс показывается пользователям, не опрашивайте `view()` из каждого запроса: пусть хук
[`on_progress`](hooks.md#on_progress) пишет снимки в вашу таблицу, а интерфейс читает её.
`handle.view()` удалённого по retention батча бросает `BatchPurged`.

## Атрибуты, memo и листинг

**Атрибуты** — неизменяемые пары «ключ → `str | int | bool`» корневого батча для корреляции и
поиска. **`memo`** — неизменяемый JSON-объект для диагностики; он не индексируется и в фильтрах не
участвует. Оба задаются только при создании корня и видны в `view.attributes` / `view.memo` любого
узла дерева, а атрибуты — ещё и в сводке, которую получают хуки.

<!-- tallyho-example: guide-batches-listing -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState, ItemState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def send(address: str) -> None:
    if address.endswith("@bounce.test"):
        item.error("hard_bounce", detail="550 user unknown")
    else:
        item.ok("sent", result={"message_id": f"msg:{address}"})


try:
    recipients = ["ada@ok.test", "grace@ok.test", "gone@bounce.test"]
    async with th.batch(
        "newsletter",
        key="issue:42",
        attributes={"tenant": "acme", "issue": 42},
        memo={"requested_by": "ops@example.test"},
    ) as batch:
        await batch.add_calls(th.call(send, address).opts(key=address) for address in recipients)
    await broker.drain()

    # Листинг корневых батчей: от новых к старым, постранично, без чтения счётчиков.
    page = await th.list_batches(
        kinds=["newsletter"],
        states=[BatchState.COMPLETED_WITH_ERRORS],
        attributes={"tenant": "acme"},
        limit=50,
    )
    assert [(info.key, info.state) for info in page.items] == [
        ("issue:42", BatchState.COMPLETED_WITH_ERRORS)
    ]
    assert page.next_cursor is None  # страниц больше нет

    view = await th.handle(page.items[0].id).view()  # за прогрессом — view()
    assert dict(view.attributes) == {"tenant": "acme", "issue": 42}
    assert view.memo == {"requested_by": "ops@example.test"}

    # Задачи батча по меткам: только помеченные (по умолчанию — ошибки).
    handle = await th.find("newsletter", "issue:42")
    bounced = [entry async for entry in handle.items(labels=["hard_bounce"])]
    assert [(entry.key, entry.label, entry.error) for entry in bounced] == [
        ("gone@bounce.test", "hard_bounce", "550 user unknown")
    ]

    # Задачи батча по состояниям: любые, включая успешные и отменённые.
    delivered = sorted([entry.key async for entry in handle.items(states={ItemState.OK})])
    assert delivered == ["ada@ok.test", "grace@ok.test"]
finally:
    await th.aclose()
```

### Правила атрибутов

* Значения — только `str`, `int` и `bool`; `UUID` превращается в строку и при записи, и в фильтре.
  `float`, `None`, `datetime` и коллекции отклоняются с `InvalidAttributesError`.
* Типы не приводятся: `{"issue": 42}` и `{"issue": "42"}` — разные значения, и фильтр по одному не
  найдёт другое.
* Ключ — непустая строка; префикс `tallyho.` зарезервирован.
* Лимиты по умолчанию: 32 атрибута, ключ до 128 байт, строковое значение до 512 байт, все атрибуты
  до 8 КиБ, `memo` до 16 КиБ ([настройки](installation.md#настройки)).
* Атрибуты и `memo` не попадают в логи и телеметрию, но хранятся в базе открытым текстом: не кладите
  в них секреты.
* Тенант — обычный атрибут. Фильтровать по нему в листинге обязано ваше приложение: tallyho не
  знает, кто вызывает.

### `th.list_batches`

| Параметр | Значение |
|---|---|
| `kinds` | коллекция `kind`; `None` — любые |
| `states` | коллекция `BatchState`; `None` — любые |
| `attributes` | пары, которые все должны совпасть с атрибутами корня |
| `created_after` / `created_before` | границы по времени создания, полуинтервал `[after, before)` |
| `limit` | размер страницы: по умолчанию 100, не больше 1 000 |
| `cursor` | значение `next_cursor` предыдущей страницы; чужой или испорченный курсор — `ConfigurationError` |

Результат — `BatchPage(items, next_cursor)`. Каждый элемент — `BatchInfo` с полями `id`, `kind`,
`key`, `state`, `attributes`, `created_at`, `finished_at`. Листинг возвращает только корни и не
читает счётчики. Батч, созданный во время обхода, на уже пройденные страницы не попадает и не
сдвигает их.

### `handle.items`

`handle.items(*, states=None, labels=None)` — асинхронный итератор `ItemView` **одного батча**, не
поддерева. Задачи этапа читайте у этапа: `await root.child("send")`.

* Хотя бы один фильтр обязателен; вызов без фильтров — `ConfigurationError`.
* `labels=` находит только помеченные задачи (по умолчанию — ошибки) и работает быстро при любом
  размере батча.
* `states=` находит задачи в любом состоянии, включая `CANCELLED`, которые не помечаются. Батч
  читается окнами, и число запросов пропорционально размеру батча, а не числу совпадений.
* Оба фильтра вместе — пересечение.
* Порядок выдачи не гарантируется. Обход не изолирован снимком: задача, изменившаяся во время
  обхода, может попасть в выдачу в любом из двух состояний. Для точного результата читайте
  финализированный батч.
* Под-батчи выдаются как задачи с заполненным `child_batch_id`.
* Поля `ItemView`: `id`, `batch_id`, `state`, `task_name`, `label`, `attempt`, `depth`, `key`,
  `weight`, `child_batch_id`, `result`, `error`, `created_at`, `finished_at`.

## Ошибки

Все исключения библиотеки наследуются от `tallyho.TallyhoError`; классы лежат в
`tallyho.model.errors`.

| Исключение | Когда |
|---|---|
| `ConfigurationError` | неверные параметры, адаптер не установлен, `add` после закрытия конструктора |
| `InvalidAttributesError` | атрибуты или `memo` нарушают правила |
| `UnsupportedOption` | опция брокера несовместима с отслеживаемыми задачами |
| `NotFoundError` | батч не найден |
| `BatchPurged` | батч удалён по retention |
| `InvalidStateError` | операция недопустима в текущем состоянии батча |
| `ClosedError` | установка закрыта `th.aclose()`: запись, операции над батчем и maintenance недоступны |
| `SealError` | `seal()` этапа с `fed_by`; добавление в закрытый, завершённый или отменяемый батч |
| `SpawnTargetError` | запись в этап, писателем которого вызывающий не является |
| `DownstreamFinalized` | `retry_failed` этапа, чьи получатели уже финализированы |
| `ConcurrentModification` | строку батча одновременно меняла другая транзакция, повторы не помогли |
| `HookTransactionError` | хук вызвал `commit()` или `rollback()` |
| `HookMissingError` | батчу нужен хук, который не зарегистрирован в процессе |
| `CompleterError` | запись завершений не прошла; задача вернётся брокеру и будет повторена |

## Что дальше

* Перенести итог и прогресс в свои таблицы — [Хуки](hooks.md).
* Проверить сценарий без брокера — [Тестирование](testing.md).
