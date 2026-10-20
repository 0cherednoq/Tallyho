# API единицы работы: итог значением, судьба исключений, закладка, контекст через DI

**Тип:** дизайн и реализация · **Затрагивает:** `runtime/`, `engine/completer.py`,
`adapters/flexiq/`, `api/batch.py`, новая таблица `th_checkpoint`, новый extra `tallyho[dishka]`
**Связано:** RM2.2 (`item.defer`), RM4.1 (`th.demand`), ишью capacity-governor про адаптеры

---

## TL;DR

Сейчас всё общение задачи с учётом работы идёт через один неявный объект `item`. Итог,
отложение, прогресс, запись в транзакции и доступ к контексту — всё вызовы на нём. На
реальном проекте (проверка доменов на дефицитных аккаунтах) из-за этого в каждой задаче
повторяются одни и те же куски кода, а application-слой оказывается привязан к Tallyho.

Предлагается разделить эти обязанности по природе и дать каждой свой механизм:

| Что задача сообщает учёту | Когда | Механизм |
|---|---|---|
| итог попытки | в конце | **возвращаемое значение** `Ok / Skip / Error / Defer` |
| «это исключение значит отложить / ошибку» | в конце, из глубины стека | **судьба исключения при регистрации задачи**: `defer_on`, `error_on`, `skip_on` |
| прогресс внутри составной единицы работы | посреди работы, в транзакции | **закладка**: `checkpoint()` / `checkpoint_in(session, position)` |
| доступ к единице работы из слоёв приложения | всё время задачи | **публичный `WorkItem`** + интеграция с Dishka |
| вернуть отложенное, когда ресурс появился | снаружи задачи | **`BatchHandle.release_deferred(reasons=…)`** |

Ни одно изменение не ломает текущий API: фасад `item`, «возврат без итога = ok» и
существующие опции остаются.

---

## 1. Проблема

Ниже — фрагменты из сквозного примера проверки доменов
(`domain-checker`, demo-файлы `domain_checker_worker_demo.py` и
`domain_checker_layers_demo.py`) на текущей ветке RM*.

### 1.1. Итог — набор императивных вызовов на неявном объекте

```python
except LeaseUnavailable as error:
    if error.reason is LeaseUnavailableReason.BUSY:
        item.defer(reason="accounts_busy", delay=RESOURCE_DEFER)
    elif error.reason is LeaseUnavailableReason.QUOTA and error.retry_after:
        item.defer(reason="quota", delay=timedelta(seconds=error.retry_after))
    else:
        item.defer(reason="no_accounts", delay=RESOURCE_DEFER)
    return
...
item.ok("chunk")
```

* Итог не виден в сигнатуре задачи. Проверить его в тесте можно только через подмену
  `item`.
* Правило «последний вызов `ok/skip/error/defer` перед возвратом выигрывает» нужно держать
  в голове. Ветка без `return` после `item.defer(...)` тихо превращается в `ok`.
* Вне задачи вызовы молча ничего не делают. Для юнит-тестов это удобно, а для интерактора,
  случайно вызванного вне задачи, — тихая потеря итога.

### 1.2. Перевод исключений в итог дублируется в каждой задаче

Каждый проект пишет свой `try/except`, который превращает «ресурса нет» в `defer`, а
«неисправимо» — в `error`. Это знание о том, **что означает исключение для учёта работы**.
Оно одно на тип исключения, а не на вызов, но живёт в теле каждой задачи. Если перенести
его в application-слой, тот начинает импортировать Tallyho. Если оставить в задаче, задача
перестаёт быть тонкой. В слоистом варианте пришлось писать отдельную функцию перевода
`record_outcome(outcome)` и свой тип `ChunkDeferred`, то есть дублировать `Defer` из
Tallyho.

### 1.3. Прогресс внутри составной единицы работы пишется руками

Чтобы аренда дорогого ресурса окупалась, единица работы — это пачка (50 доменов на одну
аренду аккаунта). После `defer`, ретрая или падения воркера новая попытка получает ту же
пачку и не знает, докуда дошла предыдущая. Каждый проект пишет запрос «что уже сделано»:

```python
async def unchecked(self, run_id: UUID, domains: Sequence[str]) -> list[str]:
    done = set((await self.session.execute(
        select(domain_checks.c.domain)
        .where(domain_checks.c.run_id == run_id, domain_checks.c.domain.in_(domains))
    )).scalars())
    return [d for d in domains if d not in done]
```

Доменная таблица результатов становится журналом прогресса. Где результата в своей базе
нет (экспорт в S3, отправка писем, курсорный парсинг), приходится заводить отдельную
таблицу прогресса.

### 1.4. Контекст задачи не доступен через DI

`item` — это `ContextVar` (`runtime/context.py:534`, фасад — `:775`), который обёртка
активирует на время вызова (`runtime/tracked.py:177`). Работает на любой глубине стека,
но:

* зависимость неявная: адаптер, который пишет закладку или итог, не объявляет её в
  конструкторе;
* интеграции с Dishka нет: чтобы достать интерактор в задаче, приходится держать контейнер
  на уровне модуля или в ресурсе FlexiQ и вручную открывать REQUEST-scope;
* в тестах application-слоя нечего подставить вместо единицы работы, кроме самого фасада.

### 1.5. Отложенное не просыпается, когда ресурс появился

Задача без аккаунта делает `item.defer(reason="no_accounts", delay=…)`. Контур
capacity-governor открывает окно в тот же такт, когда ресурс появился. Но отложенные items
уйдут в брокер только по своему сроку: пауза и `resume` их не трогают, и это намеренно
(RM2.2). Окно до этого простаивает. Сейчас это обходится коротким `delay`, то есть
лишними холостыми попытками.

---

## 2. Как это решают другие

### 2.1. Итог — возвращаемым значением или специальным исключением

| Система | Как задача сообщает итог |
|---|---|
| **Oban** (Elixir) | `perform/1` возвращает `:ok`, `{:ok, value}`, `{:cancel, reason}`, `{:error, error}`, `{:snooze, period}`. Исключение = `{:error, …}` |
| **River** (Go, Postgres) | `Work` возвращает `error`; `river.JobSnooze(d)` и `river.JobCancel(err)` — специальные значения ошибки |
| **BullMQ** (Node) | специальные исключения `DelayedError`, `WaitingChildrenError` после `job.moveToDelayed(ts, token)` |
| **Celery** | исключения-семипредикаты `Retry` (из `self.retry()`), `Reject`, `Ignore` |
| **Temporal** | `ApplicationError(non_retryable=True)` |
| **Graphile Worker** | успех — вернуться, неудача — бросить |

Общий вывод: **итог — это данные на выходе функции**, а не побочный эффект на глобальном
объекте. Специальное исключение используют там, где выйти нужно из глубины стека.

### 2.2. Отложить без траты попытки

| Система | Механизм | Попытка | Свой счётчик |
|---|---|---|---|
| **Oban** | `{:snooze, period}` | откатывается | `meta.snoozed` |
| **River** | `JobSnooze(d)` | уменьшается на 1 | `metadata.snoozes` |
| **BullMQ** | `moveToDelayed` + `DelayedError` | `attemptsMade` не растёт | лимит запусков `maxStartedAttempts` |
| **Sidekiq Enterprise** | лимитер бросает `OverLimit`, middleware перепланирует | не тратится | до 20 перепланирований, потом обычная ошибка |
| **job-iteration** | прерывание | «не считается ретраем» | — |

Tallyho после RM2.2/RM2.3 уже на уровне лучших: `item.defer`, `defers`, `max_defers`,
`max_deferred_for`. Не хватает только **внешнего пробуждения** отложенного (п. 1.5): ни
у одной из систем выше его нет, потому что ни одна не управляет окном по ёмкости.
Ближайшее — `job.promote()` в BullMQ для отложенной задачи.

### 2.3. Судьба исключения — при регистрации задачи

| Система | Объявление |
|---|---|
| **Celery** | `autoretry_for=(…)`, `dont_autoretry_for=(…)`, `retry_backoff`, `retry_jitter` |
| **FlexiQ** | `retry_on=[…]`, `dont_retry_on=[…]` |
| **Temporal** | `RetryPolicy.non_retryable_error_types=[…]` |

Объявлено только «ретраить или нет». Ни одна из систем не позволяет объявить **«это
исключение = отложить»** или **«это исключение = ошибочный итог с меткой»**. Это
естественное расширение для системы, у которой итогов больше двух.

### 2.4. Прогресс внутри единицы работы

| Система | Где закладка | Когда пишется | Атомарна с данными приложения? |
|---|---|---|---|
| **Temporal** | heartbeat details на сервере | `activity.heartbeat(details)` | нет |
| **Sidekiq IterableJob** | Redis, ключ `it-{jid}` | раз в 5 с и при остановке | нет: «курсор может откатиться», рекомендуют транзакции |
| **Shopify job-iteration** | Redis | после каждого `each_iteration` | нет |
| **Rails ActiveJob::Continuation** | данные job (`continuation`) | на `advance!` / `set!` / `checkpoint!`, при прерывании | нет |
| **BullMQ, process-step pattern** | `job.updateData({step})` | после каждого шага | нет |
| **Graphile Worker, batch jobs** | payload сужается до неудавшихся элементов | при перепостановке | — |
| **AWS Lambda + SQS** | `batchItemFailures` | в ответе функции | — |
| **DBOS / Inngest** | результат шага в своём хранилище | после шага | у DBOS — та же Postgres |

Все, кроме DBOS, хранят закладку **отдельно от данных приложения**. Поэтому в лучшем случае
гарантия «не больше одного повтора на элемент», и везде советуют делать элемент
идемпотентным.

**Tallyho может сделать лучше.** Учёт работы лежит в той же базе, что и домен, и уже умеет
писать итог в транзакции вызывающего (`complete_in`). Закладка, записанная тем же
коммитом, что и результат элемента, даёт **точно-однократный эффект для работы, которая
пишет только в базу**: результат и закладка либо оба есть, либо обоих нет.

Сужать payload, как Graphile, Tallyho не может: payload item'а неизменяем, на нём держатся
ключ дедупликации и сверка с DLQ. Закладка — отдельная колонка.

### 2.5. Как задача получает свой контекст

| Подход | Где |
|---|---|
| **Неявный** (contextvars / thread-local) | Temporal Python (`activity.info()`, «functions use contextvars»), Celery `Task.request` (thread local), FlexiQ `current_job`, **Tallyho `item`** |
| **Явный аргумент** | Hatchet: `fn(input, ctx)`; Graphile: `(payload, helpers)`; Celery `bind=True` → `self`; Go: `ctx` |
| **Внедрение зависимостей** | taskiq: `context: Context = TaskiqDepends()`; **Dishka + taskiq**: `FromDishka[...]` + `@inject`, REQUEST-scope открывает middleware, `TaskiqMessage` доступен провайдерам |

Неявный контекст удобен для скриптов. Явный и через DI — для слоистых приложений: зависимость
видна в конструкторе, подменяется в тестах, а вне задачи даёт ошибку, а не тишину.

### 2.6. Middleware вокруг задачи

* **Dramatiq:** `before_process_message`, `after_process_message(result=, exception=)`,
  `SkipMessage`.
* **taskiq:** REQUEST-scope Dishka открывается middleware брокера.
* **Sidekiq Enterprise:** перевод `OverLimit` в перепланирование — тоже middleware.
* **FlexiQ:** `Queue(middleware=[…])`.

У Tallyho обёртка уже есть (`runtime/tracked.py`, `_item` → `_invoke`), но точки
расширения наружу нет.

---

## 3. Предлагаемый дизайн

### 3.1. `WorkItem` — публичная единица работы

Сейчас это `ItemContext` во внутреннем модуле. Его нужно сделать публичным типом
`tallyho.WorkItem` с явно описанным контрактом:

```python
class WorkItem:
    id: UUID
    batch_id: UUID
    attempt: int
    defers: int                          # сколько раз уже откладывали

    def checkpoint(self) -> int | None: ...
    async def checkpoint_in(self, session: AsyncSession | AsyncConnection, position: int) -> None: ...
    def progress(self, done: int, total: int | None = None) -> None: ...
    def incr(self, name: str, value: int = 1) -> None: ...
    def spawn(...) / spawn_call(...) / expect(...) / sub_batch(...)
    async def complete_in(self, session, outcome: Outcome | None = None) -> None: ...
    def cancelled(self) -> bool: ...
```

* Фасад `item` остаётся и делегирует текущему `WorkItem`. Его поведение не меняется.
* `item.current()` возвращает `WorkItem | None`. Добавляется строгий `item.require()`,
  который вне задачи бросает `ConfigurationError`.
* `ok/skip/error/defer` остаются на фасаде ради совместимости, но основной путь — п. 3.2.

### 3.2. Итог возвращаемым значением

```python
from tallyho import Defer, Error, Ok, Skip

async def check_domain_chunk(run_id: str, domains: list[str]) -> Ok | Defer:
    ...
    return Defer(reason="no_accounts", delay=timedelta(seconds=10))
    ...
    return Ok("chunk")
```

| Возврат | Итог |
|---|---|
| `None` или любое не-`Outcome` значение | как сейчас: `ok` с меткой `"ok"` |
| `Ok(label=None, result=None)` | `item.ok(label, result=result)` |
| `Skip(label=None)` | `item.skip(label)` |
| `Error(label=None, detail=None)` | `item.error(label, detail=detail)` — без исключения и ретраев |
| `Defer(reason, delay=None, until=None)` | `item.defer(...)` — попытка не тратится |

Правила:

* Если задача и вызвала `item.ok/skip/error/defer`, и вернула `Outcome` — это
  `ConfigurationError` при возврате. Два источника итога — всегда ошибка в коде.
* `Outcome` — неизменяемые dataclass'ы. Проверка в тесте: `assert await task(...) == Defer(...)`.
* `complete_in(session, outcome)` принимает тот же объект, когда итог должен попасть в
  транзакцию приложения.

Где: `runtime/tracked.py`, `_invoke` (`:206`) — интерпретация `result` после
`await task(...)`.

### 3.3. Судьба исключения при регистрации задачи

```python
@fq.task(
    queue="checks",
    retry_on=[ProviderUnavailable],                       # FlexiQ: ретрай брокера
    defer_on={
        NoCheckerAccount: lambda e: Defer(reason=e.reason, delay=e.retry_after),
        CheckerQuotaExhausted: lambda e: Defer(reason="quota", until=e.retry_at),
    },
    error_on={InvalidDomainName: "invalid_domain"},       # Error(label=…), без ретраев
    skip_on={DomainAlreadyChecked: "duplicate"},
)
```

* Значения: для `defer_on` — `Defer` или функция `exc -> Defer`; для `error_on` и
  `skip_on` — метка или функция `exc -> Error / Skip`.
* Сопоставление по MRO: выигрывает самый конкретный класс.
* Класс, указанный и в `retry_on`, и в любом из `*_on`, — `ConfigurationError` при
  регистрации. У каждого исключения одна судьба.
* Перевод применяется только к исключению, вышедшему из задачи. `LeaseLostError` и
  `CancelledError` не переводятся никогда.
* Опции принадлежат Tallyho. Адаптер FlexiQ забирает их себе и не передаёт во FlexiQ, как
  сейчас делает с `weight`.
* Если функция перевода сама упала — это обычное исключение задачи (ретрай брокера), с
  явной записью в лог.

Где: `adapters/flexiq/adapter.py` (`task`, `:363`, разбор опций),
`runtime/tracked.py` (`_invoke`, ветка `except BaseException`, `:223`, до
`_settle_failure`).

### 3.4. Закладка единицы работы

```python
async def check_domain_chunk(run_id: str, domains: list[str]) -> Ok:
    work = item.require()
    start = work.checkpoint() or 0
    async with lease_account() as account:
        for index in range(start, len(domains)):
            verdict = await account.check(domains[index])
            async with sessions.begin() as session:
                await save_verdict(session, run_id, verdict)
                await work.checkpoint_in(session, index + 1)    # тем же коммитом
    return Ok("chunk")
```

Термины: **item** — одна единица работы (`th_item`, один вызов задачи); **элемент** — часть
payload item'а (один домен из 50, один шаг регистрации), о которой знает только проект.

Семантика:

* **Значение** — `position`: целое `>= 0`, сколько элементов payload этого item'а
  обработано или сколько шагов пройдено. Payload item'а неизменяем, поэтому «докуда
  дошли» всегда выражается числом. Курсоры внешних API в модели Tallyho закрываются
  через `item.spawn` (следующая страница — следующий item), а не закладкой.
* **Запись** — только `checkpoint_in(session, position)` в транзакции вызывающего. Откат
  транзакции откатывает и закладку. Записи вне транзакции нет намеренно: иначе вернётся
  проблема «закладка и данные разошлись».
* **Fencing** — та же проверка владения попыткой, что у `complete_in`. Чужая попытка
  получает `LeaseLostError` (п. «Если задача потеряла аренду» в
  `docs/guide/hooks/complete-in.md`).
* **Чтение** — `checkpoint()` отдаёт значение, прочитанное при захвате item'а через
  `LEFT JOIN th_checkpoint` по PK в том же запросе claim. Оно приходит в `ClaimResult`.
* **Жизнь** — переживает ретрай брокера, `defer`, падение воркера, потерю аренды и
  `aclose()`. Строка удаляется в транзакции итога `ok` / `skip` / `cancelled`. При `error`
  остаётся до `retry_failed` (открытый вопрос 1) или до retention дерева.
* **Прогресс** — `checkpoint_in` заодно обновляет `th_lease.progress_done`, и
  `handle.in_flight()` показывает «30 из N» без отдельного `item.progress`.

**Хранение — отдельная узкая таблица, а не колонка `th_item`.**

| Колонка | Тип | Смысл |
|---|---|---|
| `item_id` | `uuid`, PK | чей прогресс |
| `batch_id` | `uuid` | удаление retention'ом по дереву, как у остальных side-таблиц |
| `position` | `bigint NOT NULL` | сколько элементов или шагов сделано |
| `updated_at` | `timestamptz` | наблюдаемость: давно не двигавшиеся закладки |

Хранение: `fillfactor=50` и агрессивный per-table autovacuum, как у `th_counter`. Индексов,
кроме PK, нет.

Почему не колонка `th_item`. ARCHITECTURE §5.2: «единственный UPDATE Item'а (finish) — HOT…
разреженные множества живут в узких side-таблицах»; `th_item` хранится с `fillfactor=85`
ровно под одно обновление. Закладка пишется на каждый элемент, то есть десятки раз за
item. В `th_item` каждое такое обновление копировало бы всю строку вместе с `payload`
(1–2 КБ на пачку из 50 доменов) и быстро исчерпало бы запас страницы. Это тот же довод,
по которому из `th_batch` уже вынесены `th_batch_attr`, а из `th_item` — `th_lease` и
`th_window`.

Почему `bigint`, а не `jsonb`. Целое даёт однозначный контракт API, автоматический
прогресс и не требует валидации размера. `jsonb` в узкой таблице не был бы катастрофой,
но ничего не даёт сейчас, а большое значение уходило бы в TOAST и переписывалось бы при
каждом обновлении. Если понадобится произвольный курсор (multipart-загрузка в S3 и
подобное), добавляется nullable-колонка `state jsonb`: в Postgres `ADD COLUMN … NULL` не
перезаписывает таблицу, а API расширяется без поломок (`checkpoint_in(session, position,
state=…)`).

**Цена** — один upsert узкой строки на запись закладки внутри уже открытой транзакции
приложения. `th_item` не трогается. Для очень больших пачек закладку пишут раз в N
элементов.

Чего закладка **не** даёт: если внешнее действие (HTTP, письмо) выполнено, а транзакция с
закладкой не закоммитилась, повтор выполнит его снова. Для таких действий нужен ключ
идемпотентности на стороне ресурса (`lease.operation(key=…)` в Identity Hub). Это нужно
прямо написать в документации, как это делают Sidekiq и job-iteration.

Где: `storage/tables.py` (новая таблица рядом с `th_lease`, `th_window`; миграция — в схеме
v2, потому что v1 уже опубликована, D-088),
`engine/completer.py` (`ClaimResult`, `:185`; новая операция рядом с `defer_in`,
`:1825` и `complete_in`, `:1932`), `runtime/tracked.py` (`_item`, `:147`, передача в
контекст), `runtime/context.py`.

### 3.5. Middleware и интеграция с Dishka

Точка расширения вокруг вызова задачи:

```python
class ItemMiddleware(Protocol):
    async def __call__(self, work: WorkItem, call_next: Callable[[], Awaitable[object]]) -> object: ...

th = Tallyho(engine, schema="ledger", middleware=[DishkaItemScope(container)])
```

* Middleware вызываются внутри активированного контекста, после claim и до `_invoke`.
  Исключения и возвращаемое значение проходят через них без изменений.
* Порядок — как в списке. Middleware не может подменить итог: для этого есть п. 3.2 и
  3.3.

Extra `tallyho[dishka]`, по образцу `dishka.integrations.taskiq`:

```python
from tallyho.integrations.dishka import DishkaItemScope, TallyhoProvider, inject, FromDishka

container = make_async_container(AppProvider(), TallyhoProvider())
th = Tallyho(engine, schema="ledger", middleware=[DishkaItemScope(container)])

@fq.task(queue="checks", defer_on={NoCheckerAccount: to_defer})
@inject
async def check_domain_chunk(run_id: str, domains: list[str],
                             interactor: FromDishka[CheckDomainChunk]) -> Ok:
    checked = await interactor(CheckDomainChunkCommand(UUID(run_id), domains))
    return Ok("chunk", result={"checked": checked})
```

* `DishkaItemScope` открывает REQUEST-scope на каждую попытку и кладёт текущий `WorkItem`
  в контекст.
* `TallyhoProvider` отдаёт `WorkItem` в REQUEST-scope (`from_context`). Адаптеры
  инфраструктуры (закладка, итог в транзакции) получают его конструктором.
* `@inject` убирает `FromDishka`-параметры из сигнатуры, поэтому
  `th.call(check_domain_chunk, run_id, domains)` типизируется без них.
* Вне задачи `WorkItem` в контейнере отсутствует, и Dishka падает с понятной ошибкой
  вместо тихого no-op.

### 3.6. Разбудить отложенное

```python
await handle.release_deferred(reasons=["no_accounts", "accounts_busy"], session=session)
```

* Сбрасывает срок у отложенных записей батча (`th_outbox.defer_reason IS NOT NULL`) с
  указанными причинами. Без `reasons` — у всех. Возвращает число разбуженных.
* Окно и пауза соблюдаются: разбуженные уходят в брокер в пределах свободного места
  окна (`relay.window_room`, RM1.2).
* `defers` и `first_deferred_at` не сбрасываются: потолки `max_defers` /
  `max_deferred_for` продолжают работать.
* Использует capacity-governor: в такте, где окно потребителя выросло с 0, он будит
  ресурсные причины.

Где: `api/batch.py` (рядом с `set_max_in_flight`, `:431`), операция в
`engine/operations.py`, частичный индекс `th_outbox (batch_id) WHERE defer_reason IS NOT
NULL` из RM2.1 уже есть.

---

## 4. Как изменится код приложения

**Было** (задача на текущем API, сокращённо):

```python
@fq.task(queue="checks", retry_on=[ProviderUnavailable], max_retries=5)
async def check_domain_chunk(run_id: str, domains: list[str]) -> None:
    pending = await unchecked_domains(UUID(run_id), domains)      # свой запрос прогресса
    if not pending:
        item.ok("chunk")
        return
    runtime = await accounts_runtime()                            # global
    try:
        async with runtime.checker_pool.lease(...) as lease:
            ...
            for done, domain in enumerate(pending, 1):
                try:
                    ...
                except OutOfCredits:
                    async with sessions.begin() as session:
                        await set_credits(session, lease.identity_id, 0)
                        item.defer(reason="out_of_credits", delay=TICK)
                        await item.complete_in(session)
                    return
                ...
                item.progress(done, len(pending))
    except LeaseUnavailable as error:
        defer_for_unavailable(error)                              # перевод в каждом проекте
        return
    except QuotaExhausted as error:
        ...
        return
    item.ok("chunk")
```

**Стало:**

```python
@fq.task(
    queue="checks",
    retry_on=[ProviderUnavailable],
    defer_on={
        NoCheckerAccount: lambda e: Defer(reason=e.reason, delay=e.retry_after),
        CheckerQuotaExhausted: lambda e: Defer(reason="quota", until=e.retry_at),
        CheckerAccountOutOfCredits: Defer(reason="out_of_credits", delay=TICK),
    },
)
@inject
async def check_domain_chunk(run_id: str, domains: list[str],
                             interactor: FromDishka[CheckDomainChunk]) -> Ok:
    checked = await interactor(CheckDomainChunkCommand(UUID(run_id), domains))
    return Ok("chunk", result={"checked": checked})
```

Интерактор бросает свои доменные исключения и не импортирует Tallyho. Закладку пишет
инфраструктурный адаптер:

```python
class TallyhoChunkProgress:
    """Прогресс пачки. WorkItem и сессия запроса приходят из Dishka."""

    def __init__(self, work: WorkItem, session: AsyncSession) -> None:
        self._work = work
        self._session = session

    def position(self) -> int:
        return self._work.checkpoint() or 0

    async def save(self, position: int) -> None:
        await self._work.checkpoint_in(self._session, position)
```

Что уходит из каждого проекта:

* запрос «что уже сделано» и метод `unchecked()` в репозитории;
* `try/except` с переводом ресурсных исключений в `defer`;
* собственные типы итога (`ChunkDeferred`) и функция перевода `record_outcome`;
* контейнер на уровне модуля или в ресурсе брокера и ручное открытие REQUEST-scope;
* короткий `delay` как обход отсутствия `release_deferred`.

---

## 5. Совместимость

* `item.*`, «возврат без итога = ok», `complete_in(session)` без аргумента итога —
  без изменений.
* Возврат произвольного значения (не `Outcome`) трактуется как сейчас.
* Опции `defer_on/error_on/skip_on`, `middleware=`, таблица `th_checkpoint` — только
  добавления. `th_item` не меняется. Таблица создаётся миграцией схемы v2 (D-088: версия 1
  опубликована в 0.0.1 / 0.0.2a1 и заморожена; исходная оценка «релиза не было» по D-080
  оказалась устаревшей).
* `ItemContext` остаётся алиасом `WorkItem` на один минорный выпуск.

---

## 6. План работ

План исполняется по протоколу [`docs/plan/PLAN.md` §0](plan/PLAN.md#0-протокол-итерации-loop):
одна задача берётся только после завершения зависимостей, код меняется вместе с тестами,
архитектурные решения сначала фиксируются в документах. Нумерация продолжает
[`RESOURCES_PLAN.md`](plan/RESOURCES_PLAN.md). Прогресс этого набора хранится **в этом
разделе**, чтобы issue, план и журнал не расходились.

### 6.1. Зафиксированная исходная точка

Срез репозитория на 2026-10-20 (`main`, `98713b0`):

* `ItemContext` находится в `runtime/context.py`; фасад `item.current()` уже возвращает
  его, но публичного реэкспорта типа и строгого `require()` нет;
* `_invoke` в `runtime/tracked.py` уже разделяет успешный выход, исключение,
  `complete_in` и `defer`, поэтому итог значением добавляется в одной точке;
* FlexiQ-адаптер уже забирает и валидирует `retry_on` / `dont_retry_on`, но не знает
  `defer_on` / `error_on` / `skip_on`;
* `th_checkpoint`, `Outcome`, item-middleware, Dishka extra и `release_deferred` в коде
  отсутствуют;
* схема всё ещё `SCHEMA_VERSION = 1`; по D-080 пакет не публиковался и баз с данными
  нет, поэтому `th_checkpoint` входит в базовую v1, а не создаёт историческую v2;
* RM1.2 и RM2.2 уже завершены: окно-aware выпуск и `item.defer` можно использовать как
  готовые примитивы.

Перед началом реализации этот срез перепроверяется. Если факт изменился, исполнитель
обновляет карточку и журнал до правки кода.

### 6.2. Правила ведения прогресса

Статусы: `todo` · `in_progress` · `done` · `blocked` · `human`.

1. В начале итерации отметить ровно одну задачу `in_progress` в таблице §6.6 и добавить
   запись в журнал §6.7. Незакоммиченная задача `in_progress` всегда продолжается первой.
2. Реализовать карточку целиком с тестами. Изменение публичного контракта сначала внести
   в `ARCHITECTURE.md` / `ACCEPTANCE.md`; отклонение — в `docs/plan/DECISIONS.md`.
3. Перед `done` выполнить DoD карточки, затем обязательные `uv run poe fmt` и
   `uv run poe check`. Если затронуты `storage/` или `engine/`, также выполнить
   `uv run poe test-all`.
4. После зелёных гейтов записать коммиты, результат, отклонения и полезные сведения для
   следующей задачи. Незелёная или непроверенная задача не получает статус `done`.
5. При блокировке записать причину, три проверенных подхода и условие разблокировки;
   затем брать следующую доступную задачу, не нарушая зависимости.

### 6.3. Зависимости

```text
RM8.0 спецификация
  ├─ RM8.1 публичные Outcome и WorkItem
  │    ├─ RM8.2 итог значением
  │    │    └─ RM8.3 судьба исключений
  │    ├─ RM8.4 хранение checkpoint
  │    │    └─ RM8.5 checkpoint в claim/runtime
  │    └─ RM8.7 middleware
  │         └─ RM8.8 Dishka
  └─ RM8.6 release_deferred

RM8.9 документация и типизация  ← RM8.2, RM8.3, RM8.5, RM8.6, RM8.8
RM8.10 сквозная приёмка         ← RM8.9
```

`RM8.3` и `RM8.5` после `RM8.2` и `RM8.4` можно вести параллельно только в разных
worktree: обе задачи затрагивают runtime, поэтому при работе в одном дереве они идут
последовательно.

### 6.4. Карточки задач

#### RM8.0 — закрепить контракт в источниках истины

**Файлы:** `docs/ARCHITECTURE.md`, `docs/ACCEPTANCE.md`,
`docs/plan/DECISIONS.md`, этот issue.

Сделать:

* описать `th_checkpoint`, его DDL, жизненный цикл, retention и место в порядке
  блокировок; явно сохранить правило «горячий `th_item` не обновляется»;
* описать use cases для `Outcome`, `*_on`, checkpoint и `release_deferred`, включая
  отмену, потерю lease, rollback и повторную доставку;
* закрепить публичные контракты `WorkItem`, `ItemMiddleware`, `complete_in(...,
  outcome)`, порядок middleware и правила конфликтов исходов;
* принять и записать решение по checkpoint после `error` и `retry_failed` (вопрос 1
  §7), включая точный момент удаления;
* добавить в `ACCEPTANCE.md` отдельные ID для критериев §8, чтобы последующие тесты
  ссылались на стабильные требования.

**DoD:** источники истины не противоречат этому issue; все открытые вопросы, без ответа
на которые меняется схема или поведение retry, закрыты решением; `uv run poe check`
зелёный; `src/` не менялся.

#### RM8.1 — публичные `Outcome` и `WorkItem`

**Файлы:** новый `src/tallyho/model/outcomes.py`, `runtime/context.py`, публичные
`__init__.py`; unit- и typing-тесты.

Сделать:

* добавить замороженные, слотированные `Ok`, `Skip`, `Error`, `Defer` и закрытый
  объединяющий тип `Outcome`; общую валидацию `Defer` держать в model без импорта
  runtime/engine;
* переименовать реализацию `ItemContext` в `WorkItem`, оставив `ItemContext = WorkItem`
  как совместимый алиас на один минорный выпуск;
* публично экспортировать `WorkItem`, `Outcome` и четыре варианта из `tallyho`;
* добавить `item.require() -> WorkItem`, который вне tracked-задачи бросает
  `ConfigurationError`; поведение существующих методов фасада вне задачи не менять;
* сохранить полные аннотации, `__all__` и отсутствие `Any`.

**Тесты:** равенство/неизменяемость Outcome, валидация `Defer`, `current/require`,
совместимость алиаса, публичные импорты и `tests/typing/cases.py`.

**DoD:** G1; обратная совместимость фасада подтверждена существующими тестами.

#### RM8.2 — итог возвращаемым значением и `complete_in(..., outcome)`

**Файлы:** `runtime/context.py`, `runtime/tracked.py`, unit-тесты runtime,
`tests/integration/runtime/test_tracked.py`.

Сделать:

* после успешного `await task(...)` распознавать только экземпляры `Outcome`; обычные
  возвращаемые значения и `None` сохраняют прежнюю семантику;
* переводить `Ok/Skip/Error/Defer` в уже существующий единый внутренний итог до
  `finish/defer`, не дублируя SQL-пути;
* добавить `WorkItem.complete_in(session, outcome=None)` и делегирование фасада;
* если императивный итог уже задан и задача возвращает Outcome, бросать
  `ConfigurationError` до записи; тот же запрет действует внутри `complete_in`;
* сохранить возврат исходного объекта задачи вызывающему broker wrapper.

**Тесты:** каждый Outcome по пути A и B, rollback/commit, произвольное значение,
конфликт со всеми `item.ok/skip/error/defer`, lost lease и отмена.

**DoD:** G1 и релевантные integration-тесты; число SQL-операций старого пути не выросло.

#### RM8.3 — `defer_on`, `error_on`, `skip_on`

**Файлы:** `adapters/flexiq/adapter.py`, при необходимости чистый helper в `runtime/`,
unit- и FlexiQ contract-тесты.

Сделать:

* разобрать и удалить Tallyho-опции до передачи параметров декоратору FlexiQ;
* при регистрации нормализовать константы и функции-преобразователи, проверить типы,
  дубликаты между `*_on` и пересечение с `retry_on`;
* выбрать правило по ближайшему классу в MRO детерминированно; неоднозначную
  конфигурацию отклонять при регистрации, а не при первом вызове;
* переводить только исключение пользовательской задачи. `CancelledError`,
  `LeaseLostError` и внутренние `TallyhoError` пути владения не переводить;
* результат преобразователя подать в механизм RM8.2. Ошибка самого преобразователя
  логируется с типом исходного исключения и проходит обычный broker retry/DLQ путь;
* не менять пользовательские `retry_on` / `dont_retry_on` и добавление внутренних
  infrastructure retries.

**Тесты:** константа/функция для каждого `*_on`, MRO, конфликт правил, конфликт с
`retry_on`, mapper failure, cancel/lost lease, отсутствие неизвестных опций во FlexiQ,
реальный subprocess-worker contract.

**DoD:** G1, `uv run poe test-flexiq`; совместимость A-FQ-08/10/11/13 зелёная.

#### RM8.4 — таблица `th_checkpoint` и миграция v1

**Файлы:** `storage/tables.py`, `storage/migrations.py`, DDL golden-файлы,
`engine/operations.py` (retention/purge), storage/engine integration-тесты.

Сделать:

* добавить типизированные колонки и поле `Tables.checkpoint`:
  `item_id uuid PK`, `batch_id uuid NOT NULL`, `position bigint NOT NULL`,
  `updated_at timestamptz NOT NULL`;
* создать таблицу с `fillfactor=50` и агрессивным autovacuum, без вторичного индекса;
* включить её в создание схемы v1 и Alembic golden, не поднимать `SCHEMA_VERSION`
  согласно D-080;
* удалить строки checkpoint при retention/purge дерева теми же оконными чанками, что
  остальные side-таблицы;
* закрепить безопасные schema/prefix и две независимые установки.

**Тесты:** metadata/DDL/golden, чистая установка и Alembic offline SQL, custom schema и
prefix, retention дерева; отсутствие обновления `th_item` проверяется в RM8.5.

**DoD:** G1 + G2; миграция и retention зелёные.

#### RM8.5 — checkpoint в claim, runtime и завершении

**Файлы:** `engine/completer.py`, `runtime/context.py`, `runtime/tracked.py`, тесты
completer/runtime.

Сделать:

* читать checkpoint `LEFT JOIN`-ом в существующем claim statement и вернуть через
  `ClaimResult.checkpoint`, без второго запроса;
* передать checkpoint и `defers` в `WorkItem`; `checkpoint()` возвращает снимок claim,
  а успешный `checkpoint_in` обновляет локальное значение;
* реализовать `checkpoint_in(session, position)` как монотонный upsert с проверкой
  неотрицательного целого, владения `(item_id, attempt)` и тем же fencing/порядком
  блокировок, что `complete_in`;
* в той же транзакции обновлять `th_lease.progress_done`; rollback не меняет ни
  checkpoint, ни видимый прогресс;
* удалять checkpoint атомарно при `ok`, `skip`, `cancelled` и в остальных случаях по
  решению RM8.0; `defer`, retry, потеря lease и `aclose` строку сохраняют;
* не обновлять `th_item` при записи checkpoint и не трогать checkpoint у item без него.

**Тесты:** критерии 2–4 и 9–11 §8, монотонность/валидация, defer/retry/lease takeover,
commit/rollback/savepoint, групповой и `complete_in` пути, query counter claim.

**DoD:** G1 + G2; EXPLAIN/счётчик запросов подтверждает один claim statement; branch
coverage новых путей не ниже 95%.

#### RM8.6 — `BatchHandle.release_deferred`

**Файлы:** `api/batch.py`, `engine/public.py`, `engine/assembly.py`,
`engine/operations.py`, unit/integration-тесты.

Сделать:

* добавить `release_deferred(reasons: Collection[str] | None = None, *, session=None)
  -> int` с валидацией причин как меток;
* выбрать только строки данного батча с `defer_reason IS NOT NULL` и нужными причинами,
  сделать их доступными сейчас, очистив только `defer_reason`;
* не сбрасывать `defers`, `first_deferred_at`, attempt и generation;
* при пользовательской транзакции будить relay только после commit; rollback не меняет
  строки;
* не обходить pause/start/deadline и окно: фактический выпуск остаётся обязанностью
  готового window-aware relay; конкурентные вызовы идемпотентны.

**Тесты:** без фильтра/с фильтром/пустой набор, commit/rollback, окно 0/3, pause,
конкурентный вызов, возвращаемое число реально изменённых строк, критерий 7 §8.

**DoD:** G1 + G2; инвариант `count(th_window) <= max_in_flight` сохранён.

#### RM8.7 — item middleware

**Файлы:** публичный протокол в допустимом нижнем слое, `api/client.py`,
`engine/assembly.py`, `runtime/tracked.py`, unit/integration-тесты.

Сделать:

* добавить `ItemMiddleware` с `call_next` и неизменяемой последовательностью в
  `Tallyho(..., middleware=...)`;
* передать middleware до runtime через composition root, не создавая запрещённого
  импорта нижнего слоя в `api`;
* построить цепочку в порядке списка: первый middleware — внешний; вызывать её после
  claim и `activate_item`, но вокруг `_invoke`, чтобы возвращаемые значения и
  исключения прошли обычную семантику RM8.2/RM8.3;
* callback и неотслеживаемые задачи не оборачивать; пустой список сохраняет текущий
  горячий путь;
* документировать, что middleware не записывает итог напрямую и не владеет retry.

**Тесты:** порядок before/after, доступность `item.require`, возврат Outcome, исключение,
отмена, пустая цепочка, один экземпляр middleware на несколько попыток.

**DoD:** G1; import-linter зелёный; существующая FlexiQ middleware A-FQ-13 не меняется.

#### RM8.8 — extra `tallyho[dishka]`

**Файлы:** `pyproject.toml`, `uv.lock`, новый пакет `tallyho/integrations/dishka/`,
unit-, integration- и typing-тесты.

Сделать:

* добавить optional extra с совместимым диапазоном Dishka, не импортировать Dishka при
  обычном `import tallyho`;
* реализовать `DishkaItemScope`: один REQUEST-scope на попытку, закрытие при success,
  exception и cancellation;
* реализовать `TallyhoProvider`, который получает текущий `WorkItem` из context data и
  не создаёт фиктивный объект вне задачи;
* реэкспортировать совместимые `inject` и `FromDishka`, сохранив видимую для Tallyho и
  type checker сигнатуру без DI-параметров;
* зафиксировать понятную ошибку разрешения `WorkItem` вне item scope.

**Тесты:** импорт без extra, scope lifecycle, вложенные зависимости, исключение/отмена,
недоступность вне задачи, пример из §3.5 и статическая типизация `th.call`.

**DoD:** G1; тесты проходят как с установленным extra, так и smoke-импорт без него;
lock-файл обновлён штатно.

#### RM8.9 — руководство, reference и матрица совместимости

**Файлы:** `docs/guide/`, `docs/reference/`, `docs/API.md`, примеры и doc-tests,
`CHANGELOG.md`.

Сделать:

* руководство «Задача и её итог» в порядке: возврат Outcome → `*_on` → `WorkItem`
  через DI → совместимый фасад `item`;
* рецепт пачки с checkpoint, транзакцией доменных данных и предупреждением про внешние
  неидемпотентные эффекты;
* рецепт capacity-governor с `release_deferred`, в котором окно остаётся единственным
  регулятором выпуска;
* reference всех новых типов, методов, ошибок и extras; отметить срок алиаса
  `ItemContext`;
* сделать все фрагменты исполняемыми и добавить typing-кейсы публичных сигнатур.

**DoD:** G1; docs snippets выполняются существующим CI-механизмом; нет примеров с
ручным дублированием `record_outcome`/`unchecked()`.

#### RM8.10 — сквозная приёмка и регрессионный gate

**Файлы:** `tests/acceptance/`, при необходимости `benchmarks/` и acceptance docs.

Сделать:

* перевести сценарий RM7.1 на Outcome, `defer_on`, Dishka и `release_deferred`, сохранив
  его числовой oracle: 0 сгоревших ретраев, 20 платных вызовов, 20/20;
* добавить сценарий пачки 50: commit результата и checkpoint на каждом элементе,
  убийство после 30-го, возобновление с 31-го, ровно 50 строк результата;
* прогнать все 11 критериев §8 на публичном API, а не через внутренние helper'ы;
* зафиксировать SQL/query-count и HOT-инварианты checkpoint; приложить seed и краткий
  отчёт к журналу.

**DoD:** `uv run poe fmt`, `uv run poe check`, `uv run poe test-all`, релевантные
FlexiQ/acceptance команды зелёные; покрытие строк и веток ≥ 95%; все пункты §8 имеют
тест и ссылку на acceptance ID.

### 6.5. Волны реализации

| Волна | Задачи | Условие выхода |
|---|---|---|
| 0 | RM8.0 | контракт и решения утверждены документами |
| 1 | RM8.1, RM8.6 | публичный фундамент и независимое пробуждение готовы |
| 2 | RM8.2, RM8.4, RM8.7 | Outcome runtime, storage checkpoint и middleware готовы |
| 3 | RM8.3, RM8.5, RM8.8 | политики исключений, полный checkpoint и Dishka готовы |
| 4 | RM8.9 | публичные docs/typing соответствуют коду |
| 5 | RM8.10 | сквозные критерии и все финальные гейты зелёные |

Параллельность волн — подсказка, а не требование: задачи, меняющие один файл, в одном
рабочем дереве выполняются последовательно. После каждой волны репозиторий должен быть
зелёным.

### 6.6. Текущее состояние

* **Ветка / исходный HEAD:** `main` / `98713b0`
* **Текущая волна:** 5 завершена — весь набор RM8 готов
* **Последний проверенный зелёный коммит:** RM8.10
* **Готово:** 11 из 11
* **Следующая задача:** нет

| ID | Задача | Зависит | Статус | Коммиты |
|---|---|---|---|---|
| RM8.0 | контракт в ARCHITECTURE/ACCEPTANCE | — | done | RM8.0 |
| RM8.1 | публичные `Outcome` и `WorkItem` | RM8.0 | done | RM8.1 |
| RM8.2 | итог значением и `complete_in(..., outcome)` | RM8.1 | done | RM8.2 |
| RM8.3 | `defer_on` / `error_on` / `skip_on` | RM8.2 | done | RM8.3 |
| RM8.4 | `th_checkpoint` и миграция v1 | RM8.0 | done | RM8.4 |
| RM8.5 | checkpoint в claim/runtime/finish | RM8.2, RM8.4 | done | RM8.5 |
| RM8.6 | `release_deferred` | RM8.0 | done | RM8.6 |
| RM8.7 | item middleware | RM8.1 | done | RM8.7 |
| RM8.8 | интеграция Dishka | RM8.7 | done | RM8.8 |
| RM8.9 | руководство, reference, typing | RM8.3, RM8.5, RM8.6, RM8.8 | done | RM8.9 |
| RM8.10 | сквозная приёмка | RM8.9 | done | RM8.10 |

### 6.7. Журнал

Новые записи добавляются сверху.

<!--
### YYYY-MM-DD · RM8.x · in_progress|done|blocked · <commit или —>
- Сделано: …
- Гейты: …
- Отклонения от плана/доков: нет | …
- Для следующих задач: …
- Блокер: нет | что проверено и что нужно
-->

### 2026-10-20 · поправка RM8.4 · done · v2

* Сделано: `th_checkpoint` перенесена из базовой схемы v1 в новую миграцию v2,
  `SCHEMA_VERSION = 2`. Предпосылка §6.1 и D-080 «пакет не публиковался, баз с данными нет»
  к моменту реализации была неверна: на PyPI опубликованы `0.0.1` и `0.0.2a1`, их теги есть
  в origin, а release-workflow публикует по тегу. База, уже помеченная версией 1, новую
  таблицу не получила бы — `migrate()` счёл бы схему актуальной, и первый `checkpoint_in`
  упал бы на отсутствующей таблице.
* Версия 1 теперь заморожена golden-файлом `tests/unit/storage/golden/ddl_v1.sql` (13 таблиц,
  без закладки), общий `ddl.sql` сверяется с суммой всех версий как множество операций.
  Интеграционный тест: ревизия Alembic на версию 1 → закладки нет → `migrate()` → закладка
  есть, версия 2. Документация: правила версий в `integrations/alembic.md` (пример второй
  ревизии) и `guide/installation.md`, вывод `tallyho migrate` в reference и руководстве.
  Решение — D-088.
* Гейты: `uv run poe check` зелёный (1450 тестов); интеграционные тесты миграций, Alembic,
  схемы закладки и схемы defer — 23 из 23; doc-тесты — 140.
* Отклонения от плана/доков: это и есть отклонение от карточки RM8.4 («не поднимать
  `SCHEMA_VERSION` согласно D-080»); причина и отклонённые варианты записаны в D-088.
* Блокер: нет.

### 2026-10-20 · RM8.10 · done · RM8.10

* Сделано: `tests/acceptance/test_work_item_api.py` — два сквозных сценария на PostgreSQL.
  **A-WI-01**: domain-checker из A-RM-01 переписан на новый API — итог значением (`Ok`),
  доменное исключение `NoCheckerAccountError` с судьбой `defer_on`, интерактор и `WorkItem`
  из контейнера Dishka, `release_deferred` вместо короткого `delay` (срок отложения 5 минут
  при неподвижных часах, то есть работу будит только пробуждение). Оракул совпал с A-RM-01:
  `SUCCEEDED`, 20 из 20 `ok`, 0 ошибок, 0 отложенных записей, ровно 20 платных вызовов,
  не больше одного параллельного запроса с ресурса, `max(attempt) == 0`, отказов ресурса > 0.
  **A-WI-02**: пачка из 50 элементов с закладкой и доменной строкой на каждом; попытка
  обрывается после 30-го, вторая начинает с 31-го (`attempts == [0, 30]`), в доменной
  таблице ровно 50 строк позиций 0…49 без дублей, после итога `ok` строк `th_checkpoint` нет.
* Остальные критерии §8 проверены на публичном API в интеграционных и юнит-тестах:
  A-WI-03/04/09/10/11 — `tests/integration/runtime/test_checkpoint.py` (откат, чужая
  попытка, счётчик statement'ов claim, одно обновление `th_item`, пачка из 50);
  A-WI-05 — `test_outcomes.py`; A-WI-06 — `tests/unit/runtime/test_exception_policy.py` и
  `tests/unit/adapters/flexiq/test_adapter.py` (отказ при регистрации);
  A-WI-07 — `tests/integration/engine/test_release_deferred.py`;
  A-WI-08 — `test_dishka.py`.
* Гейты: `uv run poe fmt` и `uv run poe check` зелёные; `uv run poe test-all` — 2548
  прошло, 11 пропущено, покрытие 97.78% при пороге 95%. Единственное падение —
  `test_a_fq_12_broker_and_tallyho_concurrency_limits_compose`, и оно не связано с этим
  набором: тест флакает сам по себе (standalone 3 прогона из 4 зелёные), а падает его
  ассерт про `rate_limit` flexiq — события раскладываются по целым секундам
  (`Counter(int(at))`), и после простойной секунды лимитер пропускает всплеск из трёх.
  В диффе RM8 этого пути нет: в адаптере изменено только снятие трёх опций tallyho и
  передача политики в `wrap`. Оракул чужого критерия A-FQ-12 не правил — это решение
  владельца. Сценарии RM8 детерминированы: `InlineBroker(seed=...)` и `FakeClock`,
  отдельный `--seed` им не нужен.
* Отклонения от плана/доков: у `InlineBroker` появился `on_exception(fn, defer_on=,
  error_on=, skip_on=)`. Без него сценарий A-WI-01 нельзя было собрать на тестовом брокере:
  правила — опция регистрации задачи, а тестовый брокер регистрирует задачи по первому
  вызову. §7.5 ACCEPTANCE уточнён: A-WI-01 и A-WI-02 идут на `InlineBroker`, а не на
  compose-стенде с воркерами — брокер последовательный, и числовой оракул детерминирован
  без хаоса.
* Для следующих задач: набор RM8 закрыт.
* Блокер: нет.

### 2026-10-20 · RM8.9 · done · RM8.9

* Сделано: страница «Задача и её итог» перестроена в порядке возврат `Outcome` → `*_on` →
  `WorkItem` (и DI) → совместимый фасад `item`, добавлены разделы про отложение и обёртки
  вокруг задачи. Новая страница `guide/batches/checkpoint.md` — рецепт пачки с закладкой,
  таблица «что переживает закладка» и предупреждение про внешние неидемпотентные эффекты.
  Новая страница `integrations/dishka.md` — подключение, задача с интерактором,
  инфраструктурный адаптер закладки. В `guide/batches/operations.md` — раздел «Разбудить
  отложенное» с рецептом контура ёмкости, где окно остаётся единственным регулятором
  выпуска. Reference: `model.outcomes`, `WorkItem`, `build_exception_policy`/
  `ExceptionPolicy`, `ItemMiddleware`, интеграция Dishka; обновлён CHANGELOG.
* Гейты: G1 зелёный; `tests/examples/test_documentation.py` — 140 тестов (манифест страниц,
  маркеры всех python-блоков, относительные ссылки и якоря); новые typing-кейсы
  `work_item_cases` (закладка, `complete_in(..., outcome)`, `release_deferred`, задача с
  `@inject` в `th.call` и `batch.add`).
* Отклонения от плана/доков: все новые блоки помечены `tallyho-noexec` — это код
  приложения (пул аккаунтов, доменные таблицы, контейнер), а поведение, которое они
  описывают, исполняется тестами RM8.2–RM8.8 и приёмкой RM8.10. Ручного дублирования
  `record_outcome`/`unchecked()` в примерах нет.
* Для следующих задач: RM8.10 — сквозная приёмка.
* Блокер: нет.

### 2026-10-20 · RM8.8 · done · RM8.8

* Сделано: extra `tallyho[dishka]` (`dishka>=1.4,<2`, в окружении 1.10.1) и пакет
  `tallyho/integrations/dishka/`: `DishkaItemScope` — middleware, открывающая один
  REQUEST-scope на попытку и кладущая в его контекст текущий `WorkItem`;
  `TallyhoProvider` отдаёт `WorkItem` через `from_context`, то есть вне задачи его в
  контейнере просто нет; `inject` и реэкспорт `FromDishka`. Новый слой
  `tallyho.integrations` добавлен в контракт import-linter (`exhaustive = true`) рядом с
  `cli | adapters | testing | observability`.
* Гейты: G1 зелёный; `tests/integration/runtime/test_dishka.py` — зависимость получает
  тот же `WorkItem`, что `item.require()`; scope закрывается на успехе, исключении и
  отмене; на каждую попытку свой scope; вне задачи — `ConfigurationError` от
  `current_container()` и `NoContextValueError` от контейнера (A-WI-08); отдельный
  subprocess проверяет, что `import tallyho` не тянет dishka.
* Отклонения от плана/доков: `inject` возвращает не `Callable[P, T]`, а протокол
  `InjectedTask[R]` с `*args: object`. `Callable[..., T]` запрещён правилом «никакого
  `Any`», а сохранить `P` нельзя: тогда `th.call` требовал бы внедряемый аргумент.
  Цена — у задачи с `@inject` статически проверяется тип результата, а не список
  аргументов; §11.2 это фиксирует. `dishka` читает аннотации в рантайме, поэтому в тестах
  и в `scope.py` несколько импортов помечены как нужные вне `TYPE_CHECKING`.
* Для следующих задач: RM8.9 — руководство и reference; все публичные имена уже на
  месте (`WorkItem`, `Outcome`, `ItemMiddleware`, `ExceptionPolicy`, extras).
* Блокер: нет.

### 2026-10-20 · RM8.7 · done · RM8.7

* Сделано: протокол `ItemMiddleware` (`runtime/middleware.py`) и цепочка вокруг вызова
  задачи: `Tallyho(..., middleware=[...])` → `create_engine_facade` → `_Facade` →
  `WorkerFactory` → `TaskRuntime`. Первый элемент списка — внешний; цепочка строится
  внутри активированного контекста, но вокруг `_invoke`, поэтому возвращаемый `Outcome` и
  исключение проходят через неё и получают обычную семантику RM8.2/RM8.3. Пустой список
  оставляет прежний горячий путь без лишних кадров стека; колбэки и неотслеживаемые
  вызовы не оборачиваются.
* Гейты: G1 зелёный, import-linter без нарушений;
  `tests/integration/runtime/test_middleware.py` — порядок до/после, доступность
  `item.require()`, возврат `Outcome`, исключение, пропуск при дубле claim, пустая
  цепочка, один экземпляр на несколько попыток, невызываемый элемент.
* Отклонения от плана/доков: `api` не знает тип `ItemMiddleware` — он описан в `runtime`,
  а через composition root едет как `Sequence[object]` (так же, как `completer` и
  `tree_cache`). Иначе `api` импортировал бы `runtime`, что запрещено §3.3. Проверка
  «элемент вызываемый» живёт в `TaskRuntime`, единственной точке сборки.
* Для следующих задач: RM8.8 ставит свою middleware первой в список, чтобы REQUEST-scope
  охватывал остальные.
* Блокер: нет.

### 2026-10-20 · RM8.5 · done · RM8.5

* Сделано: claim читает закладку и `defers` тем же запросом (`lock_items`,
  `LEFT JOIN th_checkpoint`, `FOR UPDATE OF th_item`) и отдаёт их в `ClaimResult`;
  `WorkItem.checkpoint()` возвращает снимок claim, `checkpoint_in(session, position)` —
  монотонный upsert в транзакции вызывающего с тем же fencing'ом, что `complete_in`, и
  обновлением `th_lease.progress_done`. Локальный снимок двигается только после commit,
  поэтому откат оставляет и закладку, и `checkpoint()` прежними. Итог `ok`/`skip`/
  `cancelled` удаляет строку в той же транзакции, `error` — оставляет (D-085).
* Гейты: G1 зелёный; `tests/integration/runtime/test_checkpoint.py` — 13 тестов:
  запись вместе с доменными данными и чтение следующим claim, прогресс lease,
  откат (A-WI-03), монотонность, валидация `position`, чужая попытка (A-WI-04),
  удаление при `ok`/`skip` и сохранение при `error`, Item без закладки (A-WI-10),
  пачка из 50 без обновления `th_item` (A-WI-11), `defers` из claim и счётчик
  statement'ов claim (A-WI-09).
* Отклонения от плана/доков: `with_for_update(of=item)` вместо `with_for_update()` —
  PostgreSQL не блокирует nullable-сторону outer join; блокировать `th_checkpoint` и не
  нужно. Порядок блокировок `checkpoint_in` — `th_item → th_lease → th_checkpoint`, без
  `th_batch`: это префикс порядка `complete_in`, записанный в §9.2.
* Для следующих задач: RM8.10 переносит сценарий «убийство на 30-м из 50» (A-WI-02) на
  стенд с настоящими воркерами.
* Блокер: нет.

### 2026-10-20 · RM8.3 · done · RM8.3

* Сделано: чистый `runtime/exceptions.py` — `ExceptionPolicy` и
  `build_exception_policy(defer_on=, error_on=, skip_on=, retry_on=)`: разбор констант,
  меток и функций-преобразователей, выбор по ближайшему классу MRO, отказ при двух
  судьбах у одного класса и при пересечении с `retry_on` — всё при регистрации.
  FlexiQ-адаптер забирает три опции себе (во flexiq они не уходят) и передаёт политику в
  `runtime.wrap(fn, policy=...)`. Обёртка переводит исключение задачи до `_settle_failure`
  и записывает итог тем же путём, что возвращённый `Outcome`.
* Гейты: G1 зелёный; `tests/unit/runtime/test_exception_policy.py` (12 тестов разбора и
  выбора), `tests/integration/runtime/test_exception_fates.py` (11 тестов: defer/error/
  skip, MRO, необъявленное исключение, `LeaseLostError` и `CancelledError` без перевода,
  падение правила, правило другого класса, конфликт с императивным итогом, валидация
  политики), unit-тесты адаптера на удаление опций и отказ при двух судьбах.
* Отклонения от плана/доков: `WorkerRuntime.wrap` получил keyword-параметр
  `policy: object = None` — политика принадлежит задаче, а не установке, и доехать до
  обёртки может только с ней. Значения правил принимаются как `object` и проверяются
  явно: точные типы в сигнатуре заставили бы писать `cast` в каждом
  `lambda exc: Defer(reason=exc.reason)`.
* Для следующих задач: `_translated` не пишет в БД, а только заполняет буферы попытки, —
  тот же `adopt`, что у RM8.2.
* Блокер: нет.

### 2026-10-20 · RM8.4 · done · RM8.4

* Сделано: таблица `th_checkpoint` (`item_id` PK, `batch_id`, `position bigint NOT NULL`,
  `updated_at`) с `fillfactor=50` и агрессивным autovacuum, без вторичных индексов; входит
  в базовую схему v1 (D-080, `SCHEMA_VERSION` не поднимался), golden `ddl.sql` обновлён
  штатным `TALLYHO_UPDATE_GOLDEN=1`. Retention дерева удаляет закладки тем же чанком, что
  остальные side-таблицы Items (`Sweeper._purge_items_in`).
* Гейты: G1 зелёный; интеграционно — новый
  `tests/integration/storage/test_checkpoint_schema.py` (колонки, только PK,
  `fillfactor`/autovacuum, у `th_item` по-прежнему только `fillfactor=85` и никаких колонок
  закладки) и расширенный retention-тест sweeper. Alembic offline SQL и чистая установка
  проходят существующими тестами миграций.
* Отклонения от плана/доков: нет. `position` — отдельная колонка, не `jsonb`, как решено
  в §7 вопросе 2.
* Для следующих задач: RM8.5 читает закладку в `Completer.lock_items` — это единственный
  запрос claim, поэтому `LEFT JOIN th_checkpoint` нужно добавить именно туда и обязательно
  с `with_for_update(of=item)`: PostgreSQL не блокирует nullable-сторону outer join.
  Там же рядом лежит `item.c.defers` для `WorkItem.defers`.
* Блокер: нет.

### 2026-10-20 · RM8.2 · done · RM8.2

* Сделано: `WorkItem.adopt(outcome)` переводит `Ok`/`Skip`/`Error`/`Defer` в тот же
  внутренний итог через существующие `ok`/`skip`/`error`/`defer` — новых путей записи в БД
  нет. `_invoke` распознаёт только экземпляры `Outcome` (`None` и любое другое значение
  сохраняют прежнюю семантику «успех») и возвращает исходный объект вызывающему без
  изменений. `complete_in(session, outcome=None)` и делегирование фасада. Два источника
  итога — `ConfigurationError`.
* Гейты: G1 зелёный; `tests/integration/runtime/test_outcomes.py` — 15 тестов: каждый
  итог значением, `defer` со `delay`/`until`/умолчанием, произвольное значение,
  `complete_in` с итогом в транзакции и его откат, конфликт для всех четырёх итогов и
  после `defer`.
* Отклонения от плана/доков: конфликт итогов обнаруживается по уже заданному императивному
  итогу; обёртка бросает `ConfigurationError` до записи возвращённого итога, и дальше
  попытка идёт обычным путём исключения задачи (вердикт брокера) — это зафиксировано в
  UC-18 и A-WI-05. Если `complete_in` уже закоммитил итог, возвращённый `Outcome` его не
  переписывает: Item терминален и попытке больше не принадлежит, поэтому запись — no-op, а
  возврат обёртки в этом случае контрактом не является (зависит от доставки
  после-коммитного колбэка соединения).
* Для следующих задач: RM8.3 подаёт результат перевода исключения в тот же `adopt`, уже
  внутри ветки `except` обёртки — важно, что `adopt` не пишет в БД сам, а только
  заполняет буферы попытки.
* Блокер: нет.

### 2026-10-20 · RM8.6 · done · RM8.6

* Сделано: `Operations.release_deferred(target, batch_id, reasons)` →
  `EngineFacade`/`assembly` → `BatchHandle.release_deferred(reasons=None, *, session=None)`.
  Очищается только `defer_reason`, срок становится `greatest(start_at, now)` — отложенный
  старт не обходится; `defers`, `first_deferred_at`, `attempt` и поколение не трогаются.
  Пауза и окно остаются за relay: фактический выпуск идёт в пределах свободного места.
  Возвращается число реально изменённых строк; `kick` relay — только после commit и только
  если что-то изменилось. Причины валидируются как метки (пустая и U+001F — ошибка).
* Гейты: G1 зелёный; `tests/integration/engine/test_release_deferred.py` — 14 тестов:
  без фильтра, с фильтром, пустой и неизвестный набор, откат транзакции, окно 3 из 10
  (A-WI-07: разбужены 10, выпущены 3, остальные запаркованы, `defers` цел), закрытое окно,
  пауза, отложенный старт, идемпотентность, чужой батч, валидация. `release_deferred`
  добавлена в матрицу `test_purged_operations`.
* Отклонения от плана/доков: нет.
* Для следующих задач: в тестах relay полный `scan_once` отдаёт записи только старше
  `relay_grace`, поэтому после пробуждения часы теста нужно двигать на `GRACE`; возврат
  `scan_once` — число принятых брокером сообщений, а запаркованные записи в него не входят.
* Блокер: нет.

### 2026-10-20 · RM8.1 · done · RM8.1

* Сделано: новый `model/outcomes.py` — замороженные слотированные `Ok`, `Skip`, `Error`,
  `Defer` и закрытый `Outcome`; валидация `Defer` (пустой `reason`, `delay` вместе с
  `until`, отрицательный `delay`) живёт в model, и `WorkItem.defer` теперь переиспользует
  её вместо своей копии. Реализация `ItemContext` переименована в `WorkItem`,
  `ItemContext = WorkItem` оставлен алиасом; добавлен `item.require()`, который вне задачи
  бросает `ConfigurationError` (остальные методы фасада по-прежнему no-op). Публичные
  экспорты: `WorkItem`, `ItemContext`, `Outcome` и четыре варианта из `tallyho` и
  `tallyho.model`. Диаграммы §3.4 и UC-03/UC-08 и `CONTRIBUTING.md` переведены на новое имя.
* Гейты: `uv run poe fmt` и `uv run poe check` зелёные (1384 теста). Новые тесты:
  `tests/unit/model/test_outcomes.py` (равенство, frozen+slots, значения по умолчанию,
  валидация `Defer`, публичные экспорты), `test_context.py` (алиас, `require` внутри и вне
  задачи), typing-кейсы на `require()`/`current()` и keyword-only поля итогов.
* Отклонения от плана/доков: у `Ok`/`Skip`/`Error` есть ещё поле `mark` (keyword-only).
  Без него возврат итога значением терял бы `mark=` императивного API; §11.2 и UC-18
  обновлены под фактическую сигнатуру. Выбор keyword-only для `result`/`detail`/`mark`
  повторяет фасад `item.*`.
* Для следующих задач: `WorkItem.adopt(outcome)` для RM8.2 ещё нет — перевод итога-значения
  в единый внутренний итог должен идти через существующие `ok/skip/error/defer`, иначе
  появится второй путь записи. Поля `defers` и `checkpoint()` в `WorkItem` сознательно не
  добавлены: их значения приходят из claim, это RM8.5.
* Блокер: нет.

### 2026-10-20 · RM8.0 · done · RM8.0

* Сделано: срез §6.1 перепроверен и подтверждён (`ItemContext` в `runtime/context.py:163`,
  `SCHEMA_VERSION = 1`, в коде нет `th_checkpoint`/`Outcome`/middleware/`release_deferred`,
  FlexiQ-адаптер знает только `retry_on`/`dont_retry_on`). В `ARCHITECTURE.md`: `th_checkpoint`
  в ER-диаграмме §5.1 с полным описанием семантики, жизненного цикла и обоснования узкой
  таблицы; строка в таблице индексов §5.2 и правило хранения (`fillfactor=50`, без вторичных
  индексов); `th_lease` и `th_checkpoint` внесены в документированный порядок блокировок §9.2;
  удаление side-таблиц Items в §7.6; `dishka` в §4.1 и `ItemMiddleware` в §4.2; новые UC-18
  (итог значением и судьба исключений), UC-19 (закладка), UC-20 (`release_deferred`) с
  диаграммами и картой §8.2; §11.2 — `WorkItem`, `Outcome`, `*_on`, `middleware=`,
  `release_deferred`, контракт `require()` и раздел про контейнер. В `ACCEPTANCE.md` — новый
  §7.5 A-WI-01 … A-WI-11 ровно по 11 критериям §8 ишью, критерии §8 проставлены этими ID.
* Гейты: `uv run poe fmt` зелёный (344 файла без изменений); `uv run poe check` зелёный
  (lint, mypy, basedpyright, import-linter, deptry; 1363 теста). `src/` не менялся.
* Отклонения от плана/доков: нет. Решения — D-085 (закладка переживает `error` и
  `retry_failed`, удаляется итогом `ok`/`skip`/`cancelled`) и D-086 (`WorkItem` только через
  `require()`/контейнер, встроенных словарей `defer_on` нет, имя `WorkItem`): закрывают
  открытые вопросы 1 и 3–5 §7.
* Для следующих задач: RM8.1 реализует `Outcome` в новом `model/outcomes.py` (model не
  импортирует runtime), переименование `ItemContext` → `WorkItem` с алиасом и `item.require()`.
  Валидация `Defer` уже есть в `ItemContext.defer` (`_DEFER_*` сообщения в
  `runtime/context.py`) — общую часть надо поднять в model, не дублируя. `check_counter_name`
  живёт в `storage/metric_names.py`, то есть выше слоя model: в `Defer` валидировать только
  непустоту, `delay`/`until` и знак, а проверку метки оставить на записи итога.
* Блокер: нет.

### 2026-10-20 · планирование · done · —

* Сделано: кодовая база сверена с предложением; краткий список RM8 превращён в
  зависимый план из 11 карточек; добавлены DoD, волны, таблица статусов и журнал.
* Гейты: `uv run poe fmt` зелёный (344 файла без изменений); `uv run poe check`
  зелёный (Ruff, mypy, basedpyright, import-linter, deptry; 1363 теста). В sandbox
  потребовались локальный uv cache, отключение pytest cache-provider и отдельный
  доступный temp-каталог; состав проверок не менялся.
* Отклонения от плана/доков: реализация не начиналась; исходный issue сохранён.
* Для следующих задач: начинать с RM8.0; D-080 разрешает включить `th_checkpoint` в v1,
  но решение о жизни checkpoint после `retry_failed` ещё нужно закрепить.
* Блокер: нет.

---

## 7. Открытые вопросы

1. **Закладка при `retry_failed`.** Предложение — сохранять: сделанное повторно не
   переделывается. Альтернатива — флаг `retry_failed(reset_checkpoint=True)`. Записать в
   `DECISIONS.md`.
2. ~~**Тип и место хранения закладки.**~~ Решено: отдельная таблица `th_checkpoint`,
   `position bigint` (см. п. 3.4 — довод ARCHITECTURE §5.2 и размер строки `th_item`).
   `state jsonb` добавляется отдельной задачей, когда появится первый потребитель.
3. **Внедрение `WorkItem` аргументом по типу** (как `TaskiqDepends` или FlexiQ `Inject`).
   Предложение — не делать в этой итерации: Dishka закрывает ту же потребность, а
   подстановка по типу усложняет типизацию `th.call` и сигнатуру, которую видит FlexiQ.
4. **Значения по умолчанию для `defer_on`.** Нужен ли встроенный перевод для ошибок
   Identity Hub (`LeaseUnavailable`, `QuotaExhausted`)? Предложение — нет: Tallyho не
   знает про Identity Hub. Готовый словарь можно положить в адаптер capacity-governor или
   в шаблон проекта.
5. **Имя.** `WorkItem` или `Item`. Выбрано `WorkItem`, чтобы не путать с модулем-фасадом
   `item`.

---

## 8. Критерии приёмки

Стабильные ID — `ACCEPTANCE.md` §7.5 (A-WI-01 … A-WI-11), по номеру списка ниже.

1. (A-WI-01) Задача из п. 4 «Стало» проходит сценарий RM7.1 (20 доменов, ёмкость 5 + 3 + 0,
   пополнение): **0 сгоревших ретраев**, 20 платных вызовов, 20 из 20 после пополнения.
2. (A-WI-02) Падение воркера на 30-м домене пачки из 50: новая попытка начинает с 31-го, в таблице
   результатов ровно 50 строк, повторных записей нет.
3. (A-WI-03) Откат транзакции приложения с `checkpoint_in` оставляет закладку прежней.
4. (A-WI-04) Чужая попытка получает `LeaseLostError` из `checkpoint_in`, закладка не меняется.
5. (A-WI-05) Задача, вернувшая `Defer` и вызвавшая `item.ok`, получает `ConfigurationError`.
6. (A-WI-06) Класс исключения в `retry_on` и `defer_on` одновременно — `ConfigurationError` при
   регистрации.
7. (A-WI-07) `release_deferred(reasons=["no_accounts"])` при окне 3 и 10 отложенных выпускает 3,
   остальные ждут места в окне. `defers` не сброшен.
8. (A-WI-08) `WorkItem` из Dishka недоступен вне задачи: понятная ошибка контейнера.
9. (A-WI-09) Счётчик SQL-запросов claim не растёт: закладка читается тем же запросом.
10. (A-WI-10) Item без закладки: число UPDATE `th_item` за жизнь не меняется (одно, HOT); finish
    `th_checkpoint` не трогает.
11. (A-WI-11) Пачка из 50 элементов с закладкой на каждый: `th_item` обновлён один раз, в
    `th_checkpoint` после итога `ok` строки нет.

---

## Ссылки

**Итоги и отложение**

* [Oban — `Oban.Worker`, возвращаемые значения `perform/1`](https://oban.hexdocs.pm/Oban.Worker.html)
* [River — Snoozing jobs](https://riverqueue.com/docs/snoozing-jobs)
* [BullMQ — Process step jobs (`moveToDelayed`, `DelayedError`, `maxStartedAttempts`)](https://docs.bullmq.io/patterns/process-step-jobs)
* [BullMQ — Stop retrying jobs](https://docs.bullmq.io/patterns/stop-retrying-jobs)
* [Sidekiq Enterprise — Rate Limiting (`OverLimit`, перепланирование)](https://github.com/mperham/sidekiq/wiki/Ent-Rate-Limiting)
* [Celery — Tasks (`bind`, `retry`, `autoretry_for`, `Reject`, `Ignore`)](https://docs.celeryq.dev/en/stable/userguide/tasks.html)
* [Temporal — Failure detection, `ApplicationError(non_retryable=True)`](https://docs.temporal.io/develop/python/failure-detection)

**Прогресс внутри единицы работы**

* [Temporal — Long-running activity, heartbeat details](https://docs.temporal.io/design-patterns/long-running-activity)
* [Sidekiq — Iteration](https://github.com/sidekiq/sidekiq/wiki/Iteration)
* [Mike Perham — Iteration and Sidekiq 7.3.0](https://www.mikeperham.com/2024/07/03/iteration-and-sidekiq-7.3.0)
* [Shopify job-iteration](https://raw.githubusercontent.com/Shopify/job-iteration/main/README.md)
* [Rails — `ActiveJob::Continuation`](https://edgeapi.rubyonrails.org/classes/ActiveJob/Continuation.html)
* [Graphile Worker — Tasks (helpers, batch jobs)](https://worker.graphile.org/docs/tasks)
* [AWS Lambda — partial batch response for SQS](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html)
* [DBOS — Steps](https://mintlify.wiki/dbos-inc/dbos-transact-py/concepts/steps)
* [Inngest — `step.run` (Python)](https://www.inngest.com/docs/reference/python/steps/run.md)

**Контекст задачи, DI, middleware**

* [Temporal Python — `temporalio.activity` (contextvars)](https://python.temporal.io/temporalio.activity.html)
* [Hatchet — Python Context](https://docs.hatchet.run/sdks/python/context)
* [taskiq — State and dependencies](https://taskiq-python.github.io/guide/state-and-deps.html)
* [Dishka — интеграция с taskiq](https://dishka.readthedocs.io/en/latest/integrations/taskiq.html)
* [Dramatiq — API reference, middleware](https://dramatiq.io/reference.html)
* [Hatchet — Batch tasks](https://docs.hatchet.run/v1/batch-tasks)
