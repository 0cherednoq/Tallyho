# Задача и её итог

Задача - обычная `async def`. Итог попытки она сообщает **возвращаемым значением**: так итог
виден в сигнатуре, проверяется в тесте обычным сравнением и не зависит от того, не забыли ли вы
`return` после императивного вызова.

<!-- tallyho-noexec: фрагмент приложения: images и thumbnails принадлежат вашему проекту -->
```python
from tallyho import Error, Ok, Skip


@fq.task(max_retries=3, retry_on=[StorageTimeout], queue="images")
async def resize(image_id: int) -> Ok | Skip | Error:
    image = await images.get(image_id)
    if image.has_thumbnail:
        return Skip("already_resized")  # задача не понадобилась
    try:
        saved_bytes = await thumbnails.build(image)
    except CorruptImage as error:
        # ошибочный итог без исключения: ретраев не будет
        return Error("corrupt_file", detail={"image_id": image_id, "reason": str(error)})
    item.incr("bytes_saved", saved_bytes)  # своя метрика батча
    return Ok("resized")
```

| Возврат | Итог |
|---|---|
| `Ok(label=None, *, result=None, mark=None)` | успешный итог; без метки считается как `"ok"` |
| `Skip(label=None, *, mark=None)` | задача пропущена - не ошибка |
| `Error(label=None, *, detail=None, mark=None)` | ошибочный итог без исключения и без ретраев |
| `Defer(reason, delay=None, until=None)` | [отложить](#отложить-а-не-потратить-попытку), не тратя попытку |
| `None` или любое другое значение | итог `ok` с меткой `"ok"`; значение возвращается брокеру как есть |

Итоги - замороженные объекты, поэтому задачу проверяют без подмены контекста:

<!-- tallyho-noexec: фрагмент теста приложения: задача и её зависимости ваши -->
```python
async def test_duplicate_is_skipped() -> None:
    assert await resize(image_id=7) == Skip("already_resized")
```

Правила итога:

* Задача вернулась без итога - `ok` с меткой `"ok"`.
* Итог записывается после возврата из функции, вместе со всеми `spawn`, `incr` и `expect` - одной
  транзакцией.
* Исключение - это ретрай брокера, а не итог, если его [судьба](#судьба-исключений) не объявлена.
  Когда брокер исчерпал попытки, задача получает `error` с меткой `"exhausted"`. Если воркер умер и
  аренда истекла на последней попытке - метка `"lease_expired"`.
* После завершения задача неизменяема. Повторная доставка того же сообщения брокером задачу не
  вызовет.
* Ошибочные задачи помечаются для быстрого поиска по метке; `ok` и `skip` - нет. Поведение меняет
  параметр `mark=True/False` у `Ok`, `Skip` и `Error`.
* Метки и метрики - разные словари. `view.labels` (и то же поле сводки в хуках) - число
  задач по меткам итога, `view.metrics` - суммы `item.incr`. Метка и метрика с одним именем не
  смешиваются, поэтому `labels` можно целиком сохранять как разбивку итогов. Имя метки или
  метрики не может начинаться с символа U+001F - он зарезервирован, такой вызов бросает
  `ConfigurationError`.

## Отложить, а не потратить попытку

`Defer` возвращает задачу в очередь со сроком и причиной: попытка не тратится, `attempt` не
растёт, ретрая и DLQ нет. Это итог для случая «работа не выполнена, потому что не было ресурса»:

<!-- tallyho-noexec: фрагмент приложения: пул аккаунтов принадлежит вашему проекту -->
```python
from datetime import timedelta

from tallyho import Defer, Ok


@fq.task(queue="checks")
async def check_domain(domain: str) -> Ok | Defer:
    account = await accounts.try_lease()
    if account is None:
        return Defer(reason="no_accounts", delay=timedelta(seconds=10))
    async with account:
        return Ok("checked", result=await account.check(domain))
```

Отложенные задачи уходят в брокер по своему сроку; пауза их не трогает. Когда ресурс появился
раньше срока, разбудите их сами:
[`handle.release_deferred(...)`](operations.md#разбудить-отложенное). Потолки `max_defers` и
`max_deferred_for` ограничивают, как долго задача может откладываться - см.
[параметры батча](../batches.md#параметры-thbatch).

## Судьба исключений

«Это исключение значит отложить, а это - ошибочный итог» - знание об одном типе исключения, а не
об одном вызове. Поэтому оно объявляется там же, где задача регистрируется, и не повторяется в её
теле:

<!-- tallyho-noexec: фрагмент приложения: доменные исключения принадлежат вашему проекту -->
```python
@fq.task(
    queue="checks",
    retry_on=[ProviderUnavailable],  # вердикт брокера: повторить
    defer_on={
        NoCheckerAccount: lambda exc: Defer(reason=exc.reason, delay=exc.retry_after),
        CheckerQuotaExhausted: Defer(reason="quota", delay=timedelta(minutes=1)),
    },
    error_on={InvalidDomainName: "invalid_domain"},
    skip_on={DomainAlreadyChecked: "duplicate"},
)
async def check_domain_chunk(run_id: str, domains: list[str]) -> Ok:
    return Ok("chunk", result=await interactor(run_id, domains))
```

* Значение правила - готовый итог, функция `exc -> итог` или (для `error_on` и `skip_on`) метка
  строкой.
* Правило выбирается по ближайшему классу в MRO исключения: правило для подкласса сильнее правила
  для его базы, порядок объявления ни на что не влияет.
* Один класс в двух опциях или одновременно в `*_on` и `retry_on` - `ConfigurationError` **при
  регистрации**: у исключения одна судьба, и узнать об этом надо до первого вызова.
* Переводится только исключение вашей задачи. Отмена и потеря аренды принадлежат учёту работы и
  не переводятся никогда.
* Если упало само правило, исходное исключение идёт обычным путём (вердикт брокера), а падение
  правила попадает в лог.
* Опции принадлежат tallyho: во flexiq они не передаются, а ваши `retry_on` / `dont_retry_on`
  остаются без изменений.

## Единица работы

Для кода, которому нужна не метка, а сама задача - закладка, запись итога в вашей транзакции,
прогресс, - есть `WorkItem`:

| Что | Зачем |
|---|---|
| `item.require() -> WorkItem` | строгий доступ: вне задачи `ConfigurationError`, а не тишина |
| `item.current() -> WorkItem \| None` | мягкий доступ |
| `work.id`, `work.batch_id`, `work.attempt`, `work.defers` | кто выполняется и в какой попытке |
| `work.checkpoint()`, `await work.checkpoint_in(session, position)` | [закладка](checkpoint.md) составной работы |
| `work.progress(done, total=None)` | прогресс долгой задачи; виден в `handle.in_flight()` |
| `work.incr(name, value=1)` | прибавить к метрике батча |
| `work.cancelled()` | `True`, если батч отменяют: долгой задаче пора выйти |
| `await work.complete_in(session, outcome=None)` | завершить задачу [в вашей транзакции](../hooks/complete-in.md) |
| `work.spawn(...)`, `work.spawn_call(...)`, `work.expect(...)`, `work.sub_batch(...)` | [динамический fan-out](pipelines.md#spawn-задачи-порождают-задачи) |

В слоистом приложении `WorkItem` удобнее получать зависимостью конструктора, а не из фасада:
[интеграция с Dishka](../../integrations/dishka.md) открывает scope контейнера на каждую попытку и
отдаёт `WorkItem` тем, кто его просит.

`ItemContext` - прежнее имя `WorkItem`. Алиас сохраняется на один минорный выпуск.

## Обёртки вокруг задачи

`Tallyho(..., middleware=[...])` принимает последовательность обёрток вокруг вызова каждой
отслеживаемой задачи. Первая в списке - внешняя:

<!-- tallyho-noexec: фрагмент приложения: логирование и метрики ваши -->
```python
class LogAttempt:
    async def __call__(self, work: WorkItem, call_next: Callable[[], Awaitable[object]]) -> object:
        log.info("item %s attempt %d", work.id, work.attempt)
        try:
            return await call_next()
        finally:
            log.info("item %s done", work.id)


th = Tallyho(engine, schema="app", middleware=[LogAttempt()])
```

* Обёртка вызывается после захвата задачи и внутри её контекста, поэтому `item.require()` в ней
  работает.
* Возвращаемое значение и исключение она пропускает как есть: по ним определяется итог.
* Итог обёртка не записывает и ретраями не управляет - для этого есть возвращаемые итоги и
  `defer_on` / `error_on` / `skip_on`.
* Один экземпляр обслуживает все попытки процесса, поэтому состояние попытки держите в `work`, а
  не в самой обёртке.
* Колбэк-задачи и неотслеживаемые вызовы не оборачиваются; пустой список ничего не стоит.

## Совместимый фасад `item`

Фасад `item` (`from tallyho import item`) остаётся и работает как раньше: это модульный объект, а
не атрибут установки, поэтому модулю задач не нужен доступ к `th`. Он удобен для скриптов и для
того, чтобы дописать метрику из глубины стека.

| Вызов | Что делает |
|---|---|
| `item.ok(label=None, *, result=None)` | успешный итог |
| `item.skip(label=None)` | пропуск |
| `item.error(label=None, *, detail=None)` | ошибочный итог без исключения |
| `item.defer(reason=..., delay=..., until=...)` | отложить |
| `item.incr`, `item.progress`, `item.cancelled`, `item.spawn`, `item.expect`, `item.sub_batch` | то же, что у `WorkItem` |
| `item.checkpoint()`, `await item.checkpoint_in(session, position)` | закладка |
| `await item.complete_in(session, outcome=None)` | завершить задачу в вашей транзакции |
| `item.id()`, `item.current()`, `item.require()` | идентификатор и единица работы текущей задачи |

* Последний вызов `ok/skip/error` перед возвратом выигрывает. `defer` и `ok/skip/error`
  взаимоисключаются по порядку: вызов после другого бросает `ConfigurationError`.
* Задать итог и вызовом, и возвращаемым значением нельзя: два источника итога - всегда ошибка в
  коде, и обёртка бросает `ConfigurationError` до записи.
* Вне отслеживаемой задачи вызовы `item.*` ничего не делают (кроме `item.sub_batch` и
  `item.require`, которые бросают `ConfigurationError`), поэтому функцию можно вызывать напрямую в
  юнит-тестах.

## Вызовы и опции

`th.call(fn, *args, **kwargs)` готовит вызов задачи, `.opts(...)` задаёт его опции:

| Опция | Значение |
|---|---|
| `key` | ключ дедупликации задачи внутри батча |
| `weight` | вес задачи в доле прогресса `ratio`, целое от 1 (по умолчанию 1) |
| `queue` | очередь брокера |
| прочие именованные | опции брокера (`max_retries`, `priority`, `timeout` …); их проверяет адаптер - см. [опции flexiq](../../integrations/flexiq.md#опции-постановки) |

Подготовленные вызовы принимают `batch.add_calls([...])`, `item.spawn_call(call, into=...)` и
параметры колбэков `on_...=`.
