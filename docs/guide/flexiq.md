# Адаптер flexiq

[← Оглавление](README.md) · назад: [Тестирование](testing.md) · далее: [Эксплуатация PostgreSQL](operations.md)

tallyho не исполняет задачи сам: это делает брокер. Первый поддерживаемый брокер —
[flexiq](https://github.com/ByteVeda/flexiq) версии `>=2.0,<3`. Адаптер ставится extra `flexiq`:

```bash
pip install "tallyho[asyncpg,flexiq]"
```

## Подключение

<!-- tallyho-noexec: модуль приложения: нужен ваш DSN, а задачи исполняет отдельный процесс воркера flexiq -->
```python
# app/tasks.py — импортируется и в API-процессе, и в воркерах
from flexiq import Queue
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter

engine = create_async_engine("postgresql+asyncpg://app:secret@db/app")
queue = Queue(backend="postgres", db_url="postgresql://app:secret@db/app", schema="flexiq")

th = Tallyho(engine, schema="app", hook_modules=["app.mailing.hooks"])
fq = FlexiqAdapter(queue)
th.install(fq)  # до объявления задач


@fq.task(max_retries=4, retry_on=[TemporaryMailError], queue="mail")
async def send_email(campaign_id: int, contact_id: int) -> None:
    ...
    item.ok("sent")
```

Порядок важен:

1. создать `Queue` и `FlexiqAdapter(queue)`;
2. вызвать `th.install(fq)`;
3. объявлять задачи через `@fq.task(...)`. До `install` декоратор бросает `ConfigurationError`.

`@fq.task(...)` принимает те же параметры, что и `queue.task(...)` flexiq, и регистрирует в flexiq
вашу функцию, обёрнутую учётом tallyho. Имя задачи остаётся прежним: `модуль.имя_функции`.

Модуль с задачами и клиентом импортируется в каждом процессе: в API (он создаёт батчи) и в воркерах
(они выполняют задачи). Там же должны быть зарегистрированы [хуки](hooks.md#регистрация).

### Воркер

Воркер запускается средствами flexiq, **только с пулом потоков**:

<!-- tallyho-noexec: точка входа процесса воркера; работает, пока его не остановят -->
```python
# app/worker.py
from app.tasks import queue

queue.run_worker(queues=["default", "mail"], pool="thread")
```

Пул `prefork` не поддерживается — см. [ограничения](limitations.md#flexiq-только-poolthread). Чтобы
ошибка конфигурации обнаружилась сразу, передайте адаптеру тот же пул, что и воркеру:
`FlexiqAdapter(queue, pool="thread")` (значение по умолчанию). Любой другой пул — `ConfigurationError`
при `th.install`.

### Что делает адаптер

* **Транзакционная постановка.** У flexiq собственный пул соединений, поэтому «записать в свою
  таблицу и поставить задачу» одной транзакцией напрямую нельзя. tallyho пишет задачи в свои
  таблицы в вашей транзакции, а сразу после коммита тот же процесс
  [отправляет их в flexiq](operations.md#отправка-в-брокер) через этот адаптер. Откат
  транзакции — и в flexiq не попадает ничего; падение процесса после коммита задачу не теряет:
  её отправит любой другой процесс с адаптером.
* **Защита от дублей.** Отправка — at-least-once. Повторно доставленную, повторно запущенную из
  интерфейса flexiq или переигранную из DLQ джобу tallyho распознаёт и не выполняет: задача батча
  исполняется успешно не более одного раза.
* **Учёт ретраев.** Исключение в задаче — ретрай flexiq по его правилам. Когда попытки исчерпаны,
  задача батча получает итог `error` с меткой `"exhausted"`.
* **Служебный аргумент `_th`.** Адаптер добавляет в `kwargs` джобы ключ `_th` с идентификаторами
  задачи и батча (а при повторной отправке — ещё и с её номером) и убирает его перед вызовом
  вашей функции. В `metadata` и `notes` джобы tallyho
  ничего не пишет. Ключ `_th` видят ваши middleware, предикаты и хуки `before_task`/`on_enqueue`
  flexiq — не удаляйте и не меняйте его.

## Требования к задачам

* Только `async def`. Синхронная функция — `ConfigurationError` при декорировании.
* Отслеживаемые задачи ставятся **только через tallyho**: `batch.add`, `batch.map`,
  `batch.add_calls`, `item.spawn`, колбэки `on_...=`. Прямая постановка средствами flexiq создаст
  обычную джобу без учёта в батче.
* Перезапуск упавших — только `handle.retry_failed()`. `retry_dead` и `replay` из интерфейса flexiq
  задачу батча повторно не выполнят: она уже завершена.
* Итог задачи (`item.ok/skip/error`), `item.spawn` и остальные вызовы `item.*` работают так же, как
  с любым брокером, — см. [задача и её итог](batches.md#задача-и-её-итог).

## Опции постановки

Опции одного вызова задаются в `th.call(...).opts(...)`; без них действуют параметры из
`@fq.task(...)`.

<!-- tallyho-noexec: фрагмент использует задачу send_email и батч из примера выше -->
```python
call = th.call(send_email, campaign_id, contact_id).opts(
    key=email,  # дедупликация в батче — опция tallyho
    weight=2,  # вес в прогрессе — опция tallyho
    queue="mail-bulk",
    priority=5,
    max_retries=2,
    timeout=30,
    metadata='{"tenant": "acme"}',
)
await batch.add_calls([call])
```

| Опция | Поведение |
|---|---|
| аргументы задачи (позиционные, именованные, значения по умолчанию) | передаются как есть; кодируются сериализатором и кодеками, заданными для задачи во flexiq |
| `queue`, `priority`, `max_retries`, `timeout`, `result_ttl` | передаются как есть |
| `metadata` | передаётся без изменений |
| `notes` | передаётся без изменений; ограничения flexiq проверяются сразу при постановке в батч |
| `delay` | отсчитывается от момента отправки в flexiq, а не от создания батча. Отложенный старт всего батча — `start_at` |
| `expires` | передаётся как есть. Задача, которую воркер не успел взять до срока, получает итог `error` с меткой `"expired"` |
| `idempotency_key`, `unique_key`, `idempotent` | ваш ключ передаётся как есть; без него адаптер подставляет собственный ключ задачи |
| `depends_on` | **не поддерживается** — `UnsupportedOption`. Замена — этапы с [`fed_by`](batches.md#под-батчи-и-конвейеры-этапов) |
| `debounce`, `debounce_key`, `debounce_max_wait`, `debounce_replace_payload`, `batch` | **не поддерживаются** — `UnsupportedOption` |
| любая другая опция | `UnsupportedOption` |

Параметры самой задачи в `@fq.task(...)` — `retry_on`, `dont_retry_on`, `retry_backoff`,
`retry_delays`, `retry_budget`, `circuit_breaker`, `soft_timeout`, `rate_limit`, `max_concurrent`,
`middleware`, `inject`, `serializer`, `codecs`, `predicate` — работают как у обычной задачи flexiq.
Исключения: `@fq.task(batch=...)` и `@fq.task(debounce...=...)` отклоняются с `UnsupportedOption`.
Вес задачи задаётся в вызове (`.opts(weight=...)`), а не в декораторе.

Несовместимые опции отклоняются сразу, в процессе, который ставит задачу, — до записи в базу:

<!-- tallyho-example: guide-flexiq-call-options -->
```python
import tempfile
from pathlib import Path

from flexiq import Queue

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.model.errors import ConfigurationError, UnsupportedOption

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
    # Для примера хватает файловой очереди; в продакшне — Queue(backend="postgres", ...).
    queue = Queue(db_path=str(Path(directory) / "flexiq.db"))
    fq = FlexiqAdapter(queue)
    th = Tallyho(engine, schema=schema)
    th.install(fq)
    await th.migrate()

    @fq.task(max_retries=4, queue="mail")
    async def send_email(contact_id: int) -> None:
        item.ok("sent")

    rejected: list[str] = []
    for options in ({"depends_on": ["job-1"]}, {"debounce": 5}, {"no_such_option": True}):
        try:
            async with th.batch("campaign_deliveries", key="campaign:1") as batch:
                await batch.add_calls([th.call(send_email, 1).opts(**options)])
        except UnsupportedOption as error:
            rejected.append(error.option)  # транзакция батча откатилась, в базе ничего нет
    assert rejected == ["depends_on", "debounce", "no_such_option"]

    try:
        fq.task(batch=True)  # буферизация flexiq несовместима с учётом по одной задаче
    except UnsupportedOption as error:
        assert error.option == "batch"

    try:
        Tallyho(engine, schema=schema).install(FlexiqAdapter(queue, pool="prefork"))
    except ConfigurationError as error:
        assert "pool='thread'" in str(error)

    await fq.close()
```

## Ретраи и DLQ

* Решение «повторить или отправить в DLQ» принимает flexiq. Адаптер заранее вычисляет то же решение
  по параметрам задачи (`max_retries`, `retry_on`, `dont_retry_on`) и записывает итог `exhausted` на
  последней попытке.
* Если flexiq отправил джобу в DLQ раньше, чем ожидал адаптер (например, исчерпан `retry_budget`),
  итог запишет страховка: адаптер подписан на событие `JOB_DEAD` в процессе воркера и по нему
  завершает задачу батча с меткой `"exhausted"`.
* Событие доставляется без гарантий, а при недоступном PostgreSQL его обработчик не может записать
  итог. Тогда срабатывает вторая страховка — [сверка с DLQ](operations.md#сверка-с-dlq-брокера):
  процессы с адаптером раз в `sweep_interval` перечитывают DLQ flexiq и завершают с меткой
  `"exhausted"` задачи батча, у которых джоба умерла, а итог так и не записан. Типичный случай —
  PostgreSQL был недоступен дольше, чем flexiq повторяет джобу: задача ни разу не выполнилась,
  джоба в DLQ. В `item.error` такой задачи — `{"type": "DeadLetter", "message": <ошибка flexiq>}`.
* Сверке нужны и запись DLQ, и сама мёртвая джоба (по её аргументам определяется задача батча).
  Не настраивайте retention flexiq короче, чем возможный простой ваших процессов: записи,
  удалённые раньше сверки, уже не разобрать. Для батчей, которые не должны висеть бесконечно,
  по-прежнему задавайте [`deadline`](batches.md#параметры-thbatch).
* flexiq ставит время записи DLQ по часам воркера. Чтобы не пропустить запись воркера с отстающими
  часами, сверка каждый раз перечитывает DLQ с запасом — параметр
  `FlexiqAdapter(queue, dead_letter_overlap=timedelta(minutes=15))`. Увеличьте его, если часы
  воркеров расходятся сильнее.
* `circuit_breaker` flexiq не отправляет джобы в DLQ, а откладывает их; на учёт батча он не влияет.
* Воркер, убитый посреди задачи: когда истечёт аренда задачи (`lease_ttl`, 60 секунд по умолчанию),
  фоновые проверки tallyho вернут её в очередь. Повтор тратит попытку; без оставшихся попыток
  задача завершится с меткой `"lease_expired"`. Лимит попыток — `max_retries` вызова, а без него
  `max_retries` из `@fq.task` (по умолчанию 3). Повторная доставка той же джобы самим flexiq, пока
  аренда ещё жива, задачу второй раз не запустит.
* Повторная доставка при живой аренде (`requeue_job`, воркер, который flexiq счёл мёртвым) закрывает
  джобу как успешную, хотя задача ещё выполняется. Если после этого задача упадёт с повторяемой
  ошибкой, flexiq её уже не повторит, поэтому tallyho сам ставит задачу батча заново — новой джобой
  со своим счётчиком попыток flexiq.
* `retry_on` во flexiq — белый список. К непустому списку адаптер добавляет собственную ошибку
  `CompleterError` (PostgreSQL недоступен в момент захвата или записи итога задачи): такой сбой
  уходит в ретрай flexiq, а не сразу в DLQ. Чтобы отключить это, укажите `CompleterError` или её
  базовый класс в `dont_retry_on`.
* Кооперативная отмена flexiq (`TaskCancelledError`) записывается как итог `cancelled`.

## Остановка

`await fq.close()` дожидается внутренних операций адаптера и останавливает его пул отправки.
Вызывайте его при остановке API-процесса.

## Что дальше

* Какие процессы запускать и как настроить PostgreSQL — [Эксплуатация PostgreSQL](operations.md).
* Полный список ограничений — [Ограничения v1](limitations.md).
