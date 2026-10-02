# Эксплуатация PostgreSQL

[← Оглавление](README.md) · назад: [Адаптер flexiq](flexiq.md) · далее: [Ограничения v1](limitations.md)

## Процессы

tallyho — библиотека: отдельного сервера у неё нет. Она работает внутри ваших процессов.

| Процесс | Что в нём делает tallyho | Что должно быть настроено |
|---|---|---|
| API (продюсер) | создаёт батчи в вашей транзакции и **сразу после коммита отправляет их задачи в брокер**, сверяется с DLQ брокера, читает прогресс, выполняет `pause/resume/cancel/retry_failed`, финализирует после своих операций | клиент, адаптер брокера, хуки |
| Воркер брокера | учитывает выполнение задач, пишет итоги групповым коммитом, отправляет в брокер порождённые задачи, сверяется с DLQ брокера, финализирует батч после последней задачи и вызывает хуки | клиент, адаптер брокера, хуки |
| Maintenance | возвращает в очередь задачи умерших воркеров, подбирает пропущенные финализации, повторяет упавшие хуки, следит за дедлайнами, делает снимки прогресса (`on_progress`), удаляет деревья по retention | клиент, хуки; адаптер брокера — желательно |

Maintenance обязателен. Без него зависшие задачи никто не вернёт, пропущенные финализации никто
не подберёт, а `on_progress` не будет вызываться.

**Лидер.** Экземпляров maintenance может быть сколько угодно, но фоновые проверки выполняет
один — лидер. Он выбирается через advisory lock PostgreSQL; остальные ждут и подхватывают работу,
когда лидер остановился или потерял соединение. Запускайте не меньше двух экземпляров.

### Отправка в брокер

Сообщения отправляет каждый процесс, в котором вызван `th.install(adapter)`, — отдельный процесс
для этого не нужен, и от лидера maintenance отправка не зависит.

* **Сразу после коммита.** Процесс отправляет то, что сам только что записал: задачи нового
  батча, порождённые задачи, колбэки финализации. Задержка — миллисекунды.
* **Страховочный проход.** Раз в `sweep_interval` (5 секунд) процесс отправляет все сообщения
  старше `relay_grace` (5 секунд), кто бы их ни записал. Так уходят сообщения процесса, упавшего
  между коммитом и отправкой, задачи с наступившим отложенным стартом, задачи, возвращённые после
  истёкшей аренды, и всё, что поставил в очередь процесс без брокера. Задержка в этих случаях — до
  `relay_grace + sweep_interval`, по умолчанию до 10 секунд; обе настройки можно уменьшить.

Фоновая отправка начинает работать в процессе с первой его записи в очередь или с запуска
`th.maintenance().run()`. Чтобы страховочный проход шёл с самого старта, запускайте maintenance
внутри приложения (следующий раздел): тогда первый проход выполняется сразу.

Несколько процессов друг другу не мешают: одно сообщение дважды они не отправят. Если процесс
упал, уже захватив сообщение, оно вернётся в очередь через `relay_claim_ttl` (30 секунд) и может
дойти до брокера повторно — такой дубль tallyho распознаёт и не выполняет.

При остановке процесса вызовите `await th.aclose()` — в том же event loop, где работало
приложение. Он останавливает фоновую отправку и дожидается её; начатая отправка завершается.

### Сверка с DLQ брокера

Брокер может окончательно отказаться от джобы (отправить её в DLQ), а итог задачи батча при этом
останется незаписанным. Так бывает, когда PostgreSQL недоступен дольше, чем брокер повторяет
джобу: задача ни разу не выполнилась, джобы больше нет, а в tallyho задача числится отправленной —
батч не завершится. Обычная страховка — [событие брокера](flexiq.md#ретраи-и-dlq) — в такой момент
тоже не может записать итог.

Поэтому каждый процесс с адаптером после каждого страховочного прохода отправки сверяется с DLQ
брокера и завершает такие задачи с итогом `error` и меткой `"exhausted"`:

* задержка — до `sweep_interval` (5 секунд) после того, как PostgreSQL снова доступен;
* работает там же и тогда же, где фоновая отправка: в процессах с `th.install(adapter)`, от лидера
  maintenance не зависит. Процесс без брокера (команда `tallyho maintenance`) DLQ не читает;
* задача, которую tallyho после умершей джобы уже отправил заново (истекла аренда,
  `retry_failed()`), сверкой не затрагивается: её ведёт новая джоба. Задача, которая ещё
  выполняется, тоже остаётся как есть и запишет свой итог сама;
* место, до которого разобран DLQ, хранится в базе и двигается в той же транзакции, что и
  завершения задач; несколько процессов одну и ту же работу не делают.

Один проход сверки выполняет и `await th.run_maintenance_once()`.

### Maintenance внутри приложения

`th.maintenance()` возвращает сервис с методами `run()` (работать до остановки), `stop()` и
`run_once()`. Его удобно запускать в lifespan API-процесса: адаптер и хуки там уже настроены.
Пока `run()` работает, процесс выполняет и страховочную отправку — независимо от того, лидер он
или нет.

<!-- tallyho-example: guide-operations-maintenance -->
```python
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне — адаптер вашего брокера
th = Tallyho(
    engine,
    schema=schema,
    sweep_interval=timedelta(seconds=1),  # период фоновых проверок и страховочной отправки
)
th.install(broker.adapter)
await th.migrate()


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    runner = th.maintenance()
    task = asyncio.create_task(runner.run(), name="tallyho-maintenance")
    try:
        yield
    finally:
        runner.stop()  # мягкая остановка: текущий проход завершится
        await task
        await th.aclose()  # остановить фоновую отправку


async def build(section: int) -> None: ...


try:
    async with lifespan():
        async with th.batch("report_build", key="daily") as batch:
            await batch.map(build, range(3))

        async with asyncio.timeout(30):
            while broker.pending < 3:  # отправлено сразу после коммита, без ожидания relay_grace
                await asyncio.sleep(0.05)

        await broker.drain()  # в продакшне задачи выполняют воркеры
        assert (await batch.handle.view()).state is BatchState.SUCCEEDED
finally:
    await broker.close()
```

Задачу maintenance храните и дожидайтесь при остановке, как в примере: `runner.stop()`, затем
`await task`.

`InlineBroker` в тестах сам решает, когда сообщения попадают в очередь, поэтому фоновая отправка
с ним работает, только пока запущен `runner.run()`. С настоящим адаптером она работает всегда.

### Maintenance отдельным процессом

Отдельный процесс не зависит от перезапусков API и не делит с ним event loop. Это рекомендуемый
вариант при заметной нагрузке. Процесс должен собрать ту же установку, что и приложение: тот же
клиент и модули с хуками. С адаптером брокера он сам отправляет то, что ставят в очередь его
проверки (возврат задачи умершего воркера, колбэк пропущенной финализации), — сразу, без задержки
страховочного прохода.

<!-- tallyho-noexec: точка входа отдельного процесса; модуль app.tasks с клиентом и адаптером — ваш -->
```python
# app/maintenance.py — запуск: python -m app.maintenance
import asyncio

from tallyho.cli.app import serve_maintenance

from app.tasks import fq, th  # клиент с установленным адаптером и hook_modules


async def main() -> None:
    try:
        await serve_maintenance(th.maintenance())  # до SIGINT или SIGTERM
    finally:
        await th.aclose()
        await fq.close()


asyncio.run(main())
```

`serve_maintenance` запускает `runner.run()` и останавливает его по `SIGINT` и `SIGTERM`.

В пакете есть и готовая команда:

```bash
tallyho maintenance --dsn postgresql+asyncpg://app:secret@db/app --schema app \
    --hook-module app.mailing.hooks
tallyho maintenance --dsn ... --schema app --hook-module app.mailing.hooks --once   # один проход
```

Она **не знает вашего брокера**: очередь отправки не трогает и
[с DLQ брокера не сверяется](#сверка-с-dlq-брокера). Восстановление, финализация, хуки,
снимки прогресса и retention в ней работают, и лидером она быть может: отправке и сверке, которые
идут в процессах с адаптером, это не мешает.
Сообщения, которые ставят в очередь её проверки, отправляют процессы с адаптером — API и
воркеры — своим страховочным проходом, то есть с задержкой до `relay_grace + sweep_interval`.
Если такая задержка возврата задач и колбэков не подходит, используйте собственную точку входа
с адаптером.

## Требования к базе

* **Одна база.** Таблицы tallyho и ваши таблицы, которые меняют хуки, лежат в одной базе
  PostgreSQL.
* **Только primary.** Движок, переданный в `Tallyho`, должен смотреть на primary. Решения о
  финализации по данным реплики приводят к ложному «ещё не готово» или к худшему.
* **Соединения.** tallyho берёт соединения из пула вашего `AsyncEngine`. Лидер maintenance
  постоянно удерживает одно соединение и берёт ещё на время проходов; учтите это в `pool_size`.
* **Время.** Все сроки (аренда, отложенный старт, дедлайны, retention) считаются по часам базы,
  поэтому расхождение часов между серверами приложения на учёт не влияет.

## Autovacuum

tallyho пишет много и коротко. Миграция сама задаёт параметры хранения для своих таблиц — менять
их вручную не нужно, достаточно не выключать autovacuum.

| Таблицы | Что задаёт миграция | Зачем |
|---|---|---|
| `th_counter`, `th_metric` | `fillfactor=50`, `autovacuum_vacuum_scale_factor=0`, `autovacuum_vacuum_threshold=1000` | строки счётчиков обновляются постоянно; свободное место на странице даёт обновления без записи в индексы (HOT), а порог по числу мёртвых строк запускает очистку рано |
| `th_outbox`, `th_lease`, `th_counter_delta`, `th_window` | `autovacuum_vacuum_scale_factor=0`, `autovacuum_vacuum_threshold=1000` | таблицы «вставили и удалили»: их размер должен соответствовать текущей работе, а не истории |
| `th_item` | `fillfactor=85` | единственное обновление задачи (запись итога) не трогает индексы |

Что проверить на своей стороне:

* autovacuum включён, и воркеров autovacuum хватает (`autovacuum_max_workers`): при высокой нагрузке
  частые очистки маленьких таблиц tallyho не должны ждать очистки ваших больших таблиц;
* очистка действительно проходит:

```sql
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = 'app' AND relname LIKE 'th\_%'
ORDER BY n_dead_tup DESC;
```

Если `n_dead_tup` у `th_counter` или `th_outbox` стабильно на порядки больше `n_live_tup`, очистке
что-то мешает — чаще всего длинная транзакция (следующий раздел).

## Длинные транзакции и `backend_xmin`

Главный враг любых горячих таблиц, не только tallyho, — **транзакция, которая долго остаётся
открытой где угодно в кластере**. PostgreSQL не может убрать старые версии строк, пока они видны
хотя бы одной живой транзакции. Счётчики обновляются тысячи раз в секунду; если горизонт очистки
стоит, страницы счётчиков распухают, цепочки версий удлиняются, и каждое чтение и обновление
становится медленнее. Корректность при этом не страдает — падает скорость.

Что держит горизонт:

* сессии `idle in transaction` (забытый `BEGIN`, зависший обработчик запроса);
* долгие аналитические запросы и `pg_dump` на primary;
* реплика с `hot_standby_feedback = on`, на которой идёт долгий запрос;
* незавершённые подготовленные транзакции (`pg_prepared_xacts`) и отставшие слоты репликации.

Как найти:

```sql
SELECT pid, usename, application_name, state,
       now() - xact_start AS xact_age,
       age(backend_xmin)  AS xmin_age,
       left(query, 80)    AS query
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC
LIMIT 10;
```

Что настроить:

* `idle_in_transaction_session_timeout` — например, 60 секунд для роли приложения: забытые
  транзакции будут закрыты сами;
* мониторинг и алерт на возраст самой старой транзакции (`max(now() - xact_start)` и
  `max(age(backend_xmin))` из запроса выше);
* тяжёлую аналитику и `pg_dump` — на реплику без `hot_standby_feedback` либо в окно низкой нагрузки;
* в своих задачах не держите транзакцию открытой на время сетевых вызовов: открыли, записали,
  закоммитили.

Собственные транзакции tallyho короткие. Они ставят `lock_timeout` (5 секунд по умолчанию) и
`statement_timeout` и сами повторяются при дедлоке, конфликте сериализации и таймауте блокировки.
Если повторы не помогли, операция бросает `ConcurrentModification`. Хук ограничен `hook_timeout`.

### Уровень изоляции вашей транзакции

`session=` во всех операциях и `item.complete_in(session)` работают при `READ COMMITTED` и
`REPEATABLE READ`. Завершение задачи в вашей транзакции не трогает горячие строки счётчиков,
поэтому не ждёт чужих блокировок и не даёт ошибок сериализации.

## pgbouncer

tallyho работает через pgbouncer в режиме **transaction pooling** с обоими драйверами. Нужно
отключить кэш подготовленных выражений на стороне драйвера, иначе получите
`prepared statement does not exist`:

<!-- tallyho-noexec: фрагмент конфигурации: нужен запущенный pgbouncer -->
```python
from sqlalchemy.ext.asyncio import create_async_engine

# asyncpg
engine = create_async_engine(
    "postgresql+asyncpg://app:secret@pgbouncer:6432/app",
    connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
)

# psycopg 3
engine = create_async_engine(
    "postgresql+psycopg://app:secret@pgbouncer:6432/app",
    connect_args={"prepare_threshold": None},
)
```

Через transaction pooling проверены: создание батчей в вашей сессии, выполнение и завершение задач,
финализация с хуками, `pause`/`resume`.

Двум возможностям нужна **сессия**, а не транзакция, поэтому им дайте прямое подключение к
PostgreSQL (или пул pgbouncer в режиме session):

| Что | Почему |
|---|---|
| процесс maintenance | лидер удерживает advisory lock уровня сессии; через transaction pooling блокировка «уезжает» на чужое серверное соединение, и выбор лидера перестаёт работать |
| `handle.watch()` и `handle.wait()` | используют `LISTEN`/`NOTIFY`; через transaction pooling уведомления не приходят, и обновления приходят только по таймауту опроса |

Проще всего завести для процесса maintenance отдельный `AsyncEngine` с прямым DSN. Остальные
процессы (API, воркеры) могут ходить через pgbouncer.

## Наблюдаемость

Библиотека не настраивает логирование и ничего не экспортирует сама. Есть два канала.

**Логи.** Логгеры стандартного модуля `logging` с именами `tallyho.*`. Ошибки отправки в брокер,
упавшие хуки, потеря лидерства пишутся с уровнем `ERROR`. Атрибуты и `memo` батчей в логи не
попадают.

**`Observer`.** Объект, который получает события жизненного цикла. Он передаётся в
`Tallyho(observer=...)`. Методы синхронные, вызываются вне транзакций и не должны блокировать;
исключение наблюдателя на учёт не влияет. Наследуйте `NullObserver` и переопределяйте только нужное:

<!-- tallyho-example: guide-operations-observer -->
```python
from collections import Counter
from uuid import UUID

from tallyho import Tallyho, item
from tallyho.model.states import BatchState, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.testing import InlineBroker


class Metrics(NullObserver):
    """Счётчики для вашей системы метрик (Prometheus, StatsD …)."""

    def __init__(self) -> None:
        self.items: Counter[tuple[str, str | None]] = Counter()
        self.finalized: list[tuple[str, str]] = []

    def item_finished(
        self, *, batch_id: UUID, item_id: UUID, result: ResultClass, label: str | None, attempt: int
    ) -> None:
        self.items[result.name.lower(), label] += 1

    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        self.finalized.append((kind, state.name.lower()))


metrics = Metrics()
broker = InlineBroker()
th = Tallyho(engine, schema=schema, observer=metrics)
th.install(broker.adapter)
await th.migrate()


async def convert(file_id: int) -> None:
    if file_id == 2:
        item.error("unsupported_format")
    else:
        item.ok("converted")


try:
    async with th.batch("conversions", key="upload:1") as batch:
        await batch.map(convert, range(4))
    await broker.drain()

    assert metrics.items == Counter({("ok", "converted"): 3, ("error", "unsupported_format"): 1})
    assert metrics.finalized == [("conversions", "completed_with_errors")]
finally:
    await broker.close()
```

| Событие `Observer` | Когда |
|---|---|
| `batch_created(batch_id, kind)` | батч создан, транзакция закоммичена |
| `item_claimed(batch_id, item_id, attempt)` | воркер взял задачу |
| `item_finished(batch_id, item_id, result, label, attempt)` | итог задачи записан |
| `batch_finalized(batch_id, kind, state)` | батч финализирован, хук закоммичен |
| `hook_failed(batch_id, kind, hook, attempt, error)` | tx-хук упал, будет повтор |
| `hook_missing(batch_id, kind, hook)` | нужный батчу хук не зарегистрирован в этом процессе |
| `relay_dispatched(messages, duration)` | пачка сообщений отправлена в брокер |
| `relay_lag(seconds)` | возраст самого старого неотправленного сообщения |
| `completer_flush(items, duration)` | групповой коммит итогов |
| `completer_buffer(items)` | сколько итогов ждёт записи в процессе воркера |
| `oldest_lease(seconds)` | возраст самой старой аренды |
| `transaction_retry(sqlstate)` | внутренняя транзакция будет повторена |

### OpenTelemetry

Готовый наблюдатель для OpenTelemetry ставится extra `otel`:

<!-- tallyho-noexec: нужны настроенные провайдеры и экспортёр OpenTelemetry вашего приложения -->
```python
from tallyho import Tallyho
from tallyho.observability.otel import OpenTelemetryObserver

th = Tallyho(engine, schema="app", observer=OpenTelemetryObserver())
# либо явно: OpenTelemetryObserver(tracer=my_tracer, meter=my_meter)
```

Он использует глобальные провайдеры OpenTelemetry (или переданные `tracer`/`meter`) и создаёт:

| Что | Имена |
|---|---|
| спаны | `tallyho.create`, `tallyho.claim`, `tallyho.finish`, `tallyho.finalize` с атрибутами `tallyho.batch.id`, `tallyho.batch.kind`, `tallyho.batch.state`, `tallyho.item.id`, `tallyho.item.attempt`, `tallyho.item.result`, `tallyho.item.label` |
| счётчики | `th_hook_failures`, `th_hook_missing`, `th_transaction_retries` (дедлоки) |
| гистограммы | `th_relay_lag` (с), `th_completer_buffer_size`, `th_oldest_lease_age` (с) |

Аргументы и результаты задач, атрибуты и `memo` в телеметрию не передаются.

### На что ставить алерты

| Сигнал | Что означает | Что делать |
|---|---|---|
| `th_hook_failures` растёт | хук `on_finalized` падает; батчи не финализируются | смотреть `view.hook_error`, исправить хук; после исправления — `handle.retry_finalize()` или дождаться повтора |
| `th_hook_missing` > 0 | процесс финализирует батчи, для которых у него нет хука | проверить `hook_modules` воркеров и maintenance |
| `th_relay_lag` растёт | задачи не уходят в брокер | доступен ли брокер, живы ли процессы с адаптером (API, воркеры) |
| `th_oldest_lease_age` больше `lease_ttl` | аренды не продлеваются и не снимаются | живы ли воркеры и maintenance |
| `th_completer_buffer_size` близко к `completer_backpressure` | база не успевает принимать итоги | искать длинные транзакции и блокировки |
| `th_transaction_retries` растёт | дедлоки | проверить порядок блокировок в своём коде: сначала своя строка, потом tallyho |

## Диагностика

| Вопрос | Инструмент |
|---|---|
| что с батчем прямо сейчас | `tallyho inspect <uuid или kind:key> --dsn … --schema …` или `await handle.view()` |
| какие задачи выполняются и давно ли | `await handle.in_flight(limit=100)`: воркер, попытка, возраст аренды, собственный прогресс задачи |
| почему батч не завершается | `view.state`: `OPEN` — батч не закрыт (у этапа — не завершены источники); `SEALED` и `progress.pending > 0` — задачи ещё идут; `SEALED` и `pending == 0` — смотреть `view.hook_error` |
| задачи «идут», но `in_flight` давно пуст | задачи ждут в очереди брокера либо их джобы в DLQ; во втором случае итог запишет [сверка с DLQ](#сверка-с-dlq-брокера) — проверьте, что работает хотя бы один процесс с адаптером и что запись DLQ не удалена retention брокера |
| какие задачи упали | `handle.items(labels=[...])` или `handle.items(states={ItemState.ERROR})` |
| почему батч провален или отменён | `view.reason` |

## Retention

Завершённые деревья удаляет лидер maintenance — порциями, не блокируя работу. Правила и
`release()` описаны в разделе [Retention и `release()`](hooks.md#retention-и-release). Если
`retention=None`, таблицы растут без ограничений: это допустимо, но следите за размером `th_item`.

## Что дальше

* [Ограничения v1](limitations.md).
