# Наблюдаемость

Библиотека не настраивает логирование и ничего не экспортирует сама. Есть два канала.

## Логи

Логгеры стандартного модуля `logging` с именами `tallyho.*`. Ошибки отправки в брокер,
упавшие хуки, потеря лидерства пишутся с уровнем `ERROR`. Атрибуты и `memo` батчей в логи не
попадают.

## `Observer`

Объект, который получает события жизненного цикла. Он передаётся в
`Tallyho(observer=...)`. Методы синхронные, вызываются вне транзакций и не должны блокировать;
исключение наблюдателя на учёт не влияет. Наследуйте `NullObserver` и переопределяйте только нужное:

<!-- tallyho-noexec: фрагмент приложения: нужен пакет prometheus_client -->
```python
# app/metrics.py
from uuid import UUID

from prometheus_client import Counter
from typing_extensions import override

from tallyho.model.states import BatchState, ResultClass
from tallyho.protocols.observer import NullObserver

ITEMS = Counter("tallyho_items_total", "Завершённые задачи", ["result", "label"])
BATCHES = Counter("tallyho_batches_total", "Финализированные батчи", ["kind", "state"])
HOOK_FAILURES = Counter("tallyho_hook_failures_total", "Падения tx-хуков", ["kind", "hook"])


class PrometheusObserver(NullObserver):
    @override
    def item_finished(
        self, *, batch_id: UUID, item_id: UUID, result: ResultClass, label: str | None, attempt: int
    ) -> None:
        ITEMS.labels(result.name.lower(), label or "").inc()

    @override
    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        BATCHES.labels(kind, state.name.lower()).inc()

    @override
    def hook_failed(
        self, *, batch_id: UUID, kind: str, hook: str, attempt: int, error: BaseException
    ) -> None:
        HOOK_FAILURES.labels(kind, hook).inc()


# app/tasks.py
th = Tallyho(engine, schema="app", observer=PrometheusObserver())

# после батча из четырёх задач, одна из которых завершилась ошибкой:
# tallyho_items_total{result="ok",label="converted"} 3
# tallyho_items_total{result="error",label="unsupported_format"} 1
# tallyho_batches_total{kind="conversions",state="completed_with_errors"} 1
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

## OpenTelemetry

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

## На что ставить алерты

| Сигнал | Что означает | Что делать |
|---|---|---|
| `th_hook_failures` растёт | хук `on_finalized` падает; батчи не финализируются | смотреть `view.hook_error`, исправить хук; после исправления - `handle.retry_finalize()` или дождаться повтора |
| `th_hook_missing` больше нуля | процесс финализирует батчи, для которых у него нет хука | проверить `hook_modules` воркеров и maintenance |
| `th_relay_lag` растёт | задачи не уходят в брокер | доступен ли брокер, живы ли процессы с адаптером (API, воркеры) |
| `th_oldest_lease_age` больше `lease_ttl` | аренды не продлеваются и не снимаются | живы ли воркеры и maintenance |
| `th_completer_buffer_size` близко к `completer_backpressure` | база не успевает принимать итоги | искать длинные транзакции и блокировки |
| `th_transaction_retries` растёт | дедлоки | проверить порядок блокировок в своём коде: сначала своя строка, потом tallyho |

## Диагностика

| Вопрос | Инструмент |
|---|---|
| что с батчем прямо сейчас | `tallyho inspect <uuid или kind:key> --dsn … --schema …` или `await handle.view()` |
| какие задачи выполняются и давно ли | `await handle.in_flight(limit=100)`: воркер, попытка, возраст аренды, собственный прогресс задачи |
| почему батч не завершается | `view.state`: `OPEN` - батч не закрыт (у этапа - не завершены источники); `SEALED` и `progress.pending > 0` - задачи ещё идут; `SEALED` и `pending == 0` - смотреть `view.hook_error` |
| задачи «идут», но `in_flight` давно пуст | задачи ждут в очереди брокера либо их джобы в DLQ; во втором случае итог запишет [сверка с DLQ](../operations.md#сверка-с-dlq-брокера) - проверьте, что работает хотя бы один процесс с адаптером и что запись DLQ не удалена retention брокера |
| какие задачи упали | `handle.items(labels=[...])` или `handle.items(states={ItemState.ERROR})` |
| почему батч провален или отменён | `view.reason` |
