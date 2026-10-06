# Prometheus

Готового экспортёра для Prometheus в пакете нет, и он не нужен: наблюдатель на `prometheus_client`
занимает два десятка строк. tallyho сообщает события, а какие из них превращать в метрики и с
какими метками, решаете вы.

## Установка

```bash
pip install prometheus-client
```

## Подключение

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

Наблюдатель передаётся в конструктор клиента и работает в каждом процессе, где клиент создан: в
API, в воркерах, в maintenance. Отдавать метрики наружу нужно из каждого такого процесса, обычным
для `prometheus_client` способом.

## Что учесть

* Методы наблюдателя синхронные и вызываются вне транзакций. Они не должны блокировать: счётчик в
  памяти подходит, сетевой вызов нет.
* Исключение в наблюдателе на учёт не влияет.
* Метка итога задачи приходит из вашего кода (`item.ok("sent")`). Если меток много и они
  произвольные, не кладите их в метку метрики: получите взрыв кардинальности.
* Полный список событий и их аргументов есть на странице
  [Наблюдаемость](../guide/operations/observability.md#observer). Там же сказано, на что ставить
  алерты.
