# OpenTelemetry

В пакете есть готовый наблюдатель, который превращает события tallyho в спаны и метрики
OpenTelemetry.

## Установка

```bash
pip install "tallyho[asyncpg,otel]"
```

Дополнение `otel` ставит `opentelemetry-api`. SDK, экспортёр и провайдеры настраивает ваше
приложение.

## Подключение

<!-- tallyho-noexec: нужны настроенные провайдеры и экспортёр OpenTelemetry вашего приложения -->
```python
from tallyho import Tallyho
from tallyho.observability.otel import OpenTelemetryObserver

th = Tallyho(engine, schema="app", observer=OpenTelemetryObserver())
```

Без аргументов наблюдатель берёт глобальные провайдеры OpenTelemetry. Свои передаются явно:
`OpenTelemetryObserver(tracer=my_tracer, meter=my_meter)`.

## Что он создаёт

| Что | Имена |
|---|---|
| спаны | `tallyho.create`, `tallyho.claim`, `tallyho.finish`, `tallyho.finalize` с атрибутами `tallyho.batch.id`, `tallyho.batch.kind`, `tallyho.batch.state`, `tallyho.item.id`, `tallyho.item.attempt`, `tallyho.item.result`, `tallyho.item.label` |
| счётчики | `th_hook_failures`, `th_hook_missing`, `th_transaction_retries` (дедлоки) |
| гистограммы | `th_relay_lag` (с), `th_completer_buffer_size`, `th_oldest_lease_age` (с) |

## Что учесть

* Аргументы и результаты задач, атрибуты и `memo` батчей в телеметрию не передаются.
* На какие из этих метрик ставить алерты, сказано на странице
  [Наблюдаемость](../guide/operations/observability.md#на-что-ставить-алерты).
