# Интеграции

tallyho подключается к инструментам, которые у вас уже есть. Страницы раздела устроены одинаково:
что поставить, как подключить и что учесть.

| Интеграция | Что даёт | Дополнение |
|---|---|---|
| [flexiq](flexiq.md) | брокер задач: адаптер, опции постановки, ретраи и DLQ, остановка воркера | `tallyho[flexiq]` |
| [Alembic](alembic.md) | миграции tallyho внутри ваших ревизий | `tallyho[alembic]` |
| [pytest](pytest.md) | фикстура с готовой установкой для тестов | `tallyho[testing]` |
| [Prometheus](prometheus.md) | метрики через свой наблюдатель | не нужно |
| [OpenTelemetry](opentelemetry.md) | спаны и метрики из готового наблюдателя | `tallyho[otel]` |
| [pgbouncer](pgbouncer.md) | работа через transaction pooling | не нужно |

Готовых подключений к веб-фреймворкам в пакете нет. Запуск maintenance в `lifespan` приложения
показан на странице [Процессы и maintenance](../guide/operations.md#maintenance-внутри-приложения).

```{toctree}
:hidden:

flexiq
alembic
pytest
prometheus
opentelemetry
pgbouncer
```
