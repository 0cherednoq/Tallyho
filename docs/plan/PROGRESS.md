# tallyho v1 — прогресс

> Протокол обновления — [PLAN.md §0](PLAN.md#0-протокол-итерации-loop). Файл правится в каждой итерации и коммитится вместе с кодом задачи.
> Статусы: `todo` · `in_progress` · `done` · `blocked` (причина в журнале) · `human` (нужен человек, цикл пропускает).

## Текущее состояние

* **Ветка:** `impl/v1` (создаётся в T0.1)
* **Текущая задача:** —
* **Последний зелёный коммит:** —

## Задачи

| ID | Задача | Зависит | Статус | Коммиты |
|---|---|---|---|---|
| T0.1 | Репозиторий собирается, гейты зелёные на скелете | — | todo | |
| T0.2 | Инфраструктура тестов (схема на тест, xdist) | T0.1 | todo | |
| T1.1 | Перечисления состояний и иерархия ошибок | T0.1 | todo | |
| T1.2 | Value-объекты, FailurePolicy, TaskCall | T1.1 | todo | |
| T1.3 | Математика прогресса | T1.2 | todo | |
| T1.4 | Протоколы и базовые реализации (Clock, UUIDv7, Serializer, Observer) | T1.1 | todo | |
| T2.1 | Таблицы и индексы | T1.1 | todo | |
| T2.2 | Миграции, установка в схему, alembic | T2.1, T0.2 | todo | |
| T2.3 | Транзакции: сессия пользователя, ретраи, after_commit, HookSession | T2.1, T1.4 | todo | |
| T2.4 | Запросы счётчиков, дельты, свёртка, reconcile | T2.2, T2.3 | todo | |
| T3.1 | Реестр tx-хуков | T1.2 | todo | |
| T4.1 | Продюсер: батчи, под-батчи, th_feed, add, seal, expect | T2.4, T3.1 | todo | |
| T4.2 | Relay | T4.1 | todo | |
| T4.3a | Completer: буфер, claim/heartbeat/release | T4.2 | todo | |
| T4.3b | Completer: finish без spawn | T4.3a | todo | |
| T4.3c | Spawn, into=, лимиты, дедуп, sub_batch из задачи | T4.3b | todo | |
| T4.4 | Finalizer | T4.3c | todo | |
| T4.5 | Путь B: complete_in и свёртка | T4.4 | todo | |
| T4.6 | Политики ошибок, on_policy_breach | T4.4 | todo | |
| T4.7 | Операции над деревом | T4.6 | todo | |
| T4.8 | Sweeper | T4.7 | todo | |
| T4.9 | Snapshotter | T4.8 | todo | |
| T4.10 | Maintenance, лидерство, watch | T4.9 | todo | |
| T4.11 | Чтение: view, in_flight, items, find | T4.4 | todo | |
| T5.1 | Runtime: ItemContext, th.item, tracked | T4.5, T4.7 | todo | |
| T6.1 | Tallyho, Settings, install, migrate | T5.1, T4.10, T4.11 | todo | |
| T6.2 | th.batch → BatchBuilder, BatchHandle | T6.1 | todo | |
| T6.3 | th.call с ParamSpec, типовые тесты | T6.1 | todo | |
| T7.1 | tallyho.testing: InlineBroker, FakeClock | T6.2 | todo | |
| T8.0 | Спайк flexiq | T0.1 | todo | |
| T8.1 | FlexiqAdapter | T8.0, T7.1 | todo | |
| T8.2 | Контрактные тесты A-FQ | T8.1 | todo | |
| T9.1 | Пример «рассылки» (§12) как тесты | T7.1 | todo | |
| T9.2 | Пример «конвейер парсинга» (§13) как тесты | T7.1 | todo | |
| T9.3 | Исполняемые примеры из документации | T9.1, T9.2 | todo | |
| T10.1 | Приёмка A-DB | T9.1 | todo | |
| T10.2 | Стресс: дедлоки, конвейеры | T9.2 | todo | |
| T10.3 | EXPLAIN-гард | T10.2 | todo | |
| T10.4 | Наблюдаемость и логи | T6.1 | todo | |
| T10.5 | CLI | T6.1 | todo | |
| T10.6 | Мутационное тестирование | T10.2 | todo | |
| T11.1 | Эталонное приложение и генераторы | T8.2, T9.2 | todo | |
| T11.2 | Оракул инвариантов | T11.1 | todo | |
| T11.3 | Хаос-контроллер, A-CH | T11.2 | todo | |
| T11.4 | Сценарии A-UC на стенде | T11.2 | todo | |
| T11.5 | Бенчмарк-харнесс A-PERF | T11.1 | todo | |
| T12.1 | Пользовательская документация | T9.3 | todo | |
| T12.2 | CI nightly и матрица | T11.4 | todo | |
| T12.3 | Подписание релиза | всё | human | |

## Журнал

<!-- Новые записи сверху. Формат:
### 2026-10-01 · T1.1 · done · abc1234
- Сделано: …
- Отклонения от плана/доков: … (или «нет»)
- Узнали / на что обратить внимание дальше: …
-->

### 2026-09-30 · план · —
- Создан план v1.0 по ARCHITECTURE v2.1 и ACCEPTANCE 1.0-draft.
