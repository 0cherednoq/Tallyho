# Changelog

Формат — [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/),
версии — [SemVer](https://semver.org/lang/ru/).

## [Unreleased]

Содержимое будущей версии 1.0 — первой публичной. Руководство пользователя:
[docs/guide](docs/guide/README.md).

### Added

**Батчи и задачи**

- Клиент `Tallyho(engine, schema=, prefix=, hook_modules=, observer=, clock=, …)` поверх
  `AsyncEngine` SQLAlchemy; настройки — именованными аргументами.
- `th.batch(kind, key=, …)`: создание батча в собственной транзакции или в транзакции
  пользователя (`session=`), идемпотентное по `(kind, key)`; `add`, `map`, `add_calls`, `expect`,
  `seal`.
- Итог задачи через `item.ok / skip / error` с метками (labels), пользовательские метрики
  `item.incr`, собственный прогресс задачи `item.progress`, кооперативная отмена
  `item.cancelled()`, завершение в транзакции пользователя `item.complete_in(session)`.
- Вызовы `th.call(fn, …).opts(key=, weight=, queue=, …)`: дедупликация по ключу, веса для
  прогресса, опции брокера.
- Колбэк-задачи `on_succeeded`, `on_completed_with_errors`, `on_failed`, `on_cancelled`,
  `on_finalized_task`, которые ставятся в той же транзакции, что и финализация.

**Под-батчи и конвейеры**

- Под-батчи `sub_batch(key, …)` и динамические под-батчи из задачи (`item.sub_batch`).
- Конвейеры этапов: `fed_by=[…]`, `item.spawn(…, into=, key=)`, автоматическое закрытие этапа после
  финализации источников, каскад пустых этапов, `on_feeder_failed="seal" | "cancel"`.
- Лимиты разрастания: `max_items` на дерево (мягкий) и `max_depth` на самоподпитку; счётчики
  `duplicates` и `skipped_by_limit`.

**Управление и чтение**

- Операции над деревом: отложенный старт (`start_at`) и `reschedule`, `pause` / `resume`, `cancel`,
  `retry_failed(labels=)`, `retry_finalize`, дедлайн (`deadline`), окно параллелизма
  (`max_in_flight`).
- Политики ошибок `FailurePolicy.continue_() / fail_fast() / threshold(ratio=, min_processed=,
  labels=, action="fail" | "pause")`.
- Прогресс `handle.view()`: найдено / сделано / в работе, оценка итога (`expected`), доля по весам
  (`ratio`), ETA; `handle.in_flight()`, `handle.watch()`, `handle.wait()`.
- Неизменяемые `attributes` и `memo` корневого батча; листинг `th.list_batches(kinds=, states=,
  attributes=, created_after=, created_before=, limit=, cursor=)`.
- Чтение задач батча `handle.items(states=, labels=)`.

**Интеграция с доменными таблицами**

- Транзакционные хуки `@th.on_finalized(kind)`, `@th.on_progress(kind, every=)`,
  `@th.on_policy_breach(kind)`: выполняются в одной транзакции с событием батча; упавший
  `on_finalized` откатывает финализацию и повторяется с backoff.
- Retention завершённых деревьев (`retention`, `release_required`) и `handle.release()`.
- Рецепт «строка на каждого получателя»: перенос всех исходов задач в таблицы приложения до
  удаления по retention.

**Надёжность**

- Транзакционная постановка задач через outbox; повторная доставка брокером не выполняет задачу
  дважды.
- Аренда задач с продлением; возврат задач умерших воркеров; фоновые проверки пропущенных
  финализаций, дедлайнов и дрейфа счётчиков.
- Процесс maintenance с выбором лидера: `th.maintenance()`, `th.run_maintenance_once()`.

**Инструменты**

- Миграции: `th.migrate()`, встраивание в Alembic (`tallyho.storage.alembic.upgrade`), схема и
  префикс таблиц.
- CLI `tallyho migrate | maintenance | inspect`.
- Адаптер брокера flexiq (`tallyho[flexiq]`): `FlexiqAdapter`, `@fq.task(…)`, передача опций
  постановки, учёт ретраев и DLQ.
- `tallyho.testing`: `InlineBroker`, `FakeClock`, pytest-фикстура `tallyho_env`.
- Наблюдаемость: протокол `Observer`, `NullObserver`, `OpenTelemetryObserver` (`tallyho[otel]`).
- Совместимость с pgbouncer в режиме transaction pooling (asyncpg и psycopg 3).
- Руководство пользователя `docs/guide/` с примерами, которые выполняются в CI.
- Структура проекта, линтеры, тесты, CI и публикация на PyPI.

### Known limitations

Подробно — [docs/guide/limitations.md](docs/guide/limitations.md).

- Только PostgreSQL ≥ 14 и SQLAlchemy ≥ 2.1 в async-режиме; таблицы библиотеки и доменные таблицы —
  в одной базе.
- Отслеживаемые задачи — только `async def`.
- flexiq: только `pool="thread"`; опции `depends_on`, `debounce*` и `@task(batch=…)` не
  поддерживаются.
- `max_items` — мягкий лимит: параллельные воркеры могут немного его превысить.
- Задачи отправляет в брокер процесс maintenance с установленным адаптером; команда
  `tallyho maintenance` сообщения не отправляет.
- Нет хука на исход отдельной задачи, хука старта батча, изменяемых атрибутов, обратных миграций.
