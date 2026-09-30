# tallyho v1 — прогресс

> Протокол обновления — [PLAN.md §0](PLAN.md#0-протокол-итерации-loop). Файл правится в каждой итерации и коммитится вместе с кодом задачи.
> Статусы: `todo` · `in_progress` · `done` · `blocked` (причина в журнале) · `human` (нужен человек, цикл пропускает).

## Текущее состояние

* **Ветка:** `impl/v1`
* **Текущая волна:** 5 — T4.1, Fix-1
* **Последний зелёный коммит:** 2ee4bab

## Задачи

| ID | Задача | Зависит | Статус | Коммиты |
|---|---|---|---|---|
| T0.1 | Репозиторий собирается, гейты зелёные на скелете | — | done | d287135, 97cc124, 6ea6779, 4a713d4 |
| T0.2 | Инфраструктура тестов (схема на тест, xdist) | T0.1 | done | 25fcbe6..81c68c3 (5), merge 69a40fa |
| T1.1 | Перечисления состояний и иерархия ошибок | T0.1 | done | 7c5a33d..345efab (3), merge 0de5f86 |
| T1.2 | Value-объекты, FailurePolicy, TaskCall | T1.1 | done | a76f6ac..b7abd3d (3), merge 13c1b02 |
| T1.3 | Математика прогресса | T1.2 | done | f208279..f414ece (3), merge 23f7699 |
| T1.4 | Протоколы и базовые реализации (Clock, UUIDv7, Serializer, Observer) | T1.1 | done | c4943ae..e7334f8 (5), merge 721a192, cbd8059, 40241bb |
| T2.1 | Таблицы и индексы | T1.1 | done | 91aa770..42f6ef3 (5), merge 1f7278a |
| T2.2 | Миграции, установка в схему, alembic | T2.1, T0.2 | done | be7c687..09b272b (3), merge 416dc02 |
| T2.3 | Транзакции: сессия пользователя, ретраи, after_commit, HookSession | T2.1, T1.4 | done | 171307e..f665530 (4), merge 2adeed6 |
| T2.4 | Запросы счётчиков, дельты, свёртка, reconcile | T2.2, T2.3 | done | b65787f..a51c75d (5), merge 2ee4bab |
| T3.1 | Реестр tx-хуков | T1.2 | done | 27b762a..880903c (2), merge 4521aa4 |
| Fix-1 | d_* колонки в th_counter_delta для пути B | T2.4 | in_progress | |
| T4.1 | Продюсер: батчи, под-батчи, th_feed, add, seal, expect | T2.4, T3.1 | in_progress | |
| T4.2 | Relay | T4.1 | todo | |
| T4.3a | Completer: буфер, claim/heartbeat/release | T4.2 | todo | |
| T4.3b | Completer: finish без spawn | T4.3a | todo | |
| T4.3c | Spawn, into=, лимиты, дедуп, sub_batch из задачи | T4.3b | todo | |
| T4.4 | Finalizer | T4.3c | todo | |
| T4.5 | Путь B: complete_in и свёртка | T4.4, Fix-1 | todo | |
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
| T8.0 | Спайк flexiq | T0.1 | done | 4d318d0..f4155a6 (3), merge 9395ea6, b41b30d, 2a781c2 |
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

### 2026-09-30 · волна 5 запущена · T4.1, Fix-1

### 2026-09-30 · волна 4 влита · T2.4
- **T2.4 · done.** `storage/counters.py`: `CounterDelta`/`CounterTotals`, `read_counters` одним SELECT по многим батчам, `upsert_slots`, `insert_delta`, `fold_deltas`, `reconcile`, `upsert_metrics`. Фикстура `tables` и хелперы `schema_transaction`/`schema_connection`. D-027, D-028. 598 тестов, покрытие 99,77%.
- **Найден пробел:** в `th_counter_delta` нет d_* для `w_total/dispatched/duplicates/skipped_by_limit/tree_total`, поэтому путь B со spawn записать их не может. Заведена задача **Fix-1** (колонки в схему v1, D-029), T4.5 теперь зависит от неё.
- **Для следующих задач:**
  - Шаг Completer: `folded = fold_deltas(...)` → слить с буфером по `(batch_id, slot процесса)` → один `upsert_slots` → `upsert_metrics`.
  - Многострочную вставку с `func.now()` делать через `insert(t).values([...])`, не через executemany.
  - `Result.tuples()` в SQLAlchemy 2.1 устарел и даёт warning = ошибку.
  - Для basedpyright нужен типизированный список колонок (`list[ColumnElement[...]]`) и явные колонки в `returning`.

### 2026-09-30 · волна 4 запущена · T2.4
- Доступна только T2.4, на ней держится вся Ф4. Волна из одного сабагента.

### 2026-09-30 · волна 3 влита · T1.3, T2.2, T2.3, T3.1
- **T1.3 · done.** `model/progress.py`: `NodeCounters`, `ProgressSettings`, `compute_progress`, `ema_rate`, `estimate_eta`. Таблица §13.3 t1–t5 воспроизведена. D-024.
- **T2.2 · done.** `storage/migrations.py`: `migrate()` под advisory lock, `validate_prefix` и `validate_schema`; `storage/alembic.py`: `upgrade(op, version=...)`. Каталог после `migrate` совпадает с `create_all`. D-025, ARCHITECTURE §11.1 (8d43fd4).
- **T2.3 · done.** `storage/now.py`: `sql_now`; `storage/tx.py`: `resolve_connection`, `own_transaction`, `run_transaction` с повтором, `after_commit`, `HookSession`/`hook_session`. Хелпер `tests/helpers/probe.py`. D-021…D-023.
- **T3.1 · done.** `hooks/registry.py`: `HookRegistry`, `import_hook_modules`, `HookName`, `ensure`. D-026.
- **Вливание:** без конфликтов, гейты после каждого вливания зелёные, 562 теста, покрытие 99,74%.
- **Для следующих задач:**
  - Виртуальный Item под-батча — `weight=0` (D-024, карточка T4.1 обновлена).
  - Finalizer: `async with own_transaction(engine, TxSettings(statement_timeout=hook_timeout)) as conn, hook_session(conn) as s: await hook(s, summary)`, затем CAS в той же транзакции.
  - `run_transaction(work)` повторяет `work` целиком, поэтому в нём нельзя делать побочные эффекты вне БД.
  - Колбэк `after_commit` синхронный, без аргументов и быстрый. Своим транзакциям он не нужен.
  - Snapshotter и reads строят `NodeCounters` → `compute_progress(..., settings, rates)`.
  - Finalizer и Snapshotter перед хуком вызывают `registry.ensure(kind, batch.hooks)` → `HookMissingError` (лог + `th_hook_missing`, батч не финализировать).
  - В сообщениях коммитов и коде не использовать `×` (RUF003). Сообщения коммитов передавать через `-m`: scratchpad общий у всех агентов.
  - Изменение `tables.py` = новая версия миграции (D-025).

### 2026-09-30 · волна 3 запущена · T1.3, T2.2, T2.3, T3.1

### 2026-09-30 · волна 2 влита · T1.2, T1.4, T2.1
- **T1.2 · done.** `model/views.py` (Progress, BatchSummary, BatchView, ItemView, InFlightItem), `model/policy.py` (FailurePolicy, PolicyVerdict, PolicyBreach, JSON для options), `model/calls.py` (TaskCall). D-018.
- **T1.4 · done.** `protocols/`: Clock/SystemClock, IdFactory/UuidV7Factory, Serializer/JsonSerializer/PayloadCodec/SerializerCodec, Message/Verdict/Dispatcher/Runtime/DeadLetters, Observer/NullObserver. D-002 закрыт, D-016, D-017. ARCHITECTURE §3.4/§4.2 обновлены (40241bb); `typing-extensions` убран из DEP002 (cbd8059).
- **T2.1 · done.** `storage/tables.py`: `build_metadata(prefix)`, 11 таблиц на `TypedColumns`, все индексы §5.2 + `th_expiry(expires_at)`, storage-параметры, golden-DDL. Интеграционный `create_all`. SQLAlchemy ≥ 2.1 (D-019), литералы в предикатах (D-020).
- **Вливание:** без конфликтов, гейты после каждого вливания зелёные, 318 тестов, покрытие 99,5%.
- **Для следующих задач:**
  - «Сейчас» в SQL — только через хелпер `sql_now(clock)` (сделать в T2.3).
  - В запросах горячего пути условие по `state` писать литералом (D-020).
  - `create_all` в схеме теста: `conn.execution_options(schema_translate_map={None: schema})`, затем `run_sync(metadata.create_all, checkfirst=False)`.
  - Новые колонки заводить через хелперы `_uuid/_text/_utc/...` в `tables.py`.
  - DDL в юнит-тестах: `create_mock_engine("postgresql+asyncpg://", executor=print).dialect`.
  - Реализации протоколов наследуют протокол явно и помечают методы `@override`. Исключения Observer engine глотает с логом.
  - `Message(id, batch_id, kind, task_name, payload, options)`; для CALLBACK `id` — это `callback_id`. Исключение в `dispatch` → relay повторяет всю пачку.
  - Для basedpyright результат `json.loads` оборачивать в `cast("object", ...)`.
  - `BatchSummary`, `BatchView`, `TaskCall` нехешируемы.
  - `fail_fast` / `PolicyAction.FAIL`: `verdict.reason` → `cancel_reason`.

### 2026-09-30 · волна 2 запущена · T1.2, T1.4, T2.1

### 2026-09-30 · волна 1 влита · T0.2, T1.1, T8.0
- **T0.2 · done.** Фикстуры `schema`/`connection`/`session`; `tests/helpers/db.py`: `deadlock_count`, `held_locks`, `temporary_schema`. `pytest -n 4` зелёный. Решения D-009 (импорт `tests.*`), D-010 (контейнер на xdist-воркер).
- **T1.1 · done.** `model/states.py` (коды по D-005, `TERMINAL_THRESHOLD=10`), ошибки движка, реэкспорт из `tallyho.model`. Решение D-011.
- **T8.0 · done.** Спайк на живом воркере: `docs/plan/FLEXIQ_SPIKE.md`, скрипты `tests/contract/flexiq/spike_*.py`. D-006 закрыто (payload — кодек задачи flexiq), D-012…D-015; ARCHITECTURE §11.3/§16 и ACCEPTANCE A-FQ-09/13/14 поправлены (2a781c2).
- **Починка после вливания (b41b30d).** После D-009 спайки больше не находили `spike_support`. Теперь они импортируют `tests.contract.flexiq.spike_support` и запускаются через `python -m`. Оба спайка перепроверены живым прогоном.
- **Для следующих задач:**
  - SQL-проверка «активен» — `state < 10`.
  - Длинные строки собирать через переменные: basedpyright запрещает неявную конкатенацию (`reportImplicitStringConcatenation`). Если позиционных аргументов больше трёх, остальные — keyword-only (PLR0917).
  - Имя схемы в SQL — в двойных кавычках или через `table(..., schema=)`; f-строки с DML ruff ловит как S608.
  - `deadlock_count` сравнивать только по разнице и опрашивать: статистика приходит с задержкой около 1 с.
  - В `th.tracked` нужен `functools.update_wrapper`, а не `@wraps`: mypy `disallow_any_decorated`.
  - `retry_verdict` → FINAL, если `retry_count >= max_retries`, или `dont_retry_on`, или исключение не подходит под непустой `retry_on`.
  - `max_payload_bytes` (1 MiB) проверять у продюсера.
  - Жёсткий `timeout` flexiq не отменяет корутину. Claim при живом lease обязателен.
  - Имя задачи flexiq для `__main__` берётся из `__spec__.name`.

### 2026-09-30 · волна 1 запущена · T0.2, T1.1, T8.0
- Параллельный режим (PLAN §0.7): три сабагента в отдельных worktree.

### 2026-09-30 · T0.1 · done · d287135, 97cc124, 6ea6779, 4a713d4
- Сделано: каркас закоммичен в `impl/v1` (его подготовила другая сессия, talyho-27; ветка `main` указывает на d287135). Добавлены план, импорт `testcontainers.community.postgres` (старый модуль давал DeprecationWarning, а warnings = ошибки) и `poe check-all`.
- Отклонения: `poe test` уже был с `--cov-fail-under=0` — D-003 выполнено без правок; README/LICENSE/CHANGELOG/uv.lock уже существовали.
- Узнали: flexiq 2.0.0 ставится на Windows; Docker и testcontainers работают. Подавления — только `# ruff: ignore[rule-name]  # причина` (AGENTS.md). DEP002 в deptry — временный список, чистить по мере импортов. Запись файлов из Python — с `newline="\n"`.

### 2026-09-30 · план · —
- Создан план v1.0 по ARCHITECTURE v2.1 и ACCEPTANCE 1.0-draft.
