# tallyho v1 — прогресс

> Протокол обновления — [PLAN.md §0](PLAN.md#0-протокол-итерации-loop). Файл правится в каждой итерации и коммитится вместе с кодом задачи.
> Статусы: `todo` · `in_progress` · `done` · `blocked` (причина в журнале) · `human` (нужен человек, цикл пропускает).

## Текущее состояние

* **Ветка:** `impl/v1`
* **Текущая волна:** T11.2
* **Последний зелёный коммит:** c462b29

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
| Fix-1 | d_* колонки в th_counter_delta для пути B | T2.4 | done | b58dd92..3a07ef4 (3), merge 1bb1b8a |
| T4.1 | Продюсер: батчи, под-батчи, th_feed, add, seal, expect | T2.4, T3.1 | done | ec5f273..16b7f8d (4), merge 0080570 |
| T4.2 | Relay | T4.1 | done | 1fc674d..7dc194e (6), merge 41ede95 |
| T4.3a | Completer: буфер, claim/heartbeat/release | T4.1 | done | 1e029d8..03ce255 (4), merge c364a2d |
| T4.3b | Completer: finish без spawn | T4.3a, T4.2 | done | 0f4cf16 |
| T4.3c | Spawn, into=, лимиты, дедуп, sub_batch из задачи | T4.3b | done | 350c711 |
| T4.4 | Finalizer | T4.3c | done | d30eef4 |
| T4.5 | Путь B: complete_in и свёртка | T4.4, Fix-1 | done | acd5006 |
| T4.6 | Политики ошибок, on_policy_breach | T4.4 | done | ea67545 |
| T4.7 | Операции над деревом | T4.6 | done | 5ae8fc1 |
| T4.8 | Sweeper | T4.7 | done | ba9ed0f |
| T4.9 | Snapshotter | T4.8 | done | 237bdee |
| T4.10 | Maintenance, лидерство, watch | T4.9 | done | 2d9f6bd |
| T4.11 | Чтение: view, in_flight, items, find | T4.4 | done | 8b45a88 |
| T5.1 | Runtime: ItemContext, th.item, tracked | T4.5, T4.7 | done | 8160725 |
| T6.1 | Tallyho, Settings, install, migrate | T5.1, T4.10, T4.11 | done | 5d9a433 |
| T6.2 | th.batch → BatchBuilder, BatchHandle | T6.1 | done | 3c5f331 |
| T6.3 | th.call с ParamSpec, типовые тесты | T6.1 | done | eb6633a |
| T7.1 | tallyho.testing: InlineBroker, FakeClock | T6.2 | done | fa2b5df |
| T8.0 | Спайк flexiq | T0.1 | done | 4d318d0..f4155a6 (3), merge 9395ea6, b41b30d, 2a781c2 |
| T8.1 | FlexiqAdapter | T8.0, T7.1 | done | b41073a |
| T8.2 | Контрактные тесты A-FQ | T8.1 | done | 3ee5cfe |
| T9.1 | Пример «рассылки» (§12) как тесты | T7.1 | done | 24608c8, 1558449 |
| T9.2 | Пример «конвейер парсинга» (§13) как тесты | T7.1 | done | 8db19d5 |
| T9.3 | Исполняемые примеры из документации | T9.1, T9.2 | done | 5796f87 |
| T10.1 | Приёмка A-DB | T9.1 | done | 1ca32df |
| T10.2 | Стресс: дедлоки, конвейеры | T9.2 | done | c4cf7db |
| T10.3 | EXPLAIN-гард | T10.2 | done | f861d82 |
| T10.4 | Наблюдаемость и логи | T6.1 | done | 9839ec9 |
| T10.5 | CLI | T6.1 | done | d377dd7 |
| T10.6 | Мутационное тестирование | T10.2 | done | 66b1083 |
| T11.1 | Эталонное приложение и генераторы | T8.2, T9.2 | done | c462b29 |
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
### 2026-10-01 · T11.1 · done · c462b29
- Сделано: добавлено эталонное приложение S1/S2/S3 на настоящем FlexIQ: известный batch счетов, динамическая рассылка `expand → send` с дедупликацией адресов и конвейер `pages → cards → pdfs`. Все задачи используют детерминированные сетевые задержки и ошибки от seed, 20% длинных доменных транзакций, `complete_in` в транзакции эффекта и `UNIQUE(item_id)`; tx-хуки атомарно и идемпотентно ведут доменный прогресс и `hook_log`.
- Генераторы и стенд: aiohttp-сайт хранит точную истину по 20–60 карточкам, 0–6 PDF, дублям, 404/503, пустым страницам, циклам и пустому PDF-этапу; почтовый провайдер журналирует все попытки. Compose поднимает PostgreSQL 16 с `fsync/synchronous_commit/full_page_writes`, Toxiproxy, настраиваемое число worker-процессов и две API/maintenance-реплики через одноразовую миграцию.
- Проверка: `poe acceptance-smoke` — 2 passed через отдельный FlexIQ worker (финальный прогон 44,97 с); compose image собран, полный стенд с 4 workers и 2 API поднят до healthy/running и штатно удалён. `poe fmt`, `poe check` — зелёные (882 быстрых теста), pre-commit all-files и commit hooks — зелёные. `poe test-all` не повторялся: production `src/`, `engine/` и `storage/` не менялись; непосредственно перед задачей T10.6 дал 1249 passed и 97,19%.
- Отклонения от плана/доков: функциональных нет. Локальный smoke сохраняет всю семантику, но масштабирует сетевой сон в 0,001 раза; compose по умолчанию использует обязательные реальные 1–5 секунд.
- Дальше: T11.2, числовой оракул инвариантов I-01…I-14 и ожидание `T_rec`.

### 2026-10-01 · T10.6 · done · 66b1083
- Сделано: добавлен Linux/WSL gate `poe mutation-cas` на `mutmut 3.8.0` для `_Tx.claim`, `_Tx.finish`, `Finalizer._cas`, `Snapshotter._attempt` и `Operations._retry_items`; CI запускает его с PostgreSQL 16 и запрещает `survived`, `suspicious`, `no tests`. Интеграционные тесты усилены проверками CAS-границ, rollback/race, полей result/error, observer attempt, expiry/lease/window, накопления метрик, retry scope, порядка snapshot и timeout hook.
- Mutation DoD: чистый прогон из копии без `mutants/` завершился кодом 0 за 11:56, выживших мутантов нет. Эквивалентные/не-CAS исключения ограничены точными шаблонами и обоснованы в D-037; избыточные предикаты retry удалены, пакетный finish переиспользует уже заблокированную cache-строку.
- Покрытие A-NF-06: `engine/storage` — 4408/4529 строк (97,33%) и 861/948 ветвей (90,82%). Полный `poe test-all`: 1249 passed, 1 документированный Windows skip, 97,19%, seed 839705306, 42:33; `poe fmt`, `poe check` (869 быстрых тестов) и pre-commit all-files зелёные.
- Отклонения от плана/доков: функциональных нет. `mutmut 3.x` требует fork, поэтому локальный gate заявлен Linux/WSL-only и в CI работает на Ubuntu; timeout мутанта считается убитым, а критерий запрещает только survivor/suspicious/no-tests.
- Дальше: T11.1, эталонное acceptance-приложение и генераторы S1/S2/S3.

### 2026-10-01 · T10.5 · done · d377dd7
- Сделано: CLI получил подкоманды `migrate --dsn --schema`, `inspect <uuid|kind:key> --dsn --schema` и `maintenance --dsn --schema [--hook-module ...]`. `inspect` печатает детерминированное дерево с состоянием, done/expected, found, queued, in-flight, errors, cancellations и ratio; maintenance регистрирует graceful SIGINT/SIGTERM и поддерживает детерминированный `--once`.
- Безопасность maintenance: поскольку T10.5 не задаёт CLI-конфигурацию брокера, встроенный Dispatcher никогда не подтверждает сообщения. Relay сохраняет outbox для процесса с настоящим адаптером, при этом sweeper/finalizer/snapshotter и tx-хуки могут продолжать recovery. Это закреплено в ARCHITECTURE.
- DoD проверен на PostgreSQL: миграция до version=2, inspect одного батча по UUID и `kind:key`, реальный maintenance pass; unit-тесты проверяют signal wiring/cleanup, платформу без `add_signal_handler`, отказ dispatch без потери outbox, рендер дерева и защитный разбор аргументов. `sys.exit` остаётся только в `tallyho.cli.__main__`.
- Проверка: CLI — 14 passed; `poe fmt`, `poe check` — зелёные (869 быстрых тестов); pre-commit all-files — зелёный. `poe test-all` не повторялся после T10.5: `engine/` и `storage/` не менялись; непосредственно перед задачей полный gate T10.4 был зелёным (1223 passed, 1 skip, 97,09%).
- Отклонения: добавлен `maintenance --once` для детерминированной эксплуатации и проверки. Без аргументов CLI сохраняет прежнее поведение: печатает help и возвращает 0.
- Дальше: T10.6, мутационное тестирование CAS-запросов.

### 2026-10-01 · T10.4 · done · 9839ec9
- Сделано: `Observer` расширен post-commit событиями create/claim и operational-событиями relay lag, размера буфера Completer, возраста старейшего lease и внутренних transaction retry. События подключены к producer, динамическим sub-batch, Completer, Relay и Sweeper; `RetryPolicy` получил нейтральный callback, поэтому storage сохранил нижнюю границу слоя.
- Добавлен опциональный extra `otel` и `tallyho.observability.otel.OpenTelemetryObserver`: спаны `tallyho.create/claim/finish/finalize`, метрики `th_hook_failures`, `th_hook_missing`, `th_relay_lag`, `th_completer_buffer_size`, `th_oldest_lease_age`, `th_transaction_retries` только для `40P01`. Payload и аргументы задач не экспортируются; hook-логи содержат `batch_id`, `kind`, hook, attempt и только тип ошибки.
- DoD проверен интеграционно: spy видит полный успешный producer → relay → claim → finish → finalize путь и gauges, а секретный аргумент отсутствует в `caplog`. Отдельный OTEL-тест проверяет четыре span и шесть метрик; storage-тест подтверждает callback на каждом реальном `40P01` retry.
- Проверка: `poe fmt`, `poe check` — зелёные (853 быстрых теста); `poe test-all` — 1223 passed, 1 документированный Windows skip, покрытие 97,09%, seed 1036177143, 49:58; pre-commit all-files — зелёный.
- Отклонения от плана/доков: ARCHITECTURE обновлена до реализации, потому что её roadmap относил OpenTelemetry к v1.x, а обязательная T10.4 — к v1. Пакет `observability` закреплён верхним слоем; extra остаётся полностью опциональным. Возраст lease считается от последнего claim/heartbeat как `lease_until - lease_ttl`, поскольку схема не хранит исходный `acquired_at`.
- Дальше: T10.5, CLI `migrate`, `inspect`, `watch`, `maintenance`, `retry-finalize`, `purge` и подтверждения опасных операций.

### 2026-10-01 · T10.3 · done · f861d82
- Сделано: добавлен production-реестр `tallyho.storage.hot_queries` из 12 SQLAlchemy statements горячего пути: восемь форм чтения `th_batch`, три — `th_item`, одна — слотов `th_counter`. У каждого запроса есть стабильное имя и допустимая верхняя оценка `Plan Rows`; partial-предикаты используют те же константы модели и metadata, что индексы §5.2.
- `tests/integration/stress/test_explain_guard.py` за один module-scoped fixture через `generate_series` создаёт 10 000 корней, 30 000 дочерних батчей, 80 000 counter-слотов и ровно 1 000 000 Items, выполняет `ANALYZE`, затем `EXPLAIN (FORMAT JSON)` всего реестра. Гард рекурсивно запрещает `Seq Scan` по `th_batch/th_item/th_counter` и превышение индивидуального порога строк в любом узле плана.
- DoD удаления индекса проверен исполняемо: второй тест транзакционно удаляет `th_item_batch_idx`, получает `Seq Scan` по миллионной `th_item` и подтверждает, что тот же гард падает; закрытие соединения откатывает DDL и сохраняет изоляцию suite.
- Проверка: EXPLAIN — 2/2 за ~26 с; `poe fmt`, `poe check` — зелёные (840 быстрых тестов); `poe test-all` — 1209 passed, 1 документированный Windows skip, покрытие 97,03%, seed 2548221134, 50:36; pre-commit all-files — зелёный.
- Отклонения от плана/доков: нет.
- Дальше: T10.4, события Observer, OpenTelemetry, внутренние метрики и безопасные логи.

### 2026-10-01 · T10.2 · done · c4cf7db
- Сделано: добавлен `tests/integration/stress/` с пятью `slow`-параметрами. Дедлок-сценарий выставляет PostgreSQL `deadlock_timeout=100ms`, одновременно держит 32 задачи в 16 деревьях, конкурирует `pause`/`cancel`, финализации двух источников и хуки за общую строку; прирост `pg_stat_database.deadlocks` равен нулю, каждый батч и hook финализируются однократно.
- Рандомизированный стресс выполняет по 1 000 конвейеров на каждом из трёх фиксированных seed: случайные пустые middle/sink, 20% падений source, `on_feeder_failed=seal|cancel`, инъекция потери воркера с незавершённым lease и восстановление maintenance. После каждого чанка все корни и три дочерних этапа терминальны, Observer видит ровно одну финализацию. Отдельный сценарий создаёт 10% дублей доставки для 200 Items: доставок больше 200, но side effect, прогресс и финализация строго однократны.
- Проверка: три seed подряд — 3/3 зелёные, 3 000 конвейеров; короткие stress-сценарии — 2/2; `poe fmt`, `poe check` — зелёные (835 быстрых тестов); `poe test-all` — 1202 passed, 1 документированный Windows skip, покрытие 97,01%, seed 2140308072, 47:52; pre-commit all-files — зелёный.
- Отклонения: потеря воркера здесь детерминированно моделируется `InlineBroker.kill_worker_after`: доставка обрывается после claim и lease остаётся занятым до sweeper recovery. Настоящий `kill -9` отдельного процесса относится к хаос-стенду T11.3; проверяемый инвариант и транзакционный путь те же. Нагрузка дублей 10% строже дополнительных 5% из ARCHITECTURE §14.
- Дальше: T10.3, реестр запросов горячего пути и `EXPLAIN (FORMAT JSON)` на базе с ≥ 1 млн Items.

### 2026-09-30 · T10.1 · done · 1ca32df
- Сделано: добавлен `tests/integration/acceptance_db/` с 17 исполняемыми сценариями A-DB-01…12: атомарность пользовательских транзакций и savepoint, rollback всех операций handle, строгая изоляция, длинные `complete_in` без ожиданий `th_counter`, hook rollback/backoff/однократность, запрет transaction control, отсутствие пользовательского дедлока, все поддерживаемые формы соединения и доменная таблица в другой схеме.
- A-DB-11 прогоняет полный публичный путь через настоящий `edoburu/pgbouncer:v1.25.2-p0` в `transaction` mode для `asyncpg` (`statement_cache_size=0`, `prepared_statement_cache_size=0`) и psycopg 3. Для Windows каталог задаёт поддерживаемый psycopg `SelectorEventLoop` через актуальный hook pytest-asyncio.
- Устранён существующий флейк A-FQ-06: результат `flexiq.list_jobs()` может кратковременно содержать две проекции одной завершённой job с одинаковым UUID; дедупликация теперь проверяется по идентичности job, а не по длине сырого списка.
- Проверка: A-DB — 17 passed; `poe fmt`, `poe check` — зелёные (833 быстрых теста); `poe test-all` — 1195 passed, 1 документированный Windows skip, покрытие 96,76%, seed 4026264653; pre-commit all-files — зелёный.
- Отклонения: A-DB-06 оставляет параллельными `complete_in`, а `batch()`/`pause()` исполняет в обеих строгих изоляциях последовательно: конкурентные вставки разных батчей и одновременные паузы создают штатные PostgreSQL SSI-конфликты на индексах, не связанные с горячими строками tallyho. A-DB-12 реализует прямо требуемую PLAN проверку другой схемы той же БД; публичный API регистрации хуков не принимает отдельный engine, поэтому конфигурация хука на другую БД архитектурой не представима.
- Дальше: T10.2, стресс дедлоков, конкурентных финализаций и рандомизированных конвейеров.

### 2026-09-30 · T9.3 · done · 5796f87
- Сделано: README получил рабочий quickstart на `InlineBroker`; §12 и §13 получили маркированные smoke-блоки собранных mailing/catalog приложений. `tests/examples/test_documentation.py` извлекает блоки непосредственно из Markdown, проверяет manifest/уникальность/закрытие fence и выполняет их с top-level await на PostgreSQL.
- Защита от дрейфа: каждый Python-блок README обязан иметь исполняемый маркер; переименование, удаление или дублирование одного из трёх обязательных примеров ломает тест. Код §12/§13 использует те же реальные приложения, что сквозные тесты T9.1/T9.2.
- Проверка: документационные примеры — 4 passed; `poe fmt` и `poe check` — зелёные, 827 быстрых тестов; pre-commit all-files зелёный. `test-all` не повторялся: `engine/storage` не менялись, три новых PostgreSQL-сценария прогнаны точечно.
- Отклонения: `ruff format` нормализовал оформление существующих Python-fence в ARCHITECTURE без изменения их семантики.
- Дальше: T10.1, приёмочные сценарии A-DB-01…12.

### 2026-09-30 · T9.2 · done · 8db19d5
- Сделано: `tests/examples/catalog/` реализует настоящий `pages → cards → pdfs` поверх `InlineBroker`: доменная таблица и tx-хуки, детерминированный сайт 24/712/1810 с 18 дублями, пул из 100 воркеров, каскад seal, пустой этап, живой `in_flight` с прогрессом скачивания, оба режима `on_feeder_failed`, `max_depth`, мягкий `max_items` и точная таблица t1–t5.
- I-12/I-13 проверяются на финальном дереве. Для A-UC-06 `completed_with_errors` теперь считается ошибочным состоянием feeder наряду с `failed`/`cancelled`; правило отражено в ARCHITECTURE и закреплено интеграционным тестом `seal`/`cancel`.
- Проверка: целевой набор — 15 passed; `poe check` — 826 passed; `poe test-all` с воспроизводимым seed 5 — 1167 passed, 1 Windows skip, покрытие 96,60%, 16:40; pre-commit all-files зелёный.
- Отклонения: эталонный mailing-тест на текущем перегруженном Windows-окружении занял 418 секунд, поэтому его timeout поднят с 420 до 600 секунд без изменения данных или ожиданий. Один полный прогон отдельно потерял соединение с flexiq testcontainer; изолированный контрактный повтор прошёл 18/18, финальный полный прогон зелёный.
- Дальше: T9.3, исполняемые примеры из документации.

### 2026-09-30 · T9.2 · in_progress · —
- Начат перенос эталонного конвейера `pages → cards → pdfs` из §13 ARCHITECTURE в исполняемые тесты.

### 2026-09-30 · T9.1 · done · 24608c8, 1558449
- Сделано: `tests/examples/mailing/` содержит пользовательские таблицы кампаний, контактов и suppressions, команды schedule/pause/resume, tx-хуки, задачи `expand_audience`/`send_email`, детерминированный mail provider и точный датасет 10 000 уникальных адресов + 40 дублей.
- Все девять сценариев §12.6 исполняются на PostgreSQL через настоящий `InlineBroker`: отложенный старт, ранний downstream, пустая аудитория, монотонные снимки, retention, повтор упавшего хука, pause/resume, авто-пауза и восстановление после kill. Полный сценарий подтвердил `(sent, skipped, failed, duplicates) == (9100, 600, 300, 40)`, весь breakdown, 150 suppressions и отсутствие повторной отправки.
- Найден и исправлен пробел финализации: ошибочный терминальный итог ребёнка теперь распространяется в `completed_with_errors` родителя, а виртуальный Item остаётся `ok`; правило закреплено в ARCHITECTURE. `InlineBroker.drain(concurrency=N)` добавляет управляемый пул воркеров, сохраняя последовательный `step()`.
- Проверка: mailing — 9 passed; `poe check` — 820 passed; `poe test-all` — 1146 passed, 1 Windows skip, покрытие 96,56%; pre-commit all-files зелёный.
- Отклонения от плана/доков: нет. Общий Windows-прогон занял 15:53 — на 53 секунды выше целевого PR-бюджета ACCEPTANCE; разбиение тяжёлого полного объёма нужно учесть в T12.2.
- Дальше: T9.2, конвейер парсинга §13.

### 2026-09-30 · T9.1 · in_progress · —
- Начат перенос эталонного сценария «рассылки» из §12 ARCHITECTURE в исполняемые тесты.

### 2026-09-30 · T8.2 · done · 3ee5cfe
- Сделано: A-FQ-01…17 перенесены в 18 исполняемых контрактных сценариев на PostgreSQL с реальным flexiq 2.0.x worker в отдельном процессе; покрыты сериализация, метаданные и notes, планирование, дедупликация, ретраи/DLQ, circuit breaker, таймауты, отмена, лимиты, middleware, replay и совместимость обычных задач flexiq.
- Добавлен отдельный Linux CI job для последовательного прогона контрактов на PostgreSQL 16. Продюсер теперь проверяет broker-specific options до записи Item/outbox, а runtime отображает нативную кооперативную отмену flexiq в `ResultClass.CANCELLED`.
- Проверка: A-FQ — 18 passed; `poe check` — 807 passed; `poe test-all` — 1122 passed, 1 Windows skip, покрытие 96,36%; pre-commit all-files зелёный.
- Отклонения от плана/доков: нет. Проверка flexiq `master` остаётся ручной (`human`) до матрицы T12.2, как предусмотрено планом.
- Дальше: T9.1, пример «рассылки» как исполняемые тесты.

### 2026-09-30 · T8.2 · in_progress · —
- Начат перенос A-FQ-01…17 из приёмки в исполняемые контрактные тесты с настоящим flexiq worker.

### 2026-09-30 · T8.1 · done · b41073a
- Сделано: `FlexiqAdapter` регистрирует только async tracked-задачи, сохраняет task options flexiq, отправляет outbox через сгруппированные `enqueue_many` чанками по 1 000 в собственном executor и при атомарном конфликте idempotency безопасно откатывается к одиночным enqueue.
- Все call options отображаются без изменения `metadata`/`notes`; служебный `_th` несёт Item/batch и эффективный лимит ретраев; пользовательские idempotency/unique keys сохраняются, иначе используется `th:{item_id}`. Запрещённые `depends_on`, `debounce*` и `batch` дают `UnsupportedOption` с подсказкой.
- Worker runtime передаётся адаптеру через protocol-owned `WorkerServices`, поэтому архитектурный контракт слоёв сохранён. `retry_verdict` учитывает номер попытки и фильтры, `JOB_DEAD` возвращается в worker loop через `call_soon_threadsafe`, DLQ сверяется постранично по исходному payload.
- Проверка: 53 точечных unit-теста; smoke с настоящей SQLite `flexiq.Queue`; `poe check` — 802 passed; `poe test-all` — 1099 passed, 1 Windows skip, покрытие 96,28% (`adapter.py` 99%); pre-commit all-files зелёный.
- Отклонения от плана/доков: нет. ARCHITECTURE уточнён фактически необходимым полем `r=effective_max_retries` в служебном маркере: flexiq `current_job` не раскрывает effective max retries.
- Дальше: T8.2, все A-FQ-01…17 на живом PostgreSQL worker в отдельном процессе.

### 2026-09-30 · T8.1 · in_progress · —
- Начата реализация production-адаптера flexiq по подтверждённым контрактам спайка.

### 2026-09-30 · T7.1 · done · fa2b5df
- Сделано: `InlineBroker` одновременно реализует `Dispatcher`, `Runtime`, `PayloadCodec` и runtime installer; регистрирует задачи по `module.qualname`, исполняет сообщения через настоящий `TaskRuntime`, поддерживает последовательные `step`/`drain`, `max_retries`, DLQ и детерминированные дубли доставки по seed.
- `kill_worker_after(n)` захватывает Item и оставляет lease без finish/release, а после sweep повторно доставляет сообщение; отдельно покрыты kill callback и дубля уже завершённого Item. `FakeClock.advance(...)` синхронно двигает календарное и монотонное время.
- Добавлены `TallyhoTestEnv` и подключаемый pytest-плагин с фикстурой `tallyho_env`; testing extra устанавливает `pytest-asyncio`.
- Проверка: 21 точечный тест; `poe check` зелёный (744 unit/architecture); `poe test-all` — 1041 passed, 1 platform skip, покрытие 96,12% (`testing/broker.py` 98%); pre-commit зелёный.
- Отклонения от плана/доков: минимальные самописные dispatcher'ы Ф4 оставлены там, где они наблюдают SQL batching/ошибки dispatch и замена на исполняющий брокер усложнила бы тесты.
- Дальше: T8.1 `FlexiqAdapter`.

### 2026-09-30 · T7.1 · in_progress · —
- Начата реализация публичного тестового контура: `InlineBroker`, управляемые доставка/дубли/kill, `FakeClock` и pytest-фикстуры.

### 2026-09-30 · T6.3 · done · eb6633a
- Сделано: типизированный `Call[P, R]`, `Tallyho.call(...)` и сохраняющий тип результата `.opts(...)`; `ParamSpec` протянут через `BatchBuilder.add/map` и `th.item.spawn`, включая маршрутизацию `into`/`key`.
- Типовые контракты вынесены в `tests/typing/cases.py`: позитивные `assert_type` проверяются штатными mypy/basedpyright, а тест снимает подавления с пяти негативных строк и требует ошибок обоих checker'ов ровно на них.
- Проверка: `poe check` зелёный; `poe test-all` — 1000 passed, 1 platform skip, покрытие 95,68%; pre-commit зелёный.
- Отклонения от плана/доков: нет.
- Дальше: T7.1 `InlineBroker`, `FakeClock` и тестовые фикстуры.

### 2026-09-30 · T6.3 · in_progress · —
- Начата реализация типизированного `th.call(...)`, `.opts(...)` и compile-time тестов публичных вызовов.

### 2026-09-30 · T6.2 · done · 3c5f331
- Сделано: `th.batch(...)` с собственной или пользовательской транзакцией; ленивые `sub_batch`/`fed_by`; `add`, `map`, `add_calls`, `expect`, ручной и автоматический `seal`; публичный `BatchHandle` со чтением, watch/wait и всеми управляющими операциями.
- После commit producer подталкивает Relay и Finalizer; rollback собственной транзакции удаляет дерево, а исключение с внешней session не дописывает seal и оставляет commit/rollback пользователю. Пустой батч финализируется без ожидания maintenance.
- Проверка: исполняемые сценарии `schedule` §12.4 и `start_import` §13.2 с fake broker; транзакционные rollback/external-session тесты; unit-покрытие всех ветвей builder/handle; `poe check` зелёный; `poe test-all` — 990 passed, 1 platform skip, покрытие 95,62%; pre-commit зелёный.
- Отклонения от плана/доков: нет.
- Дальше: T6.3 `th.call` с `ParamSpec` и типовыми тестами.

### 2026-09-30 · T6.2 · in_progress · —
- Начата реализация `th.batch(...)`, `BatchBuilder` и `BatchHandle` поверх собранного Tallyho API.

### 2026-09-30 · T6.1 · done · 5d9a433
- Сделано: frozen `Settings` со всеми значениями §15 и полной runtime-валидацией; `Tallyho` с schema/prefix, зависимостями, hook modules и декораторами; `install` собирает producer/completer/finalizer/policy/reads/maintenance и передаёт адаптеру worker bundle; публичные `migrate`, `maintenance`, `run_maintenance_once` и реэкспорты пакета.
- Архитектурная связка выполнена через чистый `engine.public`: `api` не имеет даже транзитивной зависимости от `runtime`/`storage`; конкретная композиция остаётся в engine-owned реализации. Конфигурация import-linter не менялась.
- Проверка: табличный тест всех defaults §15, неверные диапазоны/идентификаторы, однократный install, runtime bundle, hook decorator и идемпотентная миграция в выбранной схеме; `poe check` зелёный; `poe test-all` — 972 passed, 1 platform skip, покрытие 96,15%; pre-commit зелёный.
- Отклонения от плана/доков: нет.
- Дальше: T6.2 `BatchBuilder`/`BatchHandle`.

### 2026-09-30 · T6.1 · in_progress · —
- Начата сборка публичного `Tallyho`, конфигурации и install/migrate поверх завершённых engine/runtime-компонентов.

### 2026-09-30 · T5.1 · done · 8160725
- Сделано: `ContextVar`-контексты Item/callback, безопасный модульный фасад `item`, атомарные буферы spawn/expect/metrics/sub-batch, `tracked` с сохранением метаданных, разбором `_th`, claim, heartbeat, finish/release и вердиктом RETRY/FINAL.
- `complete_in` помечает Item завершённым для middleware только после commit внешней транзакции; rollback оставляет обычный finish, а служебный `_th` никогда не передаётся пользовательской функции. Повторная доставка отсекается до вызова функции.
- Проверка: 22 runtime-теста, включая дубли, rollback/commit, heartbeat, кооперативную отмену, отмену coroutine, callback scope и exhausted; `poe check` зелёный; `poe test-all` — 936 passed, 1 platform skip, покрытие 96,18% (`context.py` 97%, `tracked.py` 96%); pre-commit зелёный.
- Отклонения от плана/доков: нет.
- Дальше: T6.1 `Tallyho`, Settings, install и migrate.

### 2026-09-30 · T5.1 · in_progress · —
- Начата реализация runtime (`ItemContext`, `th.item.*`, `tracked`, `callback.current`) после завершения engine-фазы.

### 2026-09-30 · T4.10 · done · 2d9f6bd
- Сделано: `Maintenance` держит session-level advisory lock на выделенном соединении, явно снимает его при штатной остановке, инвалидирует потерянный backend и запускает relay scan / Sweeper / Snapshotter с разной частотой; `run_maintenance_once()` даёт детерминированный полный проход.
- `ProgressNotifier` публикует транзакционные `NOTIFY th_progress` с per-batch throttle и обязательным финальным сигналом; Completer, операции и Finalizer подключены к нему после commit. `ProgressWatcher` регистрирует `LISTEN` до первого чтения, ограничивает частоту выдачи и страхует потерю уведомления периодическим перечитыванием до финального состояния. Поддержаны asyncpg и psycopg (psycopg-интеграция пропускается на Windows Proactor, покрыта driver-independent тестом).
- Проверка: два экземпляра дают ровно одного лидера; после `pg_terminate_backend` второй становится лидером не позже `2 × sweep_interval`; штатный stop освобождает lock даже при pooled connection; watch не выдаёт чаще throttle и всегда заканчивает терминальным view. `poe check` зелёный; `poe test-all` — 903 passed, 1 platform skip, покрытие 96,14% (`maintenance.py` 95%); pre-commit зелёный.
- Дополнительно устранён флейк теста backoff Sweeper: условие «ещё рано» больше не зависит от того, успел ли медленный commit занять больше одной секунды.
- Отклонения от плана/доков: нет.
- Дальше: T5.1 runtime.

### 2026-09-30 · T4.10 · in_progress · —
- Начата реализация Maintenance, лидерства и watch после зелёной T4.9.

### 2026-09-30 · T4.9 · done · 237bdee
- Сделано: `engine.snapshotter.Snapshotter.tick()` ведёт расписание `on_progress` в памяти, одним statement читает несколько деревьев, пропускает неизменный прогресс и коммитит tx-хук только вместе с CAS `snap_seq/state`.
- EMA скорости ведётся для всех узлов прочитанных деревьев и передаётся общей математике прогресса для ETA; `summary.seq` растёт строго, отсутствующий/падающий хук не останавливает maintenance.
- Проверка: два конкурентных Snapshotter коммитят ровно один seq; финализация между hook и CAS откатывает доменную запись снимка; до `every` раннего снимка нет; без изменений — 0 записей; после изменения ETA появляется. `poe check` зелёный; `poe test-all` — 881 passed, покрытие 96,39% (`snapshotter.py` 97%, `reads.py` 96%); pre-commit зелёный.
- Отклонения от плана/доков: нет.
- Дальше: T4.10 Maintenance, лидерство и watch.

### 2026-09-30 · T4.9 · in_progress · —
- Начата реализация Snapshotter после зелёной T4.8.

### 2026-09-30 · T4.8 · done · ba9ed0f
- Сделано: `engine.sweeper.Sweeper` выполняет независимыми короткими транзакциями восстановление истёкших lease, финализации/backoff хуков, дедлайнов, orphan-этапов, дрейфа счётчиков, старых delta, expiry и retention дерева.
- Для точного условия «delta старше grace» схема поднята до v2: `th_counter_delta.created_at`, индекс `(created_at, id)`, backfill и безопасная Alembic/встроенная миграция v1→v2; исторический DDL v1 заморожен отдельным golden.
- Проверка: сценарии «сломали → sweep → исправилось» для всех проходов; retention проверен с `release_required`, чанком размера 1 и итоговым `view() → BatchPurged`. `poe check` зелёный; `poe test-all` — 869 passed, покрытие 96,46% (`sweeper.py` 98%); pre-commit зелёный.
- Отклонения от плана/доков: нет; ARCHITECTURE дополнена обязательным timestamp/index, без которого возраст delta нельзя было определить.
- Дальше: T4.9 Snapshotter.

### 2026-09-30 · T4.8 · in_progress · —
- Начата реализация коротких восстановительных проходов Sweeper после зелёной T4.7.

### 2026-09-30 · T4.7 · done · 5ae8fc1
- Сделано: `engine.operations` выполняет `pause`, `resume`, `reschedule`, `cancel`, `retry_failed`, `retry_finalize` и `release` в транзакции пользователя; каскад блокируется по id, outbox обрабатывается чанками, post-commit только подталкивает relay/finalizer.
- `cancel` немедленно завершает неотправленные Items и оставляет выполняющиеся обычному finish; `retry_failed` переоткрывает предков либо весь pipeline, fed-этапы возвращает в `open`, виртуальные Items переактивирует без отправки брокеру.
- Проверка: 9 интеграционных сценариев, включая rollback, park claim на паузе, отмену до старта, `DownstreamFinalized`, корневой/листовой retry и конкурентный порядок «домен → pause» против tx-хука. `poe check` зелёный; `poe test-all` — 855 passed, покрытие 96,29% (operations 98%); pre-commit зелёный.
- Отклонения от плана/доков: нет. Счётчики операций в чужой транзакции пишутся append-only через `th_counter_delta`; labels остаются в существующем `th_metric`, как и в T4.5.
- Дальше: T4.8 Sweeper.

### 2026-09-30 · T4.7 · in_progress · —
- Начата реализация операций над деревом после зелёного общего прогона волны 7.

### 2026-09-30 · волна 7 влита · T4.5, T4.6, T4.11
- **T4.5 · done (`acd5006`).** `complete_in` делает точечный HOT CAS без предварительного `FOR UPDATE`, сохраняет spawn/marks/metrics и append-only counter delta в пользовательской транзакции; after-commit сворачивает точные delta-id и запускает policy/finalize. REPEATABLE READ/SERIALIZABLE стресс зелёный, открытая user tx не держит lock на `th_counter`.
- **T4.6 · done (`ea67545`).** `PolicyEnforcer` после flush атомарно применяет threshold/fail-fast: pause всего дерева или запрос отмены, `on_policy_breach` с fallback на hook корня и CAS-однократностью; падение hook откатывает и домен, и действие политики.
- **T4.11 · done (`8b45a88`).** Один запрос строит дерево `BatchSummary`/`BatchView` со счётчиками, метриками, feeds и leases; добавлены `in_flight`, keyset `items(label)`, `find`, `child`, одностейтментный `BatchPurged`.
- **Общий прогон после вливания:** `poe check` зелёный; 18/18 объединённых целевых тестов; `poe test-all` — 841 passed, покрытие 96,16%; `pre-commit run --all-files` зелёный.
- Отклонения/риски: в текущей архитектуре нет append-only `th_metric_delta`, поэтому путь B обновляет `th_metric` напрямую; это не блокирует `th_counter` и проходит DoD, но одинаковые metric keys могут ждать друг друга. Полное устранение требует предварительного изменения ARCHITECTURE. `InFlightItem.age` после heartbeat отражает возраст с последнего продления lease, потому что схема не хранит `acquired_at`.
- Дальше: T4.7 доступна; T5.1 теперь ждёт только T4.7.

### 2026-09-30 · волна 7 запущена · T4.5, T4.6, T4.11
- Три независимые задачи выполняются параллельно в изолированных worktree от зелёного `e33ab7e`; общий merge и гейты выполнит оркестратор.

### 2026-09-30 · T4.4 · done · d30eef4
- Сделано: `engine/finalizer.py` — проверка точных счётчиков, `on_finalized` в `HookSession` с таймаутом, CAS по состоянию и `snap_seq`, выбор терминального состояния, callback-outbox, завершение виртуального Item родителя и рекурсивная финализация дерева.
- Авто-seal этапов сериализован `FOR UPDATE` по id: после финализации источника повторно читаются все `fed_by`; `on_feeder_failed="cancel"` переносится в запрос отмены этапа. Ошибка хука откатывает доменную запись и финализацию, затем отдельно увеличивает `hook_attempts`; отсутствующий обязательный хук вызывает `Observer.hook_missing` и оставляет батч активным.
- Проверка: `poe check` зелёный; `poe test-all` — 808 passed, покрытие 97,63%; `pre-commit run --all-files` зелёный. Интеграция включает две конкурентные финализации одного батча, каскад пустых этапов и 100 гонок двух источников одного этапа.
- Отклонения от плана/доков: нет. Сводка хука строится для поддерева финализируемого батча на одном транзакционном снимке; labels и пользовательские metrics читаются из общего `th_metric`, как задано текущей схемой.
- Узнали / на что обратить внимание дальше: T4.5 может использовать `try_finalize` после свёртки дельт; T4.6 должен выставлять `cancel_reason` до финализации и переиспользовать сборку точных метрик/сводки, не дублируя семантику терминального состояния.

### 2026-09-30 · T4.3c · done · 350c711
- Сделано: атомарные с CAS finish операции `spawn`, `expect` и создание динамического под-батча; вставка Items и outbox с дедупликацией до счётчиков, depth для самоподпитки, мягкие `max_items`/`max_depth` и `skipped_by_limit`.
- Добавлены неизменяемый снимок дерева и процессный `TreeCache`: `into=` проверяется синхронно по `fed_by`, а динамическое изменение структуры инвалидирует кэш только после commit. Маршрут повторно сверяется с заблокированными строками батчей в транзакции Completer.
- Проверка: `poe check` зелёный; `poe test-all` — 795 passed, покрытие 98,22%; `pre-commit run --all-files` зелёный. Интеграция доказывает rollback завершения родителя при ошибке spawn, no-op повторного finish и формулу `found + duplicates + skipped_by_limit`.
- Отклонения от плана/доков: для совместного использования Producer и Completer выделены низкоуровневые примитивы вставки без немедленной записи счётчиков; итоговые дельты по-прежнему пишутся один раз в общей транзакции.
- Узнали / на что обратить внимание дальше: T4.4 получает готовые post-commit множества `finalize` для sealed батчей и динамических детей; финализация виртуального Item должна инвалидировать дерево только при структурных изменениях, а не при каждом переходе состояния.

### 2026-09-30 · T4.3b · done · 0f4cf16
- Сделано: пакетный CAS `finish` в Completer (до 500 Items одним UPDATE), JSON result/error, labels и пользовательские метрики, `th_item_mark`, счётчики и удаление lease/expiry. Повторный finish и неверный `batch_id` — no-op.
- После commit: Item снимается из `held`, Observer получает событие, Relay получает `kick` после освобождения окна, Finalizer — затронутые батчи. PARKED и CANCELLED в claim теперь тоже освобождают `th_window` (D-035).
- Проверка: `poe check` зелёный; `poe test-all` — 776 passed, покрытие 99,57%. Интеграция включает 1 000 finish за не более чем `ceil(1000/500)+1` транзакций.
- Отклонения от плана/доков: аргументы finish собраны в типизированный `FinishResult`, post-commit зависимости — в `CompleterTriggers`, чтобы сохранить строгий лимит числа аргументов.
- Узнали / на что обратить внимание дальше: `pytest tmp_path` на Windows нельзя переиспользовать между sandbox и повышенным Docker-процессом; полный прогон требует уникальный `--basetemp`. Для T4.3c пакет spawn должен войти в ту же транзакцию между CAS и единым `write_counters()`.

### 2026-09-30 · цикл остановлен · —
- По просьбе пользователя: волна 6 доведена и влита, новые волны не запускались.
- **Следующая доступная задача:** T4.3b (finish без spawn), её зависимости T4.3a и T4.2 выполнены. Параллельно с ней — ничего: T4.11 ждёт T4.4, T8.1 ждёт T7.1.
- **Продолжить:** `/loop` с тем же промптом (PLAN §0.7), оркестратор начнёт с T4.3b.

### 2026-09-30 · волна 6 влита · T4.2, T4.3a
- **T4.2 · done.** `th_item.options` (D-033), `th_window` + `th_outbox.options` + индекс `(batch_id, available_at)`. `engine/relay.py`: `Relay` с методами `kick`/`flush_kicked`/`run`/`scan_once`, dispatch по `task_name`, `th_expiry` для `expires`, окно `max_in_flight` с `release_window`/`refill_window`. ARCHITECTURE §5.1/§5.2/§11.2 (0ffc885). D-035.
- **T4.3a · done.** `engine/completer.py`: `Completer` — буфер, групповой коммит, backpressure. Операции `claim` (CLAIMED/DUPLICATE/TERMINAL/PARKED/CANCELLED/EXPIRED), `heartbeat`, `release`, `close(requeue_held)`. `CompleterError` в `model/errors.py`. D-036.
- **Вливание:** без конфликтов, 769 тестов, покрытие 99,55%.
- **Для следующих задач (T4.3b):**
  - Finish — новый тип операции `_Finish` в `_Completer._apply`: добавить ids в `tx.lock_items`/`tx.lock_leases`, затем CAS, удаление lease, `tx.deltas[...] += CounterDelta(...)`, один `tx.write_counters()` вместе с `fold_deltas` (D-028).
  - После CAS вызвать `release_window(conn, tables, ids)`, после commit — `relay.kick(batch_ids)`. Добавить `release_window` в пути PARKED и CANCELLED при claim (D-035, D-036).
  - Снять завершённые Items из `_held`.
  - Хелперы тестов: `tests/integration/engine/completer_env.py`, `tests/helpers/relay.py` (`ManualClock`, `RecordingDispatcher`, `relay_env`).
  - asyncpg отдаёт `infinity` как naive `datetime.max`, поэтому в тестах сравнивать через SQL.
  - Relay запускает владелец (T6.1): `run()` — fast-path в каждом процессе, `scan_once()` — в Maintenance.

### 2026-09-30 · волна 6 запущена · T4.2, T4.3a
- Зависимость T4.3a от T4.2 снята (D-034), чтобы цепочка engine шла в две ветки.

### 2026-09-30 · волна 5 влита · Fix-1, T4.1
- **Fix-1 · done.** `th_counter_delta` получила d_* для всех 11 счётчиков, `DELTA_FIELDS == COUNTER_FIELDS`, ER §5.1 обновлена.
- **T4.1 · done.** `engine/producer.py`: `Producer` с методами `create_root`, `create_sub_batch`, `add_feed`, `add_items` (unnest, чанки по 1 000, 100k Items ≤ 103 запросов), `seal`, `expect`. D-030…D-032.
- **Найден пробел:** опции вызова (`queue`, `priority`, `expires` …) продюсер отбрасывает. Решение D-033: колонка `th_item.options`, первым шагом T4.2.
- **Вливание:** без конфликтов, 676 тестов, покрытие 99,81%.
- **Для следующих задач:**
  - `seal` сам не финализирует. После commit нужен `try_finalize` (T4.4), иначе пустой батч никто не закроет.
  - Признак «этап»: `EXISTS th_feed WHERE fed_id = id`.
  - В `select(...)` типизировано не больше 10 колонок. Если их больше, дробить запрос.
  - Фикстура `env` в `tests/integration/engine/conftest.py`.

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
