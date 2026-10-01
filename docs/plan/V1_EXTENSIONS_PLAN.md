# tallyho v1 — план расширений перед релизом (Ф13)

> Дата: 2026-10-01 · Ветка: `impl/v1` · Статус: **принято, перенесено задачей T13.0.** Источник истины — ARCHITECTURE, ACCEPTANCE и карточки Ф13 в [PLAN.md](PLAN.md); этот файл остаётся обоснованием (обзор библиотек, отложенные варианты). Уточнено при переносе: листинг называется `th.list_batches`, размер окна `items(states=)` — настройка `items_scan_window`, T13.0 не ждёт T10.6 (работа шла в отдельном worktree).
> Разобраны: [tallyho-boundaries-and-attributes.md](tallyho-boundaries-and-attributes.md) (далее **ATTR**) и [V1_LIFECYCLE_HOOKS.md](V1_LIFECYCLE_HOOKS.md) (далее **HOOKS**).
> Сверено с: `ARCHITECTURE.md` §1, §5–§7, §11.2, §12, §16; `storage/tables.py`, `storage/migrations.py`, `engine/reads.py`, `engine/operations.py`, `engine/finalizer.py`, `engine/producer.py`, `api/batch.py`.

## Итог в одном абзаце

До релиза делаем две вещи: **неизменяемые атрибуты батча с листингом** и **экспорт исходов Items при финализации** на существующих механизмах (`on_finalized_task` + `handle.items()` + `release()`). Новые lifecycle-хуки (`on_started`, `on_terminal_items`), очередь результатов и Read Model в v1 не входят: первые два заменяет рецепт на существующем API, все четыре можно добавить в v1.x без поломки совместимости.

---

## 1. Атрибуты (ATTR): что берём

| Тезис ATTR | Решение |
|---|---|
| Tallyho не владеет доменным состоянием, нет `domain_state` | Принято. Уже записано в ARCHITECTURE §1 и §5.1; «критерий для будущих расширений» перенести в §1 |
| Неизменяемые `attributes` для корреляции и поиска | Принято: сейчас связь только через `key` и `th.find(kind, key)` |
| Хранение | Не колонки `th_batch` (строка часто обновляется, каждое не-HOT обновление заново пишет в GIN), а side-таблица `th_batch_attr(batch_id PK, attributes, memo)` с GIN `jsonb_path_ops`: пишется один раз, удаляется retention |
| Типы значений | `str \| int \| bool`; `UUID` нормализуется в `str` и при записи, и в фильтре. `float`, `None`, `datetime`, коллекции — отказ с ошибкой (containment строг к типу JSON) |
| Namespace | Запрещён только префикс `tallyho.`; обязательного `app.*` нет |
| `memo` | JSON-объект с лимитом размера, без индекса, в той же side-строке |
| Tenant | Обычный атрибут; фильтр по тенанту — обязанность приложения |
| Повторное создание с тем же `(kind, key)` | Первый выигрывает, как для всех параметров батча |
| Под-батчи | Атрибуты только у корня (как `retention`, `max_items`); `summary.attributes` любого узла — атрибуты корня |
| Листинг батчей | Только корни, keyset по `id DESC` (UUIDv7), фильтры `kinds`, `states`, `attributes`, `created_after/before`; индекс `(kind, id) WHERE parent_id IS NULL`; лёгкий DTO без прогресса |
| §5.1: «Колонок `status` и `data` у батча нет» | Переписать: данных домена нет, есть неизменяемый контекст корреляции |
| Пример `app.order_status_at_start` | В документацию не переносить: снимок доменного статуса в батче — то, от чего ATTR сам отговаривает |

Не входит в v1 (строки в ARCHITECTURE §16, v1.x): Read Model и PG views, `th.health()` / operational API, изменяемые search attributes.

## 2. Исходы Items: проблема и решение

### 2.1 Проблема

К моменту финализации у каждого Item один окончательный исход (после `retry_failed()` он может смениться; экспортируется текущий, а не журнал переходов). Исходы создают два актора:

* **код пользователя** — `ok` / `skip` / `error` с меткой;
* **инфраструктура**, когда код пользователя не выполнялся или упал: `exhausted`, `lease_expired`, `expired`, отмена.

Приложение, которое ведёт строку на каждый Item в своей таблице (например, `mailing_delivery`), должно получить оба вида исходов до удаления `th_*` по retention.

| № | Требование | В v1 |
|---|---|---|
| R1 | Полнота: в домен попадают и инфраструктурные исходы | да |
| R2 | Надёжность: исход не теряется при падениях и до удаления `th_*` | да |
| R3 | Один эффект: атомарно с доменной записью или идемпотентно | да |
| R4 | Исходы перенесены до того, как доменная сущность стала терминальной | да, двухфазным завершением в домене |
| R5 | Инфраструктурные исходы видны в домене, пока батч ещё идёт | **нет**, сознательно отложено |
| R6 | Нет платы на горячем пути | да |

### 2.2 Как это решают другие

| Класс решения | Примеры | R1 | R2 | R3 |
|---|---|---|---|---|
| 1. Колбэк в воркере при окончательном провале | Sidekiq `sidekiq_retries_exhausted` и death handlers, Laravel `failed()`, Oban Pro `after_process`, JobRunr `onFailedAfterRetries`, Hangfire state filters, Celery `on_failure` | частично | нет | нет |
| 2. Поток событий в процессе | River `Subscribe` (буфер 100, только задачи своего клиента), BullMQ `QueueEvents` (поток обрезается до ~10 000 событий) | да | нет | нет |
| 3. Терминальные записи хранятся, приложение читает | River (discarded 7 дней, в Pro — `river_job_dead_letter`), Laravel `failed_jobs`, GoodJob `preserve_job_records`, DLQ в SQS/Kafka | да | да | на приложении |
| 4. Колбэк батча + перечисление упавших в конце | Sidekiq Batch `on(:complete)` + `failed_jids`, Oban Pro `batch_exhausted`, Laravel `finally` + `failedJobIds`, GoodJob `on_finish`, Step Functions Distributed Map (`Succeeded/Failed/Pending.json`) | да | да | на приложении |
| 5. Слушатель внутри транзакции чанка | Spring Batch `SkipListener` (перед commit чанка), Broadway `handle_failed/2` | да | да | да |
| 6. Транзакционное завершение кодом задачи | River `JobCompleteTx`, Que | только пользовательские | да | да |

Выводы:

* Классы 1 и 2 — наблюдаемость, а не доставка.
* Стандартная пара в очередях задач — класс 6 для пользовательских исходов и класс 4 для инфраструктурных.
* Класс 5 возможен там, где фреймворк сам владеет циклом обработки и транзакцией. Плагину к брокеру для этого нужны очередь ссылок, фоновый Projector и барьер финализации — так и получался `on_terminal_items`.
* Долговечного хука «батч начался» нет ни в одной из рассмотренных библиотек; хуки начала отдельной задачи (JobRunr, Hangfire) относятся к классу 1.

Источники: [Sidekiq Batches](https://github.com/sidekiq/sidekiq/wiki/Batches), [Oban.Pro.Batch](https://oban.pro/docs/pro/Oban.Pro.Batch.html), [Oban.Pro.Worker](https://oban.pro/docs/pro/Oban.Pro.Worker.html), [River subscriptions](https://riverqueue.com/docs/subscriptions), [River Pro dead letter queue](https://riverqueue.com/docs/pro/dead-letter-queue), [Spring Batch: intercepting step execution](https://docs.spring.io/spring-batch/reference/step/chunk-oriented-processing/intercepting-execution.html), [Step Functions ResultWriter](https://docs.aws.amazon.com/step-functions/latest/dg/input-output-resultwriter.html), [Hangfire job filters](https://docs.hangfire.io/en/latest/extensibility/using-job-filters.html), [Broadway](https://hexdocs.pm/broadway/Broadway.html), [Laravel job batching](https://themsaid.com/queue-job-batching-in-laravel-how-it-works), [BullMQ events](https://docs.bullmq.io/guide/events/), [JobRunr job filters](https://www.jobrunr.io/en/documentation/pro/job-filters/), [GoodJob](https://github.com/bensheldon/good_job).

### 2.3 Что уже есть в tallyho

* Класс 6 — `th.item.complete_in(session)`.
* Класс 4 — `on_finalized_task` + `handle.items(label=)` + `release_required` / `release(session=)` (ARCHITECTURE §7.6, §12.1). Надёжнее, чем у Sidekiq: данные батча не истекают по таймеру, удаление ждёт явного `release()`.

Пробелы, которые закрывает Ф13:

1. `handle.items(label=)` читает только `th_item_mark`. По умолчанию помечаются ошибки, отменённые Items перечислить нельзя.
2. `retry_failed()` не сбрасывает `released_at`: после settle → `release()` → `retry_failed()` → второй финализации retention может удалить дерево, не дожидаясь второго экспорта.
3. Рецепт нигде не описан и не проверен тестом.

### 2.4 Рецепт финального экспорта

```text
задача (нормальный путь)
  -> записать свою строку доставки + th.item.ok/skip/error(...)
  -> th.item.complete_in(session)              # один commit

on_finalized(session, summary)                 # tx-хук, атомарно с финализацией
  -> абсолютные итоговые счётчики из summary
  -> Campaign.status = "settling"              # не терминальный

on_finalized_task -> settle_campaign           # at-least-once, идемпотентно
  -> send = await root.child("send")           # Items лежат в этапе, не в корне
  -> async for item in send.items(states={ERROR, CANCELLED}):
         bulk UPDATE строк доставок по item.key
  -> UPDATE доставок кампании, оставшихся незавершёнными -> cancelled
  -> Campaign.status = итоговый
  -> await root.release(session=session)       # release — у корня
  -> COMMIT                                    # всё одной транзакцией
```

Условия корректности, которые документация должна назвать явно:

* **Нормальный путь пишет строку в самой задаче.** Экспорт читает только `ERROR` и `CANCELLED`; Item, который после `retry_failed()` стал `ok`, исправит свою строку только сам.
* **Последний шаг — запрос по остатку.** Получатели, которые не стали Items (отмена посреди разворачивания, дубли по ключу, `skipped_by_limit`), в экспорт не попадут.
* **Счётчики ставит `on_finalized`**, а не settle: `summary` уже содержит точные абсолютные числа.
* **Колбэк идемпотентен.** Падение посередине оставляет кампанию в `settling` и дерево неосвобождённым; повтор безопасен.
* **Доменный терминальный статус ставит settle**, поэтому R4 выполняется без барьера в библиотеке.

Эталонный пример §12 (без строк на получателя) остаётся как есть; рецепт — отдельный подраздел и отдельный сценарий теста.

### 2.5 Отложено в v1.x

| Что | Когда возвращаться | Почему не ломает совместимость |
|---|---|---|
| `on_started` / `on_started_task` | Появилось правило, которое должно сработать строго до первой задачи | Новое имя в `th_batch.hooks`, новая side-таблица |
| Очередь результатов `th.results.take(session=…)` | Доказана потребность в R5 | Флаг на батче и новая таблица ссылок |
| `on_terminal_items` | Как обёртка над очередью результатов | То же |

Дизайн этих механизмов, включая найденные ошибки первых редакций (хук внутри транзакции claim, горячая строка старта, порядок хуков цепочки), остаётся в HOOKS и переписке к нему; T13.0 помечает HOOKS как отложенный.

---

## 3. Сохранение прогресса

1. **Сначала закрыть T10.6.** В рабочем дереве незакоммиченная работа по ней (`operations.py`, `completer.py`, тесты, D-037). PLAN §0.7 требует чистого дерева перед волной, а Fix-2 правит тот же `operations.py`.
2. **Существующие задачи и их номера не меняются.** Новое — `Fix-2` и фаза Ф13 (`T13.x`). Сделанный код правится только новыми задачами (PLAN §0.3).
3. **Регистрация — шагом T13.0**: карточки из §4 переносятся в `PLAN.md`, строки — в `PROGRESS.md`; у T11.1 и T12.1 добавляется зависимость от T13.6, чтобы стенд, оракул и гайды сразу покрывали расширения.
4. **Схема.** D-029 («v1 редактируема») расходится с фактом: T4.8 заморозил v1 и ввёл v2. Атрибуты идут миграцией v3; вопрос о слиянии миграций перед релизом — в T12.3.
5. **Мутационный gate (D-037) не затронут**: `_Tx.claim`, `_Tx.finish`, `Finalizer._cas`, `Snapshotter._attempt` и `Operations._retry_items` не меняются. Fix-2 правит `UPDATE th_batch` в оркестрации `retry_failed`.

## 4. Задачи

Формат карточки — как в PLAN §2. Гейты G1–G4 обязательны для всех.

| ID | Задача | Зависит | Статус |
|---|---|---|---|
| T13.0 | Зафиксировать решения в ARCHITECTURE / ACCEPTANCE / DECISIONS, зарегистрировать Ф13 | T10.6 | todo |
| Fix-2 | `retry_failed()` сбрасывает `released_at` | T13.0 | todo |
| T13.1 | model: нормализация и лимиты `attributes` / `memo` | T13.0 | todo |
| T13.2 | storage: схема v3 — `th_batch_attr`, индекс листинга | T13.0 | todo |
| T13.3 | engine + api: запись и чтение атрибутов | T13.1, T13.2 | todo |
| T13.4 | Листинг батчей | T13.3 | todo |
| T13.5 | `handle.items(states=, labels=)` | T13.0 | todo |
| T13.6 | Рецепт финального экспорта: пример, тесты, документация | Fix-2, T13.4, T13.5 | todo |

Волны: T13.0 → {Fix-2, T13.1, T13.2, T13.5} → T13.3 → T13.4 → T13.6. В первой волне задачи правят разные файлы: `operations.py`, `model/`, `storage/`, `reads.py` + `api/batch.py`.

#### T13.0 — Зафиксировать решения
* **Сделать:** только документы, отдельными коммитами.
  * ARCHITECTURE: §1 (критерий границ); §2 (Attributes, Memo); §5.1–§5.2 (`th_batch_attr`, индекс листинга, фраза про `status`/`data`); §7.6 и UC-14 / UC-16 (повтор отменяет release); §11.2 (`attributes=`, `memo=`, листинг, `items(states=, labels=)`); новый подраздел §12 «Строка на каждого получателя» по §2.4; §15 (лимиты атрибутов); §16 (v1.x: Read Model, operational API, изменяемые атрибуты, `on_started`, очередь результатов).
  * ACCEPTANCE: группа A-AT (атрибуты и листинг); сценарий экспорта в A-UC (падение колбэка посередине, повтор, `retry_failed()` после settle); инвариант «дерево с `release_required` не удаляется, пока после последней финализации не вызван `release()`».
  * DECISIONS: D-038 side-таблица атрибутов и типы значений; D-039 Read Model отложен; D-040 lifecycle-хуки и очередь результатов отложены, исходы переносятся рецептом финального экспорта; D-041 `items(states=)` обходит батч окнами фиксированного размера; D-042 нумерация миграций (пересмотр D-029). Имя метода листинга выбрать здесь: существующий API плоский (`th.find`, `th.handle`).
  * PLAN / PROGRESS: карточки и строки Fix-2 и Ф13, новые зависимости T11.1 и T12.1.
  * Пометить ATTR как «перенесено в ARCHITECTURE», HOOKS — как «отложено до v1.x, см. D-040».
* **DoD:** в ARCHITECTURE нет API, отсутствующего в карточках; D-038…D-042 записаны.

#### Fix-2 — `retry_failed()` сбрасывает `released_at`
* **Док:** ARCHITECTURE §7.6, UC-14, UC-16 (после T13.0)
* **Сделать:** при переоткрытии дерева `released_at = NULL` у корня; `release()` нужно вызвать заново после новой финализации.
* **DoD:** интеграционный тест: settle → `release()` → `retry_failed()` → финализация → retention не удаляет дерево до второго `release()`; `poe test-all`.

#### T13.1 — model: атрибуты
* **Док:** ARCHITECTURE §2, §15
* **Сделать:** `model/attributes.py`: нормализация (`str | int | bool`, `UUID → str`), запрет префикса `tallyho.`, лимиты (32 ключа, ключ 128 байт, строка 512 байт, всего 8 КиБ; `memo` — JSON-объект с лимитом), ошибка — подкласс `TallyhoError`. Лимиты в `Settings`.
* **DoD:** юнит-тесты границ каждого лимита и каждого запрещённого типа; нормализация фильтра и записи — одна функция.

#### T13.2 — storage: схема v3
* **Сделать:** `th_batch_attr(batch_id PK, attributes jsonb, memo jsonb)` + GIN `jsonb_path_ops`; индекс `th_batch (kind, id) WHERE parent_id IS NULL`; миграция v3, Alembic, golden.
* **DoD:** каталог после `migrate` совпадает с `create_all`; путь v2 → v3 на непустой БД; `poe test-all`.

#### T13.3 — engine + api: атрибуты
* **Сделать:** `th.batch(..., attributes=, memo=)` только для корня; запись в транзакции создания, строка не создаётся для батча без атрибутов; повтор с тем же `(kind, key)` — первый выигрывает; `BatchView.attributes/memo`, `BatchSummary.attributes` (у узлов поддерева — атрибуты корня; Snapshotter кэширует их в памяти); retention удаляет side-строку; атрибуты не попадают в логи и OTEL.
* **DoD:** rollback транзакции пользователя не оставляет строки; хук `on_finalized` под-батча видит атрибуты корня; тест с секретом в атрибуте и `caplog`; число запросов Snapshotter на тик не выросло.

#### T13.4 — Листинг батчей
* **Сделать:** листинг корней: `kinds`, `states` (`BatchState`), `attributes` (containment), `created_after/before`, `limit`, непрозрачный `cursor` (keyset по `id DESC`); лёгкий DTO без прогресса.
* **DoD:** пагинация без пропусков и дублей при параллельном создании батчей; запросы в `storage.hot_queries`, EXPLAIN-гард зелёный на 1 млн Items.

#### T13.5 — `handle.items(states=, labels=)`
* **Док:** ARCHITECTURE §11.2
* **Сделать:** сигнатура `items(*, states: Collection[ItemState] | None = None, labels: Collection[str] | None = None)`; вызов без фильтров — ошибка; `labels=` идёт через `th_item_mark`, как сейчас; `states=` обходит `(batch_id, id)` окнами фиксированного размера (подзапрос с `LIMIT` по индексу, фильтр по состоянию снаружи), чтобы один statement не сканировал весь остаток батча при редких совпадениях; виртуальные Items выдаются как есть и отличаются по `child_batch_id`; порядок выдачи контрактом не является. Обновить вызовы `items(label=)` в тестах и примерах.
* **DoD:** отменённые Items перечисляются; запрос в `storage.hot_queries`, EXPLAIN-гард без `Seq Scan`; замер на батче в 1 млн Items с 0,1% совпадений: ни один statement не превышает `statement_timeout`, время записано в журнал PROGRESS; `BatchPurged` для удалённого батча, как раньше.

#### T13.6 — рецепт финального экспорта
* **Док:** §2.4 этого файла, ARCHITECTURE §12 (новый подраздел)
* **Сделать:** в `tests/examples/mailing` — отдельный сценарий на малом объёме (до 1 000 контактов) с таблицей `mailing_delivery`: задача пишет строку и вызывает `complete_in`; `on_finalized` ставит счётчики и `settling`; `settle_campaign` экспортирует `ERROR` / `CANCELLED` этапа `send`, закрывает остаток, ставит итоговый статус и вызывает `release()` корня одной транзакцией. Атрибуты и листинг — в том же сценарии. README и smoke-блок документации.
* **DoD:** эталонный сценарий `(9100, 600, 300, 40)` не изменился; случаи: `exhausted`, отмена посреди разворачивания (строки без Items закрыты запросом по остатку), падение колбэка посередине и повтор, `retry_failed()` после settle с повторным экспортом; retention не удаляет дерево до `release()`; документационные тесты зелёные.

## 5. Статус решений

Подтверждено владельцем 2026-10-01:

* атрибуты — только `str | int | bool` (`UUID → str`), без обязательного `app.*`;
* Read Model исключён из v1;
* `on_started`, `on_terminal_items` и очередь результатов в v1 не делаются; исходы Items переносятся рецептом финального экспорта.

Открыто:

1. Имя метода листинга батчей (решается в T13.0).
2. Миграция v3 или слияние миграций в одну перед релизом (T12.3).
