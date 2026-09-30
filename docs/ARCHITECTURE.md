# tallyho — архитектура

> Версия документа: 2.1-draft · 2026-09-30
> Статус: проектирование, кода нет.
> Решения версии 2.0: **только PostgreSQL** в v1; **никаких доменных статусов в библиотеке** (Flow/Run, бизнес-статусы, история статусов, данные процесса удалены). Доменное состояние живёт в таблицах пользователя, tallyho даёт транзакционные хуки для его обновления (§7).
> Решения версии 2.1: **конвейеры этапов на под-батчах** — `fed_by`, `into=`, правило записи в этап, каскад пустых этапов, лимиты `max_items`/`max_depth`, расширенная модель прогресса (найдено / сделано / оценка / ETA). Отдельной сущности «stage» нет: этап — это под-батч (§8.1, §9.4, §13). Основание — [DYNAMIC_WORKFLOWS.md](DYNAMIC_WORKFLOWS.md).
> [DESIGN.md](DESIGN.md), [COUNTERS.md](COUNTERS.md) и [API.md](API.md) — исторический ресёрч. При расхождениях прав этот документ.

## Содержание

1. [Назначение и границы](#1-назначение-и-границы)
2. [Термины](#2-термины)
3. [Архитектура верхнего уровня](#3-архитектура-верхнего-уровня)
4. [Зависимости](#4-зависимости)
5. [Модель данных](#5-модель-данных)
6. [Состояния](#6-состояния)
7. [Интеграция с доменом: прогресс, финализация, retention](#7-интеграция-с-доменом-прогресс-финализация-retention)
8. [Use cases (UC-01 … UC-16)](#8-use-cases)
9. [Счётчики и групповой коммит](#9-счётчики-и-групповой-коммит)
10. [Гарантии и отказы](#10-гарантии-и-отказы)
11. [Публичный API и адаптер flexiq](#11-публичный-api-и-адаптер-flexiq)
12. [Сквозной пример: email-рассылки](#12-сквозной-пример-email-рассылки)
13. [Второй пример: конвейер парсинга](#13-второй-пример-конвейер-парсинга)
14. [Производительность и критерии релиза](#14-производительность-и-критерии-релиза)
15. [Конфигурация по умолчанию](#15-конфигурация-по-умолчанию)
16. [Roadmap и открытые вопросы](#16-roadmap-и-открытые-вопросы)

---

## 1. Назначение и границы

**tallyho** — async-библиотека для Python + PostgreSQL. К любому брокеру задач она добавляет:

* **групповой учёт** задач: батчи, вложенные батчи, прогресс, финализация ровно один раз;
* **динамический fan-out**: задачи порождают задачи, общее число заранее неизвестно;
* **конвейеры этапов**: этапы работают параллельно, следующий стартует, не дожидаясь конца предыдущего, и сам закрывается, когда закончились его источники; прогресс показывается как «найдено / сделано / оценка итога»;
* **технические операции над группой**: отложенный старт, пауза, отмена, повтор упавших, ограничение параллелизма;
* **транзакционные хуки** для переноса прогресса и итога в доменные таблицы пользователя;
* **гарантии**: ничего не зависает и не теряется при падениях процессов, брокера и сети.

**Не делаем:**
* исполнение задач, ретраи, cron, rate limit — это работа брокера;
* доменные статусы, бизнес-процессы, стейт-машины, хранение бизнес-данных — это домен пользователя. Мы отдаём ему точные факты о группе задач в его транзакции, а что они значат для бизнеса, решает он;
* durable execution с replay, как Temporal/DBOS.

**Нефункциональные требования**

| Требование | Как выполняется |
|---|---|
| PostgreSQL (≥ 14) | SQLAlchemy 2.1 Core, PG-специфика используется свободно: `SKIP LOCKED`, `LISTEN/NOTIFY`, advisory locks, `unnest` для bulk, partial-индексы |
| Таблицы в схеме пользователя | `schema=` + `schema_translate_map` |
| Интеграция с любым брокером | Протоколы `Dispatcher` + `Runtime`, первый адаптер — flexiq |
| Быстрые запросы на больших объёмах | Узкие индексы без изменяемых колонок, side-таблицы для разреженных множеств, UUIDv7, групповой коммит (§9) |
| «Ничего не зависнет» | Outbox, lease + heartbeat, sweeper, CAS-переходы, reconcile (§10) |
| Своя сессия БД пользователя | `session=` во всех пишущих методах, `th.item.complete_in(session)` |
| Доменные таблицы не зависят от retention | Транзакционные хуки финализации и снимков прогресса, `release()` (§7) |
| Расширяемость | Протоколы: брокер, сериализатор, хуки наблюдаемости, часы, генератор ID |
| Только async | `AsyncEngine` / `AsyncSession` / `AsyncConnection` |

---

## 2. Термины

| Термин | Значение |
|---|---|
| **Batch** | Группа задач. Техническое состояние, счётчики, колбэки, хуки |
| **Kind** | Строковый тип батча (`"campaign_deliveries"`). По нему находятся хуки |
| **Key** | Ключ батча для идемпотентного создания и связи с доменом. У корня уникален в пределах `kind` (`"campaign:42"`), у под-батча — в пределах дерева (`"send"`) |
| **Item** | Одна задача в брокере |
| **Sub-batch** | Батч, который для родителя выглядит одним Item (виртуальный Item). У под-батча есть `key`, уникальный внутри дерева (`"cards"`) |
| **Этап конвейера** | Не отдельная сущность, а под-батч, который наполняют задачи других под-батчей |
| **Источник (`fed_by`)** | Под-батч, задачи которого добавляют Items в данный. Данный под-батч автоматически закрывается (seal), когда все источники финализированы |
| **Spawn** | Добавление Items изнутри выполняющегося Item, атомарно с его завершением: в свой батч или `into=` в этап, для которого свой батч — источник |
| **Seal** | «Новых Items не будет». Корневой и обычные батчи закрывает продюсер, этапы с `fed_by` — библиотека. Без seal батч не финализируется |
| **found / done** | `found` — уникальные Items, добавленные в батч (растёт по ходу); `done` — завершённые в любом классе итога |
| **expected** | Ожидаемый итог батча: точный после seal; до seal — `expect(n)` от пользователя или оценка по наблюдаемому ветвлению источника |
| **Result class** | Технический класс итога Item: `ok / skip / error / cancelled` |
| **Label** | Свободная строка-категория итога (`"sent"`, `"hard_bounce"`) — разрез счётчиков. Не статус и не стейт-машина |
| **Tx-хук** | Код пользователя, который tallyho выполняет **внутри своей транзакции** на событии батча: `on_finalized`, `on_progress`, `on_policy_breach` |
| **Callback** | Задача брокера, поставленная через outbox на событии батча (at-least-once, вне транзакции) |
| **Release** | Явное разрешение пользователя удалить батч по retention |
| **Completer** | In-process компонент воркера: групповой коммит claim/heartbeat/finish |
| **Relay** | Отправляет записи outbox в брокер |
| **Sweeper** | Фоновые проверки: истёкшие lease, пропущенные финализации, повтор хуков, дедлайны, дрейф, retention |

---

## 3. Архитектура верхнего уровня

### 3.1 Контекст

```mermaid
flowchart LR
    subgraph App["Приложение пользователя"]
        API["API-процесс"]
        W["Воркеры flexiq"]
        DOM[("Доменные таблицы<br/>campaigns, contacts, ...")]
    end
    subgraph TH["tallyho (библиотека внутри процессов)"]
        P["Producer API<br/>batch / handle"]
        MW["th.tracked + Completer"]
        R["Relay"]
        S["Sweeper + снимки прогресса"]
        HK["Tx-хуки пользователя"]
    end
    DB[("PostgreSQL<br/>таблицы th_* в схеме пользователя")]
    BR{{"flexiq"}}

    API --> P
    W --> MW
    P -->|"INSERT в транзакции пользователя"| DB
    MW -->|"групповой коммит"| DB
    R -->|"outbox"| DB
    R -->|"dispatch"| BR
    BR -->|"доставка"| W
    S -->|"починка, снимки"| DB
    S --> HK
    MW --> HK
    HK -->|"та же транзакция"| DOM
```

Доменные таблицы и `th_*` лежат в **одной базе PostgreSQL** (схемы могут быть разными). Это условие атомарности tx-хуков.

### 3.2 Процессы и размещение компонентов

```mermaid
flowchart TB
    subgraph apiproc["API-процесс"]
        A1["Tallyho client"]
        A2["Relay fast-path<br/>after_commit kick"]
        A3["Maintenance в lifespan<br/>Relay scan, Sweeper, Snapshotter"]
    end
    subgraph wproc["Процесс воркера flexiq (N штук)"]
        B1["flexiq worker, pool=thread"]
        B2["th.tracked обёртка"]
        B3["Completer<br/>в async-loop flexiq"]
        B4["Relay fast-path"]
        B5["Finalizer + tx-хуки"]
    end
    subgraph opt["Альтернатива"]
        C1["tallyho maintenance<br/>отдельный процесс"]
    end
    B1 --> B2 --> B3 --> B5
    A3 -.-|"leader election<br/>pg_try_advisory_lock"| C1
```

* **Relay fast-path** работает в каждом процессе: сразу после commit отправляет то, что этот процесс только что записал.
* **Maintenance** (relay scan, sweeper, снимки прогресса) работает в одном экземпляре-лидере. Лидер выбирается через advisory lock. Relay scan безопасен и в нескольких экземплярах (`SKIP LOCKED`).
* **Finalizer** работает там, где завершился последний Item (воркер), либо в sweeper'е. Поэтому **модули с tx-хуками должны импортироваться и в воркерах, и в maintenance**: `Tallyho(..., hook_modules=[...])` импортирует их при инициализации (§7.5).
* **Completer** живёт в event loop исполнителя async-задач flexiq и создаётся лениво при первой задаче.

### 3.3 Модули пакета и зависимости между ними

```mermaid
flowchart TB
    api["tallyho.api<br/>Tallyho, BatchBuilder, BatchHandle, call"]
    hooks["tallyho.hooks<br/>registry, on_finalized, on_progress, on_policy_breach"]
    item["tallyho.runtime<br/>tracked, ItemContext, system task"]
    eng["tallyho.engine<br/>Completer, Relay, Sweeper,<br/>Finalizer, Snapshotter, Counters"]
    st["tallyho.storage<br/>tables, queries, migrations"]
    model["tallyho.model<br/>states, summaries, views, errors"]
    proto["tallyho.protocols<br/>Dispatcher, Runtime, Serializer, Clock, Observer"]
    ad["tallyho.adapters.flexiq"]
    test["tallyho.testing<br/>InlineBroker, FakeClock, fixtures"]
    cli["tallyho.cli"]

    api --> eng
    api --> hooks
    api --> model
    eng --> hooks
    eng --> st
    eng --> model
    eng --> proto
    item --> eng
    item --> proto
    st --> model
    ad --> proto
    ad --> item
    test --> proto
    test --> item
    cli --> eng
```

Правило слоёв: `storage` не знает про брокер, `engine` — только про протоколы, адаптеры — только про `protocols` и `runtime`. Циклов нет. Правило проверяется в CI через `import-linter`.

### 3.4 Основные классы

```mermaid
classDiagram
    class Tallyho {
        +AsyncEngine engine
        +str schema
        +install(adapter) None
        +batch(kind, key, ...) BatchBuilder
        +handle(batch_id) BatchHandle
        +find(kind, key) BatchHandle
        +on_finalized(kind) decorator
        +on_progress(kind, every) decorator
        +on_policy_breach(kind) decorator
        +maintenance() Maintenance
    }
    class BatchBuilder {
        +add(fn, *args, **kwargs) None
        +map(fn, iterable) None
        +add_calls(calls) None
        +sub_batch(key, fed_by, ...) BatchBuilder
        +expect(n) None
        +seal() None
        +handle BatchHandle
    }
    class BatchHandle {
        +UUID id
        +view() BatchView
        +watch() AsyncIterator~BatchView~
        +wait(timeout) BatchView
        +in_flight(limit) list~InFlightItem~
        +reschedule(start_at, session) None
        +pause(session) None
        +resume(session) None
        +cancel(session) None
        +retry_failed(labels, session) None
        +retry_finalize() None
        +release(session) None
        +items(label) AsyncIterator~ItemView~
    }
    class ItemContext {
        +UUID id
        +UUID batch_id
        +int attempt
        +int depth
        +spawn(fn, *args, into, key) None
        +spawn_call(call, into) None
        +sub_batch(key, ...) BatchBuilder
        +expect(n, into) None
        +progress(done, total) None
        +incr(name, value) None
        +ok(label, result) None
        +skip(label) None
        +error(label, detail) None
        +complete_in(session) None
        +cancelled() bool
    }
    class BatchSummary {
        +UUID id
        +str kind
        +str key
        +BatchState state
        +Progress progress
        +dict labels
        +dict metrics
        +dict~str, BatchSummary~ children
        +int seq
        +datetime finished_at
    }
    class Progress {
        +int found
        +int queued
        +int in_flight
        +int ok
        +int skip
        +int error
        +int cancelled
        +int duplicates
        +int skipped_by_limit
        +bool final
        +int expected
        +bool expected_is_estimate
        +int estimate_basis
        +float ratio
        +timedelta eta
    }
    class Completer {
        +claim(item) Future
        +heartbeat(item) None
        +finish(item, result, spawns, metrics) Future
        +release(item) Future
    }
    class Finalizer {
        +try_finalize(batch_id) bool
    }
    class Snapshotter {
        +tick() int
    }
    class Relay {
        +kick(batch_ids) None
        +scan_once() int
    }
    class Sweeper {
        +expire_leases() int
        +finalize_stuck() int
        +enforce_deadlines() int
        +reconcile_drift() int
        +retention() int
    }
    class Dispatcher {
        <<Protocol>>
        +task_name(fn) str
        +dispatch(messages) None
    }
    class Runtime {
        <<Protocol>>
        +wrap(fn) fn
        +retry_verdict(exc) Verdict
        +reconcile_dead(cursor) DeadLetters
    }

    Tallyho --> BatchBuilder
    Tallyho --> BatchHandle
    Tallyho --> Relay
    Tallyho --> Sweeper
    Tallyho --> Snapshotter
    ItemContext --> Completer
    Completer ..> Finalizer : после commit
    Sweeper ..> Finalizer
    Finalizer ..> BatchSummary : передаёт в on_finalized
    BatchSummary --> Progress
    Snapshotter ..> BatchSummary : передаёт в on_progress
    Relay --> Dispatcher
    Runtime ..> ItemContext : создаёт на время задачи
```

---

## 4. Зависимости

### 4.1 Внешние пакеты

| Пакет | Версия | Обязателен | Зачем |
|---|---|---|---|
| Python | ≥ 3.11 | да | `TaskGroup`, `StrEnum`, `Self`, `ExceptionGroup` |
| PostgreSQL | ≥ 14 | да | партиционирование и `MERGE` — задел на v2; всё остальное работает и на 12+ |
| `sqlalchemy[asyncio]` | ≥ 2.1 | да | Core, `schema_translate_map`, `postgresql_with` у `Table` (storage-параметры), приём сессии пользователя, Alembic |
| `asyncpg` или `psycopg[binary]` | ≥ 0.29 / ≥ 3.1 | один из | драйвер |
| `typing-extensions` | ≥ 4.10 | да | `ParamSpec`/`TypeVar` defaults на 3.11 |
| `flexiq` | `>=2.0,<3` | extra `flexiq` | адаптер |
| `alembic` | ≥ 1.13 | нет | встраивание миграций в проект пользователя |

UUIDv7 генерируем сами (≈30 строк). В Python 3.14+ используем `uuid.uuid7()`.

### 4.2 Точки расширения

| Протокол | Кто реализует | Для чего |
|---|---|---|
| `Dispatcher`, `Runtime`, `PayloadCodec` | адаптер брокера | отправка; обёртка исполнения, вердикт ретрая, сверка с DLQ по курсору (§11.3); кодек payload Items (без своего кодека — `SerializerCodec` поверх `Serializer`) |
| Tx-хуки `on_finalized / on_progress / on_policy_breach` | пользователь | перенос итога и прогресса в доменные таблицы (§7) |
| `Serializer` | пользователь (есть json/msgspec) | аргументы задач, `result` Item |
| `Observer` | пользователь | метрики, OpenTelemetry, логи — вне транзакций, fire-and-forget |
| `Clock` | тесты | управление временем |
| `IdFactory` | редко | свой формат ID |

---

## 5. Модель данных

### 5.1 ER-диаграмма

```mermaid
erDiagram
    TH_BATCH ||--o{ TH_ITEM : "содержит"
    TH_BATCH ||--o{ TH_BATCH : "parent_id"
    TH_ITEM |o--o| TH_BATCH : "child_batch_id (виртуальный Item)"
    TH_BATCH ||--|{ TH_COUNTER : "слоты"
    TH_BATCH ||--o{ TH_COUNTER_DELTA : "дельты из транзакций пользователя"
    TH_BATCH ||--o{ TH_METRIC : "labels и пользовательские метрики"
    TH_BATCH ||--o{ TH_OUTBOX : "к отправке"
    TH_BATCH ||--o{ TH_FEED : "feeder_id: кого наполняет"
    TH_BATCH ||--o{ TH_FEED : "fed_id: кто наполняет"
    TH_ITEM ||--o| TH_LEASE : "пока выполняется"
    TH_ITEM ||--o| TH_ITEM_MARK : "только помеченные"
    TH_ITEM ||--o| TH_WINDOW : "отправлен, окно max_in_flight"

    TH_BATCH {
        uuid id PK "UUIDv7"
        uuid root_id
        uuid parent_id "NULL"
        uuid parent_item_id "NULL"
        text kind
        text key "NULL"
        smallint state
        timestamptz paused_at "NULL"
        timestamptz cancel_requested_at "NULL"
        text cancel_reason "NULL: cancel/deadline/fail_fast"
        timestamptz start_at "NULL, отложенный старт"
        jsonb options "колбэки, политики"
        text[] hooks "требуемые tx-хуки"
        bigint expected_total "NULL, растёт через expect()"
        int max_in_flight "NULL"
        bigint max_items "NULL, только у корня: лимит на дерево"
        smallint max_depth "NULL, глубина самоподпитки"
        smallint on_feeder_failed "seal / cancel"
        timestamptz deadline_at "NULL"
        int snap_seq "версия снимков прогресса"
        smallint hook_attempts
        text hook_error "NULL"
        interval retention "NULL = хранить вечно"
        bool release_required
        timestamptz released_at "NULL"
        timestamptz created_at
        timestamptz updated_at
        timestamptz finished_at "NULL"
    }
    TH_ITEM {
        uuid id PK
        uuid batch_id
        smallint state "active/ok/skip/error/cancelled"
        text label "NULL"
        smallint attempt
        smallint depth "глубина самоподпитки"
        text task_name
        bytea payload
        jsonb options "NULL, queue и опции брокера вызова"
        text key "NULL"
        uuid child_batch_id "NULL"
        int weight
        jsonb result "NULL"
        jsonb error "NULL"
        timestamptz created_at
        timestamptz finished_at "NULL"
    }
    TH_OUTBOX {
        uuid id PK
        smallint kind "item/callback"
        uuid batch_id
        uuid item_id "NULL"
        text task_name "NULL"
        bytea payload "NULL"
        jsonb options "NULL, опции колбэка"
        timestamptz available_at
        smallint attempts
    }
    TH_LEASE {
        uuid item_id PK
        uuid batch_id
        timestamptz lease_until
        text worker_id
        smallint attempt
        bigint progress_done "NULL, th.item.progress"
        bigint progress_total "NULL"
    }
    TH_FEED {
        uuid feeder_id PK
        uuid fed_id PK
    }
    TH_COUNTER {
        uuid batch_id PK
        smallint slot PK
        bigint total
        bigint ok
        bigint skip
        bigint error
        bigint cancelled
        bigint dispatched
        bigint w_total
        bigint w_done
        bigint duplicates
        bigint skipped_by_limit
        bigint tree_total "только у корня: Items всего дерева"
    }
    TH_COUNTER_DELTA {
        bigint id PK
        uuid batch_id
        bigint d_total
        bigint d_ok
        bigint d_skip
        bigint d_error
        bigint d_cancelled
        bigint d_dispatched
        bigint d_w_total
        bigint d_w_done
        bigint d_duplicates
        bigint d_skipped_by_limit
        bigint d_tree_total
        timestamptz created_at "для sweeper fold после grace"
    }
    TH_METRIC {
        uuid batch_id PK
        text name PK
        smallint slot PK
        bigint value
    }
    TH_ITEM_MARK {
        uuid batch_id PK
        text label PK
        uuid item_id PK
    }
    TH_WINDOW {
        uuid item_id PK
        uuid batch_id
    }
```

`th_meta(key PK, value)` хранит версию схемы и на диаграмме не показана. Колонок `status` и `data` у батча нет: доменное состояние живёт у пользователя.

### 5.2 Индексы и запросы горячего пути

Принцип: **на `th_item` нет индексов по изменяемым колонкам**. Единственный UPDATE Item'а (finish) — HOT, без записи в индексы. Разреженные множества («в очереди», «выполняется», «упал с hard_bounce») живут в узких side-таблицах, размер которых ≈ объёму текущей работы, а не истории.

| Таблица | Индекс | Обслуживает запрос | Сложность |
|---|---|---|---|
| th_batch | PK | всё по id | O(log n) |
| th_batch | `UNIQUE (kind, key) WHERE parent_id IS NULL AND key IS NOT NULL` | идемпотентное создание корня, `th.find(kind, key)` | O(log n) |
| th_batch | `UNIQUE (root_id, key) WHERE parent_id IS NOT NULL` | под-батч по ключу внутри дерева: `into="cards"`, идемпотентный `sub_batch` | O(log n) + кэш в процессе |
| th_batch | `(parent_id) WHERE parent_id IS NOT NULL` | каскад pause/cancel, дерево | O(log n + k) |
| th_batch | `(updated_at) WHERE state IN (open, sealed, finalizing)` | sweeper: зависшие батчи, повтор хуков | размер = активные |
| th_batch | `(deadline_at) WHERE deadline_at IS NOT NULL AND state IN (open, sealed)` | sweeper: дедлайны | размер = активные |
| th_batch | `(id) WHERE state IN (open, sealed) AND 'progress' = ANY(hooks)` | Snapshotter: активные батчи со снимками | размер = активные с хуком |
| th_batch | `(finished_at) WHERE id = root_id AND finished_at IS NOT NULL AND retention IS NOT NULL AND (NOT release_required OR released_at IS NOT NULL)` | retention деревьями | размер = готовые к удалению |
| th_item | PK | claim/finish по id | O(log n), UUIDv7 → горячие страницы справа |
| th_item | `(batch_id, id)` | листинг, cancel, reconcile | O(log n + k) |
| th_item | `UNIQUE (batch_id, key) WHERE key IS NOT NULL` | дедуп spawn/add | O(log n) |
| th_outbox | `(available_at)` | relay | размер = неотправленное |
| th_outbox | `(batch_id, available_at)` | pause/resume/cancel/reschedule; окно `max_in_flight`: запаркованные (`∞`) и готовые записи батча | размер = неотправленное |
| th_window | PK `(item_id)`, `(batch_id)` | окно `max_in_flight`: сколько Items батча отправлено и не завершено | размер ≤ сумма окон активных батчей |
| th_lease | PK, `(lease_until)` | claim, истёкшие lease | размер = in-flight |
| th_lease | `(batch_id)` | `handle.in_flight()`, `in_flight` в прогрессе | размер = in-flight |
| th_feed | PK `(feeder_id, fed_id)` | при финализации источника: какие этапы он наполняет | O(log n) |
| th_feed | `(fed_id)` | все ли источники этапа финализированы; правило записи в этап | O(log n) |
| th_counter | PK `(batch_id, slot)` | прогресс, финализация | O(slots) |
| th_counter_delta | `(batch_id)` | точное чтение и свёртка | размер = несвёрнутое |
| th_counter_delta | `(created_at, id)` | sweeper: свёртка дельт старше `finalize_grace` | размер = несвёрнутое |
| th_metric | PK `(batch_id, name, slot)` | разбивка по labels | O(names × slots) |
| th_item_mark | PK `(batch_id, label, item_id)` | «все hard_bounce батча» для экспорта | O(log n + k) |

Хранение: `th_item` `fillfactor=85` (место для HOT). `th_counter`/`th_metric` `fillfactor=50` + агрессивный per-table autovacuum. Состояния — `smallint`. FK на горячих таблицах не объявляем: целостность держит библиотека, retention удаляет деревом чанками.

---

## 6. Состояния

### 6.1 Батч

```mermaid
stateDiagram-v2
    [*] --> open : create
    open --> sealed : seal() продюсера
    open --> sealed : fed_by: все источники финализированы
    open --> finalizing : cancel_requested и pending == 0
    sealed --> finalizing : pending == 0 (CAS)
    finalizing --> open : tx-хук упал, откат, повтор с backoff
    finalizing --> sealed : tx-хук упал, откат, повтор с backoff
    finalizing --> succeeded : нет error
    finalizing --> completed_with_errors : есть error, порог не превышен
    finalizing --> failed : порог error превышен / deadline / fail_fast
    finalizing --> cancelled : cancel_requested
    succeeded --> [*]
    completed_with_errors --> [*]
    failed --> [*]
    cancelled --> [*]
    completed_with_errors --> sealed : retry_failed()
    failed --> sealed : retry_failed()
```

* `finalizing` существует только внутри одной транзакции: tx-хук `on_finalized` → CAS в терминальное → outbox колбэков → завершение виртуального Item родителя. Если хук упал, откатывается всё, и батч остаётся `sealed` с `hook_error`. Sweeper повторяет с backoff (§7.3).
* **Этап с `fed_by` продюсер не закрывает** — `seal()` для него ошибка. Его закрывает транзакция финализации последнего из источников (§8.1, UC-17). Источник в любом терминальном состоянии считается финализированным. Если он `completed_with_errors`/`failed`/`cancelled`, этап по умолчанию закрывается и доделывает полученное (`on_feeder_failed="seal"`) либо получает запрос отмены (`"cancel"`).
* **Пустой батч финализируется сразу.** Закрытый батч с `pending = 0` (в том числе с `found = 0`) финализируется в той же цепочке «после commit», что и любой другой, и каскадом закрывает этапы, которые он наполняет. Этап без единого Item — нормальная ситуация, а не зависание.
* **Отмена, дедлайн и `fail_fast` — не мгновенный переход, а флаг** `cancel_requested_at` (с причиной). Флаг запрещает новые `add`/`spawn`, неотправленные Items сразу становятся `cancelled`, отправленные отменяются лениво при claim, выполняющиеся доделываются. Когда `pending = 0`, срабатывает обычная финализация с `on_finalized`, и хук получает `summary.state = cancelled` или `failed` с `summary.reason`. Поэтому все пути в терминальное состояние проходят через один и тот же транзакционный хук.

**Пауза и отложенный старт** — ортогональные флаги, не состояния:

```mermaid
stateDiagram-v2
    state "Ждёт start_at" as Waiting
    state "Активен" as Active
    state "На паузе" as Paused
    [*] --> Waiting : start_at в будущем
    [*] --> Active : start_at не задан
    Waiting --> Waiting : reschedule()
    Waiting --> Active : наступил start_at
    Active --> Paused : pause() / policy action=pause
    Waiting --> Paused : pause()
    Paused --> Active : resume()
    note right of Paused
        relay не отправляет Items батча
        пришедшие из брокера паркуются при claim
        выполняющиеся доделываются
        финализация разрешена, если pending стал 0
    end note
```

`start_at` — это просто `available_at` у записей outbox батча. Отдельного планировщика нет: relay отправляет Items, когда время наступило.

### 6.2 Item (производное состояние)

В `th_item.state` хранится только `active` или класс итога. Детальное состояние выводится из наличия строк в side-таблицах:

| Производное состояние | Признак |
|---|---|
| queued | `state=active`, есть в `th_outbox`, `available_at ≤ now` |
| parked | `state=active`, есть в `th_outbox`, `available_at` в будущем (start_at) или `∞` (пауза, окно `max_in_flight`) |
| dispatched | `state=active`, нет ни в outbox, ни в lease |
| running | `state=active`, есть в `th_lease` |
| terminal | `state ∈ {ok, skip, error, cancelled}` + `label` |

```mermaid
stateDiagram-v2
    [*] --> queued : add / spawn
    [*] --> parked : add при start_at в будущем
    queued --> parked : pause / окно max_in_flight
    parked --> queued : resume / окно освободилось / start_at наступил
    queued --> dispatched : relay.dispatch
    dispatched --> running : claim (lease)
    dispatched --> parked : claim при паузе
    running --> dispatched : ошибка, брокер повторит
    running --> queued : lease истёк, attempt меньше max
    running --> ok : ok(label)
    running --> skip : skip(label)
    running --> error : error(label) / попытки исчерпаны
    queued --> cancelled : cancel()
    parked --> cancelled : cancel()
    dispatched --> cancelled : cancel(), ленивая отмена при claim
    ok --> [*]
    skip --> [*]
    error --> [*]
    cancelled --> [*]
```

После завершения Item неизменяем. Поздние события вроде webhook «доставлено» или «bounce» — это домен пользователя (§12).

### 6.3 Запись outbox

```mermaid
stateDiagram-v2
    [*] --> pending : INSERT в транзакции продюсера
    pending --> claimed : relay UPDATE available_at = now + 30s SKIP LOCKED
    claimed --> [*] : dispatch ok → DELETE
    claimed --> pending : relay упал → available_at наступил снова
    pending --> parked : pause (available_at = ∞)
    parked --> pending : resume
```

At-least-once отправка. Дубль в брокере отсекает claim по `th_lease` и состоянию Item.

---

## 7. Интеграция с доменом: прогресс, финализация, retention

### 7.1 Проблема

Пользователь хочет видеть прогресс и итог батча в **своей** доменной таблице (например, `campaigns.sent`, `campaigns.status`). У этого три причины:
1. Таблицы `th_*` чистятся retention'ом, а итог кампании нужен навсегда.
2. Списки, сортировки и фильтры по доменной таблице («кампании, отсортированные по прогрессу») не должны JOIN'ить нашу.
3. Доменный статус (`completed_with_errors`) должен меняться **ровно тогда**, когда батч реально завершился. Нельзя «батч завершён, а кампания всё ещё running» и нельзя наоборот.

Наивные решения ломаются так:

| Наивно | Что ломается |
|---|---|
| Колбэк-задача в брокере обновляет домен | Два commit'а: батч завершён, колбэк упал или ещё в очереди → домен рассинхронизирован. Retention может удалить батч раньше, чем колбэк выполнится |
| Каждый Item делает `UPDATE campaigns SET sent = sent + 1` | Горячая строка в **доменной** таблице — ровно та проблема, которую решали шардированными счётчиками (§9) |
| Пользователь сам поллит `view()` и копирует | Если поллер отстал дольше retention, данные потеряны. Лишний код в каждом проекте |

### 7.2 Решение: три механизма

```mermaid
flowchart LR
    subgraph th["tallyho"]
        F["Finalizer<br/>CAS в терминальное"]
        SN["Snapshotter<br/>раз в every на батч"]
        PB["Политика ошибок<br/>action=pause"]
        RT["Retention"]
    end
    subgraph user["Код пользователя: tx-хуки"]
        H1["on_finalized<br/>итог + доменный статус"]
        H2["on_progress<br/>снимок прогресса"]
        H3["on_policy_breach<br/>доменный статус paused"]
    end
    DOM[("campaigns")]
    F -->|"одна транзакция"| H1
    SN -->|"одна транзакция"| H2
    PB -->|"одна транзакция"| H3
    H1 --> DOM
    H2 --> DOM
    H3 --> DOM
    RT -.->|"только после финализации<br/>и release, если он требуется"| th
```

| Механизм | Когда вызывается | Гарантия |
|---|---|---|
| **`on_finalized(session, summary)`** | При переходе батча в терминальное состояние: succeeded / completed_with_errors / failed / cancelled | **Ровно один успешный commit**, атомарно с финализацией. Хук упал → финализации нет, повтор с backoff. Батч не станет терминальным, пока хук не закоммитится |
| **`on_progress(session, summary)`** | Не чаще `every` на батч и только если счётчики изменились | Снимки монотонны (`summary.seq`), устаревший снимок не перезапишет новый и не перезапишет итог финализации |
| **`on_policy_breach(session, summary, breach)`** | Политика ошибок сработала с `action="pause"` или `"fail"` | Атомарно с постановкой батча на паузу или провалом |
| **Retention + `release()`** | Удаляет только терминальные деревья старше `retention`, и только после `release()`, если `release_required=True` | Данные не исчезнут раньше, чем домен их забрал |

### 7.3 Как устроена транзакция хука

**Порядок внутри транзакции: сначала хук пользователя, потом наш CAS.**

```
BEGIN
  1. прочитать итоговые счётчики (без блокировок): sum(th_counter) + sum(th_counter_delta), th_metric
     и проверить pending=0 AND (state=sealed OR cancel_requested_at IS NOT NULL)
  2. await on_finalized(session, summary)      ← пользователь блокирует и меняет СВОИ строки
  3. UPDATE th_batch SET state=:final, finished_at=now(), snap_seq=snap_seq+1
       WHERE id=:id AND state IN ('open','sealed') RETURNING      ← CAS
     0 строк → ROLLBACK (другой процесс финализировал; изменения хука откатились вместе с нами)
  4. INSERT th_outbox колбэков; завершить виртуальный Item родителя
COMMIT
```

Почему такой порядок:
* **Порядок блокировок совпадает с кодом пользователя.** В API он обычно пишет «сначала доменная строка, потом `handle.pause(session)`». Порядок «домен → tallyho» везде исключает дедлок между хуком и API-операцией пользователя.
* **Проигравший CAS откатывает и свои изменения домена.** Два процесса могут одновременно начать финализацию одного батча: хук выполнится дважды, но закоммитится ровно один раз.
* **Итог стабилен между шагами 1 и 3.** Когда `pending = 0` и батч `sealed` (или запрошена отмена), новые Items появиться не могут: spawn возможен только из активного Item, внешний `add` после seal или запроса отмены запрещён. `retry_failed` конкурирует с нами через тот же CAS.

Правила для хука:
* Сессия — `AsyncSession`, привязанная к нашему соединению и транзакции. **`commit()`/`rollback()` внутри хука запрещены**: будет исключение.
* Только операции с БД. HTTP, письма, брокер — через колбэк-задачу `on_finalized_task=th.call(...)`, которая ставится в outbox той же транзакцией.
* Хук должен укладываться в `hook_timeout` (по умолчанию 10 с, через `SET LOCAL statement_timeout` и `asyncio.timeout`).
* Хук упал → откат → `hook_attempts += 1`, `hook_error` = текст ошибки, повтор с экспоненциальным backoff (1 с … 5 мин). Батч остаётся `sealed` и **не** финализируется без хука: домен и tallyho не расходятся. Наблюдаемость — метрика `th_hook_failures` и событие `Observer.hook_failed`. Починили код → sweeper повторит сам, или вызовите `handle.retry_finalize()`.

### 7.4 Снимки прогресса

* Snapshotter работает в лидере maintenance. Он хранит **в памяти** расписание «когда следующий снимок» для активных батчей с хуком `progress` (partial-индекс из §5.2). Если счётчики не менялись, в БД ничего не пишется.
* Снимок — отдельная транзакция: `on_progress(session, summary)` → `UPDATE th_batch SET snap_seq = snap_seq + 1 WHERE id AND snap_seq = :seen AND state IN (open, sealed)`. 0 строк означает, что батч финализирован или снимок уже сделан: откат, и изменения хука тоже откатываются. Поэтому **снимок никогда не перезапишет итог финализации**.
* `summary.seq` монотонно растёт в пределах батча, у финализации он тоже больше последнего снимка. Рекомендуемая защита на стороне домена — `WHERE progress_seq < :seq` (пример в §12).
* Цена: `N_активных_батчей / every` чтений счётчиков в секунду. При 10 000 активных батчей и `every=2s` это 5 000 PK-чтений по ~8 строк/с на лидере. Записи — только для изменившихся батчей.

### 7.5 Регистрация хуков

```python
th = Tallyho(
    engine, schema="app", hook_modules=["app.mailing.hooks"]
)  # импортируются при инициализации в КАЖДОМ процессе


# app/mailing/hooks.py
@th.on_finalized("campaign_deliveries")
async def save_result(session: AsyncSession, s: BatchSummary) -> None: ...


@th.on_progress("campaign_deliveries", every=timedelta(seconds=2))
async def save_progress(session: AsyncSession, s: BatchSummary) -> None: ...
```

Защита от «хук не импортирован в этом процессе»: при создании батча в `th_batch.hooks` записывается список хуков, зарегистрированных для `kind`. Если процесс-финализатор не находит у себя требуемый хук, он **не финализирует** батч, пишет ошибку в лог и метрику `th_hook_missing`. Батч дождётся процесса, в котором хук есть (sweeper в maintenance). Тихо пропустить запись итога в домен невозможно.

### 7.6 Retention

| Настройка батча | Поведение |
|---|---|
| `retention=timedelta(days=14)` (по умолчанию) | Дерево удаляется через 14 дней после `finished_at` корня |
| `retention=None` | Хранить вечно |
| `release_required=True` | Удаление только после `handle.release(session)` **и** истечения `retention`. Для случаев, когда домену нужны детали по Items (экспорт упавших получателей) |

Удаление идёт чанками по 1 000 Items (`DELETE ... WHERE ctid IN (SELECT ... LIMIT)`), по деревьям, от листьев к корню. `handle.view()` удалённого батча бросает `BatchPurged`. Итог к этому моменту уже в домене.

---

## 8. Use cases

### 8.1 Конвейер этапов: правила

Этап — обычный под-батч. Новых таблиц состояния нет, есть только связь «кто кого наполняет» (`th_feed`).

```mermaid
flowchart LR
    R["корень<br/>kind=catalog_parse"]
    P["pages<br/>seal продюсером"]
    C["cards<br/>fed_by=[pages]"]
    D["pdfs<br/>fed_by=[cards]"]
    R --- P
    R --- C
    R --- D
    P -->|"spawn into=cards"| C
    C -->|"spawn into=pdfs"| D
    P -->|"spawn в свой батч, depth+1"| P
    P -. "финализирован → seal" .-> C
    C -. "финализирован → seal" .-> D
```

1. **Структура.** `fed_by` ссылается только на под-батчи того же родителя и образует ациклический граф: цикл → ошибка при создании. Самоподпитка (страница → страница) — это spawn в свой батч, а не `fed_by`.
2. **Кто может писать в этап.** У этапа с `fed_by` писатели только задачи самого этапа и задачи его источников (`into=`). Продюсер добавлять в него не может. Иначе spawn бросает `SpawnTargetError`: проверка по кэшу структуры дерева в процессе, без запросов к БД.
   **Почему этого достаточно без блокировок:** пишущая задача активна → её батч-источник не финализирован → этап не может быть закрыт, ведь его закрывают только после финализации **всех** источников. На нарушении этого правила обжигались Oban (гонки graft) и Sidekiq («снаружи добавлять небезопасно»).
3. **Автоматический seal.** В транзакции финализации источника F, после CAS F:
   ```
   для каждого X из th_feed WHERE feeder_id = F (в порядке id):
       SELECT ... FROM th_batch WHERE id = X FOR UPDATE      -- сериализует параллельных источников
       если все источники X терминальны (новый statement → видит закоммиченное):
           UPDATE th_batch SET state = sealed WHERE id = X AND state = open
   COMMIT → try_finalize(X) → если X пуст, он финализируется и каскадом закрывает свои этапы
   ```
   Два источника X финализируются одновременно: тот, кто взял блокировку X вторым, после commit первого видит оба источника терминальными и закрывает X. Страховка — sweeper: «этап open, все источники терминальны» → seal.
4. **Источник с ошибками** (`completed_with_errors`, `failed` или `cancelled`) тоже закрывает этап (`on_feeder_failed="seal"`, по умолчанию): этап доделывает полученное. Вариант `"cancel"` ставит этапу запрос отмены. У Dagster в такой ситуации нижние шаги просто брошены.
5. **Дедупликация до счётчиков.** `key` уникален в целевом батче (`UNIQUE (batch_id, key)`). Spawn — это `INSERT ... ON CONFLICT DO NOTHING RETURNING`: `found += вставлено`, `duplicates += отсечено`. Дедуп постоянный, пока жив батч. Повторно найденная ссылка на уже скачанный PDF не создаст Item.
6. **Лимиты разрастания** — задачи сверх лимита не вставляются и учитываются в `skipped_by_limit` целевого батча, это не ошибка:
   * `max_items` на корне — лимит Items на всё дерево. Счётчик `tree_total` в слотах корня обновляется в той же групповой транзакции. Лимит **мягкий**: параллельные flush'и могут превысить его не больше чем на размер одного flush;
   * `max_depth` на под-батче — глубина самоподпитки: `depth = depth_родителя + 1` при spawn в свой батч, `0` при `into=`.
7. **Порядок финализации в дереве.** Этап финализируется со своим `on_finalized` и завершает виртуальный Item у родителя в той же транзакции. Родитель финализируется только после всех детей: `on_finalized` детей всегда коммитится раньше родительского.
   Терминальный итог ребёнка распространяется вверх: если хотя бы один прямой ребёнок завершился как `completed_with_errors`, `failed` или `cancelled`, родитель без собственной более сильной причины завершится как `completed_with_errors`. Виртуальный Item при этом остаётся `ok`: он означает «под-батч завершён», а не «все его Items успешны».

### 8.2 Карта use cases

```mermaid
flowchart LR
    prod(["Код продюсера"])
    work(["Код задачи"])
    ops(["Оператор / UI"])
    dom(["Доменные таблицы"])
    time(["Время"])

    prod --> UC01["UC-01 Создать батч"]
    prod --> UC02["UC-02 Стриминговое добавление и seal"]
    work --> UC03["UC-03 Выполнить Item"]
    work --> UC04["UC-04 Ретрай и финальная ошибка"]
    work --> UC05["UC-05 Spawn"]
    work --> UC06["UC-06 Sub-batch"]
    time --> UC07["UC-07 Финализация, on_finalized, колбэки"]
    UC07 --> dom
    work --> UC08["UC-08 Завершение в транзакции пользователя"]
    time --> UC09["UC-09 Снимок прогресса в домен"]
    UC09 --> dom
    ops --> UC10["UC-10 Отложенный старт и перенос"]
    ops --> UC11["UC-11 Пауза, продолжение, авто-пауза"]
    ops --> UC12["UC-12 Отмена"]
    ops --> UC13["UC-13 Прогресс и watch"]
    time --> UC14["UC-14 Retention и release"]
    time --> UC15["UC-15 Sweeper: восстановление"]
    ops --> UC16["UC-16 Повтор упавших"]
    work --> UC17["UC-17 Конвейер этапов: into, авто-seal, каскад"]
```

### UC-01 Создать батч в транзакции пользователя

```mermaid
sequenceDiagram
    autonumber
    participant U as Код продюсера
    participant S as AsyncSession пользователя
    participant TH as tallyho
    participant DB as PostgreSQL
    participant R as Relay fast-path
    participant B as Брокер

    U->>S: begin, доменные записи
    U->>TH: th.batch(kind, key, session=S)
    TH->>DB: INSERT th_batch ON CONFLICT (kind,key) DO NOTHING RETURNING
    alt батч с таким key уже есть
        TH-->>U: handle существующего батча
    end
    U->>TH: batch.map(task, rows)
    loop чанки по 1000
        TH->>DB: INSERT th_item, th_outbox через unnest
        TH->>DB: UPSERT th_counter slot total += 1000
    end
    U->>TH: выход из async with → seal
    TH->>DB: UPDATE th_batch SET state=sealed
    TH->>S: after_commit hook
    U->>S: commit
    S-->>R: after_commit → kick(batch_id)
    R->>DB: claim outbox rows SKIP LOCKED
    R->>B: dispatch
    R->>DB: DELETE th_outbox, dispatched += n
    Note over U,B: rollback пользователя → в брокер не уходит ничего
```

### UC-02 Стриминговое добавление и seal

```mermaid
sequenceDiagram
    autonumber
    participant U as Продюсер
    participant TH as tallyho
    participant DB as PostgreSQL
    participant W as Воркеры

    U->>TH: create batch, state=open
    loop пока читаем источник
        U->>TH: add_many(chunk)
        TH->>DB: INSERT items + outbox, total += n, commit
        Note over W: воркеры уже выполняют первые Items
    end
    Note over DB: pending может стать 0 раньше конца чтения,<br/>но state=open → финализации нет
    U->>TH: seal()
    TH->>DB: UPDATE state=sealed, commit
    TH->>DB: после commit — проверка pending == 0 → UC-07
```

### UC-03 Выполнить Item

```mermaid
sequenceDiagram
    autonumber
    participant B as Брокер
    participant MW as th.tracked
    participant C as Completer
    participant DB as PostgreSQL
    participant T as Функция задачи

    B->>MW: вызов задачи со служебным _th
    MW->>C: claim(item)
    C->>DB: групповая tx: state=active, paused_at, INSERT th_lease ON CONFLICT DO NOTHING
    alt дубль или Item терминальный
        C-->>MW: skip
        MW-->>B: успех, задача не вызывалась
    else батч на паузе
        C->>DB: INSERT th_outbox available_at=∞ (parked)
        MW-->>B: успех, задача не вызывалась
    else захвачен
        C-->>MW: ok
        MW->>MW: ContextVar = ItemContext, старт heartbeat
        MW->>T: await task(*args)
        T->>MW: th.item.spawn / incr / ok(label) → в буфер
        T-->>MW: return
        MW->>C: finish(item, result, spawns, metrics)
        C->>DB: групповая tx, см. §9.2
        C-->>MW: future resolved после commit
        MW-->>B: return, брокер фиксирует успех
    end
```

### UC-04 Ретрай и финальная ошибка

```mermaid
sequenceDiagram
    autonumber
    participant B as Брокер
    participant MW as th.tracked
    participant A as Adapter
    participant C as Completer
    participant DB as PostgreSQL

    MW->>MW: задача бросила исключение exc
    MW->>A: retry_verdict(exc)
    alt RETRY, брокер повторит
        MW->>C: release(item, attempt+1)
        C->>DB: DELETE th_lease, attempt += 1
        MW-->>B: пробросить exc, брокер планирует ретрай
    else FINAL
        MW->>C: finish(item, error, label=exhausted или mapped(exc))
        C->>DB: CAS state=error, счётчики, th_item_mark
        MW-->>B: пробросить exc, брокер отправит в DLQ
    end
    opt брокер всё же отправил в DLQ после вердикта RETRY
        B-->>A: DLQ-хук или сверка reconcile_dead
        A->>C: finish(item, error, exhausted), CAS идемпотентен
    end
```

### UC-05 Spawn: динамический fan-out

```mermaid
sequenceDiagram
    autonumber
    participant T as Задача expand_page
    participant Ctx as ItemContext
    participant C as Completer
    participant DB as PostgreSQL
    participant R as Relay
    participant B as Брокер

    T->>Ctx: spawn(send_email, c1, into=send) ... spawn(send_email, c1000, into=send)
    T->>Ctx: spawn(expand_page, after=last_id)
    Note over Ctx: в БД ничего не пишется, только буфер.<br/>into проверяется по кэшу дерева: expand — источник send
    T-->>Ctx: return
    Ctx->>C: finish(parent, ok, spawns=1001)
    C->>DB: BEGIN
    C->>DB: CAS parent → ok
    C->>DB: лимиты: tree_total корня и max_depth → лишнее в skipped_by_limit
    C->>DB: INSERT items ON CONFLICT (batch_id,key) DO NOTHING RETURNING + outbox
    C->>DB: found += вставлено, duplicates += отсечено, ok += 1 у родителя
    C->>DB: COMMIT
    Note over DB: pending не проходит через 0:<br/>+1001 и −1 в одном commit
    C->>R: kick(batch)
    R->>B: dispatch 1001
```

Падение до commit — нет ни детей, ни завершения родителя, и родитель перезапустится. Повтор родителя после commit невозможен: CAS вернёт 0 строк. Дубли детей при повторе исключены ключами.

### UC-06 Sub-batch

```mermaid
sequenceDiagram
    autonumber
    participant T as Задача родителя
    participant C as Completer
    participant DB as PostgreSQL
    participant F as Finalizer

    T->>C: sub_batch(kind=parts) + map(...) → в буфер
    T-->>C: return
    C->>DB: tx: CAS родителя, INSERT th_batch(parent_id), виртуальный th_item(child_batch_id),<br/>items под-батча, счётчики обоих батчей
    Note over DB: для родительского батча виртуальный Item = pending 1
    Note over F: ... Items под-батча выполняются ...
    F->>DB: tx: on_finalized под-батча, CAS → succeeded, виртуальный Item → ok,<br/>ok += 1 у родителя, outbox колбэков
    F->>DB: после commit — проверка финализации родителя, рекурсивно вверх
```

### UC-07 Финализация, `on_finalized` и колбэки

```mermaid
sequenceDiagram
    autonumber
    participant X as Finalizer после commit<br/>Completer / seal / sweeper
    participant DB as PostgreSQL
    participant H as on_finalized пользователя
    participant D as Доменная таблица
    participant R as Relay
    participant CB as Колбэк-задача

    X->>DB: SELECT sum(counter) + sum(delta), state, cancel_requested_at
    alt pending > 0 или (не sealed и отмена не запрошена)
        X-->>X: ничего
    else pending == 0 и (sealed или отмена запрошена)
        X->>DB: BEGIN
        X->>H: await on_finalized(session, summary)
        H->>D: UPDATE campaigns SET status, итоги WHERE id AND status не терминальный
        alt хук бросил исключение
            X->>DB: ROLLBACK, hook_attempts+1, hook_error, повтор с backoff
        else хук ок
            X->>DB: UPDATE th_batch SET state=итог WHERE id AND state IN (open, sealed) RETURNING
            alt 0 строк
                X->>DB: ROLLBACK, изменения хука откатились, другой процесс финализировал
            else CAS выигран
                X->>DB: INSERT th_outbox колбэков, завершить виртуальный Item родителя
                X->>DB: COMMIT
                X->>R: kick
                R->>CB: dispatch, callback_id стабилен для идемпотентности
            end
        end
    end
```

### UC-08 Завершение Item в транзакции пользователя

```mermaid
sequenceDiagram
    autonumber
    participant T as Задача
    participant S as AsyncSession пользователя
    participant DB as PostgreSQL
    participant C as Completer
    participant MW as th.tracked

    T->>S: begin, бизнес-записи
    T->>DB: th.item.complete_in(S): CAS th_item, DELETE th_lease,<br/>INSERT th_counter_delta, дельты th_metric, spawns
    Note over DB: горячие строки th_counter не трогаем:<br/>нет ожидания блокировок, нет 40001 при REPEATABLE READ
    T->>S: commit
    S-->>C: after_commit → fold(batch_id)
    C->>DB: tx: DELETE th_counter_delta WHERE batch_id RETURNING → += в th_counter
    C->>DB: после commit — проверка финализации
    T-->>MW: return
    MW->>MW: Item уже завершён → повторно не пишем
```

### UC-09 Снимок прогресса в доменную таблицу

```mermaid
sequenceDiagram
    autonumber
    participant SN as Snapshotter (лидер)
    participant DB as PostgreSQL
    participant H as on_progress пользователя
    participant D as Доменная таблица

    loop каждые snapshot_tick
        SN->>SN: батчи, у которых наступил срок по расписанию в памяти
        SN->>DB: прочитать счётчики пачкой
        alt счётчики не изменились с прошлого снимка
            SN->>SN: сдвинуть срок в памяти, в БД ничего не пишем
        else изменились
            SN->>DB: BEGIN
            SN->>H: await on_progress(session, summary с seq = snap_seq + 1)
            H->>D: UPDATE campaigns SET sent, progress, progress_seq=:seq WHERE id AND progress_seq меньше :seq
            SN->>DB: UPDATE th_batch SET snap_seq=snap_seq+1 WHERE id AND snap_seq=:seen AND state IN (open, sealed)
            alt 0 строк: батч уже финализирован
                SN->>DB: ROLLBACK, снимок не перезапишет итог
            else
                SN->>DB: COMMIT
            end
        end
    end
```

### UC-10 Отложенный старт и перенос

```mermaid
sequenceDiagram
    autonumber
    participant U as API-код
    participant DB as PostgreSQL
    participant R as Relay scan
    participant B as Брокер

    U->>DB: th.batch(..., start_at=T, session) → items + outbox с available_at=T
    Note over R: relay не видит записи до T
    U->>DB: handle.reschedule(T2, session): UPDATE th_batch.start_at,<br/>UPDATE th_outbox SET available_at=T2 WHERE batch_id
    Note over DB: уже отправленные Items перенос не затрагивает, возвращается их число
    R->>DB: наступил T2 → claim SKIP LOCKED
    R->>B: dispatch
```

### UC-11 Пауза, продолжение, авто-пауза по политике

```mermaid
sequenceDiagram
    autonumber
    participant O as API-код
    participant TH as tallyho
    participant DB as PostgreSQL
    participant MW as th.tracked
    participant B as Брокер
    participant H as on_policy_breach

    O->>DB: UPDATE campaigns SET status=paused, в своей транзакции
    O->>TH: handle.pause(session) в той же транзакции
    TH->>DB: paused_at=now для батча и активных потомков<br/>UPDATE th_outbox SET available_at=∞ WHERE batch_id IN (...)
    B->>MW: уже отправленное сообщение Item батча на паузе
    MW->>DB: claim видит paused_at → park, ack без выполнения
    Note over MW: выполняющиеся Items доделываются
    O->>TH: handle.resume(session) + UPDATE campaigns SET status=running
    TH->>DB: paused_at=NULL, parked → available_at=now
    Note over TH,H: авто-пауза
    TH->>TH: Completer: доля error по labels выше порога после min_processed
    TH->>DB: BEGIN, on_policy_breach(session, summary, breach), paused_at=now, COMMIT
    H->>DB: UPDATE campaigns SET status=paused, pause_reason
```

### UC-12 Отмена

```mermaid
sequenceDiagram
    autonumber
    participant O as API-код
    participant DB as PostgreSQL
    participant MW as th.tracked
    participant T as Выполняющаяся задача
    participant F as Finalizer

    O->>DB: handle.cancel(session): cancel_requested_at=now для дерева, add и spawn запрещены
    O->>DB: чанками: Items из outbox → cancelled, DELETE outbox, cancelled += n
    Note over MW: отправленные, но не начатые → ленивая отмена при claim
    T->>T: th.item.cancelled() == True → кооперативный выход
    F->>DB: когда pending = 0: on_finalized(summary.state=cancelled) + CAS → cancelled
```

### UC-13 Прогресс и watch

```mermaid
sequenceDiagram
    autonumber
    participant UI as UI / SSE
    participant H as BatchHandle
    participant DB as PostgreSQL

    UI->>H: view()
    H->>DB: один SELECT по дереву: sum(th_counter) + sum(th_counter_delta) + th_metric + th_feed + count(th_lease)
    H-->>UI: Progress на каждый батч дерева: found, done по классам, in_flight, expected, ratio, eta, labels
    UI->>H: watch()
    loop
        DB-->>H: NOTIFY th_progress, payload=batch_id, не чаще 1 раза в 500 мс на батч
        H-->>UI: новый BatchView
    end
```

Для UI, который читает доменную таблицу, `watch()` не нужен: там есть снимки из UC-09.

### UC-14 Retention и release

```mermaid
sequenceDiagram
    autonumber
    participant F as Finalizer
    participant CB as Колбэк export_failures
    participant D as Доменная таблица
    participant DB as PostgreSQL
    participant SW as Sweeper retention

    F->>DB: финализация + on_finalized, итог в домене, outbox колбэка
    CB->>DB: handle.items(label=hard_bounce) страницами по th_item_mark
    CB->>D: INSERT campaign_failures ... чанками
    CB->>DB: handle.release(session) в той же транзакции, что и последний чанк
    SW->>DB: корни WHERE finished_at + retention меньше now AND (NOT release_required OR released_at IS NOT NULL)
    SW->>DB: DELETE деревом, чанками по 1000
```

### UC-15 Sweeper: восстановление

```mermaid
sequenceDiagram
    autonumber
    participant SW as Sweeper (лидер)
    participant DB as PostgreSQL
    participant F as Finalizer
    participant R as Relay

    loop каждые sweep_interval
        SW->>DB: th_lease WHERE lease_until меньше now SKIP LOCKED
        SW->>DB: attempt меньше max → DELETE lease, INSERT outbox
        SW->>DB: attempt исчерпан → finish error, label=lease_expired
        SW->>DB: lease у терминального Item → DELETE
        SW->>F: sealed, pending 0, updated_at старше grace или hook_error и backoff истёк → try_finalize
        SW->>DB: deadline_at меньше now → cancel_requested_at, reason=deadline → итог failed
        SW->>DB: этап open, все источники в th_feed терминальны → seal, страховка к UC-17
        SW->>DB: sealed, pending больше 0, но нет lease и outbox → reconcile по count(*)
        SW->>DB: несвёрнутые th_counter_delta старше grace → fold
        SW->>DB: retention (UC-14)
        SW->>R: kick
    end
```

### UC-16 Повтор упавших

```mermaid
sequenceDiagram
    autonumber
    participant O as API-код
    participant DB as PostgreSQL
    participant R as Relay

    O->>DB: retry_failed(labels=[exhausted], session): CAS completed_with_errors / failed → sealed
    O->>DB: чанками по th_item_mark: state=active, attempt=0, error −n, INSERT outbox
    O->>DB: доменный статус меняет сам пользователь в этой же транзакции
    R->>R: отправка → UC-03 … UC-07, on_finalized вызовется снова с новым итогом
```

`on_finalized` после `retry_failed` вызывается повторно. Хук должен быть написан как «установить итог», а не «прибавить к итогу».

`retry_failed` у этапа, который наполняет другие (например, `cards`), возможен, только пока его этапы-получатели не финализированы. Иначе новые Items не смогут никуда добавлять, и будет ошибка `DownstreamFinalized`. Повтор всего конвейера — `retry_failed` на корне: он переоткрывает этапы от источников к получателям.

### UC-17 Конвейер этапов: into, авто-seal, каскад

```mermaid
sequenceDiagram
    autonumber
    participant U as Продюсер
    participant P as pages
    participant C as cards fed_by pages
    participant D as pdfs fed_by cards
    participant DB as PostgreSQL
    participant F as Finalizer

    U->>DB: корень + pages + cards + pdfs + th_feed, одна транзакция
    U->>P: add(parse_page, 1), seal(pages)
    loop страницы
        P->>DB: finish страницы: spawn страниц в pages, spawn карточек into=cards
    end
    Note over C: cards уже работает, пока pages ещё идут.<br/>cards open → даже при pending 0 не финализируется
    loop карточки
        C->>DB: finish карточки: spawn PDF into=pdfs, дубли отсечены ключом
    end
    F->>DB: последняя страница → финализация pages + on_finalized(pages)
    F->>DB: в той же tx: lock cards FOR UPDATE, все источники терминальны → cards sealed
    Note over C: found карточек теперь точный: все карточки<br/>закоммичены вместе с завершением своих страниц
    F->>DB: последняя карточка → финализация cards → pdfs sealed
    F->>DB: последний PDF → финализация pdfs → виртуальный Item корня ok
    F->>DB: все дети финализированы → финализация корня + on_finalized(корень)
    alt в cards не пришло ни одной карточки
        F->>DB: cards sealed при found 0 → сразу финализирован → pdfs sealed → финализирован
    end
```

---

## 9. Счётчики и групповой коммит

Подробный ресёрч — в [COUNTERS.md](COUNTERS.md); разделы про SQLite там устарели.

### 9.1 Два пути записи, одно хранилище

```mermaid
flowchart LR
    subgraph A["Путь A: 99% завершений"]
        a1["th.tracked"] --> a2["Completer buffer"]
        a2 -->|"тик 20 мс или 500 шт"| a3["одна короткая tx"]
        a3 --> ctr[("th_counter<br/>слот процесса")]
    end
    subgraph B["Путь B: complete_in(session)"]
        b1["tx пользователя"] --> b2[("th_counter_delta<br/>только INSERT")]
        b2 -->|"after_commit / sweeper<br/>DELETE ... RETURNING"| fold["fold в Completer"]
        fold --> ctr
    end
    ctr --> read["view / финализация / снимки:<br/>sum(counter) + sum(delta)<br/>одним SELECT"]
    b2 --> read
```

### 9.2 Транзакция Completer (путь A)

```
BEGIN; SET LOCAL lock_timeout = '5s'
 1. SELECT id FROM th_item WHERE id = ANY(:ids) ORDER BY id FOR UPDATE      -- порядок блокировок
 2. UPDATE th_item SET state, label, result, error, finished_at
      WHERE id = ANY(:ids) AND state = active RETURNING id, batch_id, state, label, weight
                                                                            -- считаем только вернувшиеся
 3. DELETE FROM th_lease WHERE item_id = ANY(:ids)
 4. лимиты spawn: sum(tree_total) корней затронутых деревьев (слоты, PK) и depth → лишнее в skipped_by_limit
 5. INSERT spawned items + outbox (unnest) ON CONFLICT (batch_id, key) DO NOTHING RETURNING
      → found += вставлено, duplicates += отсечено (по целевым батчам)
 6. INSERT th_item_mark для помеченных
 7. expect(n): UPDATE th_batch SET expected_total = GREATEST(expected_total, :n) — редкая запись
 8. агрегировать в памяти → UPSERT th_counter (batch_id, slot процесса) ORDER BY batch_id,
      включая tree_total у корней
 9. UPSERT th_metric (batch_id, name, slot) ORDER BY batch_id, name
COMMIT → resolve futures → kick relay → try_finalize для затронутых sealed-батчей
```

Порядок блокировок: `[доменные строки пользователя] → th_batch → th_item (по id) → th_counter (по batch_id, slot) → th_metric`. Нарушение — баг. Ловится стресс-тестом со счётчиком дедлоков.

### 9.3 Чтение

```sql
SELECT
  sum(c.total)  + coalesce(d.total, 0)  AS total,
  sum(c.ok)     + coalesce(d.ok, 0)     AS ok,
  ...
FROM th_counter c
LEFT JOIN LATERAL (
  SELECT sum(d_total) total, sum(d_ok) ok, ... FROM th_counter_delta WHERE batch_id = :b
) d ON true
WHERE c.batch_id = :b
GROUP BY d.total, d.ok, ...;
```

* `found = total` — уникальные Items (ретраи и дубли не входят)
* `done = ok + skip + error + cancelled`, `pending = found − done`
* `in_flight` — точное число строк `th_lease` батча; `queued = pending − in_flight`

### 9.4 Модель прогресса

Прогресс батча — это `Progress` (§3.4), который собирается в Python из уже прочитанных строк счётчиков. Дополнительных запросов к БД нет, кроме `in_flight` (count по индексу `th_lease(batch_id)`).

**Ожидаемый итог (`expected`)** — по правилам, от точного к оценке:

| Условие | `expected` | `expected_is_estimate` |
|---|---|---|
| батч `sealed` или терминальный | `found` | нет |
| задан `expect(n)` или `expected_total` | `max(found, expected_total)` | да, пока не sealed |
| есть источники `fed_by`, выборка достаточна | оценка по ветвлению (ниже) | да |
| иначе | `None` — показываем только «найдено» | — |

**Оценка по ветвлению** (оценщик Кнута для размера дерева). Каждый Item этапа X закоммичен вместе с завершением своего родителя в источнике F. Поэтому `found_X / done_F` — точное среднее число детей у **завершённых** родителей:

```
ratio_X       = found_X / Σ done_F                       по всем источникам F
expected_X    = max(found_X, ratio_X × Σ expected_F)     рекурсивно вверх по fed_by
estimate_basis_X = Σ done_F                              на скольких родителях построена оценка
```

Оценку не показываем, пока `estimate_basis < min(estimate_min_basis, estimate_min_share × expected_F)`: нужно 20 завершённых родителей **или** 5% источника, что наступит раньше. У каталога из 24 страниц оценка появится со 2-й страницы, у аудитории в 10 000 — с 20-й. Неточность одна: у ещё не завершённых родителей детей может быть больше или меньше среднего. Оценка сходится по ходу работы и становится точной, когда источники закончились и этап закрыт.

**Доля (`ratio`)** — по весам задач, с оценкой объёма:

```
ratio_батча = w_done / (w_total / found × expected)        если expected известен
ratio_корня = Σ w_done_детей / Σ ожидаемый w_total_детей   по дереву
```

`ratio` может немного откатиться назад, если оценка выросла. Библиотека отдаёт честные числа; рецепт для домена — `progress = GREATEST(progress, :ratio)` в `on_progress`.

**ETA** — время до опустошения, а не процент: `(expected − done) / скорость`, где скорость — экспоненциальное скользящее среднее `done` в секунду по снимкам (окно `eta_window`, по умолчанию 60 с). Считается в Snapshotter и в `watch()`, не хранится. Без `expected` ETA нет.

**Собственный прогресс задачи.** `th.item.progress(done, total)` пишется в `th_lease` вместе с ближайшим heartbeat — лишних транзакций нет. Он виден в `handle.in_flight()`: id, возраст lease, попытка, `progress_done/progress_total`. На общий прогресс батча не влияет, служит для отладки долгих и застрявших задач.

---

## 10. Гарантии и отказы

**Семантика:**
* выполнение задач — at-least-once;
* учёт — идемпотентный;
* финализация — ровно один commit вместе с `on_finalized`;
* колбэки — exactly-once постановка и at-least-once выполнение со стабильным `callback_id`.

| Отказ | Защита | Время восстановления |
|---|---|---|
| Падение между commit и dispatch | Outbox + relay scan | `relay_grace` (5 с) |
| Брокер доставил дважды | claim через `th_lease` + CAS state | мгновенно |
| Воркер убит посреди задачи | lease + heartbeat → sweeper | `lease_ttl` (60 с) |
| Воркер убит посреди flush Completer | транзакция откатилась, задача не вернула результат → повтор брокера | как у брокера |
| Две параллельные «последние» задачи | проверка после commit + CAS финализации | мгновенно |
| Пропущенная проверка финализации | sweeper по `updated_at` | `finalize_grace` (30 с) |
| `on_finalized` упал (баг, блокировка, таймаут) | откат всей финализации, батч остаётся `sealed`, повтор с backoff | после починки ≤ 5 мин или `retry_finalize()` |
| Хук не импортирован в процессе | финализация отложена до процесса с хуком, метрика `th_hook_missing` | как только maintenance подберёт |
| Снимок прогресса опоздал и пришёл после итога | CAS по `snap_seq` и `state` → откат вместе с изменениями хука | — |
| Retention раньше, чем домен забрал детали | `release_required` + `release()` | — |
| Этап закрылся, пока в него ещё добавляют | правило записи: писать могут только сам этап и его источники, а источник с живой задачей не финализирован (§8.1) | невозможно по построению |
| Два источника этапа финализировались одновременно, и этап не закрылся | `FOR UPDATE` строки этапа сериализует проверку + sweeper | мгновенно / `sweep_interval` |
| Этап никто не наполнил (0 Items) | закрытый пустой батч финализируется сразу, каскадом дальше | мгновенно |
| Источник упал | считается финализированным: этап закрывается (`seal`) или отменяется (`cancel`) | мгновенно |
| Дубликат при spawn | `ON CONFLICT DO NOTHING RETURNING` до счётчиков, `duplicates += n` | — |
| Бесконечное разрастание (циклические ссылки, ошибка парсера) | `max_items` на дерево, `max_depth` на самоподпитку → `skipped_by_limit` | — |
| Часы воркеров расходятся | все сроки (`lease_until`, `start_at`, `deadline_at`, `available_at`, retention) считаются по `now()` БД, а не по локальным часам процесса | — |
| Колбэк не отправлен | outbox, вставлен в той же tx, что и CAS | `relay_grace` |
| Дрейф счётчика | reconcile по `count(*)` | цикл sweeper'а |
| Дедлок | глобальный порядок блокировок + retry `40P01` | мгновенно |
| Чужая блокировка | `lock_timeout` + retry с backoff | ≤ 5 с |
| Задачи висят по бизнес-причине | `deadline` батча | как задано |
| Долгая транзакция в кластере | не ломает корректность, деградирует скорость. Мониторинг `backend_xmin`, рекомендации в доке | — |

---

## 11. Публичный API и адаптер flexiq

### 11.1 Установка

```python
from tallyho import Tallyho
from tallyho.adapters.flexiq import FlexiqAdapter

th = Tallyho(engine, schema="app", hook_modules=["app.mailing.hooks"])
fq = FlexiqAdapter(queue)  # flexiq.Queue пользователя
th.install(fq)  # системная задача tallyho.system и DLQ-хук


@fq.task(max_retries=4)  # = queue.task(...)(th.tracked(fn)), см. §11.3
async def my_task(x: int) -> None: ...


await th.migrate()  # или ревизии Alembic: upgrade(..., version=1), затем version=2
```

### 11.2 Сводка

| Область | Методы |
|---|---|
| Батч | `th.batch(kind, key=, start_at=, on_succeeded=, on_completed_with_errors=, on_failed=, on_cancelled=, on_finalized_task=, failure_policy=, max_in_flight=, expected_total=, max_items=, deadline=, retention=, release_required=, session=)` → `BatchBuilder`: `add`, `map`, `add_calls`, `sub_batch`, `expect`, `seal` |
| Под-батч / этап | `builder.sub_batch(key, fed_by=[...], on_feeder_failed="seal" или "cancel", max_in_flight=, max_depth=, expected_total=, failure_policy=, on_...=)` — те же параметры, что у батча, кроме `retention`/`release_required`/`max_items` (наследуются от корня) |
| Поиск | `th.handle(batch_id)`, `th.find(kind, key)`, `handle.child(key)` |
| Handle | `view`, `watch`, `wait`, `in_flight(limit=)`, `reschedule`, `pause`, `resume`, `cancel`, `retry_failed(labels=)`, `retry_finalize`, `release`, `items(label=)` |
| Задача | `th.item.id()`, `spawn(fn, *args, into=, key=, **kwargs)`, `spawn_call(call, into=)`, `sub_batch`, `expect(n, into=)`, `progress(done, total)`, `incr`, `ok(label=, result=)`, `skip(label)`, `error(label, detail=)`, `complete_in(session)`, `cancelled()`, `current()` |
| Вызовы | `th.call(fn, *args, **kwargs).opts(key=, weight=, queue=)` — типизировано через `ParamSpec` |
| Tx-хуки | `@th.on_finalized(kind)`, `@th.on_progress(kind, every=)`, `@th.on_policy_breach(kind)` |
| Политики | `th.FailurePolicy.continue_() / fail_fast() / threshold(ratio=, min_processed=, labels=, action="fail" или "pause")` |

`into=` — ключ под-батча внутри дерева (`"cards"`) или `BatchHandle`. `key=` — ключ дедупликации Item в целевом батче. Для URL рекомендуем нормализованный адрес без фрагмента, как `uniqueKey` у Crawlee.

`max_in_flight` действует **на этот экземпляр батча**. Глобальный лимит на тип задачи для всех батчей сразу — это забота брокера (flexiq `max_concurrent`, `rate_limit`). Это разделение важно: у Airflow `max_active_tis_per_dag` неожиданно действует на все запуски.

Окно считается по узкой таблице `th_window`: relay при захвате записи outbox вставляет строку `(item_id, batch_id)`, завершение Item её удаляет и возвращает в очередь столько запаркованных записей батча, сколько мест освободилось. Захват по батчу с окном сериализуется `pg_try_advisory_xact_lock`: занятый батч relay пропускает до следующего прохода. Записи сверх окна паркуются (`available_at = ∞`), scan relay страхует возврат мест. Строка окна ключом по `item_id`, поэтому повторный захват после падения relay место не удваивает.

Метки итога — свободные строки. По умолчанию `ok()` без label → `"ok"`, исчерпанные попытки → `error("exhausted")`, lease истёк на последней попытке → `error("lease_expired")`, отмена → `cancelled`. `error()` по умолчанию помечается в `th_item_mark`, `ok()`/`skip()` — нет (переопределяется `mark=`).

### 11.3 Адаптер flexiq

Основано на чтении исходников `ByteVeda/flexiq` (master `7e2b5c2`, 2026-09-29) и wheel `flexiq==2.0.0`. Живой воркер не запускали — поведение подтверждаем контрактными тестами адаптера.

| Факт о flexiq | Следствие | Решение в адаптере |
|---|---|---|
| Нет своего job id при enqueue: id генерирует Rust (`Uuid::now_v7()`) | `item.id` ≠ id джобы flexiq | Relay добавляет в kwargs служебный `_th={"i": item_id, "b": batch_id, "r": effective_max_retries}` (`r` нужен runtime, потому что `current_job` лимит не показывает), обёртка `th.tracked` вынимает его до вызова функции. Kwargs переносятся в DLQ, по ним идёт сверка. **`metadata` и `notes` пользователя не трогаем** (§11.4) |
| Middleware только синхронные `before/after`, around-хука нет; `on_retry/on_dead_letter` вызываются вне задачи с `SimpleNamespace(id, task_name)` | На sync-хуках нельзя `await` Completer | **`@fq.task(...)` = `queue.task(...)(th.tracked(fn))`**: обёртка — `async def` в том же event loop, что и задача, то есть настоящий around. `functools.wraps` сохраняет `module.qualname`, имя задачи не меняется |
| Async-задачи идут в одном event loop на процесс (поток `flexiq-async-executor`, семафор `async_concurrency=100`) | Completer должен жить в этом loop | Completer создаётся лениво в loop первой задачи. Отслеживаемые задачи — только `async def` (проверка при декорировании) |
| Prefork-пул исполняет async-задачу через `asyncio.run` в новом event loop на каждую джобу, на Windows — `NotImplementedError` (спайк T8.0) | Completer и lease не переживают джобу | v1 поддерживает только `pool="thread"`. Prefork — ошибка при `install` |
| В задаче известен `current_job.retry_count`, но не `max_retries`; решение «ретрай или DLQ» принимает Rust **после** задачи (`retry_on/dont_retry_on`, `retry_budget`, circuit breaker) | Задача не знает точно, последняя ли это попытка | `retry_verdict(exc)` считает по конфигу `TaskWrapper` и `retry_count`. Страховка: событие `JOB_DEAD` (`queue.on_event`, пул `flexiq-events`) через `loop.call_soon_threadsafe` + периодическая сверка `dead_letters_after(cursor)` → `get_job(original_job_id)` → `_th` из payload (D-014) → `finish(error)` с идемпотентным CAS |
| Нет transactional enqueue: у flexiq свой пул соединений в Rust | Без нашего outbox — dual write | Наш outbox и relay обязательны. Это прямая ценность библиотеки для flexiq |
| `idempotency_key` дедуплицирует только пока джоба pending/running | Повтор relay после падения может создать дубль | Relay передаёт `idempotency_key=f"th:{item_id}"`. Поздние дубли отсекает наш claim |
| `aenqueue_many` — это sync `enqueue_many` в общем `ThreadPoolExecutor(max_workers=2)`; один набор `task_name, queue, priority, max_retries, timeout` на вызов; `None` берёт умолчания Queue, а не `@task`; дубль `idempotency_key` роняет всю пачку | Узкое место отправки, потеря опций задачи | Relay передаёт опции задачи явно, группирует по `(task_name, queue, priority, max_retries, timeout)`, шлёт чанками по 1 000 через **свой** executor; при дубле ключа — поштучный `enqueue` |
| Нет per-job heartbeat; мёртвый воркер обнаруживается через ~43 с (порог 30 с + heartbeat воркеров + цикл reaper), его джобы уходят в retry и тратят попытку | Ретрай flexiq может прийти при ещё живом нашем lease | Claim при живом чужом lease отдаёт успех. Item остаётся за lease, sweeper переотправит его по истечении. Задержка ≤ `lease_ttl` |
| `retry_dead`, `replay` и авто-ретраи DLQ создают **новый** job id; kwargs (и `_th`) переносятся, `metadata` пользователя — нет (`retry_dead` добавляет служебные ключи, `replay` заменяет) | Повтор из UI flexiq исполнит Item повторно | Claim видит, что Item терминальный, → no-op. Перезапуск упавших — только `handle.retry_failed()` |
| Встроенные `group/chord` — оркестрация в потоке вызывающего без записи в хранилище; `Workflow` — статичный DAG без добавления детей в работающий граф; прогресса группы нет | — | Не конфликтуем: tallyho закрывает то, чего во flexiq нет |
| Проект молодой: 7 месяцев, 2 мажорные версии за 3 недели, ~20 звёзд | Риск ломающих изменений | Адаптер изолирован, `flexiq>=2.0,<3`, контрактные тесты против каждого релиза flexiq в CI |

```mermaid
sequenceDiagram
    autonumber
    participant R as Relay
    participant Q as flexiq Queue
    participant X as flexiq async executor loop
    participant W as th.tracked обёртка
    participant C as Completer в том же loop
    participant F as Функция пользователя
    participant H as on_dead_letter (sync)

    R->>Q: enqueue_many(task, kwargs_list с _th={i,b,r}, metadata, idempotency_key=th:item)
    Q->>X: run_coroutine_threadsafe(job)
    X->>W: await wrapper(*args, _th=...)
    W->>C: claim(item)
    alt дубль / терминальный / чужой живой lease
        W-->>X: return None → flexiq: success
    else захвачен
        W->>F: await fn(*args, **kwargs без _th)
        alt успех
            W->>C: await finish(ok)
            W-->>X: return result
        else исключение
            W->>W: retry_verdict(exc, current_job.retry_count)
            alt FINAL
                W->>C: await finish(error)
            else RETRY
                W->>C: await release(item)
            end
            W-->>X: raise exc → Rust решает retry или DLQ
        end
    end
    opt Rust отправил в DLQ вопреки вердикту RETRY
        Q->>H: on_dead_letter(ctx.id)
        H->>C: call_soon_threadsafe(finish_dead(job_id)) → aget_job → kwargs._th → item
    end
```

### 11.4 Опции постановки flexiq

Отслеживаемая задача ставится через наш outbox и relay, но для пользователя это должно выглядеть как обычный `apply_async`. Все параметры постановки flexiq задаются в `th.call(...).opts(...)` или `spawn(..., opts=...)`. Они сохраняются в `payload` Item и передаются в `enqueue_many` при отправке.

| Опция flexiq | Поведение | Проверка в приёмке |
|---|---|---|
| позиционные и именованные аргументы, значения по умолчанию, `*args/**kwargs` | передаются как есть; сериализатор flexiq (cloudpickle/msgpack/cbor) | A-FQ-01 |
| `metadata` (JSON-строка) | **байт в байт**; наш служебный идентификатор туда не пишется | A-FQ-02 |
| `notes` (dict ≤ 15 ключей, ≤ 4096 байт) | **без изменений**; валидация flexiq срабатывает при постановке в продюсере, а не в relay | A-FQ-03 |
| `priority`, `queue`, `max_retries`, `timeout`, `result_ttl` | передаются как есть | A-FQ-04 |
| `expires` | передаётся как есть. Просроченную джобу flexiq не выполнит, поэтому relay при отправке пишет `th_expiry(item_id, expires_at)` — узкую side-таблицу, только для Items с `expires`. Claim удаляет строку; sweeper завершает не захваченные вовремя Items как `error("expired")`. Иначе такой Item висел бы в `dispatched` вечно | A-FQ-04 |
| `delay` | отсчитывается от момента отправки relay'ем. Отложенный старт всего батча — `start_at` | A-FQ-05 |
| `idempotency_key` / `unique_key` / `idempotent` | если пользователь задал свой ключ, передаётся его ключ, а повторную отправку отсекает наш claim. Иначе наш `th:{item_id}` | A-FQ-06 |
| `depends_on` | **не поддерживается** для отслеживаемых задач: id джоб flexiq неизвестны при постановке. Явная ошибка `UnsupportedOption`, альтернатива — этапы `fed_by` | A-FQ-07 |
| `debounce*`, `@task(batch=...)` | **не поддерживается**: flexiq сливает или буферизует задачи в памяти, и это ломает правило «один Item — одна джоба». Ошибка при декорировании или постановке | A-FQ-07 |
| параметры задачи (`retry_on`, `dont_retry_on`, `retry_backoff`, `retry_delays`, `retry_budget`, `circuit_breaker`, `soft_timeout`, `rate_limit`, `max_concurrent`, `middleware`, `inject`, `serializer`, `codecs`, `predicate`) | работают как у обычной задачи flexiq; `retry_verdict` учитывает фильтры ретраев, бюджет и breaker страхуются сверкой с DLQ | A-FQ-08 … A-FQ-12 |

---

## 12. Сквозной пример: email-рассылки

### 12.1 Требования и разделение ответственности

* Пользователь создаёт **кампанию**: получатели (аудитория), отправители (почтовые ящики), письмо, дата запуска.
* У кампании есть прогресс по отправленным письмам с разбивкой по исходам.
* Статусы кампании: `draft`, `scheduled`, `running`, `paused`, `completed`, `completed_with_errors`, `failed`, `cancelled`.

| Что | Где | Кто меняет |
|---|---|---|
| Кампания: письмо, дата, аудитория, ящики, **статус**, **итоги**, **снимок прогресса** | `campaigns` пользователя | API пользователя + tx-хуки |
| Контакты, отписки, suppression list | таблицы пользователя | домен |
| Разворачивание аудитории, доставка каждому получателю, живые счётчики | дерево `th_*`: корень + этапы `expand` → `send` | tallyho |
| Список проблемных получателей навсегда (опционально) | `campaign_failures` пользователя | колбэк-экспорт + `release()` |
| События webhook (delivered, bounce, complaint) | `suppressions` / `delivery_events` пользователя | домен |

Граф статусов кампании — **код пользователя**: `if`, словарь или python-statemachine, на его выбор. tallyho о нём не знает.

**Структура дерева** — двухэтапный конвейер (§8.1):

```mermaid
flowchart LR
    R["campaign_deliveries<br/>key=campaign:42, start_at"]
    E["expand<br/>страницы аудитории по 1000"]
    S["send<br/>fed_by=[expand], max_in_flight=500,<br/>expected_total=размер аудитории"]
    R --- E
    R --- S
    E -->|"spawn следующей страницы"| E
    E -->|"spawn send_email into=send, key=email"| S
    E -. "финализирован → seal" .-> S
```

`send` начинает отправку с первой же страницы, не дожидаясь конца разворачивания. Он закрывается сам, когда разобрана последняя страница. Дубли адресов отсекаются ключом и видны в `duplicates`.

### 12.2 Статусы кампании (домен пользователя)

```mermaid
stateDiagram-v2
    [*] --> draft
    draft --> scheduled : schedule, создаётся дерево со start_at
    scheduled --> scheduled : reschedule
    scheduled --> draft : unschedule, дерево отменяется
    scheduled --> running : первая задача expand_audience
    running --> paused : pause / on_policy_breach
    paused --> running : resume
    running --> completed : on_finalized, succeeded
    running --> completed_with_errors : on_finalized
    running --> failed : on_finalized, failed
    paused --> completed : on_finalized, in-flight доделались на паузе
    paused --> completed_with_errors : on_finalized
    draft --> cancelled
    scheduled --> cancelled : cancel, дерево отменяется
    running --> cancelled : cancel
    paused --> cancelled : cancel
    completed --> [*]
    completed_with_errors --> [*]
    failed --> [*]
    cancelled --> [*]
```

Переходы `paused → completed*` обязательны: если на паузу нажали, когда оставались только выполняющиеся письма, дерево может завершиться во время паузы. `on_finalized` должен уметь закрыть кампанию из `paused`.

### 12.3 Метки итога доставки (labels)

| Label | Класс | Когда |
|---|---|---|
| `sent` | ok | провайдер принял письмо |
| `recipient_not_found` | skip | контакт удалён |
| `unsubscribed` | skip | получатель отписан |
| `suppressed` | skip | адрес в suppression list |
| `invalid_address` | error | некорректный адрес |
| `hard_bounce` | error | синхронный 5xx «ящик не существует» → в suppressions |
| `rejected` | error | провайдер отверг по политике или контенту |
| `exhausted` | error | временные ошибки (4xx) исчерпали ретраи |
| — | cancelled | кампания отменена до отправки |

Временный отказ (`421`, `451`) — это исключение `TemporaryMailError` и ретрай брокера, а не label. Поздние события (`delivered`, асинхронный bounce, `complaint`) приходят через webhook после завершения Item и живут в домене. Items tallyho неизменяемы.

### 12.4 Код

#### Доменная модель пользователя

```python
class Campaign(Base):
    __tablename__ = "campaigns"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
    subject: Mapped[str]
    html_template: Mapped[str]
    audience_id: Mapped[int]
    mailbox_ids: Mapped[list[int]] = mapped_column(ARRAY(Integer))
    scheduled_at: Mapped[datetime | None]

    status: Mapped[str] = mapped_column(default="draft")
    pause_reason: Mapped[str | None]
    batch_id: Mapped[UUID | None]

    # снимок прогресса и итоги — принадлежат домену, переживают retention
    audience_size: Mapped[int | None]
    sent: Mapped[int] = mapped_column(default=0)
    skipped: Mapped[int] = mapped_column(default=0)
    failed: Mapped[int] = mapped_column(default=0)
    duplicates: Mapped[int] = mapped_column(default=0)
    breakdown: Mapped[dict] = mapped_column(JSONB, default=dict)
    progress: Mapped[float] = mapped_column(default=0.0)
    progress_seq: Mapped[int] = mapped_column(default=0)
    finished_at: Mapped[datetime | None]


class Contact(Base):  # keyset-пагинация по (audience_id, id)
    __tablename__ = "contacts"
    id: Mapped[int] = mapped_column(primary_key=True)
    audience_id: Mapped[int]
    email: Mapped[str]
    unsubscribed_at: Mapped[datetime | None]
    deleted_at: Mapped[datetime | None]
    __table_args__ = (Index("ix_contacts_audience_id_id", "audience_id", "id"),)


class Suppression(Base):
    __tablename__ = "suppressions"
    email: Mapped[str] = mapped_column(primary_key=True)
    reason: Mapped[str]
```

#### Команды кампании (API пользователя)

```python
KIND = "campaign_deliveries"
ACTIVE = ("scheduled", "running", "paused")


async def schedule(session: AsyncSession, campaign_id: int, at: datetime) -> None:
    c = await session.get(Campaign, campaign_id, with_for_update=True)
    if c.status != "draft":
        raise Conflict(c.status)
    size = await count_audience(session, c.audience_id)
    async with th.batch(kind=KIND, key=f"campaign:{c.id}", start_at=at, session=session) as root:
        expand = root.sub_batch("expand")
        send = root.sub_batch(
            "send",
            fed_by=[expand],
            expected_total=size,  # прогресс виден сразу, до конца разворачивания
            max_in_flight=500,
            failure_policy=th.FailurePolicy.threshold(
                ratio=0.05,
                min_processed=500,
                labels=["hard_bounce"],
                action="pause",
            ),
        )
        await expand.add(expand_audience, c.id, after_id=0)
    # выход из async with: seal корня и этапов без fed_by (expand); send закроется сам
    c.status, c.scheduled_at, c.batch_id, c.audience_size = "scheduled", at, root.handle.id, size
    # commit делает вызывающий: доменная запись и дерево — атомарно


async def reschedule(session, campaign_id: int, at: datetime) -> None:
    c = await session.get(Campaign, campaign_id, with_for_update=True)
    if c.status != "scheduled":
        raise Conflict(c.status)
    await th.handle(c.batch_id).reschedule(at, session=session)  # на всё дерево
    c.scheduled_at = at


async def pause(session, campaign_id: int) -> None:
    c = await session.get(Campaign, campaign_id, with_for_update=True)  # домен → tallyho
    if c.status != "running":
        raise Conflict(c.status)
    await th.handle(c.batch_id).pause(session=session)  # каскадом на expand и send
    c.status, c.pause_reason = "paused", "manual"


async def resume(session, campaign_id: int) -> None:
    c = await session.get(Campaign, campaign_id, with_for_update=True)
    if c.status != "paused":
        raise Conflict(c.status)
    await th.handle(c.batch_id).resume(session=session)
    c.status, c.pause_reason = "running", None


async def cancel(session, campaign_id: int) -> None:
    c = await session.get(Campaign, campaign_id, with_for_update=True)
    if c.status not in (*ACTIVE, "draft"):
        raise Conflict(c.status)
    if c.batch_id:
        await th.handle(c.batch_id).cancel(session=session)  # итог придёт в on_finalized(cancelled)
    else:
        c.status = "cancelled"
```

`action="pause"` у политики ставит на паузу **всё дерево**: при всплеске жёстких отказов разворачивание аудитории тоже останавливается.

#### Tx-хуки (`app/mailing/hooks.py`)

Хуки регистрируются на `kind` корня и получают сводку всего дерева: `s.children["send"]`. Хук `on_policy_breach` вызывается для `kind` батча, где сработала политика. Если там хука нет, вызывается хук `kind` корня, а `breach.batch_key` говорит, где именно.

```python
FINAL = {
    BatchState.SUCCEEDED: "completed",
    BatchState.COMPLETED_WITH_ERRORS: "completed_with_errors",
    BatchState.FAILED: "failed",
    BatchState.CANCELLED: "cancelled",
}


def _figures(s: BatchSummary) -> dict:
    send = s.children["send"]
    return {
        "sent": send.labels.get("sent", 0),
        "skipped": send.progress.skip,
        "failed": send.progress.error,
        "duplicates": send.progress.duplicates,
        "breakdown": send.labels,
        "progress": send.progress.ratio or 0.0,
        "progress_seq": s.seq,
    }


@th.on_finalized(KIND)
async def save_result(session: AsyncSession, s: BatchSummary) -> None:
    await session.execute(
        update(Campaign)
        .where(Campaign.batch_id == s.id, Campaign.status.in_(ACTIVE))
        .values(status=FINAL[s.state], finished_at=s.finished_at, **_figures(s))
    )
    # «установить итог», а не «прибавить»: после retry_failed хук вызовется снова


@th.on_progress(KIND, every=timedelta(seconds=2))
async def save_progress(session: AsyncSession, s: BatchSummary) -> None:
    f = _figures(s)
    await session.execute(
        update(Campaign)
        .where(Campaign.batch_id == s.id, Campaign.progress_seq < s.seq)  # монотонность снимков
        .values(
            **f, progress=func.greatest(Campaign.progress, f["progress"])
        )  # полоска не едет назад
    )


@th.on_policy_breach(KIND)
async def auto_pause(session: AsyncSession, s: BatchSummary, breach: PolicyBreach) -> None:
    await session.execute(
        update(Campaign)
        .where(Campaign.batch_id == s.id, Campaign.status == "running")
        .values(status="paused", pause_reason=f"{breach.labels} rate {breach.ratio:.1%}")
    )
```

#### Задачи

`fq.task(...)` принимает те же параметры, что и `queue.task(...)` flexiq, и оборачивает функцию в `th.tracked`.

```python
PAGE = 1000


@fq.task(max_retries=5)
async def expand_audience(campaign_id: int, after_id: int) -> None:
    """Этап expand: одна страница аудитории. Число страниц заранее неизвестно."""
    async with db.begin() as s:
        c = await s.get(Campaign, campaign_id)
        if after_id == 0:  # первая страница = фактический старт кампании, доменный переход
            await s.execute(
                update(Campaign)
                .where(Campaign.id == campaign_id, Campaign.status == "scheduled")
                .values(status="running")
            )
        contacts = (
            await s.scalars(
                select(Contact)
                .where(Contact.audience_id == c.audience_id, Contact.id > after_id)
                .order_by(Contact.id)
                .limit(PAGE)
            )
        ).all()

    boxes = c.mailbox_ids
    for ct in contacts:
        th.item.spawn(
            send_email,
            campaign_id,
            ct.id,
            mailbox_id=boxes[ct.id % len(boxes)],
            into="send",
            key=normalize_email(ct.email),
        )  # дедуп адресов
    if len(contacts) == PAGE:
        th.item.spawn(expand_audience, campaign_id, after_id=contacts[-1].id)  # в свой этап
    # всё записывается атомарно с завершением этой задачи


@fq.task(max_retries=4, retry_on=[TemporaryMailError], retry_backoff=2.0, max_retry_delay=300)
async def send_email(campaign_id: int, contact_id: int, mailbox_id: int) -> None:
    async with db.begin() as s:
        contact = await s.get(Contact, contact_id)
        if contact is None or contact.deleted_at:
            return th.item.skip("recipient_not_found")
        if contact.unsubscribed_at:
            return th.item.skip("unsubscribed")
        if await s.get(Suppression, normalize_email(contact.email)):
            return th.item.skip("suppressed")
        c = await s.get(Campaign, campaign_id)
    if not is_valid_email(contact.email):
        return th.item.error("invalid_address")
    if th.item.cancelled():
        return

    try:
        message_id = await mail_provider.send(
            from_=await mailbox_address(mailbox_id),
            to=contact.email,
            subject=c.subject,
            html=render(c.html_template, contact),
            headers={"X-Campaign": str(campaign_id)},
        )
    except HardBounce as e:
        async with db.begin() as s:  # suppression и итог Item — одной транзакцией
            s.add(Suppression(email=normalize_email(contact.email), reason="hard_bounce"))
            th.item.error("hard_bounce", detail=str(e))
            await th.item.complete_in(s)
        return
    except Rejected as e:
        return th.item.error("rejected", detail=str(e))
    # TemporaryMailError пробрасывается → ретрай flexiq → error("exhausted") на последней попытке
    th.item.ok("sent", result={"message_id": message_id})
```

#### Чтение прогресса

UI читает **только доменную таблицу**. Живой прогресс приходит снимками раз в 2 с, итог — атомарно с финализацией, и после retention всё остаётся на месте.

```python
@router.get("/campaigns/{id}")
async def get_campaign(id: int, session=Depends(db)):
    c = await session.get(Campaign, id)
    return {
        "status": c.status,
        "progress": c.progress,
        "sent": c.sent,
        "skipped": c.skipped,
        "failed": c.failed,
        "duplicates": c.duplicates,
        "breakdown": c.breakdown,
    }
```

### 12.5 Сквозная последовательность

```mermaid
sequenceDiagram
    autonumber
    actor User as Пользователь
    participant API as API пользователя
    participant D as campaigns
    participant TH as tallyho
    participant BR as flexiq
    participant EX as expand_audience
    participant SE as send_email
    participant MP as Mail provider (мок)

    User->>API: POST /campaigns → draft
    User->>API: POST /schedule
    API->>D: status=scheduled, batch_id
    API->>TH: корень со start_at, этапы expand и send, 1 Item expand — одна транзакция с доменом
    Note over TH: ... start_at ...
    TH->>BR: relay отправляет expand_audience
    BR->>EX: after_id=0
    EX->>D: scheduled → running
    loop страницы аудитории
        EX->>TH: spawn 1000 send_email into=send + следующая страница в expand
    end
    par send стартует с первой страницы, до 500 одновременно
        BR->>SE: send_email
        SE->>MP: send
        MP-->>SE: message_id / HardBounce / Rejected / Temporary
        SE->>TH: ok / skip / error
    end
    TH->>TH: последняя страница → expand финализирован → send sealed
    loop каждые 2 с
        TH->>D: on_progress: sent, progress, breakdown, duplicates
    end
    User->>API: POST /pause
    API->>D: running → paused
    API->>TH: handle.pause корня, каскадом на этапы, одна транзакция
    User->>API: POST /resume
    API->>D: paused → running
    API->>TH: handle.resume
    TH->>D: последний Item send → финализация send → корня → on_finalized: completed_with_errors
    Note over TH: через 14 дней retention удалит дерево, campaigns не затронуты
```

### 12.6 Реализация на моках

#### Мок-провайдер

```python
class TemporaryMailError(Exception): ...


class HardBounce(Exception): ...


class Rejected(Exception): ...


class FakeMailProvider:
    """Поведение детерминировано доменом адреса."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.attempts: Counter[str] = Counter()

    async def send(self, *, from_: str, to: str, subject: str, html: str, headers: dict) -> str:
        self.attempts[to] += 1
        match to.rsplit("@", 1)[1]:
            case "bounce.test":
                raise HardBounce("550 5.1.1 user unknown")
            case "reject.test":
                raise Rejected("554 5.7.1 message content rejected")
            case "flaky.test" if self.attempts[to] <= 2:
                raise TemporaryMailError("421 4.7.0 try again later")
            case "down.test":
                raise TemporaryMailError("451 4.3.0 local error")
        mid = f"msg-{len(self.sent) + 1}"
        self.sent.append({"to": to, "from": from_, "id": mid})
        return mid
```

#### Набор данных (10 000 контактов + 40 дублей адресов)

| Группа | Кол-во | Ожидаемый label |
|---|---|---|
| `user{n}@ok.test` | 9 000 | sent |
| `user{n}@flaky.test` (2 временных отказа, потом успех) | 100 | sent, attempt = 3 |
| удалённые контакты | 300 | recipient_not_found |
| отписанные | 200 | unsubscribed |
| в suppression list | 100 | suppressed |
| `not-an-email` | 50 | invalid_address |
| `@bounce.test` | 150 | hard_bounce (+ запись в suppressions) |
| `@reject.test` | 50 | rejected |
| `@down.test` (все попытки — 451) | 50 | exhausted |
| повторы уже существующих адресов (`User1@OK.test`) | 40 | не становятся Items → `duplicates = 40` |

Ошибок `300 / 10 000 = 3%`, из них `hard_bounce` — `1.5%`. Это ниже порога авто-паузы 5% → `completed_with_errors`.

```mermaid
pie showData
    title Ожидаемые исходы мок-сценария (10 000 уникальных адресов)
    "sent" : 9100
    "recipient_not_found" : 300
    "unsubscribed" : 200
    "hard_bounce" : 150
    "suppressed" : 100
    "invalid_address" : 50
    "rejected" : 50
    "exhausted" : 50
```

#### Тесты

`tallyho.testing` даёт:
* `InlineBroker` — адаптер, который выполняет отправленные сообщения в этом же процессе через `th.tracked` и эмулирует ретраи и дубли доставки;
* `FakeClock` — управление временем для `start_at`, lease и снимков;
* `run_maintenance_once()`.

PostgreSQL поднимается через testcontainers.

```python
@pytest.fixture
async def env(pg_url):
    engine = create_async_engine(pg_url)
    clock = FakeClock(datetime(2026, 10, 1, 9, 0, tzinfo=UTC))
    broker = InlineBroker(duplicate_delivery_rate=0.05, seed=42)  # 5% дублей доставки
    th = Tallyho(engine, schema="app", clock=clock, hook_modules=["app.mailing.hooks"])
    th.install(broker.adapter)
    await th.migrate()
    await seed(engine, MOCK_DATASET)
    mail = FakeMailProvider()
    with override(mail_provider, mail):
        yield Env(th=th, broker=broker, clock=clock, mail=mail, engine=engine)


async def test_scheduled_campaign_completes_with_errors(env):
    cid = await create_campaign(env)
    async with env.session() as s:
        await schedule(s, cid, at=env.clock.now() + timedelta(hours=1))
        await s.commit()
    await env.drain()
    assert (await get(env, cid)).status == "scheduled" and env.mail.sent == []

    env.clock.advance(hours=1)
    await env.drain()  # expand → send → финализация этапов → корня → on_finalized

    c = await get(env, cid)
    assert c.status == "completed_with_errors"
    assert (c.sent, c.skipped, c.failed, c.duplicates) == (9100, 600, 300, 40)
    assert c.breakdown == {
        "sent": 9100,
        "recipient_not_found": 300,
        "unsubscribed": 200,
        "suppressed": 100,
        "invalid_address": 50,
        "hard_bounce": 150,
        "rejected": 50,
        "exhausted": 50,
    }
    assert len(env.mail.sent) == 9100  # никто не получил письмо дважды


async def test_send_starts_before_expand_finishes(env):
    cid = await start_now(env)
    await env.broker.step(3)  # 1-я страница expand + 2 письма
    v = await env.th.handle((await get(env, cid)).batch_id).view()
    assert v.children["expand"].progress.final is False
    assert v.children["send"].progress.done > 0
    assert (
        v.children["send"].progress.expected == 10_000
        and v.children["send"].progress.expected_is_estimate
    )


async def test_empty_audience_completes_immediately(env):
    cid = await create_campaign(env, audience=[])
    async with env.session() as s:
        await schedule(s, cid, at=env.clock.now())
        await s.commit()
    await env.drain()  # expand: 1 пустая страница → send sealed при found 0 → финализирован
    c = await get(env, cid)
    assert c.status == "completed" and c.sent == 0


async def test_progress_snapshots_are_monotonic(env):
    cid = await start_now(env)
    seen = []
    for _ in range(10):
        await env.broker.step(1_000)
        env.clock.advance(seconds=2)
        await env.th.run_maintenance_once()
        seen.append((await get(env, cid)).progress)
    assert seen == sorted(seen)


async def test_result_survives_retention(env):
    cid = await start_now(env)
    await env.drain()
    env.clock.advance(days=15)
    await env.th.run_maintenance_once()
    c = await get(env, cid)
    with pytest.raises(BatchPurged):
        await env.th.handle(c.batch_id).view()
    assert c.status == "completed_with_errors" and c.sent == 9100  # домен не пострадал


async def test_failing_hook_blocks_finalization_then_recovers(env, monkeypatch):
    cid = await start_now(env)
    monkeypatch.setattr(hooks, "FINAL", {})  # хук падает с KeyError
    await env.drain()
    c = await get(env, cid)
    assert c.status == "running"  # домен не ушёл вперёд без итога
    assert (await env.th.handle(c.batch_id).view()).state == BatchState.SEALED
    monkeypatch.undo()
    env.clock.advance(minutes=5)
    await env.th.run_maintenance_once()
    assert (await get(env, cid)).status == "completed_with_errors"


async def test_pause_stops_sending_and_resume_finishes(env):
    cid = await start_now(env)
    await env.broker.step(3_000)
    async with env.session() as s:
        await pause(s, cid)
        await s.commit()
    sent_at_pause = len(env.mail.sent)
    await env.drain()
    assert len(env.mail.sent) == sent_at_pause  # сообщения из брокера паркуются
    async with env.session() as s:
        await resume(s, cid)
        await s.commit()
    await env.drain()
    assert (await get(env, cid)).status == "completed_with_errors"


async def test_auto_pause_on_bounce_rate(env):
    await seed(env.engine, bounce_heavy_dataset(bounce_ratio=0.08))
    cid = await start_now(env)
    await env.drain()
    c = await get(env, cid)
    assert c.status == "paused" and c.pause_reason.startswith("['hard_bounce'] rate")


async def test_worker_crash_mid_flight_recovers(env):
    cid = await start_now(env)
    env.broker.kill_worker_after(500)  # эмуляция kill -9
    await env.drain()
    env.clock.advance(seconds=61)
    await env.th.run_maintenance_once()
    await env.drain()
    assert (await get(env, cid)).status == "completed_with_errors"
```

### 12.7 Как выглядит прогресс

Иллюстрация поведения (не замер): кампания на 10 000 писем, пауза с 12-й по 16-ю минуту. Доменная таблица обновляется снимками раз в 2 с. Итог известен сразу благодаря `expected_total=size`, поэтому полоска не прыгает, пока expand ещё разворачивает аудиторию.

```mermaid
xychart-beta
    title "campaigns.progress, % (иллюстрация)"
    x-axis "минуты" [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]
    y-axis "progress %" 0 --> 100
    line [0, 8, 17, 26, 35, 44, 52, 53, 53, 62, 75, 90, 100]
```

```mermaid
gantt
    title Кампания: домен и дерево батчей (иллюстрация)
    dateFormat YYYY-MM-DD HH:mm
    axisFormat %d.%m %H:%M
    section campaigns.status
    draft                 :done, d1, 2026-10-01 08:00, 40m
    scheduled             :done, d2, after d1, 80m
    running               :active, d3, 2026-10-01 10:00, 12m
    paused                :crit, d4, after d3, 4m
    running               :active, d5, after d4, 8m
    completed_with_errors :milestone, d6, after d5, 0m
    section th_batch
    ждёт start_at          :b1, 2026-10-01 08:40, 80m
    этап expand            :b2, 2026-10-01 10:00, 3m
    этап send              :b3, 2026-10-01 10:00, 24m
    хранение до retention  :b4, 2026-10-01 10:24, 14d
```

### 12.8 Исполняемая проверка

Полная реализация раздела находится в `tests/examples/mailing/`. Этот smoke-блок извлекается
из документа и буквально выполняется в CI через `InlineBroker`.

<!-- tallyho-example: architecture-mailing -->
```python
from tallyho.model.states import BatchState
from tests.examples.mailing.app import MailingApp
from tests.examples.mailing.dataset import compact_dataset

app = await MailingApp.create(engine, schema)
try:
    campaign_id = await app.create_campaign(compact_dataset(12))
    batch_id = await app.start_now(campaign_id)
    await app.drain()

    campaign = await app.get(campaign_id)
    view = await app.th.handle(batch_id).view()
    assert campaign.status == "completed"
    assert campaign.sent == 12
    assert view.state is BatchState.SUCCEEDED
finally:
    await app.close()
```

---

## 13. Второй пример: конвейер парсинга

Задача: разобрать каталог. Число страниц становится известно после первой, число карточек на странице неизвестно, число PDF в карточке неизвестно, каждый PDF нужно скачать. Доменная таблица пользователя — `catalog_imports` со статусом и jsonb-снимком прогресса этапов.

### 13.1 Дерево

```mermaid
flowchart LR
    R["catalog_parse<br/>key=catalog:7, max_items=200 000"]
    P["pages<br/>max_depth=1"]
    C["cards<br/>fed_by=[pages], max_in_flight=100"]
    D["pdfs<br/>fed_by=[cards], max_in_flight=50"]
    R --- P
    R --- C
    R --- D
    P -->|"страница 1 → страницы 2..N"| P
    P -->|"into=cards, key=url"| C
    C -->|"into=pdfs, key=url"| D
    P -. "seal" .-> C
    C -. "seal" .-> D
```

### 13.2 Код

```python
async def start_import(session: AsyncSession, catalog_id: int, url: str) -> None:
    imp = CatalogImport(catalog_id=catalog_id, status="running")
    session.add(imp)
    await session.flush()
    async with th.batch(
        kind="catalog_parse", key=f"catalog:{imp.id}", max_items=200_000, session=session
    ) as root:
        pages = root.sub_batch(
            "pages", max_depth=1
        )  # страница 1 порождает остальные, глубже нельзя
        cards = root.sub_batch("cards", fed_by=[pages], max_in_flight=100)
        pdfs = root.sub_batch("pdfs", fed_by=[cards], max_in_flight=50)
        await pages.add(parse_page, url, page=1)
    imp.batch_id = root.handle.id


@fq.task(max_retries=3)
async def parse_page(url: str, page: int) -> None:
    html = await fetch(url, page)
    if page == 1:
        n = total_pages(html)
        th.item.expect(n)  # у pages будет n
        for p in range(2, n + 1):
            th.item.spawn(parse_page, url, p)
    for card in cards_of(html):
        th.item.spawn(parse_card, card.url, into="cards", key=normalize_url(card.url))


@fq.task(max_retries=3, weight=2)
async def parse_card(url: str) -> None:
    for pdf in pdf_links(await fetch(url)):
        th.item.spawn(download_pdf, pdf, into="pdfs", key=normalize_url(pdf))


@fq.task(max_retries=5, weight=4)
async def download_pdf(url: str) -> None:
    async for done, total in stream_download(url):
        th.item.progress(done, total)  # видно в handle.in_flight()
    th.item.ok("downloaded")


@th.on_progress("catalog_parse", every=timedelta(seconds=2))
async def save_progress(session: AsyncSession, s: BatchSummary) -> None:
    stages = {
        k: {
            "done": c.progress.done,
            "found": c.progress.found,
            "expected": c.progress.expected,
            "estimate": c.progress.expected_is_estimate,
            "final": c.progress.final,
            "duplicates": c.progress.duplicates,
            "eta_s": c.progress.eta and c.progress.eta.seconds,
        }
        for k, c in s.children.items()
    }
    await session.execute(
        update(CatalogImport)
        .where(CatalogImport.batch_id == s.id, CatalogImport.progress_seq < s.seq)
        .values(
            stages=stages,
            progress_seq=s.seq,
            progress=func.greatest(CatalogImport.progress, s.progress.ratio or 0.0),
        )
    )


@th.on_finalized("catalog_parse")
async def save_result(
    session: AsyncSession, s: BatchSummary
) -> None: ...  # статус импорта + итоговые stages, как в on_progress
```

### 13.3 Как меняется прогресс

Один прогон: 24 страницы, в среднем около 30 карточек на страницу и около 2,5 PDF на карточку. Заранее это неизвестно. Веса: страница 1, карточка 2, PDF 4.

| Момент | pages | cards | pdfs | Общий % |
|---|---|---|---|---|
| t1: разобрана страница 1 | 1 / 24 (из `expect`) | 0 / —, найдено 30. Оценки нет: выборка 1 страница меньше min(20, 5% × 24 = 1,2) | 0 / — | — |
| t2 | 12 / 24 | 200 / ≈700 (по 12 страницам), найдено 350 | 300 / ≈1 785 (по 200 карточкам), найдено 510 | ≈19% |
| t3: pages финализирован | 24 / 24 ✓ | 600 / 712 ✓ — точный, этап закрыт | 1 300 / ≈1 827, найдено 1 540 | ≈73% |
| t4: cards финализирован | ✓ | 712 / 712 ✓ | 1 700 / 1 810 ✓ — точный | ≈95% |
| t5 | ✓ | ✓ | ✓ | 100% → `on_finalized` |

Расчёт t2:
* `ratio_cards = 350 / 12`, `expected_cards = 29,2 × 24 ≈ 700`;
* `ratio_pdfs = 510 / 200`, `expected_pdfs = 2,55 × 700 ≈ 1 785`;
* общий % = `(12·1 + 200·2 + 300·4) / (24·1 + 700·2 + 1785·4) = 1 612 / 8 564 ≈ 19%`.

Что увидит UI из `catalog_imports.stages`:

```
Импорт каталога                                   ≈73% (оценка) · ETA ~6 мин
  pages  24 / 24                ✓
  cards  600 / 712              ✓ · в работе 40/100 · дублей 18
  pdfs   1 300 / 1 540 найдено  · ≈1 827 ожидается (по 600 карточкам) · в работе 50/50 · ETA 5 мин
```

### 13.4 Исполняемая проверка

Полное приложение и детерминированный генератор находятся в `tests/examples/catalog/`.
Маркированный блок запускается в CI на настоящем PostgreSQL и `InlineBroker`.

<!-- tallyho-example: architecture-catalog -->
```python
from tallyho.model.states import BatchState
from tests.examples.catalog.app import CatalogApp
from tests.examples.catalog.generator import CatalogSite

app = await CatalogApp.create(engine, schema)
try:
    batch_id = await app.start_import(CatalogSite.small())
    await app.drain()

    result = await app.get()
    view = await app.th.handle(batch_id).view()
    assert result.status == "completed"
    assert view.state is BatchState.SUCCEEDED
    assert view.children["cards"].progress.found == 8
    assert view.children["pdfs"].progress.found == 8
finally:
    await app.close()
```

---

## 14. Производительность и критерии релиза

**Правила приёмки — отдельный документ [ACCEPTANCE.md](ACCEPTANCE.md).** Ниже — только архитектурные цели производительности. Методика счётчиков — [COUNTERS.md §4](COUNTERS.md).

**Матрица масштаба**: 1k батчей × 1k Items, **50k × 1k**, **1k × 50k**, конвейер из трёх этапов на 5M Items. Каждый профиль гоняется в режимах «живая нагрузка» и «50M Items истории».

**Критерий**: p99 каждой операции в конце прогона ≤ 1.5× p99 в начале.

```mermaid
xychart-beta
    title "Целевой профиль: p99 finish не зависит от объёма (критерий, не замер)"
    x-axis "Items в таблице, млн" [1, 5, 10, 20, 30, 40, 50]
    y-axis "p99, отн. ед." 0 --> 2
    line [1.0, 1.0, 1.05, 1.1, 1.1, 1.15, 1.2]
    line [1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5]
```

Вторая линия — допустимый предел. Если реальный замер её пересекает, релиз блокируется.

Дополнительно:
* EXPLAIN-гард в CI: нет `Seq Scan` по горячим таблицам на БД ≥ 1M строк.
* 0 дедлоков в стресс-тесте, включая параллельные `pause`/`cancel` из API, финализацию с хуками и одновременную финализацию нескольких источников одного этапа.
* Конвейер: 1 000 прогонов со случайными пустыми этапами, падениями источников и `kill -9` воркеров. Ни одного этапа, оставшегося `open`, когда все его источники терминальны; ровно одна финализация на батч.
* Дедупликация: `found + duplicates` равно числу вызовов spawn с учётом `skipped_by_limit`.
* 0 `40001` в тесте с REPEATABLE READ у пользователя.
* Snapshotter: 10 000 активных деревьев с `every=2s` — лидер укладывается в интервал, без накопления отставания.
* 5% дублей доставки → ровно 1 успешная финализация на батч и 0 повторных писем в тесте кампании.

---

## 15. Конфигурация по умолчанию

| Параметр | Значение | Комментарий |
|---|---|---|
| `schema` / `prefix` | `None` / `"th_"` | |
| `hook_modules` | `[]` | модули с tx-хуками, импортируются в каждом процессе |
| `counter_slots` | 8 | слот = процесс, строки создаются лениво |
| `completer_tick` / `completer_max_batch` | 20 мс / 500 | задержка возврата результата задачи брокеру |
| `completer_backpressure` | 10 000 | как у River |
| `lease_ttl` / `heartbeat_every` | 60 с / 20 с | |
| `relay_grace` / `relay_claim_ttl` | 5 с / 30 с | |
| `finalize_grace` | 30 с | |
| `hook_timeout` | 10 с | `statement_timeout` + `asyncio.timeout` |
| `hook_backoff` | 1 с → 5 мин, экспонента | повтор упавшего `on_finalized` |
| `snapshot_tick` | 500 мс | цикл Snapshotter; `every` задаётся в хуке |
| `estimate_min_basis` / `estimate_min_share` | 20 / 5% | минимальная выборка родителей для оценки итога |
| `eta_window` | 60 с | окно скользящего среднего скорости |
| `max_items` | `None` | лимит на дерево, задаётся на корне |
| `sweep_interval` | 5 с | |
| `lock_timeout` | 5 с | retry на `55P03/40P01/40001` |
| `retention` | 14 дней | `None` — вечно; учитывает `release_required` |
| `watch_throttle` | 500 мс | NOTIFY не чаще на батч |

---

## 16. Roadmap и открытые вопросы

**v1**: всё из §§5–13 на PostgreSQL.
* батчи, spawn, под-батчи, конвейеры этапов (`fed_by`, `into=`, каскад пустых этапов, `max_items`/`max_depth`), labels;
* модель прогресса: найдено / сделано / оценка / ETA, `in_flight`, `th.item.progress`;
* отложенный старт, pause/resume/cancel, retry_failed;
* групповой коммит, sweeper;
* tx-хуки `on_finalized / on_progress / on_policy_breach`, retention + release;
* миграции, адаптер flexiq, `tallyho.testing`, бенчмарк-стенд.

**v1.x**: `watch()` + SSE-хелпер, OpenTelemetry, CLI `inspect`, admin read-only эндпоинты.

**v2**:
* SQLite (сниппеты из COUNTERS.md §3.7 как отправная точка);
* барьерная зависимость `after=[batch]` — этап стартует только после финализации другого, без наполнения. Сейчас она выражается через `on_finalized_task`;
* партиционирование `th_item`, chunk-режим, сессии не-SQLAlchemy (asyncpg).

**Открытые вопросы**
1. **Адаптер flexiq** — закрыт спайком T8.0 ([plan/FLEXIQ_SPIKE.md](plan/FLEXIQ_SPIKE.md)): обёртка сохраняет имя задачи, `_th` проходит сериализацию и доходит до DLQ, `on_dead_letter`/`JOB_DEAD` срабатывают при исчерпании `retry_budget`, prefork с async не поддерживается. Остаётся подтверждать контрактными тестами на каждом релизе flexiq.
2. **`on_progress` из Completer.** Сейчас снимки делает только лидер maintenance. Если нужно обновлять домен чаще раза в секунду на тысячах деревьев, можно добавить второй источник снимков в Completer с тем же CAS по `snap_seq`.
3. **Хранение `payload` Items**: сжатие больших аргументов или правило «в payload только id, данные в БД пользователя», как в примерах.
4. **Мягкий `max_items`.** Превышение не больше одного flush на процесс. Если нужен жёсткий лимит, это блокировка строки корня на каждый flush, то есть горячая строка. Предлагаю оставить мягким и задокументировать.
