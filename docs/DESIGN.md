> **Исторический ресёрч.** Актуальная архитектура — [ARCHITECTURE.md](ARCHITECTURE.md) v2.0: только PostgreSQL в v1, доменные статусы (Flow/Run) из библиотеки удалены.

# batchwise — дизайн (черновик v0.1)

> Рабочее название. Python 3.11+, SQLAlchemy 2.0 Core, async-first (+ sync-фасад), PostgreSQL и SQLite.

Библиотека даёт **групповое отслеживание задач поверх любого брокера**: батчи, вложенные батчи,
динамический fan-out с заранее неизвестным количеством задач, финализацию ровно один раз,
защиту от зависаний и (опционально) бизнес-статусы процесса — без собственных таблиц у пользователя.

Брокер остаётся транспортом и исполнителем. Мы — источник истины о состоянии группы.

---

## 1. Что смотрели и что берём

| Решение | Как устроено | Что берём | Чего избегаем |
|---|---|---|---|
| **Oban Pro Batch** (Elixir/PG) | `batch_id` в meta каждой джобы; колбэки `completed / exhausted / discarded / cancelled / retryable / attempted`, каждый колбэк — отдельная джоба, вставляется один раз; `append/2` дописывает джобы в существующий батч | Набор событий-колбэков; колбэк = задача в брокере; вставка колбэка ровно один раз; append | Подсчёт по таблице джоб (у нас брокер внешний, джоб в БД нет) |
| **Oban Pro Workflow** | Зависимые джобы ждут в `on_hold`; `append`, sub-workflow, **graft** (плейсхолдер, который в рантайме «прирастает» под-графом, и downstream ждёт весь привитый граф); `ignore_cancelled / ignore_discarded` | Идея «узел, который раскрывается в под-батч в рантайме» — ровно наш кейс неизвестного количества; политики реакции на падение зависимостей | — |
| **Oban migrations** | `Oban.Migration.up(version: N, prefix: "schema")` — пользователь вызывает версионированную миграцию в своей | Версионированные миграции с параметром схемы, встраиваемые в Alembic пользователя | — |
| **Oban Lifeline** | Плагин спасает «осиротевшие» джобы и застрявшие workflow | Sweeper как обязательная часть, а не опция | — |
| **Sidekiq Pro Batches** | Счётчики `pending / failures` в Redis; колбэки `complete` (всё отработало) / `success` (всё успешно) / `death`; **батч можно дополнять изнутри выполняющейся джобы этого же батча**; вложенные батчи | Семантика complete vs success; правило «дополнять запечатанный батч можно только изнутри его же задачи» — тогда счётчик никогда не падает в 0 преждевременно | Их же предупреждение: несколько `batch.jobs` при создании = гонка «батч завершился, пока ещё добавляли». У нас решается явным `seal` |
| **BullMQ Flows** | Родитель в состоянии `waiting-children`; атомарное добавление дерева; `getChildrenValues()`; `failParentOnFailure / ignoreDependencyOnFailure / continueParentOnFailure` | Состояние «ждёт детей»; чтение результатов детей; политики падения ребёнка | — |
| **River Pro** (Go/PG) | DAG с `Deps`; `InsertManyTx` — **расширение workflow в той же транзакции, что и завершение текущей задачи**; retention, удаляющий workflow целиком | Атомарный spawn + complete в одной транзакции (главный приём для fan-out); транзакционная вставка в сессию пользователя; retention «деревом» | — |
| **Celery group/chord** | Счётчик chord в result backend, либо `chord_unlock` поллинг | — | Всё: потерянные chord'ы при падении заголовка, поллинг, нет прогресса, нет вложенности, нет fan-out |
| **Hatchet / Temporal** | Durable execution, дети как first-class | Удобство `ctx.spawn(...)` | Тяжёлая собственная инфраструктура — мы библиотека, а не сервер |

**Вывод**: оптимальная модель — Sidekiq-семантика батча + River-атомарность spawn/complete + Oban-подход к миграциям и lifeline, с хранением в SQL и отказом от count(*) в пользу шардированных счётчиков.

---

## 2. Модель

```
Batch (kind, ref, state, [бизнес-status], data)
 ├── Item ── обычная задача брокера (id Item == task_id в брокере)
 ├── Item ── ctx.spawn(...) ──► новые Items в этом же батче
 └── Item(kind=batch) ──► Child Batch ──► Items ...   (дерево произвольной глубины)
```

* **Batch** — группа. Имеет техническое состояние и (опционально) бизнес-статус.
* **Item** — одна задача в брокере. Её `id` передаётся брокеру как `task_id`, поэтому дубли доставки распознаются.
* **Child batch** — Item, который «раскрывается» в под-батч. Для родителя это одна единица работы;
  он завершится, когда финализируется под-батч (аналог graft в Oban).

### 2.1 Состояния батча

```
open ──seal()──► sealed ──(pending==0)──► finalizing ──► succeeded
  │                │                                    ├─► completed_with_errors
  │                │                                    └─► failed
  └────────────────┴──cancel()/deadline/fail_fast──────────► cancelled | failed
```

* `open` — продюсер ещё добавляет задачи. Финализация **невозможна**, даже если все задачи уже отработали.
* `sealed` — продюсер закончил. Новые задачи можно добавлять **только изнутри Item'а этого батча** (`ctx.spawn`) — тогда pending не может обнулиться между «родитель завершился» и «дети добавлены».
* Условие финализации: `state = sealed AND sum(pending по шардам) = 0`.
* `finalizing → terminal` — переход CAS'ом (`UPDATE ... WHERE state='sealed' RETURNING`), выигрывает ровно один процесс; в той же транзакции в outbox кладутся колбэки.

### 2.2 Состояния Item

```
held ─► created ─► dispatched ─► running ─► succeeded
          ▲            ▲            │  ├──► failed      (попытки брокера исчерпаны)
          │            └─retrying◄──┤  └──► cancelled
          └──── sweeper (lease истёк / не отправлен) ──┘
```

* `held` — ждёт окна `max_in_flight` (ограничение параллелизма на батч).
* `created` — записан, ещё не отправлен в брокер (outbox-семантика).
* `running` — захвачен воркером с **lease**; воркер продлевает lease heartbeat'ом.
* Все переходы — CAS по текущему состоянию. **Счётчики меняются только если переход реально произошёл** → повторная доставка / повторный вызов complete ничего не ломает.

---

## 3. Гарантии «ничего не зависнет»

| Сценарий | Что делаем |
|---|---|
| Упали между commit в БД и отправкой в брокер (dual write) | Items лежат в `created`. После commit — быстрая отправка; иначе sweeper-relay подберёт `created` старше `grace` (`FOR UPDATE SKIP LOCKED`) |
| Брокер доставил дважды | `claim`: `UPDATE item SET state='running' WHERE id=? AND state IN (created, dispatched, retrying)`. Второй воркер получает 0 строк и молча ack'ает |
| Воркер умер посреди задачи | Lease истёк → sweeper: `attempt < max_attempts` → `retrying` + повторная отправка, иначе `failed` (reason=`lease_expired`) |
| Брокер потерял сообщение | Опционально `dispatch_timeout`: `dispatched`, не стартовавший за N — переотправляется (по умолчанию выключено, т.к. при забитой очереди даст дубли) |
| Последние Items двух шардов завершились одновременно | Проверка финализации делается **после commit** своей транзакции → последний закоммитивший гарантированно видит все остальные. Плюс CAS на переход |
| `seal()` и завершение последнего Item одновременно | Обе стороны проверяют условие после своего commit — кто-то из двух точно увидит оба изменения |
| Процесс упал после завершения Item, но до проверки финализации | Sweeper: батчи `sealed` c pending=0 дольше `grace` → финализирует |
| Колбэк финализации не ушёл в брокер | Колбэки в outbox, отправляются relay'ем. Вставляются ровно один раз (в транзакции CAS-перехода), выполняются at-least-once; в контекст колбэка передаётся стабильный `callback_id` для идемпотентности |
| Задачи висят вечно по бизнес-причине | `deadline` на батч → `failed(reason=deadline)`, оставшиеся Items → `cancelled` |
| Воркер, делающий spawn, упал после spawn, но до complete | По умолчанию spawn буферизуется и пишется **в одной транзакции** с complete (приём River). Для огромных fan-out — `spawn_now()` со стабильными idempotency-ключами, повтор родителя не создаст дублей |

Честно: exactly-once выполнение невозможно. Гарантируем **at-least-once выполнение + идемпотентный учёт + exactly-once постановку финализации**.

---

## 4. Быстрые счётчики

Проблема: `UPDATE batch SET done = done + 1` при тысячах задач/сек — горячая строка, все ждут одну блокировку.

Решение — **шардированные счётчики**:

```
bw_counter(batch_id, shard, total, succeeded, failed, cancelled)   PK(batch_id, shard)
```

* `shard` назначается Item'у при создании (`hash(item_id) % shards`), `shards` — на батч (по умолчанию 16 для PG, 1 для SQLite; можно указать `expected_size` и мы подберём).
* Завершение Item = 1 транзакция: CAS строки Item + `UPDATE bw_counter ... WHERE batch_id=? AND shard=?`. Строка батча **не трогается**.
* Оптимизация проверки: сумму по всем шардам читаем, только когда **свой** шард дошёл до `pending=0` (последний Item батча обязательно обнулит свой шард). Большинство завершений не читают ничего лишнего.
* В `bw_counter` нет индексов кроме PK и индексированные колонки не меняются → **HOT-updates** в PG, `fillfactor=50` → почти нет bloat.
* Прогресс = `SUM` по ≤ N строкам через PK — O(shards), не O(items).
* Пользовательские метрики (`ctx.incr("rows_imported", 500)`) — такая же шардированная таблица `bw_metric`.
* Добавление 100k Items = чанковая вставка + **один** инкремент `total` на шард за чанк.

---

## 5. Схема (PostgreSQL; SQLite — те же таблицы без схемы/partial-специфики)

Все имена: `{schema}.{prefix}batch` и т.д., `schema` и `prefix` настраиваются. ID — UUIDv7 (время-упорядоченные → локальность B-tree, дешёвые вставки).

```sql
CREATE TABLE {s}.bw_batch (
    id              uuid PRIMARY KEY,
    root_id         uuid NOT NULL,
    parent_id       uuid NULL,
    parent_item_id  uuid NULL,
    kind            text NOT NULL,
    ref             text NULL,              -- внешний ключ сущности пользователя: 'import:42'
    idempotency_key text NULL,
    state           smallint NOT NULL,
    status          text NULL,              -- бизнес-статус (см. §8)
    status_version  integer NOT NULL DEFAULT 0,
    shards          smallint NOT NULL,
    options         jsonb NOT NULL,         -- failure policy, callbacks, max_in_flight...
    data            jsonb NULL,             -- бизнес-данные пользователя
    deadline_at     timestamptz NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    finished_at     timestamptz NULL
) WITH (fillfactor = 90);

CREATE UNIQUE INDEX ON {s}.bw_batch (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX ON {s}.bw_batch (kind, ref) WHERE ref IS NOT NULL;               -- «найди батч импорта 42»
CREATE INDEX ON {s}.bw_batch (kind, status, created_at DESC);                  -- «все импорты в статусе X»
CREATE INDEX ON {s}.bw_batch (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX ON {s}.bw_batch (updated_at) WHERE state IN (0 /*open*/, 1 /*sealed*/, 2 /*finalizing*/);  -- sweeper
CREATE INDEX ON {s}.bw_batch (deadline_at) WHERE deadline_at IS NOT NULL AND state IN (0, 1);
CREATE INDEX ON {s}.bw_batch (finished_at) WHERE root_id = id AND finished_at IS NOT NULL;              -- retention по корням

CREATE TABLE {s}.bw_item (
    id              uuid PRIMARY KEY,       -- == task_id в брокере
    batch_id        uuid NOT NULL,
    shard           smallint NOT NULL,
    state           smallint NOT NULL,
    attempt         smallint NOT NULL DEFAULT 0,
    task_name       text NOT NULL,
    payload         bytea NOT NULL,         -- для переотправки sweeper'ом
    idempotency_key text NULL,
    child_batch_id  uuid NULL,
    weight          integer NOT NULL DEFAULT 1,
    lease_until     timestamptz NULL,
    result          jsonb NULL,
    error           jsonb NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
) WITH (fillfactor = 85);

CREATE INDEX ON {s}.bw_item (batch_id, id);                                                    -- листинг в порядке вставки, cancel, retention
CREATE UNIQUE INDEX ON {s}.bw_item (batch_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX ON {s}.bw_item (updated_at) WHERE state IN (1 /*created*/, 2 /*dispatched*/);   -- relay
CREATE INDEX ON {s}.bw_item (lease_until) WHERE state = 3 /*running*/;                         -- истёкшие lease
CREATE INDEX ON {s}.bw_item (batch_id, id) WHERE state = 0 /*held*/;                            -- окно max_in_flight
CREATE INDEX ON {s}.bw_item (batch_id) WHERE state = 11 /*failed*/;                             -- «покажи упавшие»

CREATE TABLE {s}.bw_counter (
    batch_id uuid, shard smallint,
    total bigint, succeeded bigint, failed bigint, cancelled bigint, in_flight bigint,
    PRIMARY KEY (batch_id, shard)
) WITH (fillfactor = 50);

CREATE TABLE {s}.bw_metric (
    batch_id uuid, name text, shard smallint, value bigint,
    PRIMARY KEY (batch_id, name, shard)
) WITH (fillfactor = 50);

CREATE TABLE {s}.bw_outbox (                      -- колбэки и события для отправки в брокер
    id uuid PRIMARY KEY, batch_id uuid NOT NULL, kind text NOT NULL,
    task_name text NOT NULL, payload bytea NOT NULL,
    state smallint NOT NULL, attempts smallint NOT NULL DEFAULT 0, available_at timestamptz NOT NULL
);
CREATE INDEX ON {s}.bw_outbox (available_at) WHERE state = 0 /*pending*/;

CREATE TABLE {s}.bw_status_history (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL, from_status text NULL, to_status text NOT NULL,
    reason text NULL, actor text NULL, at timestamptz NOT NULL, data jsonb NULL
);
CREATE INDEX ON {s}.bw_status_history (batch_id, id);

CREATE TABLE {s}.bw_meta (key text PRIMARY KEY, value text NOT NULL);   -- версия схемы, leader-lease для SQLite
```

Решения по индексам:

* **Нет индекса `(batch_id, state)` на всю таблицу** — он бы менялся на каждом переходе. Вместо него узкие partial-индексы только по «редким / активным» состояниям: они маленькие, т.к. основная масса строк терминальна.
* Состояния — `smallint`, а не text: меньше индексы, дешевле сравнения.
* FK не объявляем на горячих таблицах (каскады и проверки на каждой вставке 100k строк дороги); целостность держим сами, retention удаляет деревом чанками.
* Опционально (v2): партиционирование `bw_item` по `created_at` для очень больших инсталляций — retention становится `DROP PARTITION`.

**SQLite**: WAL + `busy_timeout` обязательно (проверяем при старте), `shards=1` (один писатель всё равно), `INSERT ... ON CONFLICT DO NOTHING`, `RETURNING` (SQLite ≥ 3.35), partial-индексы поддерживаются; вместо `SKIP LOCKED` — claim через `UPDATE ... WHERE id IN (SELECT ... LIMIT n) RETURNING`; вместо advisory lock — leader-lease в `bw_meta`. «Схема» → префикс таблиц (или `ATTACH DATABASE ... AS schema`).

---

## 6. Интеграция с брокером

### 6.1 Протокол

```python
@dataclass(frozen=True, slots=True)
class Message:
    task_id: str            # == item.id
    task_name: str
    args: tuple
    kwargs: dict
    headers: dict[str, str] # bw-batch-id, bw-item-id, bw-attempt
    queue: str | None = None
    eta: datetime | None = None

class Broker(Protocol):
    async def dispatch(self, messages: Sequence[Message]) -> None: ...

class WorkerAdapter(Protocol):
    """Как достать наш контекст из сообщения конкретного брокера и узнать, будет ли ретрай."""
    def extract(self, raw_ctx: Any) -> ItemRef | None: ...
    def will_retry(self, raw_ctx: Any, exc: BaseException) -> bool: ...
```

Адаптеры из коробки (extras): `batchwise[taskiq]`, `batchwise[celery]`, `batchwise[dramatiq]`, `batchwise[arq]`, плюс `InMemoryBroker` для тестов (выполняет задачи inline). Для своего брокера достаточно реализовать два метода.

На стороне воркера — middleware/обёртка: `claim → heartbeat → run → complete|fail(+spawn)`. Ретраи остаются у брокера; мы через `will_retry` понимаем, это `retrying` или финальный `failed`.

### 6.2 Своя сессия пользователя

```python
async with db.begin() as session:                       # сессия пользователя
    order = Order(...); session.add(order)
    batch = await bw.create_batch(session, kind="order_export", ref=f"order:{order.id}")
    await batch.add_many(export_line.s(l.id) for l in lines)
    await batch.seal()
# commit сделал пользователь → after_commit-хук отправляет Items в брокер
```

* Принимаем `AsyncSession | AsyncConnection | Session | Connection`.
* Если сессия чужая — **никогда не коммитим**, только пишем; отправка в брокер — через `after_commit` сессии (+ relay как страховка). Rollback у пользователя → в брокер ничего не уйдёт.
* На стороне воркера так же: `async with bw.item_tx(session) as ctx:` — бизнес-запись пользователя и `complete` Item'а в одной транзакции.

---

## 7. API (эскиз)

### Продюсер

```python
bw = Batchwise(engine, broker=TaskiqBroker(broker), schema="app", prefix="bw_")

async with bw.batch(
    kind="csv_import",
    ref="import:42",
    on_success=finalize_import.s(import_id=42),
    on_complete=notify.s(),                   # всё отработало, с ошибками или без
    on_failure=rollback_import.s(import_id=42),
    failure_policy=FailurePolicy.threshold(max_failed_ratio=0.01),   # continue | fail_fast | threshold
    max_in_flight=200,                        # окно параллелизма на батч
    deadline=timedelta(hours=2),
    session=session,                          # опционально
) as batch:
    async for chunk in read_rows_in_chunks(file):
        await batch.add_many(process_row.s(r) for r in chunk)   # стриминг, пачками
# выход из контекста = seal()
```

### Воркер

```python
@broker.task
async def process_row(row: dict, ctx: ItemContext = BW) -> None:
    ...
    ctx.incr("rows_imported")
    if row["has_children"]:
        ctx.spawn(process_child.s(row["id"]))                 # в этот же батч, атомарно с complete
    if row["heavy"]:
        sub = ctx.spawn_batch(kind="heavy_row", on_success=merge.s(row["id"]))
        sub.add_many(process_part.s(p) for p in parts)       # под-батч; родитель ждёт его целиком
```

### Чтение

```python
view = await bw.get(batch_id)          # total, succeeded, failed, pending, progress (с учётом weight), eta, state, status
async for v in bw.watch(batch_id):     # PG: LISTEN/NOTIFY, SQLite: поллинг; удобно для SSE/WebSocket
    ...
await bw.failed_items(batch_id)        # упавшие с ошибками
await bw.retry_failed(batch_id)        # переоткрыть батч и перезапустить упавшие
await bw.cancel(batch_id)              # каскадно по под-батчам; воркеры видят ctx.cancelled
```

---

## 8. Бизнес-статусы — ответ на вопрос

**Да, можно, и это сильная фича.** Батч — это и есть «процесс» (импорт, выгрузка, пересчёт), поэтому у него
может быть пользовательская стейт-машина прямо в нашей строке: колонка `status` + `bw_status_history`.

```python
class ImportStatus(StrEnum):
    UPLOADED = "uploaded"; PARSING = "parsing"; IMPORTING = "importing"
    DONE = "done"; REJECTED = "rejected"

csv_import = bw.flow(
    kind="csv_import",
    status=ImportStatus,
    initial=ImportStatus.UPLOADED,
    transitions={
        ImportStatus.UPLOADED:  {ImportStatus.PARSING},
        ImportStatus.PARSING:   {ImportStatus.IMPORTING, ImportStatus.REJECTED},
        ImportStatus.IMPORTING: {ImportStatus.DONE, ImportStatus.REJECTED},
    },
    # автоматические переходы по техническим событиям
    on_succeeded={ImportStatus.IMPORTING: ImportStatus.DONE},
    on_failed=ImportStatus.REJECTED,
)

@csv_import.on_enter(ImportStatus.IMPORTING)
async def start_import(batch: BatchHandle) -> None:     # вход в статус = следующая стадия
    batch.add_many(...)

await csv_import.transition(batch_id, ImportStatus.PARSING, reason="user clicked", session=session)
await csv_import.find(status=ImportStatus.IMPORTING)          # индекс (kind, status, created_at)
```

* Переходы валидируются и делаются CAS'ом по `status_version` → конкурентно-безопасно.
* Переход пишется в транзакции пользователя (если передана сессия), история — в `bw_status_history`.
* Стадии процесса = статусы; каждая стадия может запускать свой под-батч → многошаговый пайплайн без спагетти.
* `ref` + `data` (jsonb) позволяют вообще не заводить таблицу `imports`, если сущность простая.

Граница, которую стоит зафиксировать: если у сущности богатая доменная модель (заказ с позициями, деньгами, инвариантами) — она остаётся в таблицах пользователя, а батч ссылается на неё через `ref`. Мы — стейт-машина **процесса обработки**, а не замена доменной модели. Иначе через год пользователи будут делать JOIN'ы по нашему jsonb.

---

## 9. Предложения по функционалу

**v1 (ядро)**
1. Батчи с `seal`, шардированные счётчики, прогресс с весами, ETA по пропускной способности.
2. Колбэки `on_success / on_complete / on_failure` через outbox, exactly-once постановка.
3. Динамический fan-out `ctx.spawn` (атомарно с complete) и под-батчи произвольной глубины.
4. Политики ошибок: `continue`, `fail_fast` (отменить остаток), `threshold` (абсолют/процент).
5. Sweeper: relay неотправленных, истёкшие lease, пропущенные финализации, deadline, outbox, retention. Запуск: как периодическая задача брокера, встроенной asyncio-задачей или CLI-процессом; безопасен при нескольких экземплярах.
6. Транзакционность с сессией пользователя.
7. Идемпотентное добавление (`idempotency_key`) и идемпотентное создание батча.
8. Бизнес-статусы (§8).
9. Адаптеры Taskiq + Celery, `InMemoryBroker`, pytest-фикстуры.
10. Миграции: `bw.install()` для быстрого старта + версионированные операции для Alembic (`batchwise.alembic.upgrade(op, version=1, schema="app")`).

**v1.x**
11. `max_in_flight` — окно параллелизма на батч (Celery так не умеет; нужно для rate-limit внешних API).
12. `watch()` через LISTEN/NOTIFY; готовый SSE-хелпер.
13. Пользовательские метрики `ctx.incr` и результаты Items с агрегацией в финализаторе (`batch.results()` стримом).
14. `retry_failed`, `cancel` с кооперативной отменой.
15. OpenTelemetry-спаны и события-хуки (`on_item_failed`, `on_batch_finished`) для расширения.
16. CLI: `batchwise inspect | sweep | migrate`.

**v2**
17. DAG-зависимости между Items/под-батчами (`depends_on`), политики `ignore_failed` как в Oban/River.
18. Партиционирование `bw_item`.
19. Chunk-режим: один Item обрабатывает N записей (снижение нагрузки на брокер при миллионах мелких задач).
20. Admin-UI (read-only дашборд).

**Точки расширения**: `Broker`, `WorkerAdapter`, `Serializer`, `Hooks`, `Clock` (тесты), `IdFactory`, стратегия шардирования счётчиков.

---

## 10. Открытые вопросы

1. Python + SQLAlchemy 2.0 Core, async-first — подтвердить. Нужен ли sync-API в v1 (Celery/Dramatiq/Django синхронные)?
2. Какие брокеры в приоритете для v1?
3. Бизнес-статусы в v1 или отдельным этапом после ядра?
4. Название пакета.
