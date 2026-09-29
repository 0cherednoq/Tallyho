> **Исторический ресёрч.** Актуальная архитектура — [ARCHITECTURE.md](ARCHITECTURE.md) v2.0: только PostgreSQL в v1, доменные статусы (Flow/Run) из библиотеки удалены.

# tallyho — дизайн публичного API

## 1. Разбор API существующих SDK

| SDK | Что в API хорошо | Что плохо / не подходит нам |
|---|---|---|
| **Temporal (Python)** | Ссылки на код типизированы: `client.start_workflow(MyWf.run, arg, id="order-42")` — IDE и mypy проверяют аргументы. **`id` — бизнес-ключ и одновременно идемпотентность.** Возвращается `handle` (`result() / cancel() / signal() / query()`). Контекст активности — модульные функции поверх contextvar: `activity.heartbeat()`, `activity.info()`, без проброса ctx через параметры. **Typed search attributes** (`SearchAttributeKey.for_keyword("CustomerId")`, `upsert_search_attributes`, `list_workflows(query)`) — индексируемые бизнес-атрибуты процесса | Требует детерминизма workflow-кода (replay), свой сервер. Для библиотеки поверх брокера это слишком много магии |
| **Oban Pro** | Иммутабельный builder: `Workflow.new() \|> Workflow.add(:a, A.new(..)) \|> Workflow.add(:b, B.new(..), deps: [:a])`, затем одна вставка. **Вставка композируется с транзакцией пользователя** (`Ecto.Multi`). Колбэки батча — функции самого воркера (`handle_completed/1`). Типизированные аргументы (`args_schema`). `append` к батчу/workflow | Колбэки в модуле воркера привязывают батч к одному типу задач |
| **River Pro (Go)** | Аргументы — типизированная структура с `Kind()`: продюсеру не нужен код воркера. `InsertTx(ctx, tx, args)` — вставка в транзакции пользователя. **`JobCompleteTx(ctx, tx, job)` — завершение джобы в транзакции пользователя**: успешный commit гарантирует, что джоба не перезапустится. Workflow: `wf.Add("b", args, nil, &WorkflowTaskOpts{Deps: []string{"a"}})`, `Prepare` → `InsertManyTx` | Go-специфичная многословность |
| **DBOS (Python)** | Транзакционный шаг получает SQLAlchemy-сессию, и результат шага записывается **в той же транзакции** → exactly-once для БД-шагов. Идемпотентность через явный workflow id | Durable execution с replay — другая модель |
| **Hatchet (Python)** | Pydantic-валидация входа (`input_validator=`), DAG через `@wf.task(parents=[step1])`, `ctx.task_output(step1)`, bulk-запуск детей `aio_run_many` | Свой сервер |
| **Celery canvas** | Сигнатуры `task.s(...)` как значения — удобно передавать колбэки | Не типизировано, магия `chord` с result backend |
| **Taskiq** | DI через значение по умолчанию: `ctx: Context = TaskiqDepends()` | DI-магия зависит от брокера |

## 2. Принципы нашего API (что берём)

1. **Типизированные ссылки на задачи** (Temporal): `batch.add(process_row, row)` проверяется mypy/pyright через `ParamSpec`. Никаких строковых имён в пользовательском коде.
2. **Бизнес-ключ = идемпотентность** (Temporal `id`): `key="import:42"` уникален в пределах `kind`. Повторный `start` возвращает существующий процесс (или ошибку — настраивается).
3. **Handle-объекты** (Temporal): `start()` возвращает `BatchHandle`/`Run` с `progress() / wait() / watch() / cancel() / transition()`.
4. **Контекст задачи — через contextvar** (Temporal `activity.*`): `th.item.spawn(...)`, `th.item.incr(...)`. Не нужен DI брокера, работает с любым брокером, в тестах подставляется одной строкой.
5. **Транзакция пользователя — явный параметр** (River `InsertTx` / `JobCompleteTx`, Oban `Ecto.Multi`): `session=` на всех пишущих методах и `th.item.complete_in(session)` для завершения в его транзакции.
6. **Два стиля постановки**: стриминговый (`async with th.batch(...)`) для больших/неизвестных объёмов и builder (Oban/River) для статических DAG в v2.
7. **Типизированные бизнес-данные и статусы** (Hatchet pydantic, Temporal search attributes): `Flow[Status, Data]` с индексируемыми `status`/`key`.
8. **Колбэки — обычные задачи брокера** (Celery signatures), а не методы специального класса.
9. **Никакого детерминизма и replay.** Мы не durable execution. Код задач — обычный код.

## 3. Кто в кого встраивается

### Вариант A — tallyho как плагин брокера
Пользователь пишет задачи декоратором своего брокера. Мы ставим middleware и отправляем через клиент брокера.

### Вариант B — брокер как транспорт tallyho
Задачи объявляются нашим `@th.task`, брокер — лишь канал доставки байтов, воркер-цикл наш.

| | A: плагин | B: брокер как транспорт |
|---|---|---|
| Внедрение в существующий проект | Одна строка `th.install(broker)`, задачи не трогаем | Переписывать объявления задач |
| Ретраи, rate limit, cron, приоритеты, DLQ брокера | Работают как есть | Надо проксировать или переписывать |
| «Ещё одна прослойка» (ваше требование) | Нет | Да — фактически свой task-фреймворк |
| Контроль жизненного цикла | Через middleware; зависим от его возможностей | Полный |
| Единообразие API между брокерами | Продюсер одинаковый, объявление задач — брокерное | Полное |
| Объём работы | Адаптер ≈ 2 класса | Воркер-цикл, сериализация, ретраи… |

**Рекомендация: A.** Выполнение задачи остаётся за брокером, мы добавляем **учёт**. Контракт адаптера двусторонний:

```python
class Dispatcher(Protocol):                    # сторона продюсера
    def task_name(self, fn: object) -> str: ...
    async def dispatch(self, messages: Sequence[Message]) -> None: ...

class Runtime(Protocol):                        # сторона воркера
    def install(self, hooks: WorkerHooks) -> None: ...   # повесить наш around-хук
    def will_retry(self, raw_ctx: Any, exc: BaseException) -> bool: ...
```

Единственное, что мы регистрируем в брокере сами, — **одна системная задача** `tallyho.system`. Через неё выполняются колбэки финализации и обработчики бизнес-статусов (§5.2).

Минимальные требования к брокеру для адаптера: (1) around-хук исполнения, (2) заголовки/метаданные сообщения или кастомный id задачи, (3) async-enqueue. Проверка flexiq на эти три пункта сейчас идёт.

## 4. API v0.2

### 4.1 Установка

```python
th = Tallyho(engine, schema="app")               # AsyncEngine; prefix по умолчанию "th_"
th.install(FlexiqAdapter(app))                   # middleware + системная задача
```

### 4.2 Батч (продюсер)

```python
async with th.batch(
    kind="csv_rows",
    key=f"import:{import_id}",                   # идемпотентность
    on_success=th.call(finalize_import, import_id),        # типизировано через ParamSpec
    on_failure=th.call(rollback_import, import_id),
    failure_policy=th.FailurePolicy.threshold(ratio=0.01),
    max_in_flight=200,
    deadline=timedelta(hours=2),
    session=session,                             # опционально: всё в транзакции пользователя
) as batch:
    await batch.map(process_row, rows)                           # bulk, чанками, Callable[[T], ...] + Iterable[T]
    await batch.add(process_file, path, mode="strict")           # одиночный, ParamSpec
    await batch.add_calls(th.call(f, x).opts(key=x.id, weight=5) for x in items)   # полный контроль
# выход = seal()

handle = batch.handle                            # BatchHandle
```

### 4.3 Внутри задачи

```python
import tallyho as th

@app.task(retries=3)                             # декоратор ВАШЕГО брокера, наш не нужен
async def process_row(row: Row) -> None:
    ...
    th.item.incr("rows_imported")
    th.item.spawn(process_child, row.child_id)                # в тот же батч, атомарно с завершением
    async with th.item.sub_batch(kind="parts", on_success=th.call(merge, row.id)) as sub:
        await sub.map(process_part, row.parts)                # родитель ждёт под-батч целиком

    if th.item.cancelled():                                   # кооперативная отмена
        return

@app.task
async def import_row_tx(row: Row) -> None:
    async with db.begin() as session:
        session.add(Entity(...))
        await th.item.complete_in(session)                   # как River JobCompleteTx
```

Вне батча `th.item.*` безопасны: `th.item.current()` возвращает `None`, остальные методы — no-op или явная ошибка (настраивается).

### 4.4 Бизнес-процесс (Flow)

```python
class ImportStatus(StrEnum):
    UPLOADED = "uploaded"; PARSING = "parsing"; IMPORTING = "importing"
    DONE = "done"; REJECTED = "rejected"

class ImportData(BaseModel):                     # pydantic / dataclass / msgspec
    file_url: str
    rows_total: int | None = None

class CsvImport(th.Flow[ImportStatus, ImportData]):
    kind = "csv_import"
    initial = ImportStatus.UPLOADED
    transitions = {
        ImportStatus.UPLOADED:  {ImportStatus.PARSING},
        ImportStatus.PARSING:   {ImportStatus.IMPORTING, ImportStatus.REJECTED},
        ImportStatus.IMPORTING: {ImportStatus.DONE, ImportStatus.REJECTED},
    }

    @th.on_enter(ImportStatus.PARSING)
    async def parse(self, run: th.Run[ImportStatus, ImportData]) -> None:
        async with run.batch(kind="parse", on_success=run.goto(ImportStatus.IMPORTING),
                             on_failure=run.goto(ImportStatus.REJECTED)) as b:
            await b.map(parse_chunk, chunks_of(run.data.file_url))

    @th.on_enter(ImportStatus.IMPORTING)
    async def import_rows(self, run: th.Run[ImportStatus, ImportData]) -> None:
        async with run.batch(kind="rows", on_success=run.goto(ImportStatus.DONE)) as b:
            ...

# старт и управление
run = await CsvImport.start(key=f"import:{upload.id}", data=ImportData(file_url=url), session=session)
await run.transition(ImportStatus.PARSING, reason="user confirmed", session=session)
view = await run.view()                          # status, data, progress текущей стадии, история
async for r in CsvImport.find(status=ImportStatus.IMPORTING, limit=100): ...
```

## 5. Как это работает под капотом

### 5.1 Задача внутри батча (`th.item.*`)

`@app.task` — декоратор брокера, мы его не заменяем. Всю работу делает **middleware**, которую `th.install()` вешает на брокер.

```
Продюсер: batch.map(process_row, rows)
  1. adapter.task_name(process_row) → "myapp.process_row"
  2. INSERT th_item (id=uuid7, payload=args) + th_outbox  [+ total += n]  — в сессии пользователя или нашей
  3. после commit: adapter.dispatch(Message(task_id=item.id, headers={"th-item": id, "th-batch": batch_id}))
  4. DELETE th_outbox для отправленных

Воркер получает сообщение → брокер вызывает нашу middleware (around):
  1. нет заголовка "th-item"?  → просто вызываем задачу, мы невидимы
  2. claim → Completer (групповой коммит): INSERT th_lease ON CONFLICT DO NOTHING
        конфликт / Item уже терминальный → дубль → возвращаем успех брокеру, задачу НЕ вызываем
  3. создаём ItemContext(item_id, batch_id, spawn_buffer=[], metrics={}) и кладём в ContextVar
  4. запускаем heartbeat-таск (продлевает lease каждые lease/3 через Completer)
  5. await original_task(*args)          ← здесь th.item.spawn/incr пишут в буфер ContextVar
  6. успех → Completer.finish(item, succeeded, spawns, metrics) и ЖДЁМ future:
        одна групповая транзакция: CAS th_item → DELETE lease → INSERT spawned items+outbox
                                   → total/succeeded += … → commit → dispatch spawned → проверка финализации
  7. исключение → adapter.will_retry(exc)?
        да  → Completer.release(item, attempt+1)  (lease удаляется, Item остаётся активным), пробрасываем exc брокеру
        нет → Completer.finish(item, failed, error) и пробрасываем exc
  8. ContextVar сбрасывается, heartbeat останавливается, брокер делает ack
```

`th.item.spawn()` не пишет в БД сразу. Он кладёт вызов в буфер контекста, и буфер сбрасывается **в той же транзакции**, что и завершение (п. 6). Если задача упала, детей не будет; если упал commit, не будет ни детей, ни завершения, и задача перезапустится начисто. Это семантика River `JobCompleteTx`.

`th.item.complete_in(session)`: завершение пишется в сессию пользователя (путь B из COUNTERS.md), а middleware на шаге 6 видит флаг «уже завершено» и ничего не делает.

### 5.2 `class CsvImport(th.Flow[...])` и `@th.on_enter`

**Объявление (импорт модуля)**:
1. `@th.on_enter(S)` не оборачивает функцию. Он только вешает на неё метку `__th_on_enter__ = S`.
2. `Flow.__init_subclass__` при создании класса:
   * проверяет граф `transitions` (все статусы из enum, `initial` достижим, нет переходов из терминальных);
   * собирает методы с метками в `handlers: dict[Status, method]`;
   * регистрирует класс в глобальном реестре по `kind`. Дубль `kind` → ошибка на импорте.
3. Реестр живёт **в памяти процесса**. В БД хранятся только строки `kind` и `status`. Поэтому модуль с Flow должен импортироваться и в API-процессе, и в воркере, как и обычные задачи брокера.

**Переход** `run.transition(PARSING, session=s)`:
```
в транзакции пользователя (или нашей):
  1. проверка по графу в памяти: UPLOADED → PARSING разрешён?
  2. UPDATE th_batch SET status='parsing', status_version = v+1
       WHERE id=:id AND status='uploaded' AND status_version=:v   — CAS; 0 строк → ConcurrentTransition
  3. INSERT th_status_history
  4. есть handler для PARSING? → INSERT th_outbox (system task: flow_enter, kind, run_id, status, version=v+1)
commit (пользователь)
  5. after_commit → dispatch системной задачи tallyho.system в брокер
```
Обработчик **не вызывается внутри transition**: транзакция остаётся короткой, rollback пользователя отменяет и переход, и запуск обработчика, а падение процесса не теряет обработчик (outbox + relay).

**Исполнение в воркере** (`tallyho.system`):
```
  1. реестр[kind] → CsvImport, handlers[PARSING] → parse
  2. guard: SELECT status, status_version FROM th_batch WHERE id=:run_id
       если статус уже ушёл дальше или версия другая → пропуск (устаревший/дублирующий вызов)
  3. await CsvImport().parse(Run(...))    — внутри run.batch(...) создаёт под-батч с parent_id=run_id
  4. маркер «handler для version=v+1 выполнен» пишется вместе с созданием под-батча
     (idempotency key под-батча = f"{run_id}:{v+1}:parse") → повтор обработчика не создаст второй под-батч
```

**Автопереход** `on_success=run.goto(IMPORTING)`: это не задача, а декларация, сохранённая в `options` под-батча. В транзакции финализации под-батча (CAS `sealed → succeeded`) тот же код делает шаги 2–4 перехода. «Батч завершился» и «процесс перешёл в следующий статус» — один атомарный commit.

### 5.3 Колбэки `on_success=th.call(finalize_import, 42)`
`th.call` вычисляет `task_name` и сериализует аргументы **в момент создания батча** и кладёт их в `options`. При финализации в той же CAS-транзакции делается `INSERT th_outbox`, relay отправляет обычное сообщение брокеру с заголовком `th-callback=<batch_id>:success`. В задаче колбэка доступен `th.callback.current()` → `batch_id`, итоговые счётчики и стабильный `callback_id` для идемпотентности.
