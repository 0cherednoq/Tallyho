# Спайк T8.0: проверка фактов о flexiq

> Задача T8.0 ([PLAN.md](PLAN.md) §2, Ф8). Проверены таблица фактов [ARCHITECTURE §11.3](../ARCHITECTURE.md#113-адаптер-flexiq)
> и открытые вопросы §16 п.1 на `flexiq==2.0.0` (wheel `cp311-win_amd64`, Python 3.11.15), 2026-09-30.
> Итог для T8.1/T8.2 — разделы 3–6. Решения в DECISIONS.md вносит оркестратор по отчёту.

## 0. Как проверяли

| Скрипт | Что делает | Запуск |
|---|---|---|
| `tests/contract/flexiq/spike_facts.py` | PostgreSQL 16 (testcontainers) + живой воркер `pool="thread"` в фоновом потоке; модель `th.tracked` (async-обёртка через `functools.update_wrapper`, вынимает `_th`); middleware-регистратор всех хуков; ~15 сценариев | `uv run python tests/contract/flexiq/spike_facts.py` |
| `tests/contract/flexiq/spike_dead_worker.py` | воркер в отдельном процессе убивается посреди джобы (`Popen.kill`), второй воркер подхватывает | `uv run python tests/contract/flexiq/spike_dead_worker.py` |
| `tests/contract/flexiq/spike_prefork.py` | `pool="prefork"` с `async def`-задачей; самодостаточный (flexiq + SQLite) | Windows: `uv run python …`; Linux: `docker run --rm -v "$PWD/tests/contract/flexiq:/spike" -w /spike python:3.11-slim sh -c "pip install -q flexiq==2.0.0 && python spike_prefork.py"` |
| `tests/contract/flexiq/spike_support.py` | общая обвязка: PostgreSQL, воркер в потоке, вывод | — |

Скрипты не собираются pytest (имена не `test_*`), проходят ruff/mypy/basedpyright без исключений в конфиге.
Каждая строка вывода — `<факт>: <JSON>`; ниже цитируются реальные строки (трейсбеки в `error` сокращены «…»).

Статусы: **подтверждён** (живой прогон), **подтверждён по коду** (исходники `.venv/Lib/site-packages/flexiq`, без прогона),
**опровергнут / уточнён**, **не проверен**.

## 1. Таблица фактов ARCHITECTURE §11.3

| # | Факт §11.3 | Статус | Доказательство |
|---|---|---|---|
| 1 | Нет своего job id при enqueue: id генерирует Rust (`Uuid::now_v7()`) | **подтверждён** | У `enqueue`/`enqueue_many` нет параметра id (сигнатуры в §4). Id в прогоне — UUIDv7: `01a0ef56-03b1-7040-a6e5-…` (версия `7`). |
| 2 | Middleware только синхронные `before/after`, around-хука нет; `on_retry/on_dead_letter` вызываются вне задачи с `SimpleNamespace(id, task_name)` | **подтверждён, уточнён** | `task_lifecycle.run_lifecycle`: `mw.before(current_job)` без `await`, затем `fn(*args, **kwargs)`, затем `after` — around нет. Живой прогон (`F5.hooks`): `before/after` — `JobContext`, поток `flexiq-async-executor` (то есть **внутри event loop задачи**: синхронный хук блокирует loop); `on_retry`/`on_dead_letter` — `ctx_type: "SimpleNamespace"`, `ctx_attrs: ["id","queue_name","retry_count","task_name"]`, поток `spike-worker` — **поток, вызвавший `run_worker`**, а не loop. Уточнения: `error` в этих хуках — не исходное исключение, а `RuntimeError('{"errtype":"RuntimeError","message":"fail 1","traceback":[…]}')`; `ctx.retry_count` в `on_dead_letter` = `0`, хотя джоба умерла на `retry_count=2` (`F5.dead_letters_after … "retry_count": 2`) — **полю верить нельзя**. |
| 3 | Async-задачи идут в одном event loop на процесс (поток `flexiq-async-executor`, семафор `async_concurrency=100`) | **подтверждён** | `F3.loop: {"distinct_loops": 1, "jobs": 20, "max_concurrent": 20, "threads": ["flexiq-async-executor"]}` при `workers=4` — async-задачи не ограничены числом потоков. Код: `AsyncTaskExecutor.start()` — `asyncio.new_event_loop()` + `Semaphore(max_concurrency)` + поток `flexiq-async-executor`; `_run_job` вызывает `self._registry[task_name]._flexiq_async_fn`, то есть нашу обёртку. В wheel есть `PyResultSender` (нативный async включён). |
| 4 | Prefork-пул не создаёт async-исполнитель (как исполняет корутины — не проверено) | **подтверждён, вопрос закрыт** | Windows: `P1.prefork_error: "NotImplementedError(\"pool='prefork' is not supported on Windows. …\")"` (`run_worker` бросает до старта). Linux (`python:3.11-slim`): `P2.summary: {"distinct_loops": 6, "distinct_pids": 2, "jobs": 6, "threads": ["MainThread"]}` — **новый event loop на каждую джобу**. Код `prefork/child.py`: `result = run_maybe_async(wrapper(*args, **kwargs))` → `asyncio.run(...)`. Completer, живущий в loop, в prefork невозможен → решение «только `pool="thread"`» верно. |
| 5 | В задаче известен `current_job.retry_count`, но не `max_retries`; решение «ретрай или DLQ» принимает Rust после задачи (`retry_on/dont_retry_on`, `retry_budget`, circuit breaker) | **подтверждён, уточнён** | `JobContext`: `id, task_name, retry_count, queue_name, namespace` — `max_retries` нет (он есть только в аргументах `AsyncTaskExecutor.submit_job` и не доходит до контекста). `max_retries=2` → `F5.retry_count_per_attempt: [0, 1, 2]`: выполнений `max_retries + 1`, последняя попытка имеет `retry_count == max_retries`. `dont_retry_on=[ValueError]` → `F5.dont_retry_on: {"attempts_retry_counts": [[0]], "hooks": [["on_dead_letter", 0]], "status": ["dead"]}`. Фильтры применяет Python (`AsyncTaskExecutor._check_retry` по `queue._task_retry_filters`), итог — Rust. Уточнение про circuit breaker — факт 5a ниже. |
| 5a | (часть 5) circuit breaker отправляет в DLQ вопреки вердикту RETRY | **опровергнут** | `F6.circuit_breaker`: 4 джобы, `threshold=2` → `"state": "open"`, у всех одна попытка, хуки только `on_retry`, статусы `["pending","pending","pending","pending"]`. Открытый breaker **откладывает** джобы (до `cooldown`), в DLQ не отправляет. Docstring `Queue.task(on_excess=…)`: «A tripped circuit_breaker always defers regardless». |
| 5b | (часть 5) `retry_budget` отправляет в DLQ до исчерпания `max_retries` | **подтверждён** | `max_retries=10`, `retry_budget="1/m"`, 3 джобы: `F6.retry_budget: {"attempts_retry_counts": [[0], [0], [0, 1]], "hooks": [["on_retry", null], ["on_dead_letter", …], ["on_dead_letter", …], ["on_dead_letter", …]], "status": ["dead","dead","dead"]}`; лог flexiq: `WARNING retry budget exhausted for spike_facts.budget_fail; dead-lettering …`. |
| 6 | Нет transactional enqueue: у flexiq свой пул соединений в Rust | **подтверждён по коду** | `Queue(backend="postgres", db_url=…, pool_size=…)` — только URL; ни `enqueue`, ни `enqueue_many` не принимают соединение/сессию; запись идёт через `PyQueue.enqueue_batch` в Rust. |
| 7 | `idempotency_key` дедуплицирует только пока джоба pending/running | **подтверждён, есть важное уточнение** | `F7.idempotency`: `enqueue` с занятым ключом возвращает существующий id (`"enqueue_single_dup_returns_existing_id": true`); после завершения тот же ключ даёт новую джобу (`"after_complete_same_key_new_id": true`). **Но `enqueue_many` с ключом, занятым pending-джобой, падает целиком**: `"RuntimeError: storage error: duplicate key value violates unique constraint \"idx_jobs_unique_key\""`, пачка атомарна (`"enqueue_many_dup_batch_is_atomic": true` — ни одна строка не вставлена). См. §3.2. |
| 8 | `aenqueue_many` — sync `enqueue_many` в общем `ThreadPoolExecutor(max_workers=2)`; одно `task_name` на вызов | **подтверждён, уточнён** | Код: `AsyncQueueMixin.aenqueue_many(**kwargs)` → `loop.run_in_executor(self._executor, …)`; `Queue.__init__`: `self._executor = ThreadPoolExecutor(max_workers=2)` — общий для всех `a*`-методов (`aget_job`, `adead_letters_after`…). Прогон: `F11.aenqueue_many: {"executor_max_workers": 2, "jobs": 2}`. Уточнение: общими на вызов являются не только `task_name`, но и **`priority`, `queue`, `max_retries`, `timeout`** (по-джобные только `delay_list`, `metadata_list`, `notes_list`, `expires_list`, `result_ttl_list`, `unique_keys`/`idempotency_keys`), и `None` в них — **умолчания `Queue`, а не задачи** (§3.1). |
| 9 | Нет per-job heartbeat; мёртвый воркер обнаруживается через 30 с, его джобы уходят в retry | **подтверждён, уточнён** | `D2.timeline_after_kill_s: [[0.0, "running", 0], [43.2, "running", 1], [43.7, "complete", 1]]` — после `kill` джоба переисполнена через **~43 с** (порог 30 с + heartbeat воркеров раз в 5 с + цикл reaper), и это **съедает попытку** (`retry_count` 0 → 1). Heartbeat — только у воркера (`worker_heartbeat`, «Called from Python every 5s»), у джобы нет. |
| 9a | (к факту 9) ретрай flexiq может прийти при ещё живом нашем lease | **подтверждён** | Жёсткий `timeout=1`, задача спит 12 с: `F8.hard_timeout: {"timeline": [["slow_start", 0, 0.0], ["slow_start", 1, 8.03], ["slow_end", 0, 12.02], ["slow_end", 1, 20.05]], "status": "complete"}` — корутина по таймауту **не отменяется**, reaper через 6–8 с запускает вторую попытку параллельно первой; обе завершаются успехом; `on_timeout`/`on_retry` не вызываются. |
| 10 | `retry_dead`, `replay` и авто-ретраи DLQ создают **новый** job id; kwargs и metadata переносятся | **kwargs — подтверждён; metadata — опровергнут** | `F9.retry_dead_replay`: `"retry_dead_new_id": true`, `"retry_dead_kwargs_th": {"i": "item-dlq"}`, `"replay_new_id": true`, `"replay_wrapper_th": [{"b": "batch-1", "i": "item-1"}]`. Но metadata **переписывается**: после `retry_dead` — `{"__dlq_retry_count":1,"__origin_job_id":"…","n":[1,2],"user":"как есть"}` (JSON пересобран, пробелы потеряны, добавлены служебные ключи); после `replay` — `{"replayed_from":"…"}` (исходная metadata потеряна). Авто-ретрай DLQ (`dlq_auto_retry_delay`) — не проверен (тот же `retry_dead` в Rust, ожидаемо так же). |
| 11 | Встроенные `group/chord` — оркестрация в потоке вызывающего без записи в хранилище; `Workflow` — статичный DAG; прогресса группы нет | **не проверен** | Вне объёма спайка: на адаптер не влияет (tallyho их не использует). |
| 12 | Проект молодой: 7 месяцев, 2 мажорные версии за 3 недели, ~20 звёзд | **не проверен** | Метаданные репозитория; на код адаптера не влияет. Косвенно: `flexiq.task` затеняет подмодуль (комментарий в `flexiq/__init__.py` о ломающем изменении) — API действительно движется. |

## 2. Открытые вопросы ARCHITECTURE §16 п.1

| Вопрос | Статус | Доказательство |
|---|---|---|
| `functools.wraps`-обёртка не ломает регистрацию и имя задачи | **подтверждён** | `F1.name: {"tracked": "spike_facts.echo", "expected_for_bare_fn": "spike_facts.echo", "registry_is_async": true, "async_fn_is_wrapper": true}`. Имя = `f"{_resolve_module_name(fn.__module__)}.{fn.__qualname__}"` (`Queue.task`), а `update_wrapper` копирует оба атрибута. `inspect.iscoroutinefunction(обёртка)` истинно → `_flexiq_is_async=True`, `_flexiq_async_fn` = наша обёртка, исполняется нативным async-executor. Оговорка: `Inject["res"]`-аннотации flexiq ищет через `typing.get_type_hints(fn, globalns=fn.__globals__)`; у обёртки `__globals__` — модуль tallyho, поэтому строковые аннотации с `Inject` из модуля пользователя могут не распознаться (явный `inject=[…]` работает) — проверить в A-FQ-13. |
| служебный kwarg `_th` проходит сериализацию | **подтверждён** | `F2.args.direct` / `F2.args.relay_roundtrip`: `"th_seen_by_wrapper": [{"b": "batch-1", "i": "item-1"}]`, `"th_in_function_kwargs": false`; с `serializer=JsonSerializer()` тоже (`F2.json_serializer_task: {"status": "complete", "th": [{"i": "x"}]}`) — значения `_th` должны быть JSON-совместимыми (строки/числа). `_th` лежит в payload джобы и переживает DLQ: `F5.dead_job_payload: {"kwargs": {"_th": {"i": "item-dlq"}}, "status": "dead"}`, `retry_dead` и `replay` (факт 10). |
| `on_dead_letter` срабатывает при исчерпании `retry_budget` | **подтверждён** | Факт 5b: три `on_dead_letter` на три джобы, две из них умерли на первой попытке (`retry_count=0 < max_retries=10`). Также срабатывает при исчерпании `max_retries` и при `dont_retry_on`. Событие `EventType.JOB_DEAD` приходит в тех же случаях (`F5.job_dead_events: {"count": 6, …}`, поток `flexiq-events_N`, payload `{"job_id", "task_name", "error", "duration_ms"}`). |
| работает ли async в `pool="prefork"` | **закрыт: работает, но несовместимо с tallyho** | Факт 4: каждая джоба — `asyncio.run` в `MainThread` дочернего процесса, новый loop на джобу; на Windows prefork недоступен вовсе. |

## 3. Расхождения с ARCHITECTURE и что они значат для адаптера

### 3.1. `enqueue_many` не берёт умолчания задачи

`F4.enqueue_many_none_defaults: {"task_config": {"max_retries": 5, "priority": 9, "timeout_ms": 77000}, "job": {"max_retries": 3, "priority": 0, "timeout_ms": 300000}}`.
`None` в `priority/max_retries/timeout` (и отсутствие `expires`) — это умолчания `Queue(default_retry=3, default_timeout=300, default_priority=0)`, а не `@queue.task(...)`.
`TaskWrapper.delay/map` сами подставляют `_default_*` задачи. Следовательно, **relay обязан передавать `priority, queue, max_retries, timeout, expires` явно**: опция вызова (`th.call(...).opts`) → иначе умолчание задачи.
Умолчания задачи адаптер фиксирует сам в `fq.task(**opts)`: `opts` поверх умолчаний сигнатуры `Queue.task` (`max_retries=3, timeout=300, priority=0, queue="default", expires=None`). Публично у `TaskWrapper` есть только `default_max_retries`.

### 3.2. Дубль ключа в `enqueue_many` роняет всю пачку

Повтор отправки relay'ем после сбоя (отправили, но не отметили outbox) с `idempotency_keys=[f"th:{item_id}"]` получит `RuntimeError` на весь чанк, пока старые джобы pending/running. Варианты для T8.1:
* при `RuntimeError` с `idx_jobs_unique_key` — повторить чанк поштучно через `enqueue(..., idempotency_key=...)` (он возвращает существующий id);
* текст ошибки — строка PostgreSQL из Rust (`storage error: duplicate key value …`); для SQLite/Redis будет другой → распознавать узко и покрыть контрактным тестом.

### 3.3. Группировка relay

Ключ группы для `enqueue_many`: `(task_name, queue, priority, max_retries, timeout, idempotent)`, а не только `task_name` (факт 8). По-джобно: `args_list, kwargs_list, delay_list, idempotency_keys, metadata_list, notes_list, expires_list, result_ttl_list`.

### 3.4. DLQ: где брать `_th`

Запись `dead_letters_after` не содержит payload: ключи `["dlq_retry_count","error","failed_at","id","metadata","original_job_id","queue","retry_count","task_name"]`. `metadata` пользователя — как есть (`"metadata_byte_for_byte": true`), но мы в неё не пишем. Путь: `original_job_id` → `queue.get_job(id)` (джоба в статусе `dead` остаётся читаемой) → `JobResult._py_job.payload_bytes` → `queue._deserialize_payload(task_name, payload)` → `kwargs["_th"]`. Для `on_dead_letter` так же: `ctx.id` → `get_job`. Джобы вычищаются retention flexiq (`result_ttl`/`Retention`) — сверка должна успевать раньше; иначе Item добьёт sweeper по lease.

### 3.5. Хуки DLQ

* `TaskMiddleware.on_dead_letter` вызывается в потоке `run_worker` (не в loop) → в Completer только через `loop.call_soon_threadsafe`, как и задумано. Регистрация: глобально — только в `Queue(middleware=[…])` при создании (публичного `add_middleware` нет), поэтому адаптеру проще добавлять свою middleware **в каждую задачу** (`queue.task(middleware=[*user_mw, th_mw])`) или подписаться `queue.on_event(EventType.JOB_DEAD, cb)` в `install()` (публично, глобально; колбэк в пуле `flexiq-events`). Middleware можно выключить из дашборда (`disable_middleware_for_task`), событие — нет → рекомендую `on_event(JOB_DEAD)` + сверку.
* `on_excess="drop"` по докстрингу dead-letter'ит джобу **без** хуков и событий — такие Items закроет только сверка `dead_letters_after`.

### 3.6. `before/after` в event loop

Middleware пользователя и наш код в обёртке делят один loop; синхронные хуки блокируют все async-задачи процесса. Наша обёртка не должна делать синхронного I/O (Completer — только async).

### 3.7. Размер payload

`Queue.max_payload_bytes = 1 MiB` (по умолчанию) проверяется в `enqueue_many`, то есть **в relay**, а не у продюсера. Продюсер должен проверять размер сам (длина закодированного payload + запас на `_th`) и падать сразу. A-FQ-01 «большие (1 МБ)» с умолчаниями flexiq не пройдёт — в тесте поднять `max_payload_bytes` или брать < 1 MiB.

### 3.8. `_th` видят не только мы

`on_enqueue` middleware, `predicate` (enqueue и dispatch), `queue.before_task/on_failure` хуки и `debounce_key` получают kwargs **с** `_th` (они работают до нашей обёртки). Для A-FQ-13 задокументировать: предикаты пользователя видят служебный ключ.

## 4. Сигнатуры API flexiq 2.0.0, нужные адаптеру

```python
# flexiq.Queue (app.py)
Queue(db_path=".flexiq/flexiq.db", workers=0, default_retry=3, default_timeout=300,
      default_priority=0, result_ttl=None, serializer=None, codec=None, codecs=None,
      middleware=None, backend="sqlite", db_url=None, schema="flexiq", pool_size=None,
      drain_timeout=30, …, async_concurrency=100, event_workers=4, …,
      dlq_auto_retry_delay=None, dlq_auto_retry_max=1, retention=None, max_pending=None,
      auto_migrate=True, middleware_timeout=5.0)
queue.max_payload_bytes: int = 1024 * 1024

# Декоратор (mixins/decorators.py); не keyword-only — передавать только по имени
queue.task(name=None, max_retries=3, retry_backoff=1.0, timeout=300, expires=None, priority=0,
           rate_limit=None, queue="default", circuit_breaker=None, retry_on=None,
           dont_retry_on=None, soft_timeout=None, middleware=None, retry_delays=None,
           inject=None, serializer=None, codecs=None, max_retry_delay=None,
           max_concurrent=None, idempotent=False, compensates=None, batch=None,
           predicate=None, on_false="defer", predicate_extras=None,
           default_defer_seconds=60.0, max_in_flight_per_task=None, retry_budget=None,
           on_excess="defer", debounce=None, debounce_key=None, debounce_max_wait=None,
           debounce_replace_payload=False) -> Callable[[Callable[..., Any]], TaskWrapper]
# имя по умолчанию: f"{module}.{qualname}"; TaskWrapper.name, .default_max_retries

queue.enqueue_many(task_name: str, args_list: list[tuple], kwargs_list: list[dict] | None = None,
                   priority=None, queue=None, max_retries=None, timeout=None, delay=None,
                   delay_list=None, unique_keys=None, metadata=None, metadata_list=None,
                   notes=None, notes_list=None, expires=None, expires_list=None,
                   result_ttl=None, result_ttl_list=None, idempotency_keys=None,
                   idempotent=None) -> list[JobResult]
# ValueError для задач с debounce; нет depends_on; batch= игнорируется (идёт мимо аккумулятора)
await queue.aenqueue_many(**kwargs) -> list[JobResult]      # только именованные; пул на 2 потока
queue.enqueue(task_name, args=(), kwargs=None, priority=None, delay=None, queue=None,
              max_retries=None, timeout=None, unique_key=None, metadata=None, notes=None,
              depends_on=None, expires=None, result_ttl=None, idempotency_key=None,
              idempotent=None, debounce=None, …) -> JobResult  # дубль ключа → существующий id

# Контекст задачи (context.py): contextvar в async-пути, thread-local в sync
from flexiq import current_job
current_job.id; .task_name; .retry_count; .queue_name; .namespace   # max_retries нет
current_job.check_cancelled()  # TaskCancelledError;  .check_timeout() # SoftTimeoutError

# Хуки (middleware.py): все синхронные
class TaskMiddleware:
    def before(self, ctx: JobContext) -> None
    def after(self, ctx: JobContext, result: Any, error: Exception | None) -> None
    def on_retry(self, ctx, error: Exception, retry_count: int) -> None     # ctx = SimpleNamespace
    def on_dead_letter(self, ctx, error: Exception) -> None                 # ctx = SimpleNamespace
    def on_timeout(self, ctx) -> None;  def on_cancel(self, ctx) -> None
    def on_enqueue(self, task_name, args, kwargs, options: dict) -> None    # видит _th
queue.on_event(EventType.JOB_DEAD, callback)  # callback(event_type, {"job_id","task_name","error","duration_ms"})

# DLQ и джобы (mixins/operations.py, inspection.py)
queue.dead_letters_after(limit: int = 10, after: str | None = None) -> Page[dict]  # .items, .next_cursor
await queue.adead_letters_after(limit=10, after=None) -> Page[dict]
queue.get_job(job_id) -> JobResult | None;  await queue.aget_job(job_id)
JobResult.id / .status / .metadata / .notes / .refresh();  JobResult._py_job.payload_bytes (приватно)
queue.retry_dead(dead_id) -> str;  queue.replay(job_id) -> JobResult
queue.cancel_job(job_id) -> bool;  queue.cancel_running_job(job_id) -> bool
queue.circuit_breakers() -> list[dict]

# Сериализация (приватно, но это ровно то, что делает flexiq)
queue._get_serializer(task_name) -> Serializer            # per-task или queue-level (+ codec chain)
queue._encode_payload(task_name, args: tuple, kwargs: dict) -> bytes   # + per-task codecs
queue._deserialize_payload(task_name, payload: bytes) -> tuple[tuple, dict]

# Воркер программно (mixins/lifecycle.py): блокирует; в не-главном потоке сигналы не ставит
queue.run_worker(queues: Sequence[str] | None = None, tags=None, pool: str = "thread",
                 app: str | None = None, mesh=None) -> None
queue.shutdown() -> None   # мягкая остановка из любого потока
# pool="prefork": NotImplementedError на win32; на Linux требует app="module:queue"
```

Параллельность: `Queue(workers=N)` — потоки для sync-задач (0 → число CPU); `async_concurrency` — семафор async-executor (все async-задачи процесса в одном loop); по задаче — `max_concurrent` (кластерно, через БД) и `max_in_flight_per_task` (в процессе).

## 5. Вывод по D-006 (сериализация payload)

**Что получает функция задачи.** flexiq сериализует пару `(args, kwargs)` сериализатором задачи (`@task(serializer=…)`, иначе `Queue(serializer=…)`, по умолчанию `SmartSerializer`: msgpack с ExtType для кортежей и откатом на cloudpickle), затем применяет codec chain. Воркер делает обратное и вызывает функцию. С умолчаниями функция получает **ровно те объекты**, что передал продюсер: `F2.args.direct: {"exact_repr": true, …}` для `(1, "юникод ✓", None, (1, (2, 3)), [1, (2, 3)], {1: "a", "k": b"\x00\xff"}, datetime(…, tzinfo=UTC), Decimal("1.10"), frozenset({1, 2}), Point(1, 2))` и `{"flag": True, "nested": {"t": (1,)}}`.

**JSON не годится как кодек payload для flexiq.** `F2.args.json_codec: {"full_args": "TypeError: Object of type bytes is not JSON serializable", "tuple_roundtrip": [[1, 2]], "int_key_roundtrip": {"1": "a"}}` — JSON падает на `bytes/datetime/Decimal/set/dataclass` и молча меняет кортежи на списки и int-ключи на строки. `JsonSerializer` по умолчанию нарушит A-FQ-01.

**Как адаптеру хранить аргументы.** `FlexiqAdapter` предоставляет свой `Serializer` для `th_item.payload`:
* `dumps`: `queue._encode_payload(task_name, args, kwargs)` — те же байты, что положил бы `apply_async` (сериализатор задачи + общий и per-task кодеки; шифрование `AesGcmCodec` пользователя распространяется и на хранение в `th_item`);
* `loads` в relay: `queue._deserialize_payload(task_name, payload)` → `args, kwargs` → `kwargs["_th"] = {"i": item_id, "b": batch_id}` → `enqueue_many`.

Круг «закодировать → раскодировать → `enqueue_many`» сохраняет объекты: `F2.args.relay_roundtrip: {"exact_repr": true, …}`. Опции постановки (`priority, queue, delay, metadata, notes, expires, …`) — отдельное JSON-поле рядом с payload: они JSON-совместимы по контракту flexiq (`metadata` — строка, `notes` — dict ≤ 15 ключей). Цена — лишний цикл сериализации в relay и зависимость от трёх приватных методов `Queue` → проверять их наличие в `install()` (A-FQ-17: «понятная ошибка при несовместимом API») и закрепить контрактным тестом. Альтернатива без приватного API — свой кодек в адаптере (например, cloudpickle) — хуже: не учитывает пользовательские `serializer`/`codecs` задачи и тащит в tallyho класс рисков `pickle`, который правила проекта запрещают.

## 6. Что проверить/учесть дальше

* **T8.1**: `retry_verdict(exc)` = FINAL, если `retry_count >= max_retries` джобы (эффективное значение: опция вызова или умолчание задачи, §3.1), или исключение попадает под `dont_retry_on` / не попадает под непустой `retry_on` (логика `AsyncTaskExecutor._check_retry`; фильтры брать из `opts` декоратора, а не из `queue._task_retry_filters`). `retry_budget` → страховка `on_event(JOB_DEAD)` + сверка. Circuit breaker джобу не убивает, а откладывает на `cooldown` → Item висит `dispatched` до ретрая; это нормально, но `lease_ttl`/sweeper не должны считать его потерянным раньше.
* **T8.1**: `install()` для `prefork` — ошибка; проверять также наличие `_encode_payload`, `_deserialize_payload`, `JobResult._py_job`.
* **T8.1**: mypy (`disallow_any_decorated`) не принимает `@functools.wraps(fn)` на обёртке — в `th.tracked` использовать `functools.update_wrapper(wrapper, fn)` вызовом, как в спайке.
* **T8.2 / ACCEPTANCE**: A-FQ-09 ожидает DLQ от circuit breaker — по факту 5a это откладывание; A-FQ-14 и §11.3 про перенос `metadata` при `retry_dead/replay` — опровергнуто (факт 10); A-FQ-10 — жёсткий таймаут не отменяет корутину, вторая попытка приходит через ~`timeout` + цикл reaper (6–8 с при `timeout=1`); A-FQ-01 — лимит `max_payload_bytes` 1 MiB (§3.7).
