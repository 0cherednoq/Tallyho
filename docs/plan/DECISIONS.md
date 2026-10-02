# tallyho v1 — журнал решений

> Решения, принятые при реализации поверх [ARCHITECTURE.md](../ARCHITECTURE.md). Новые записи — в конец.
> Статусы: `ACCEPTED` — действует; `ACCEPTED (автономно, пересмотреть)` — принято циклом без человека; `SUPERSEDED by D-NNN`.

## D-001 · Иерархия документов · ACCEPTED
При расхождениях прав `ARCHITECTURE.md` v2.1. `ACCEPTANCE.md` задаёт критерии тестов. `DESIGN.md`, `API.md`, `COUNTERS.md` — исторический ресёрч: брать из них только то, что не противоречит ARCHITECTURE (в частности, **нет** SQLite, Flow/Run, бизнес-статусов, `bw_status_history`).

## D-002 · Время · ACCEPTED (окончательно в T1.4)
Все сроки (`lease_until`, `available_at`, `start_at`, `deadline_at`, retention, backoff хуков) считаются по времени БД (ARCHITECTURE §10, ACCEPTANCE A-CH-09). В движке нет `datetime.now()`.
Для тестов `FakeClock` должен подменять «сейчас» в SQL. Предлагаемая механика: `Clock.now() -> datetime | None`; `None` → storage использует `now()` БД, значение → storage биндит его параметром. `protocols` не импортирует SQLAlchemy (import-linter). Интервалы в процессе (тик Completer, heartbeat) — `asyncio` loop time.
Итог T1.4: `Clock.now() -> datetime | None`. `SystemClock.now()` всегда `None` → storage подставляет `now()` БД (время начала транзакции); aware `datetime` (FakeClock) storage биндит как `DateTime(timezone=True)`. Хелпер storage `sql_now(clock)` — единственный способ получить «сейчас» в SQL. `Clock.monotonic()` — только для длительностей в событиях Observer.

## D-003 · Покрытие и гейты · ACCEPTED
`fail_under = 95` в `pyproject.toml` нельзя выполнить юнит-тестами: код storage/engine покрывается интеграционными тестами с PostgreSQL. Поэтому `poe test` (unit, быстрый гейт pre-push) идёт без покрытия, а `poe test-all` (unit + integration) — с покрытием 95%. Порог не снижается.

## D-004 · Соединения в storage/engine · ACCEPTED
Функции `storage` и `engine` принимают `AsyncConnection`. `AsyncSession` пользователя разворачивается через `await session.connection()` на границе (`tallyho.storage.tx.resolve_connection`). Мы никогда не делаем commit/rollback чужой транзакции.

## D-005 · Коды состояний (smallint) · ACCEPTED
* `th_batch.state`: `open=0, sealed=1, finalizing=2, succeeded=10, completed_with_errors=11, failed=12, cancelled=13` (терминальные ≥ 10 — удобно для partial-индексов и `state < 10`). `finalizing` в БД не сохраняется (существует только внутри транзакции, ARCHITECTURE §6.1), код зарезервирован.
* `th_item.state`: `active=0, ok=10, skip=11, error=12, cancelled=13`.
* `th_outbox.kind`: `item=0, callback=1`.
* `th_batch.on_feeder_failed`: `seal=0, cancel=1`.
Значения фиксируются снимок-тестом в T1.1: менять только миграцией.

## D-006 · Сериализация payload — кодеком задачи flexiq · ACCEPTED (T8.0)
JSON не подходит: он теряет bytes, datetime и Decimal, превращает кортежи в списки, а int-ключи в строки (нарушается A-FQ-01). `th_item.payload` хранит байты `queue._encode_payload(task, args, kwargs)`. Relay декодирует их через `queue._deserialize_payload`, добавляет `_th` и вызывает `enqueue_many`. Так пользовательские `serializer`/`codecs`/шифрование flexiq действуют и на хранение в `th_item`. Цена — зависимость от приватного API: `install()` проверяет его наличие, контрактный тест его закрепляет. Протокол `Serializer` остаётся для адаптеров без собственного кодека (по умолчанию JSON). Подробности — [FLEXIQ_SPIKE.md](FLEXIQ_SPIKE.md).

## D-007 · Ветка и коммиты · ACCEPTED
Работа в ветке `impl/v1` (pre-commit `no-commit-to-branch` запрещает `main`). Коммиты небольшие и зелёные, несколько на задачу; сообщения на русском, формат — PLAN §0.6. Push и PR — только по просьбе человека.

## D-008 · Автономность · ACCEPTED
Цикл работает без ограничения по времени, пока библиотека не доделана, и не ждёт человека: неясности решает сам с записью `ACCEPTED (автономно, пересмотреть)` в этом файле. Статус `OPEN` не используется. Правила — PLAN §0.4.

## D-009 · Общие хелперы тестов — пакет `tests.*` · ACCEPTED (T0.2)
Хелперы импортируются как `tests.helpers.*`. Для этого pytest `pythonpath=["."]`, mypy `mypy_path=["src", "."]` + `explicit_package_bases`, basedpyright `extraPaths=["."]` для tests, ruff isort `tests` в known-first-party. Иначе mypy видит один файл под двумя именами. Следствие: скрипты в `tests/` запускаются только как модули (`python -m tests....`).

## D-010 · PostgreSQL на xdist-воркер · ACCEPTED (T0.2)
При `pytest -n` каждый xdist-воркер лениво поднимает свой контейнер (только если ему достались интеграционные тесты), поэтому unit-прогоны не требуют Docker. С `TALLYHO_TEST_DSN` все воркеры делят одну БД; тесты изолирует уникальная схема (фикстура `schema`).

## D-011 · Коды ResultClass/OutboxKind и CancelReason · ACCEPTED (T1.1)
`ResultClass` — `IntEnum` с кодами 10–13, совпадающими с терминальными `ItemState`: в `th_item.state` хранится `active` или класс итога. `OutboxKind` (item=0, callback=1) закреплён снимком. `CancelReason` — `StrEnum` (`cancel, deadline, fail_fast, policy`; `th_batch.cancel_reason` — text). `policy` — порог политики с `action="fail"`. Итог: `cancel` → cancelled, остальные причины → failed. Имена ошибок без суффикса `Error` (`BatchPurged`, `DownstreamFinalized`, `UnsupportedOption`, `ConcurrentModification`) — как в ARCHITECTURE/ACCEPTANCE; N818 подавлено точечно.

## D-012 · Relay передаёт умолчания задачи явно · ACCEPTED (автономно, пересмотреть) (T8.0)
`enqueue_many` с `None` берёт умолчания Queue (3 ретрая, приоритет 0, таймаут 300 с), а не `@task`. Relay передаёт `priority, queue, max_retries, timeout, expires` явно: из опций вызова, иначе из опций декоратора, зафиксированных в `fq.task`. Ключ группы для `enqueue_many` — `(task_name, queue, priority, max_retries, timeout)`.

## D-013 · Дубль `idempotency_key` в `enqueue_many` → поштучный повтор · ACCEPTED (автономно, пересмотреть) (T8.0)
Пачка с ключом, занятым pending-джобой, целиком падает (`RuntimeError: duplicate key … idx_jobs_unique_key`) и ничего не вставляет; одиночный `enqueue` возвращает существующий id. Relay при этой ошибке повторяет чанк поштучно через `enqueue`.

## D-014 · Страховка DLQ: событие + сверка · ACCEPTED (автономно, пересмотреть) (T8.0)
Основной путь — `queue.on_event(EventType.JOB_DEAD)` в `install()`. Сверка: `dead_letters_after` → `get_job(original_job_id)` → декодирование payload → `_th` (в записи DLQ payload нет). Middleware `on_dead_letter` не используется: она ставится только при создании Queue, её можно выключить из дашборда, а `ctx.retry_count` в ней равен 0.

## D-015 · Поправки к ARCHITECTURE §11.3 и ACCEPTANCE по итогам спайка · ACCEPTED (T8.0)
* Circuit breaker не отправляет джобы в DLQ, а откладывает их до `cooldown` (A-FQ-09 — только `retry_budget`).
* `retry_dead`/`replay` не сохраняют `metadata` пользователя; kwargs (и `_th`) переносятся (A-FQ-02, A-FQ-14).
* Мёртвый воркер обнаруживается за ~43 с (не 30), повтор съедает попытку.
* Prefork выполняет async-задачу через `asyncio.run` в новом loop, на Windows — `NotImplementedError`: `install()` даёт явную ошибку.
* `_th` видят чужие предикаты, `on_enqueue` и `before_task` (задокументировать в A-FQ-13).
Документы исправлены отдельным коммитом (AGENTS.md: «сначала обнови документ»).

## D-016 · PayloadCodec — кодек payload адаптера · ACCEPTED (автономно, пересмотреть) (T1.4)
Протокол `PayloadCodec.encode(task_name, args, kwargs) -> bytes` / `decode(task_name, data) -> (args, kwargs)`. `FlexiqAdapter` реализует его через `_encode_payload`/`_deserialize_payload` (D-006); для остальных адаптеров есть `SerializerCodec(Serializer)` (по умолчанию JSON, `{"args": [...], "kwargs": {...}}`). Api выбирает кодек так: адаптер, если `isinstance(adapter, PayloadCodec)`, иначе `SerializerCodec()`.

## D-017 · `Runtime.reconcile_dead(cursor) -> DeadLetters` · ACCEPTED (автономно, пересмотреть) (T1.4)
Сверка DLQ идёт по непрозрачному курсору (`dead_letters_after(after=...)`, D-014), курсор хранит engine. ARCHITECTURE §3.4 и §4.2 обновлены (40241bb). `Dispatcher.task_name(fn: Callable[P, object])`: `Callable[..., object]` mypy считает явным Any.

## D-018 · Политики ошибок: `PolicyVerdict` и семантика порога · ACCEPTED (автономно, пересмотреть) (T1.2)
* `FailurePolicy.evaluate` возвращает `PolicyVerdict(action, ratio, processed, failed, reason)`. Имя `Verdict` занято протоколом ретраев (`RETRY/FINAL`). При `action=FAIL` в `reason` лежит `CancelReason.FAIL_FAST` или `CancelReason.POLICY`.
* `threshold`: обработанные — `ok + skip + error`, отменённые не учитываются. Числитель — `error` или, если задан фильтр, сумма меток из `labels`. Срабатывает при `processed ≥ min_processed` и доле **строго больше** `ratio`. По умолчанию `min_processed=0`, `action="fail"`. `fail_fast` срабатывает на первой ошибке без учёта `min_processed`.
* `PolicyBreach.labels: list[str]`: пример в §12.4 ждёт `['hard_bounce']` в f-строке. Из-за этого объект нехешируемый.

## D-019 · SQLAlchemy ≥ 2.1 · ACCEPTED (автономно, пересмотреть) (T2.1)
`Table(..., postgresql_with=...)` (fillfactor, per-table autovacuum) в 2.0.x даёт `ArgumentError`. Кроме того, `TypedColumns` из 2.1 дают типизированные `table.c.*`: голый `Table` раскрывается в `Column[Any]`, а это ломает `disallow_any_explicit`. ARCHITECTURE §1/§4.1 и `pyproject.toml` обновлены.

## D-020 · Предикаты partial-индексов — литералы · ACCEPTED (T2.1)
Индекс sweeper'а по `th_batch.updated_at` построен с условием `state < 10` (активные). Индексы дедлайнов и снимков — `state IN (0, 1)`. **В запросах горячего пути условие по `state` пишется литералом, а не bind-параметром.** Иначе после пяти выполнений asyncpg переходит на generic plan и перестаёт брать partial-индекс. Условие запроса должно логически следовать из предиката индекса. Добавлен индекс `th_expiry(expires_at)`, которого нет в §5.2.

## D-021 · `sql_now(clock)` принимает структурный `NowSource` · ACCEPTED (автономно, пересмотреть) (T2.3)
storage не импортирует `tallyho.protocols`, поэтому в `storage/now.py` объявлен минимальный Protocol с методом `now() -> datetime | None`, и `Clock` ему структурно соответствует. Модуль назван `now.py`, а не `time.py`: ruff A005 запрещает затенять stdlib.

## D-022 · `after_commit` на AsyncConnection срабатывает перед COMMIT · ACCEPTED (автономно, пересмотреть) (T2.3)
У SQLAlchemy нет события «после COMMIT» для соединения. Для AsyncSession колбэк вызывается после реального COMMIT корневой транзакции, для AsyncConnection — перед DBAPI-коммитом (возможен ложный вызов). Колбэки — только подсказки (kick relay или fold): потребитель перечитывает БД, а страховку дают relay scan и sweeper. Колбэк синхронный и быстрый (`Event.set`, `put_nowait`). Откат savepoint отбрасывает колбэки, зарегистрированные внутри него.

## D-023 · Повтор своих транзакций · ACCEPTED (автономно, пересмотреть) (T2.3)
`run_transaction(engine, work)`: `RetryPolicy` — 5 попыток, base 50 мс, cap 2 с, equal jitter; повтор на `40001/40P01/55P03`. Когда попытки кончились, бросается `ConcurrentModification from exc`. `statement_timeout` по умолчанию 30 с (в §15 не задан), `lock_timeout` 5 с. `work` вызывается заново на каждой попытке, поэтому побочные эффекты вне БД в нём запрещены. `own_transaction` делает одну попытку.

## D-024 · Математика прогресса: вход, виртуальные Items, ETA · ACCEPTED (автономно, пересмотреть) (T1.3)
Вход `compute_progress` — плоский список `NodeCounters`, связи через `parent_id` и `fed_by`. Узел закрыт для правила `expected`, если состояние не `open`. `Progress.final` = терминальное состояние. Оценка Кнута округляется целочисленно. **Виртуальный Item под-батча вставляется с `weight=0`**, и из `found`/`expected` родителя вычитается по одному виртуальному Item на ребёнка. ETA = `(expected − done) / EMA`, вес `1 − exp(−Δt / eta_window)`; состояние EMA хранит вызывающий (Snapshotter, `watch`). ETA узла с детьми — максимум по поддереву.

## D-025 · Установка схемы: операции без bind-параметров, advisory lock по схеме · ACCEPTED (автономно, пересмотреть) (T2.2)
`migration_statements()` — единый источник операций для `migrate()` и `tallyho.storage.alembic.upgrade(op, version=...)`. В операциях нет bind-параметров, иначе не работает offline Alembic (`--sql`). Advisory lock берётся по схеме (blake2b), а не по префиксу, и до `SET LOCAL lock_timeout`. `migrate` сам создаёт схему и принимает `schema=None` (search_path). **Схема v1 строится из текущего `build_metadata`: при первом изменении `tables.py` заморозить операции v1 и добавить версию 2.** Модуль Alembic — `tallyho.storage.alembic` (ARCHITECTURE §11.1 исправлен, 8d43fd4).

## D-026 · Имена хуков в `th_batch.hooks` · ACCEPTED (автономно, пересмотреть) (T3.1)
В `th_batch.hooks` пишутся `finalized`, `progress`, `policy_breach` (`HookName`). Значение `progress` совпадает с `storage.tables.PROGRESS_HOOK`, это закреплено тестом. Наличие хука в процессе проверяет `HookRegistry.ensure(kind, row.hooks)` → `HookMissingError("on_<name>")`. У реестра нет глобального состояния: `Tallyho` держит свой экземпляр.

## D-027 · `reconcile` работает приращениями · ACCEPTED (автономно, пересмотреть) (T2.4)
Под `FOR UPDATE` строки батча на одном снимке считаются факт по `th_item` (FILTER по литеральным состояниям) и сумма counter + delta. Их разница — дрейф: он прибавляется к слоту 0, остальные слоты переносятся в слот 0 и обнуляются (по возрастанию slot). Буквальная перезапись (COUNTERS §3.4) потеряла бы завершение от параллельного Completer, а удаление дельт в `reconcile` дало бы цикл блокировок со свёрткой.

## D-028 · `fold_deltas` возвращает дельты и не пишет в слот · ACCEPTED (автономно, пересмотреть) (T2.4)
Completer складывает результат свёртки со своим буфером и выполняет **один** `upsert_slots` в той же транзакции. Два отдельных отсортированных прохода по `th_counter` нарушили бы глобальный порядок блокировок. `read_counters(...).pending == 0` — проверка финализации.

## D-029 · Схема v1 редактируема до первого релиза · ACCEPTED (автономно, пересмотреть)
Пока нет ни одного релиза и установок, недостающие колонки добавляются прямо в v1 (`tables.py` + golden), без миграции v2. Правило D-025 «заморозить v1» начинает действовать с первого опубликованного релиза. Первый случай — Fix-1 (d_* колонки в `th_counter_delta`).

## D-030 · kind под-батча по умолчанию — `<kind родителя>.<key>` · ACCEPTED (автономно, пересмотреть) (T4.1)
В §11.2 у `sub_batch` нет `kind`, а хуки регистрируются на kind корня (§12.4). Если бы этап наследовал kind корня, `on_finalized` корня срабатывал бы на каждом этапе. Явный `kind=` разрешён (UC-06).

## D-031 · Виртуальный Item и запись outbox · ACCEPTED (автономно, пересмотреть) (T4.1)
Виртуальный Item: `task_name="tallyho.sub_batch"`, `payload=b""`, `key=NULL`, `weight=0`. В outbox он не попадает и в `tree_total` не считается: `max_items` ограничивает только реальные Items. Запись outbox Item'а: `id = item_id`, `task_name` заполнен, `payload=NULL` — relay берёт payload из `th_item`.

## D-032 · Блокировки продюсера и колбэки в options · ACCEPTED (автономно, пересмотреть) (T4.1)
* `add_items` держит строку батча `FOR SHARE`: параллельные продюсеры не мешают друг другу, а seal, отмена и CAS финализации ждут commit. `create_sub_batch`, `add_feed` и `seal` берут `FOR UPDATE` в порядке id. `add_feed` отказывает, если источник уже финализирован или этап закрыт.
* `options = {"failure_policy": …, "callbacks": {"on_succeeded": {task_name, payload (base64 кодека), queue, options}}}` читается через `StoredCallback.from_json`.
* Ошибки: `add`/`sub_batch` продюсера в этап → `SpawnTargetError`; в закрытый, финализированный или отменяемый батч → `SealError`. `seal` не запускает финализацию сам: после commit вызывающий зовёт `try_finalize`.
* Продюсеру в транзакции пользователя api даёт **отдельный слот** счётчиков, не слот Completer процесса. Иначе длинная транзакция пользователя держит строку `th_counter`.

## D-033 · Опции вызова хранятся в `th_item.options` · ACCEPTED (автономно, пересмотреть)
`TaskCall.queue` и опции брокера (priority, max_retries, timeout, expires …) нужны relay при каждой отправке, в том числе при повторной отправке sweeper-ом после истёкшего lease. Поэтому они хранятся в колонке `th_item.options jsonb NULL`, а не в outbox. Колонка добавляется в v1 (D-029). Делается первым шагом T4.2.

## D-034 · Completer claim/heartbeat/release не зависит от Relay · ACCEPTED
T4.3a зависит только от T4.1, T4.3b — от T4.3a и T4.2 (там нужны `relay.kick` и окно `max_in_flight`). Так T4.2 и T4.3a идут в одной волне.

## D-035 · Окно `max_in_flight` — таблица `th_window` · ACCEPTED (автономно, пересмотреть) (T4.2)
В `th_window(item_id PK, batch_id)` одна строка на отправленный и ещё не завершённый Item, поэтому таблица не больше суммы окон. По счётчикам окно не посчитать: `dispatched` накопительный, отмена и виртуальные Items его искажают. Захват окна батча сериализован `pg_try_advisory_xact_lock`: занятый батч пропускается, а не ждёт. **Все пути, которые завершают Item или удаляют его запись outbox, вызывают `release_window`** — finish, отмена неотправленных, ленивая отмена при claim, парковка при claim, sweeper `lease_expired`/`expired`. Страховка — `refill_window` в `scan_once`. Если освобождённое место вернуло запись из парковки, у неё `available_at = -infinity`, а пауза и окно паркуют в `infinity`. Колонка `th_outbox.options` хранит опции колбэков. Индекс outbox — `(batch_id, available_at)`. ARCHITECTURE §5.1, §5.2, §11.2 обновлены (0ffc885).

## D-036 · Completer: claim, перехват истёкшего lease, возврат в outbox · ACCEPTED (автономно, пересмотреть) (T4.3a)
* Claim блокирует `th_batch FOR SHARE` → `th_item FOR UPDATE` → `th_lease FOR UPDATE`, пачки отсортированы по id. Без этих блокировок гонки с pause/resume/cancel оставляли Item в outbox с `infinity` навсегда. Это отход от COUNTERS §3.2; HOT при этом сохраняется.
* Истёкший lease claim перехватывает сам: `attempt += 1`, исход CLAIMED. Живой lease даёт DUPLICATE. Sweeper берёт `th_lease` через SKIP LOCKED и перепроверяет `lease_until`.
* Возврат Item в outbox (PARKED, `close(requeue_held=True)`) делает `dispatched -= 1`, но только если запись реально вставлена.
* Ленивая отмена при claim: `label='cancelled'`, счётчики `cancelled` и `w_done`. Схему label-метрик задаёт T4.3b, отмену в T4.7 согласовать с ней.
* `heartbeat` возвращает `bool` («lease ещё мой»). Ошибка групповой транзакции → `CompleterError` (с `__cause__`) каждой операции пачки.
* **Для T4.3b:** claim при PARKED и CANCELLED тоже должен вызывать `release_window` (D-035), сейчас он этого не делает.

## D-037 · Граница мутационного gate для CAS · ACCEPTED (автономно, пересмотреть) (T10.6)
`mutmut` запускается на Linux/WSL и проверяет функции, в которых находятся сами условные SQL-переходы: `_Tx.claim`/`_Tx.finish`, `Finalizer._cas`, `Snapshotter._attempt` и `Operations._retry_items`. Публичные orchestration-методы (`try_finalize`, `tick`, `retry_failed`) покрыты интеграционными тестами, но не входят в mutation gate целиком: мутации их логирования, расписания и post-commit уведомлений не являются мутациями CAS и раздувают прогон без изменения критерия A-NF-06.

Точечные `do_not_mutate_patterns` допустимы только для эквивалентных или нефункциональных изменений, которые отдельно проверяются другими гейтами: в T10.6 это сортировка строк и размер чанка (`stress`/EXPLAIN), имя SQL alias у `unnest` и терминальное `state` временной cache-строки скалярного finish (после CAS оно больше не читается; остальные поля нужны spawn/accounting/observer и мутируются). Накопление возвращаемого количества между чанками помечено `pragma: no mutate`: это orchestration-арифметика вне CAS, тогда как переходы каждого чанка полностью входят в gate. Повторный `state=error` под уже удерживаемым `FOR UPDATE` и фильтр виртуальных Items после обязательной `_reactivate_virtuals` удалены как избыточные предикаты. Timeout мутанта считается убитым: это ожидаемый результат для удаления условия выхода или подмены хук-сессии; gate запрещает `survived`, `suspicious` и `no tests`.

## D-038 · Атрибуты батча: side-таблица, три типа значений · ACCEPTED (владелец, 2026-10-01) (T13.0)
Неизменяемые `attributes` и `memo` корня хранятся в `th_batch_attr(batch_id PK, attributes jsonb, memo jsonb)` с GIN `jsonb_path_ops`, а не в колонках `th_batch`: строка батча часто обновляется, и каждое не-HOT обновление заново писало бы jsonb в GIN. Значения — только `str | int | bool`, `UUID` нормализуется в строку одной функцией и при записи, и в фильтре; `float`, `None`, `datetime` и коллекции отклоняются `InvalidAttributesError`, потому что containment строг к типу JSON. Зарезервирован только префикс `tallyho.`, обязательного `app.*` нет. Атрибуты есть только у корня, сводка любого узла отдаёт атрибуты корня. Повторное создание с тем же `(kind, key)` атрибуты не меняет. Листинг — `th.list_batches(...)`: имя плоское, как `th.find` и `th.handle`; только корни, keyset по `id DESC`, лёгкий `BatchInfo` без счётчиков. Источник — [tallyho-boundaries-and-attributes.md](tallyho-boundaries-and-attributes.md), разбор — [V1_EXTENSIONS_PLAN.md](V1_EXTENSIONS_PLAN.md) §1.

## D-039 · Read Model, operational API и изменяемые атрибуты отложены до v1.x · ACCEPTED (владелец, 2026-10-01) (T13.0)
PG views поверх `th_*`, `th.health()` и изменяемые search attributes в v1 не входят: потребителя нет, а стабильные views фиксируют схему раньше первого релиза. В v1 обходятся `th.list_batches`, `handle.view()` и доменными таблицами через tx-хуки. Все три добавляются позже без поломки совместимости (ARCHITECTURE §16).

## D-040 · Lifecycle-хуки и очередь результатов отложены; исходы Items переносит рецепт финального экспорта · ACCEPTED (владелец, 2026-10-01) (T13.0)
`on_started`, `on_terminal_items` и очередь `th.results.take(session=)` из [V1_LIFECYCLE_HOOKS.md](V1_LIFECYCLE_HOOKS.md) в v1 не делаются. Обзор Sidekiq, Oban, River, Laravel, GoodJob, Spring Batch, Broadway и Step Functions ([V1_EXTENSIONS_PLAN.md](V1_EXTENSIONS_PLAN.md) §2.2) показал: стандартная пара — транзакционное завершение кодом задачи плюс колбэк батча с перечислением упавших, и оба механизма в tallyho уже есть (`complete_in`, `on_finalized_task` + `handle.items()` + `release()`). Долговечного хука старта батча нет ни в одной из библиотек; слушатель внутри транзакции чанка потребовал бы очереди ссылок, фонового Projector и барьера финализации. Вместо этого закрываются три пробела: `handle.items(states=, labels=)` (D-041), сброс `released_at` при `retry_failed` (Fix-2) и документированный и проверенный тестом рецепт (ARCHITECTURE §12.9). Сознательно не выполняется одно требование: инфраструктурные исходы не видны в домене, пока батч идёт. Вернуться, когда появится правило, обязанное сработать до первой задачи, или потребность в исходах «по ходу».

## D-041 · `handle.items(states=)` обходит батч окнами фиксированного размера · ACCEPTED (автономно, пересмотреть) (T13.0)
Индекса по `th_item.state` нет и не будет (§5.2: на `th_item` нет индексов по изменяемым колонкам, иначе finish перестаёт быть HOT). Запрос `WHERE batch_id = … AND state IN (…) ORDER BY id LIMIT n` при редких совпадениях сканировал бы весь остаток батча одним statement и упирался в `statement_timeout`. Поэтому каждый запрос читает по индексу `(batch_id, id)` не больше `items_scan_window` строк (подзапрос с `LIMIT`), а фильтр по состоянию применяется снаружи; курсор сдвигается на последнюю строку окна, даже если совпадений нет. Цена — число запросов пропорционально размеру батча. Альтернативы: помечать отменённые Items в `th_item_mark` (отмена миллиона Items удвоила бы запись) и partial-индекс по терминальным состояниям (ломает HOT).

## D-042 · Номера миграций после заморозки v1 (пересмотр D-029) · ACCEPTED (автономно, пересмотреть) (T13.0)
D-029 разрешал править v1 до первого релиза, но T4.8 уже заморозил v1 отдельным golden и ввёл v2. Возвращаться к редактируемой v1 значило бы переписывать два golden и путь v1 → v2. Поэтому изменения схемы до релиза идут обычными версиями: атрибуты — миграция v3. Слить v1…vN в одну базовую версию перед публикацией — отдельное решение владельца на T12.3.

## D-043 · Finalizer блокирует `th_batch` до CAS и до `th_item`/`th_counter` · ACCEPTED (автономно, пересмотреть) (Fix-4)
Порядок §9.2 `[домен] → th_batch → th_item → th_counter → th_metric` обязателен и для финализации. Раньше финализатор источника брал слот счётчика родителя раньше строки наполняемого этапа — это давало дедлок с финализацией отменённого этапа (наблюдался в стрессе) и с Completer со spawn в этап (воспроизведён тестом). Теперь после хука одним `FOR UPDATE` по id берутся сам батч и его этапы, затем CAS, seal этапов, outbox, виртуальный Item, счётчики. Хук по-прежнему вызывается до любых блокировок `th_batch` (A-DB-08). Цена — один дополнительный запрос к `th_feed`: связи читаются до блокировки и перечитываются после CAS; при расхождении попытка повторяется, хук вызывается заново. ARCHITECTURE §8.1 п.3 и §9.2 обновлены.

## D-044 · Маркеры документации `tallyho-example` и `tallyho-noexec` · ACCEPTED (автономно, пересмотреть) (T12.1)
README и `docs/guide` строгие: каждый python-блок помечен `<!-- tallyho-example: name -->` (исполняется на PostgreSQL с `InlineBroker`) или `<!-- tallyho-noexec: причина -->`; в README noexec запрещён. ARCHITECTURE остаётся нестрогой: исполняются только помеченные блоки. Тот же тест проверяет относительные ссылки и якоря. Исполняемый пример flexiq использует настоящий `flexiq.Queue` на SQLite без воркера, поэтому тест документации требует extra `flexiq`.

## D-045 · Хаос-стенд: умолчания библиотеки и критерий T_rec · ACCEPTED (автономно, пересмотреть) (T11.3)
* Стенд работает на умолчаниях (`lease_ttl=60`, `heartbeat=20`, `sweep_interval=5`, T_rec = 100 с). При `lease_ttl` < ~45 с повторная отправка упирается в `idempotency_key=th:<item>` ещё «выполняющейся» джобы убитого воркера, которую flexiq реапит через ~43 с.
* «Восстановление уложилось в T_rec» = учёт не стоит без изменений дольше T_rec, пока есть нетерминальные батчи; оракул запускается не раньше чем через T_rec после остановки хаоса.
* I-07 под хаосом считает эталон от фактически развернувшихся источников. I-08 упорядочивает `hook_log` по времени commit (`track_commit_timestamp=on`). I-10 сверяет последнюю джобу Item: если после мёртвой джобы Item переотправлен по истёкшему lease и выполнен, он законно не `error`.
* Запуск — `poe acceptance`, не `make`: стенд должен работать на Windows без make.

## D-046 · Нестрогий `xfail` для недетерминированных дефектов хаоса · ACCEPTED (автономно, пересмотреть) (T11.3)
Строгий `xfail` — только там, где дефект воспроизводится в каждом прогоне (A-CH-01 × S1, Fix-5). Зависания Items в A-CH-02/03/04/05/09/10/12 зависят от того, успела ли джоба стартовать в окно отказа, поэтому помечены `xfail(strict=False, raises=AssertionError)` с задачей-владельцем; все метки снимает T11.3b.

## D-047 · Эффективный `max_retries` для sweeper — через `RetryLimits`; истёкший lease тратит попытку · ACCEPTED (автономно, пересмотреть) (Fix-5)
Умолчание задачи sweeper спрашивает у адаптера по `task_name` (необязательный протокол `RetryLimits` в `protocols.broker`), опция вызова в `th_item.options` сильнее, без протокола умолчание 0. Запись лимита в `th_item.options` при создании Item отклонена: она заморозила бы умолчание декоратора в данных и изменила бы `Message.options`, что расходится с D-012. Sweeper при возврате Item в outbox делает `attempt += 1`, как claim при перехвате истёкшего lease и `release`; иначе `lease_expired` недостижим и Item с постоянно гибнущим исполнителем переотправлялся бы бесконечно. Следствие: `attempt` суммирует ретраи брокера и потерянные lease, общее число исполнений может превысить `max_retries + 1`.

## D-048 · Подтверждённый дубль доставки: `th_lease.redelivered` и возврат в outbox при `release`; схема v4 · ACCEPTED (автономно, пересмотреть) (Fix-7)
Claim с исходом DUPLICATE при живом lease ставит `th_lease.redelivered = true`; `release` для такого lease в той же транзакции возвращает Item в outbox (на паузе — запаркованным) и делает `dispatched -= 1`. Отметка и её чтение сериализованы блокировкой строки `th_item`. Стоимость: +1 `UPDATE th_lease` по PK на первый дубль, у release — `th_batch FOR SHARE` и, только для помеченного lease, вставка в outbox по PK. Отклонено: проход sweeper по `active` без lease/outbox (неотличимо от «ждёт в очереди брокера», индекса по `state` нет); отметка в памяти процесса (дубль обычно приходит другому воркеру); опрос статуса джобы у брокера (гонка); страховочная запись outbox на каждый release (нагрузка на горячий путь). Возврат не сверяется с `max_retries`: новая джоба получает свой бюджет ретраев. Колонка добавлена миграцией v4 (D-042), у пользователей Alembic нужна ревизия `version=4`. Адаптер flexiq добавляет `CompleterError` к непустому `retry_on`, чтобы отказ PostgreSQL не уводил джобу в DLQ; `dont_retry_on` пользователя сильнее.

## D-049 · Итог финализации выбирается под блокировкой строки батча · ACCEPTED (автономно, пересмотреть) (Fix-9)
После хука `Finalizer` берёт `FOR UPDATE` на строку батча (вместе с этапами, порядок D-043), заново читает строку и счётчики и выбирает итог повторно. Батч не готов → откат. `(state, reason)` не совпали с показанными хуку → повтор попытки с повторным вызовом хука (до 5 раз, дальше sweeper). Счётчики в сравнение не входят: relay пишет `dispatched` после отправки, это давало бы ложные повторы. Цена — до 6 дополнительных запросов на финализацию. Вариант «сверять только флаг отмены» отклонён: он не закрывает `retry_failed()` потомка. Утверждение ARCHITECTURE §7.3 «итог стабилен между шагами 1 и 3» было неверным и исправлено. Терминальную строку по-прежнему отсекает предикат CAS, а не ранняя проверка, — иначе мутант «убрать предикат» стал бы эквивалентным (D-037).

## D-050 · Relay — цикл на процесс, scan не привязан к лидерству; установка без брокера · ACCEPTED (автономно, пересмотреть) (Fix-10)
Fast-path и страховочный scan выполняет один фоновый цикл в каждом процессе с адаптером; период scan равен `sweep_interval`, время восстановления в §10 — `relay_grace + sweep_interval`. Отдельная advisory-блокировка для relay отклонена: scan безопасен при конкуренции (`FOR UPDATE SKIP LOCKED`), а блокировка вернула бы единую точку отказа. Цена — N процессов сканируют outbox каждые 5 с. `th.install(None)` — процесс без брокера: relay не создаётся, `th.batch` и `th.call` бросают `ConfigurationError`, maintenance и чтение работают; заменяет `_RejectingDispatcher` из T10.5. Необязательный протокол `RelayPolicy.relay_autostart` нужен детерминированным тестовым брокерам (`InlineBroker`). `Tallyho.aclose()` введён частично (останавливает relay), расширяет его Fix-11.

## D-051 · Сверка с DLQ: поколение отправки Item, схема v5 · ACCEPTED (автономно, пересмотреть) (Fix-6)
Сверка идёт в цикле relay процессов с адаптером (лидер maintenance бывает без адаптера); курсор — `th_meta.dead_letter_cursor` под `FOR UPDATE SKIP LOCKED`, двигается в транзакции завершений; чтение DLQ ограничено 30 с и 5 порциями за проход. Завершается только Item, у которого мёртвая джоба **текущего поколения** (`th_item.generation`, растёт при каждом возврате в outbox, уходит в `_th["g"]`) и нет ни lease, ни outbox: `error("exhausted")` или `cancelled`, если батч отменяется. Живой lease того же поколения получает `redelivered = true`, истёкший остаётся sweeper-у. Отклонено: правило «active без lease и outbox» (неотличимо от ожидания в очереди брокера; поздняя запись DLQ убивала бы Items после `retry_failed()`); сравнение `failed_at` с временем БД (часы воркеров расходятся). Завершение идёт общим `finish_active`, вынесенным из Sweeper: условие по lease и поколению в `_Tx.finish` не выразить без правки мутационного gate. `Operations._retry_items` (функция gate) теперь увеличивает поколение.

## D-052 · Курсор сверки flexiq — водяной знак и перекрытие · ACCEPTED (автономно, пересмотреть) (Fix-6)
`dead_letters_after` листает DLQ от новых записей к старым и на последней странице отдаёт `None`, поэтому «дочитывать новое» им нельзя. Адаптер ведёт свой курсор: водяной знак `failed_at` и обход вниз до «водяной знак − `dead_letter_overlap`» (15 минут), 200 записей за вызов. Записи внутри перекрытия отдаются повторно (правило сверки идемпотентно), соответствие «запись → Item» кэшируется в процессе. Водяной знак ограничен «часы процесса + перекрытие». Допущение, не проверено: `failed_at` ставит воркер по своим часам.

## D-053 · `complete_in` завершает Item только попыткой-владельцем lease; `LeaseLostError` · ACCEPTED (автономно, пересмотреть) (Fix-8)
Путь B проверяет `(worker_id, attempt)` под `FOR UPDATE` строки `th_item`, затем `th_lease` (`_Tx.holds`); при отказе не пишет ничего и чужой lease не удаляет. Истёкший, но не перехваченный lease завершению не мешает. `item.complete_in` при потере бросает `LeaseLostError` — не `ConcurrentModification`: тот означает повторяемый конфликт, а потерянный Item к попытке не вернётся. Обёртка `tracked` возвращает брокеру успех без записи итога: ретрай бесполезен, а DLQ через `JOB_DEAD` записал бы `error` в Item, который выполняет другой воркер. Условие `attempt` внутри CAS `_Tx.finish` отклонено: меняет мутационный gate и не ловит смену владельца без `attempt += 1`. Цена — два SELECT на вызов. Доменный код не должен глотать `LeaseLostError` внутри блока транзакции.

## D-054 · `th.aclose()`: общий бюджет `close_timeout`, `ClosedError` без перезапуска, закрытие в loop владельца · ACCEPTED (автономно, пересмотреть) (Fix-11)
Фоновые задачи и Completer (`close(requeue_held=True)`) закрываются одновременно, relay — после них; общий бюджет `close_timeout` = 10 с. По истечении задачи отменяются, `aclose` не бросает; недоделанное подбирают sweeper и scan. После закрытия запись бросает `ClosedError(InvalidStateError)`, чтение и `migrate` работают; ленивый перезапуск отклонён — фоновые задачи появлялись бы снова после закрытия. Каждая часть закрывается в loop своего владельца: чужой работающий — через `run_coroutine_threadsafe`, чужой остановленный (flexiq после `run_worker`) — докрутка в служебном потоке, закрытый — пропуск с warning. Heartbeat привязан к Completer (`Completer.attach`), протокол `WorkerRuntime` не меняется. Адаптер flexiq добавляет `ClosedError` к непустому `retry_on` наравне с `CompleterError`. Выполняющиеся задачи `aclose` не отменяет.

## D-055 · Схема установки записана в таблицах, а не в `schema_translate_map` · ACCEPTED (автономно, пересмотреть) (Fix-12)
`build_metadata(prefix, schema=)` → `MetaData(schema=schema)`; `Installation.engine` — движок пользователя без изменений. Per-execute translate map отклонён: `Connection.execution_options()` в SQLAlchemy 2.x меняет соединение на месте, а обёртка над `AsyncConnection` опиралась бы на внутренности SQLAlchemy и не чинила бы flush ORM в tx-хуке. Следствия: `session=` и `complete_in` работают на любом соединении той же базы; сессия tx-хука адресует таблицы пользователя по опциям его движка (раньше таблицы без схемы в хуке попадали в схему tallyho — изменение поведения); при `schema=None` адрес таблиц определяет соединение; ключ `schema_translate_map` пользователя, равный имени схемы tallyho, действует и на таблицы tallyho; имя advisory-блокировки лидера — `Installation.maintenance_identity`.

## D-056 · I-10: Item `ok`/`skip` с джобой в DLQ допустим при выполненном I-04 · ACCEPTED (автономно, пересмотреть) (волна 10)
Хаос показывает Items в состоянии `ok`, последняя джоба которых лежит в DLQ flexiq (до 87 в A-CH-09 × S1; A-CH-02/03/04/12). Механизм: finish закоммичен, ответ брокеру потерян, повторные доставки падают на claim при недоступной БД и исчерпывают ретраи. Доменный эффект один, итог в tallyho верен; запись DLQ — остаток at-least-once, сверка (D-051) такие Items не трогает. Истина о результате Item — в tallyho, а не в DLQ брокера. I-10 уточняется: каждый Item с джобой текущего поколения в DLQ имеет класс `error` **либо** терминален как `ok`/`skip` и I-04 для него выполнен; Items `active` с джобой в DLQ дольше T_rec по-прежнему нарушение. Альтернатива «чистить DLQ из библиотеки» отклонена: вмешательство в хранилище брокера, у flexiq нет для этого публичного API. Вопрос выносился владельцу, ответа не было; решение принято по PLAN §0.4 и подлежит пересмотру. Оракул и ACCEPTANCE §4 правит T11.3b.
