# tallyho v1 — план имплементации

> Версия плана 1.0 · 2026-09-30 · к [ARCHITECTURE.md](../ARCHITECTURE.md) v2.1 и [ACCEPTANCE.md](../ACCEPTANCE.md) 1.0-draft.
> Прогресс — [PROGRESS.md](PROGRESS.md). Принятые по ходу решения — [DECISIONS.md](DECISIONS.md).
> План рассчитан на автономное выполнение в `/loop`: **одна итерация = волна параллельных сабагентов по независимым задачам** (§0.7), каждая задача — серия небольших коммитов (§0.6).

---

## 0. Протокол итерации `/loop`

Основной режим — **параллельные волны сабагентов** (§0.7). Шаги ниже описывают работу над одной задачей: так её выполняет каждый сабагент, а оркестратор — если в волне всего одна задача. Репозиторий после каждой итерации остаётся зелёным.

### 0.1 Шаги

1. **Сориентироваться.**
   `git status`, `git log --oneline -5`, прочитать [PROGRESS.md](PROGRESS.md) (таблицу и последние 5 записей журнала) и [DECISIONS.md](DECISIONS.md).
   Если есть незакоммиченные изменения и задача в статусе `in_progress` — продолжить её, а не брать новую.
   Если есть незакоммиченные изменения без `in_progress` — разобраться (`git diff`), довести до зелёного или откатить **только свои** изменения этой задачи.
2. **Выбрать задачу.** Первая сверху в таблице PROGRESS со статусом `todo`, у которой все зависимости `done`.
   Задачи со статусом `human` и `blocked` пропускаются.
3. **Отметить** её `in_progress` в PROGRESS.md.
4. **Прочитать ссылки задачи** в документах (секции указаны в поле «Док»). Историческим документам (DESIGN/API/COUNTERS) верить только там, где они не противоречат ARCHITECTURE.
5. **Реализовать задачу логически цельным блоком** вместе с тестами. Во время разработки запускать только нужные точечные проверки, когда они помогают локализовать риск или ошибку; полный набор тестов после каждого изменения не требуется. Тесты — в `tests/unit/...` (без БД) или `tests/integration/...` (PostgreSQL).
6. **Прогнать полные гейты** (§0.2) один раз, когда задача или её самостоятельный коммитируемый блок готов. После исправления найденной ошибки сначала повторить только затронутую проверку, а полные гейты — перед завершением задачи. Все зелёные — иначе чинить.
7. **Обновить PROGRESS.md**: статус `done`, хеши коммитов задачи, запись в журнал (дата, задача, что сделано, отклонения, что узнали). Новое архитектурное решение → запись в DECISIONS.md. Это отдельный последний коммит задачи (`T<id>: отметить задачу выполненной`).
8. Не пушить.
9. **Решить, продолжать ли цикл** (§0.4).

### 0.2 Гейты (Definition of Done для любой задачи)

| # | Команда | Когда |
|---|---|---|
| G1 | `uv run poe check` — ruff format/check, mypy, basedpyright, import-linter, deptry, unit + architecture | перед завершением задачи или самостоятельного блока |
| G2 | `uv run poe test-all` — все тесты с PostgreSQL (Docker/testcontainers), покрытие ≥ 95% | перед завершением задачи, если затронуты `storage/` или `engine/`; не после каждого изменения |
| G3 | DoD задачи из её карточки (конкретные тесты/критерии) | перед завершением задачи |
| G4 | `uv run pre-commit run --all-files` | перед завершающим коммитом задачи |

Повторно запускать неизменившиеся дорогие тесты в одной итерации не нужно. Точечный прогон не заменяет финальные гейты, но используется для быстрой обратной связи во время разработки.

Правила, нарушать которые нельзя:
* **Правила [AGENTS.md](../../AGENTS.md) обязательны** (ошибки, типы, слои, формат подавлений).
* **Не ослаблять** линтеры, типизацию, import-linter, покрытие. Точечное подавление — только в формате AGENTS.md (`# ruff: ignore[rule-name]  # причина`), и это исключение, а не приём. `DEP002` в deptry — временный список: удалять пакеты из него по мере появления импортов.
* Файлы, записываемые из Python, — с `newline="\n"` (иначе pre-commit `mixed-line-ending`).
* **Не удалять и не отключать** существующие тесты, чтобы стало зелёным. `xfail`/`skip` — только с задачей-владельцем в PROGRESS.
* **Не менять** архитектурные решения из ARCHITECTURE молча. Если реализация требует отклонения — запись в DECISIONS.md с обоснованием, затем код (отдельным коммитом).
* Все исключения — подклассы `TallyhoError` (`tallyho.model.errors`). Все модули объявляют `__all__`.
* Время в SQL — только через `Clock` (D-002). Никаких `datetime.now()` в движке.
* Только async. Никаких `asyncio.get_event_loop()`, `pickle`, `typing.Any`, `unittest.mock.patch`.

### 0.3 Если задача не получается

* Задача оказалась слишком большой для одной итерации → разбить её в этом файле на подзадачи `Tx.ya`, `Tx.yb` (карточки + строки в PROGRESS), закоммитить сделанную зелёную часть как первую подзадачу.
* Три честных попытки починить гейты не помогли, или нужен внешний ресурс/решение человека → статус `blocked`, в журнале: причина, что пробовали, что нужно для разблокировки. Незелёный код не коммитить: сохранить его в `git stash` с именем задачи и указать это в журнале. Перейти к следующей доступной задаче.
* Нашёлся дефект в уже `done` задаче → новая задача `Fix-N` в начало очереди (зависимость — ничего), а не правка «заодно».

### 0.4 Когда останавливать цикл

Цикл работает **без ограничения по времени, пока библиотека не доделана**. Человек недоступен, поэтому цикл не ждёт ответов, а решает сам.

Остановить `/loop` (ScheduleWakeup `stop: true`) и написать сводку **только** если не осталось задач `todo` с выполненными зависимостями (всё `done`, `blocked` или `human`).

Во всех остальных случаях — продолжать:
* **Противоречие в документах / неясность** → выбрать вариант, наиболее согласованный с ARCHITECTURE и гарантиями §10, записать в DECISIONS.md как `ACCEPTED (автономно, пересмотреть)` с альтернативами и продолжить.
* **`blocked`-задача** → перейти к следующей доступной. Перед остановкой цикла (когда кажется, что задач нет) один раз вернуться к каждой `blocked` и попробовать снова с учётом того, что сделано позже.
* **Docker/PostgreSQL недоступен** → не останавливаться: брать задачи, для которых хватает G1 (чистый Python), либо запланировать следующую итерацию через 10 минут и проверить снова. Задачу, требующую G2, не отмечать `done`, пока финальный прогон не выполнен.
* **Сеть/установка пакетов упала** → повторить позже (10 минут), не считать это блокировкой задачи сразу.

Задержка между итерациями — 60 с (минимальная).

### 0.5 Окружение

* Windows 11, Git Bash / PowerShell, `uv` 0.10, Docker Desktop. Python ставит `uv` (`uv python install 3.13` при необходимости).
* Ветка работы — `impl/v1`: pre-commit запрещает коммиты в `main` (`no-commit-to-branch`).
* PostgreSQL для тестов — testcontainers (`postgres:16-alpine`) или `TALLYHO_TEST_DSN`.
* Если пакет не ставится под Windows (например, `flexiq` с Rust-расширением) — гонять соответствующие тесты в Linux-контейнере через `scripts/in-docker.sh` (создаётся в T8.0).

### 0.6 Коммиты

* **Небольшие.** Один коммит — одно логически законченное изменение, которое можно прочитать за пару минут: один тип/функция с тестами, один запрос storage, одна правка конфигурации. Ориентир — до ~200 изменённых строк без учёта golden-файлов и lock-файла. Задача обычно даёт 2–6 коммитов.
* **Завершённая задача зелёная**: гейты G1–G4 проходят перед её финальным коммитом. Промежуточные коммиты допустимы после релевантных точечных проверок; не коммитить заведомо сломанный код, «тест без кода» или «половину функции».
* **Не смешивать**: рефакторинг, форматирование, новая функциональность и правка документов — разными коммитами.
* **Сообщение — на русском**:
  ```
  T4.3b: CAS завершения Item в групповой транзакции Completer

  Completer блокирует строки th_item в порядке id и обновляет только
  активные Items; счётчики считаются по вернувшимся строкам, поэтому
  повторный finish ничего не меняет.

  Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
  ```
  Заголовок — `T<id>: <что сделано>`, до 72 символов, в безличной форме без точки в конце («добавить», «CAS завершения…», а не «добавил»). Тело — зачем и что важно знать ревьюеру, если это не очевидно из заголовка. Идентификаторы кода, таблиц и команд — как есть, латиницей.
* **Комментарии и docstring в коде — тоже на русском** (как в текущем скелете), идентификаторы — на английском.

### 0.7 Параллельный режим: оркестратор и сабагенты

Основной режим цикла. Итерация `/loop` — это **волна**: оркестратор (основная сессия) запускает сабагентов на независимые задачи, ждёт завершения **всех**, вливает их работу и запускает следующую волну.

**Оркестратор, шаг за шагом:**
1. Сориентироваться (§0.1 п.1). Рабочее дерево `impl/v1` должно быть чистым.
2. **Собрать волну**: все задачи `todo`, у которых все зависимости `done`. Взять до **4** задач. Если две задачи явно правят одни и те же файлы (одна фаза, общий модуль), в волну идёт только одна. Пустая волна → правило остановки §0.4.
3. Отметить задачи волны `in_progress` в PROGRESS.md и закоммитить (`план: волна N — T.., T..`).
4. **Запустить сабагентов одним сообщением** (несколько вызовов `Agent` в одном ответе), по одному на задачу: `subagent_type: "general-purpose"`, `isolation: "worktree"`. Промпт — шаблон ниже.
5. **Дождаться всех.** Уведомления о завершении приходят сами. До последнего уведомления ничего не вливать и новых агентов не запускать. Fallback-пробуждение — 30 минут: если проснулся, а агенты ещё работают, просто перезапланировать ожидание.
6. **Влить ветки по одной** в порядке ID задач: `git merge --no-ff agent/T<id> -m "T<id>: влить задачу — <кратко>"`. Конфликты (`__init__.py`/`__all__`, `pyproject.toml` DEP002, golden-файлы) разрешает оркестратор. После отдельного вливания запускать только затронутые проверки, если был конфликт или повышенный риск; G1 + G2 прогнать один раз после сборки всей волны. Если итоговый прогон красный: локализовать сбой точечной проверкой и чинить отдельным коммитом (`T<id>: починить после вливания — …`), а если за 3 попытки не вышло — откатить проблемное вливание, задача → `blocked`.
7. **Обновить PROGRESS.md и DECISIONS.md** по отчётам агентов: статусы, хеши коммитов, журнал, решения. Один коммит `план: итоги волны N`. Удалить влитые ветки и worktree (`git worktree remove`, `git branch -d`).
8. Сразу следующая волна (ScheduleWakeup 60 с).

**Сабагенты не трогают** `docs/plan/PROGRESS.md` и `docs/plan/DECISIONS.md` — только оркестратор, иначе будут конфликты на каждой волне. Сабагенты не вливают, не пушат и не переключают `impl/v1`.

**Шаблон промпта сабагента:**
```
Ты реализуешь задачу T<id> библиотеки tallyho в изолированном git worktree
(репозиторий <repository>, базовая ветка impl/v1).

1. Создай ветку agent/T<id> от текущего HEAD и работай в ней.
2. Выполни `uv sync --all-extras`.
3. Прочитай AGENTS.md, docs/plan/PLAN.md §0.2, §0.3, §0.6 и карточку T<id>
   в docs/plan/PLAN.md §2, а также разделы документов из поля «Док».
   DECISIONS.md читай, но НЕ правь. PROGRESS.md НЕ правь.
4. Реализуй задачу вместе с тестами логически цельными коммитами
   с русскими сообщениями `T<id>: …` и трейлером Co-Authored-By (§0.6).
   Во время разработки запускай релевантные точечные проверки. Полные G1–G4
   прогони один раз перед завершающим коммитом задачи.
5. Не пушь, не вливай, не трогай другие ветки.
6. Человек недоступен: вопросов не задавай, неясности решай сам.

В конце ответь отчётом строго в формате:
СТАТУС: done | blocked
ВЕТКА: agent/T<id>
КОММИТЫ: <hash> <заголовок> (по строке на коммит)
СДЕЛАНО: <2–5 пунктов>
РЕШЕНИЯ: <что добавить в DECISIONS.md: заголовок + 2–3 строки обоснования, или «нет»>
ОТКЛОНЕНИЯ: <от карточки/документов, или «нет»>
ДЛЯ СЛЕДУЮЩИХ ЗАДАЧ: <что важно знать, или «нет»>
БЛОКЕР: <если blocked — причина, что пробовал, что нужно; иначе «нет»>
```

**Ресурсы.** Каждый сабагент поднимает свой контейнер PostgreSQL через testcontainers. 4 агента — это 4 контейнера и 4 `uv sync` (кэш uv общий). Если Docker начинает падать от нагрузки, уменьшить волну до 2 и записать это в журнал.

---

## 1. Карта фаз

```
Ф0 фундамент ─► Ф1 model/protocols ─► Ф2 storage ─► Ф3 hooks ─► Ф4 engine ─► Ф5 runtime ─► Ф6 api ─► Ф7 testing
                                                                                                  │
                                           Ф8 flexiq ◄────────────────────────────────────────────┤
                                           Ф9 примеры ARCHITECTURE §12–13 как тесты ◄─────────────┤
                                           Ф10 надёжность, наблюдаемость, CLI ◄───────────────────┤
                                           Ф11 приёмочный стенд ACCEPTANCE ◄──────────────────────┘
                                           Ф12 документация и релиз
                                           Ф13 расширения перед релизом (атрибуты, экспорт исходов) — до Ф11 и Ф12
```

Слои пакета заданы import-linter в `pyproject.toml` (ARCHITECTURE §3.3):
`cli | adapters | testing` → `api | runtime` → `engine` → `hooks | storage` → `protocols` → `model`.
Код каждой задачи кладётся в свой слой; нарушение контракта ловит G1.

---

## 2. Задачи

Формат карточки: **Зависит** · **Док** (ссылки) · **Сделать** · **DoD** (помимо гейтов G1–G4).

### Ф0. Фундамент

#### T0.1 — Репозиторий собирается и гейты зелёные на скелете
* **Зависит:** —
* **Док:** `pyproject.toml`, `.github/workflows/ci.yml`, `.pre-commit-config.yaml`
* **Сделать:**
  * `git checkout -b impl/v1` (коммитов ещё нет — ветка создаётся на «нерождённом» HEAD).
  * Создать `README.md` (кратко: что это, статус pre-alpha, ссылка на docs), `LICENSE` (MIT, автор из `pyproject`), `CHANGELOG.md` (Keep a Changelog, `Unreleased`). Без них не собирается wheel.
  * `.gitignore` (Python, uv, `.idea/`, coverage, `.venv`).
  * `uv lock`, `uv sync --all-extras`. Если `flexiq` не ставится на Windows — `uv sync --extra asyncpg --extra psycopg --extra alembic`, записать в DECISIONS и журнал.
  * Разнести покрытие: `poe test` = unit без покрытия (`--no-cov`), `poe test-all` = всё с покрытием `fail_under=95` (D-003). Добавить `poe check-all` = `check` + `test-all`.
  * `uv run pre-commit install`.
* **DoD:** G1–G4 зелёные на скелете; первый коммит в `impl/v1`, включающий `docs/`, `pyproject.toml`, `uv.lock`.

#### T0.2 — Инфраструктура тестов
* **Зависит:** T0.1
* **Док:** ARCHITECTURE §12.6 (фикстуры), ACCEPTANCE §1
* **Сделать:** в `tests/integration/conftest.py` — фикстура `schema` (уникальная схема на тест, `DROP SCHEMA ... CASCADE` после), фикстура `connection`/`session` (`AsyncSession`), маркер `slow`. Хелпер `tests/helpers/db.py`: счётчик дедлоков (`pg_stat_database.deadlocks`), чтение `pg_locks`. Проверить, что `pytest -n auto` работает с testcontainers (один контейнер на сессию xdist-воркера или общий DSN).
* **DoD:** пример интеграционного теста использует `schema`; `pytest -n 4` зелёный.

### Ф1. model и protocols (чистый Python, без БД)

#### T1.1 — Перечисления состояний и иерархия ошибок
* **Зависит:** T0.1
* **Док:** ARCHITECTURE §2, §6, §11.2; ACCEPTANCE A-DB-05, A-FQ-07, A-UC-13, A-UC-18
* **Сделать:** `tallyho.model.states`: `BatchState` (`open, sealed, finalizing, succeeded, completed_with_errors, failed, cancelled`), `ItemState` (`active, ok, skip, error, cancelled`), `ResultClass`, `OnFeederFailed`, `CancelReason` (`cancel, deadline, fail_fast, policy`) — `IntEnum`/`StrEnum` со значениями `smallint` из D-005; `is_terminal`. Ошибки: `SpawnTargetError`, `DownstreamFinalized`, `UnsupportedOption`, `HookTransactionError`, `HookMissingError`, `BatchPurged`, `ConcurrentModification`, `SealError` (seal этапа с `fed_by`, add после seal/cancel). Превышение `max_items`/`max_depth` — не ошибка, а счётчик `skipped_by_limit`.
* **DoD:** юнит-тесты: значения enum стабильны (снимок), терминальность, все ошибки в иерархии.

#### T1.2 — Value-объекты: Progress, BatchSummary, PolicyBreach, FailurePolicy, TaskCall
* **Зависит:** T1.1
* **Док:** ARCHITECTURE §3.4 (классы), §7.2, §11.2 (политики), §12.4 (использование в хуках)
* **Сделать:** frozen `dataclass(slots=True)`: `Progress`, `BatchSummary` (с `children: Mapping[str, BatchSummary]`, `labels`, `metrics`, `seq`, `reason`), `BatchView`, `ItemView`, `InFlightItem`, `PolicyBreach` (`batch_key`, `labels`, `ratio`, `action`). `FailurePolicy.continue_() / fail_fast() / threshold(ratio=, min_processed=, labels=, action="fail"|"pause")` + `evaluate(counts, labels) -> Verdict`. `TaskCall` (task_name, args, kwargs, opts: key, weight, queue, broker-опции как `Mapping[str, object]`).
* **DoD:** тесты на `FailurePolicy.evaluate` (граничные значения, `min_processed`, фильтр labels); hypothesis: `threshold` монотонна по числу ошибок.

#### T1.3 — Математика прогресса (чистые функции)
* **Зависит:** T1.2
* **Док:** ARCHITECTURE §9.3–9.4, §13.3 (таблица t1–t5 и расчёт t2), DYNAMIC_WORKFLOWS §4
* **Сделать:** `tallyho.model.progress`: `found/done/pending/queued`, правило `expected` (4 строки таблицы §9.4), оценка Кнута по `fed_by` рекурсивно, порог `min(estimate_min_basis, estimate_min_share × expected_F)`, `ratio` по весам для батча и корня, ETA через EMA (`eta_window`). Вход — «сырые» счётчики дерева, выход — `Progress` на каждый узел.
* **DoD:** тест-таблица, воспроизводящая §13.3 t1…t5 (включая «≈700», «≈1 785», «≈19%»); кейсы §12.6 `expected == 10_000 and expected_is_estimate`; hypothesis: `expected ≥ found`, `0 ≤ ratio ≤ 1` для sealed, sealed ⇒ `expected == found`.

#### T1.4 — Протоколы и базовые реализации
* **Зависит:** T1.1
* **Док:** ARCHITECTURE §4.2, §11.3; API.md §3 (контракт адаптера — исторический); D-002, D-006
* **Сделать:** `tallyho.protocols`: `Message`, `Verdict` (`RETRY/FINAL`), `Dispatcher` (`task_name(fn)`, `dispatch(messages)`), `Runtime` (`wrap(fn)`, `retry_verdict(exc)`, `reconcile_dead(since)`), `Serializer` + `JsonSerializer`, `Clock` (`sql_now()` → SQL-выражение, `monotonic()`), `SystemClock` (`func.now()`), `IdFactory` + `UuidV7Factory` (свой UUIDv7 ≈30 строк; на 3.14 — `uuid.uuid7`), `Observer` + `NullObserver` (события: `item_finished`, `batch_finalized`, `hook_failed`, `hook_missing`, `relay_dispatched`, `completer_flush` …).
  Внимание: `protocols` не может импортировать `sqlalchemy` (контракт import-linter). Поэтому `Clock.sql_now()` отдаёт не SQL-выражение, а **маркер** — решение, как DB-время передаётся в `storage`, зафиксировать в DECISIONS (D-002 уточнить): например, `Clock.now() -> datetime | None`, где `None` = «используй `now()` БД», а `FakeClock` возвращает значение, которое storage биндит параметром.
* **DoD:** тест UUIDv7: версия/вариант, монотонность в пределах миллисекунды, сортировка = порядок генерации; `JsonSerializer` round-trip; `runtime_checkable` протоколы проверяются на фейках.

### Ф2. storage (SQLAlchemy Core, PostgreSQL)

#### T2.1 — Описание таблиц и индексов
* **Зависит:** T1.1
* **Док:** ARCHITECTURE §5.1, §5.2 (все индексы), §11.4 (`th_expiry`), §5 «`th_meta`»; COUNTERS §3.6 (storage params)
* **Сделать:** `tallyho.storage.tables`: фабрика `build_metadata(prefix: str) -> Tables` со всеми таблицами: `th_batch, th_item, th_outbox, th_lease, th_feed, th_counter, th_counter_delta, th_metric, th_item_mark, th_expiry, th_meta`. Все partial-индексы §5.2, `fillfactor`, per-table autovacuum для `th_counter/th_metric/th_lease/th_outbox/th_counter_delta`. **Никаких индексов по изменяемым колонкам `th_item`.** Никаких FK. Схема — через `schema_translate_map`.
* **DoD:** юнит: DDL компилируется под диалект PG, снимок DDL (golden-файл) — чтобы изменения схемы были видны в diff; тест-правило: ни один индекс `th_item` не содержит `state/label/result/error/finished_at`.

#### T2.2 — Миграции и установка в схему
* **Зависит:** T2.1, T0.2
* **Док:** ARCHITECTURE §11.1 (`migrate`, alembic), ACCEPTANCE A-NF-01..03
* **Сделать:** `tallyho.storage.migrations`: версия 1 как список операций; `migrate(engine, schema, prefix)` под `pg_advisory_xact_lock`, `SET LOCAL lock_timeout`, запись версии в `th_meta`, идемпотентность. `tallyho.storage.alembic.upgrade(op, version=1, schema=...)` (модуль импортирует alembic лениво; extra `alembic`). Экранирование имён схемы/префикса, валидация префикса (`^[a-z_][a-z0-9_]{0,15}$`).
* **DoD:** интеграция: повторный `migrate` — no-op; две установки в разных схемах одной БД не видят друг друга; схема со спецсимволами в имени работает (A-NF-03); миграция через alembic `op` в тестовом окружении.

#### T2.3 — Транзакции: приём сессии пользователя, свои транзакции, ретраи
* **Зависит:** T2.1, T1.4
* **Док:** ARCHITECTURE §1 (NFR), §7.3 (правила хука), §10 (дедлок/чужая блокировка), COUNTERS §3.6, §2 P10; ACCEPTANCE A-DB-05, A-DB-09, A-DB-10
* **Сделать:** `tallyho.storage.tx`: `resolve_connection(session | connection)` (`AsyncSession`, `AsyncConnection`; учёт `begin_nested`), `own_transaction(engine)` c `SET LOCAL lock_timeout/statement_timeout`, retry на `40001/40P01/55P03` с экспоненциальным backoff и джиттером (детерминированным от `Clock`/seed в тестах), `after_commit(session, callback)` — регистрация через события SQLAlchemy (для `AsyncSession` — `sync_session`), срабатывает только при реальном commit внешней транзакции. `HookSession` — обёртка `AsyncSession`, у которой `commit/rollback/close` бросают `HookTransactionError`.
* **DoD:** интеграция: after_commit не вызывается при rollback и при откате savepoint; вызывается один раз при commit; retry повторяет на искусственном `40P01`; `HookSession.commit()` → `HookTransactionError`.

#### T2.4 — Запросы счётчиков: чтение, upsert слота, дельты, свёртка
* **Зависит:** T2.2, T2.3
* **Док:** ARCHITECTURE §9.1–9.3; COUNTERS §3.3–3.4
* **Сделать:** `tallyho.storage.counters`: `read_counters(conn, batch_ids)` одним statement (`sum(counter) + sum(delta)` LATERAL, §9.3), `upsert_slots(conn, deltas)` в порядке `(batch_id, slot)`, `insert_delta(conn, …)`, `fold_deltas(conn, batch_ids)` через `DELETE … RETURNING`, `reconcile(conn, batch_id)` по `count(*) GROUP BY state` под `FOR UPDATE` строки батча, `upsert_metrics`.
* **DoD:** интеграция: чтение не «мигает» при параллельной свёртке (конкурентный тест 1 000 итераций); reconcile чинит искусственный дрейф; дельты из незакоммиченной транзакции не сворачиваются.

#### Fix-1 — Полный набор d_* колонок в `th_counter_delta`
* **Зависит:** T2.4
* **Док:** ARCHITECTURE §5.1 (TH_COUNTER, TH_COUNTER_DELTA), §9.1, UC-08; DECISIONS D-025, D-029
* **Сделать:** в `th_counter_delta` не хватает дельт для `w_total`, `dispatched`, `duplicates`, `skipped_by_limit`, `tree_total`, а путь B (`complete_in` со spawn внутри транзакции пользователя, T4.5) должен уметь записать их все. Добавить колонки `d_w_total, d_dispatched, d_duplicates, d_skipped_by_limit, d_tree_total` (bigint, default 0) в `tables.py` **прямо в схему v1** (D-029: релиза ещё не было), обновить golden-DDL, `DELTA_FIELDS`/`insert_delta`/`fold_deltas`/`read_counters`/`reconcile` в `storage/counters.py` (теперь `DELTA_FIELDS == COUNTER_FIELDS`, проверка «поле вне DELTA_FIELDS» становится ненужной или остаётся как страховка), ER-диаграмму ARCHITECTURE §5.1.
* **DoD:** round-trip каждой из 11 дельт через `insert_delta` → `read_counters` → `fold_deltas`; тест «каталог после migrate == create_all» зелёный; `reconcile` переносит все поля.

### Ф3. hooks

#### T3.1 — Реестр tx-хуков и `hook_modules`
* **Зависит:** T1.2
* **Док:** ARCHITECTURE §7.2, §7.5, §12.4 (правило fallback `on_policy_breach` на kind корня)
* **Сделать:** `tallyho.hooks.registry`: `HookRegistry` с `on_finalized(kind)`, `on_progress(kind, every)`, `on_policy_breach(kind)`; дубль регистрации → `ConfigurationError`; `required_hooks(kind) -> tuple[str, ...]` (то, что пишется в `th_batch.hooks`); импорт `hook_modules`; типизированные сигнатуры (`Protocol` для хуков).
* **DoD:** юнит: регистрация, дубль, fallback breach-хука на корень, `required_hooks`.

### Ф4. engine — ядро

> Все задачи Ф4 тестируются интеграционно на PostgreSQL. Брокер — минимальный фейковый `Dispatcher`, записывающий сообщения в список (полноценный InlineBroker — T7.1). Воркер эмулируется прямыми вызовами Completer.

#### T4.1 — Продюсер: создание батча, под-батчей, `th_feed`, добавление Items, seal, expect
* **Зависит:** T2.4, T3.1
* **Док:** ARCHITECTURE UC-01, UC-02, §8.1 п.1–2, §6.1 (seal этапа — ошибка), §11.2 (параметры batch/sub_batch)
* **Сделать:** `tallyho.engine.producer`: `create_root` (`INSERT … ON CONFLICT (kind,key) DO NOTHING RETURNING` → существующий), `create_sub_batch` (идемпотентно по `(root_id, key)`, виртуальный Item у родителя **с `weight=0`** — иначе искажается `ratio`, D-024, наследование `retention/release_required/max_items`), `add_feed` + проверка ациклимости и «тот же родитель», `add_items` чанками по 1 000 через `unnest` (items + outbox + `total/w_total`, дедуп по `key`: `found/duplicates`), `seal` (ошибка для `fed_by`-этапа, для отменённого), `expect(n)` (`GREATEST`). `start_at` → `available_at` outbox. Всё — на переданном соединении (транзакция пользователя или своя).
* **DoD:** интеграция: повторный `create_root` с тем же key → тот же id; rollback пользователя не оставляет ни строки; цикл `fed_by` → ошибка; 100k Items добавляются O(чанков) запросов; дубли по key считаются в `duplicates`.

#### T4.2 — Relay: fast-path и scan, окно `max_in_flight`, `start_at`, пауза
* **Зависит:** T4.1
* **Сначала (D-033):** в схему v1 добавить колонку `th_item.options jsonb NULL` (D-029, golden-DDL). Продюсер пишет в неё `TaskCall.queue` и опции брокера — сейчас он их отбрасывает. Relay передаёт опции в `Message.options`, а при `expires` пишет `th_expiry(item_id, expires_at)` (§11.4). Опции хранятся в `th_item`, а не в outbox, потому что должны пережить повторную отправку sweeper-ом.
* **Док:** ARCHITECTURE §6.3, UC-01 (шаги relay), UC-10, §11.3 (группировка по task_name, чанки 1 000), §15 (`relay_grace`, `relay_claim_ttl`)
* **Сделать:** `tallyho.engine.relay.Relay`: `kick(batch_ids)` (fast-path после commit), `scan_once()` (claim `UPDATE th_outbox SET available_at = now + claim_ttl … FOR UPDATE SKIP LOCKED RETURNING`), сборка `Message` (с `_th`/заголовками через адаптер), `dispatch` группами по `task_name`, `DELETE th_outbox` + `dispatched += n`. Окно `max_in_flight`: при превышении запись остаётся с `available_at = ∞` (parked), освобождение — при finish (T4.3b) и в scan. Пауза: не отправлять Items батча с `paused_at`.
* **DoD:** интеграция: падение между claim и dispatch → повторная отправка после `relay_claim_ttl`; два параллельных scan не отправляют одно и то же; `max_in_flight=3` — одновременно отправлено ≤ 3; `start_at` в будущем → 0 отправок до срока (FakeClock).

#### T4.3a — Completer: буфер, групповой коммит, claim / heartbeat / release
* **Зависит:** T4.1 (claim/heartbeat/release не требуют relay; D-034)
* **Док:** ARCHITECTURE UC-03, UC-04, §6.2 (производные состояния), §11.3 (чужой живой lease → успех), §15 (tick 20 мс / 500 / backpressure 10 000, lease 60 с / heartbeat 20 с); COUNTERS §3.2
* **Сделать:** `tallyho.engine.completer.Completer`: очередь операций с futures, flush по тику или размеру, backpressure; ленивое создание в текущем loop; `claim` (`INSERT th_lease ON CONFLICT DO NOTHING` при `state=active`; исходы: `CLAIMED / DUPLICATE / TERMINAL / PARKED(pause) / CANCELLED(lazy cancel) / EXPIRED`), `heartbeat(+progress)`, `release(attempt+1)`. Graceful shutdown: дослать буфер; `SIGTERM`-путь освобождает lease сразу (A-CH-08).
* **DoD:** интеграция: 1 000 claim → ≤ ceil(1000/500)+1 транзакций; повторный claim того же Item → `DUPLICATE`; claim на паузе → Item в outbox с `available_at=∞`; heartbeat продлевает `lease_until`.

#### T4.3b — Completer: finish (путь A) без spawn
* **Зависит:** T4.3a, T4.2
* **Док:** ARCHITECTURE §9.2 шаги 1–3, 6, 8–9, §11.2 (метки по умолчанию, `mark=`), UC-03
* **Сделать:** `finish(item, result_class, label, result, error, metrics)`: `SELECT … ORDER BY id FOR UPDATE`, CAS `state=active` с `RETURNING` (считать только вернувшиеся), `DELETE th_lease`, `th_item_mark` для `error` (и `mark=True`), агрегация дельт по `(batch_id, slot процесса)` в порядке сортировки, `th_metric` (labels + `incr`), освобождение окна `max_in_flight`. После commit — резолв futures, `relay.kick`, вызов `finalizer.try_finalize` для затронутых `sealed` батчей (интерфейс-заглушка до T4.4).
* **DoD:** интеграция: двойной finish → счётчики изменились один раз; 64 конкурентных «воркера» × 10 000 finish → счётчики = `count(*)` (I-05), `deadlocks = 0`; `th_lease` пуст после finish.

#### T4.3c — Spawn, sub_batch из задачи, `into=`, лимиты, дедуп, depth, expect
* **Зависит:** T4.3b
* **Док:** ARCHITECTURE UC-05, UC-06, §8.1 п.2, 5, 6; §9.2 шаги 4–5, 7; DYNAMIC_WORKFLOWS §3
* **Сделать:** в finish: вставка spawned Items в свой батч или `into=` (разрешение ключа этапа через кэш структуры дерева в процессе), правило записи (`SpawnTargetError` — проверяется **в момент `spawn()`**, до БД), `ON CONFLICT (batch_id,key) DO NOTHING RETURNING` → `found/duplicates`, `max_items` по `tree_total` корня (мягкий), `max_depth` (depth+1 в свой батч, 0 при `into=`) → `skipped_by_limit`, sub_batch из задачи (виртуальный Item), `expect(n, into=)`. Всё в той же транзакции, что CAS родителя: `pending` не проходит через 0.
* **DoD:** интеграция: падение транзакции → нет ни детей, ни завершения родителя; повторный finish родителя не создаёт детей; `found + duplicates + skipped_by_limit` = число вызовов spawn; `max_depth=1` отсекает цикл; spawn в этап, не являющийся получателем, → `SpawnTargetError`.

#### T4.4 — Finalizer: транзакция хука, CAS, колбэки, дерево, авто-seal этапов
* **Зависит:** T4.3c
* **Док:** ARCHITECTURE §6.1 (все переходы и примечания), §7.3, §7.5 (hook missing), UC-07, UC-17, §8.1 п.3, 4, 7; §10
* **Сделать:** `tallyho.engine.finalizer.Finalizer.try_finalize(batch_id)`: чтение счётчиков + проверка условия; `BEGIN` → `on_finalized(HookSession, summary)` с `hook_timeout` → CAS `state IN (open, sealed)` → итог (succeeded / completed_with_errors / failed по `FailurePolicy` и `cancel_reason` / cancelled) → outbox колбэков (`on_succeeded`, `on_completed_with_errors`, `on_failed`, `on_cancelled`, `on_finalized_task`) со стабильным `callback_id` → завершение виртуального Item родителя (счётчики родителя) → авто-seal получателей `th_feed` (`FOR UPDATE` в порядке id, `on_feeder_failed`) → `COMMIT` → рекурсивно `try_finalize` для родителя и закрытых этапов. Ошибка хука → откат, `hook_attempts+1`, `hook_error`, `updated_at` для backoff. Хук требуется, но не зарегистрирован → не финализировать, `Observer.hook_missing`.
* **DoD:** интеграция: две конкурентные финализации → один commit, хук выполнился ≤ 2 раз, закоммитился 1 раз; пустой этап финализируется сразу и каскадом закрывает следующий; два источника финализируются одновременно (100 повторов) → этап всегда закрыт; `on_finalized` детей закоммичен раньше родителя; хук с `session.commit()` → `HookTransactionError`, финализации нет.

#### T4.5 — Путь B: `complete_in(session)` и свёртка дельт
* **Зависит:** T4.4, Fix-1
* **Док:** ARCHITECTURE UC-08, §9.1; COUNTERS §3.3 «Путь B»; ACCEPTANCE A-DB-01, A-DB-06, A-DB-07, A-DB-09
* **Сделать:** `tallyho.engine.completion.complete_in(conn, item, …)`: HOT-update `th_item`, `DELETE th_lease`, `INSERT th_counter_delta` (+ дельты метрик, spawns — те же правила, что в T4.3c, но через дельты), флаг «уже завершён» для обёртки; `after_commit` → `Completer.fold(batch_id)` → `try_finalize`.
* **DoD:** интеграция: исключение до commit → нет ни доменной строки, ни завершения; savepoint откатился → Item не завершён; пользователь в `REPEATABLE READ` и `SERIALIZABLE` под нагрузкой → 0 `40001` из-за таблиц tallyho; 20% транзакций держатся 2 с → в `pg_locks` нет ожиданий на `th_counter` > 100 мс.

#### T4.6 — Политики ошибок и `on_policy_breach`
* **Зависит:** T4.4
* **Док:** ARCHITECTURE UC-11 (авто-пауза), §7.2, §11.2 (политики), §12.4 (fallback хука), ACCEPTANCE A-UC-10
* **Сделать:** оценка политики после flush Completer для затронутых батчей (`min_processed`, labels, ratio); `action="pause"` → пауза **всего дерева** + `on_policy_breach` в одной транзакции; `action="fail"` / `fail_fast` → `cancel_requested_at`, `cancel_reason`; однократность срабатывания.
* **DoD:** интеграция: 8% `hard_bounce` при пороге 5% после `min_processed` → дерево на паузе, хук вызван ровно один раз; `fail_fast` → остаток `cancelled`, итог `failed`.

#### T4.7 — Операции над деревом: pause / resume / cancel / reschedule / retry_failed / retry_finalize / release
* **Зависит:** T4.6
* **Док:** ARCHITECTURE UC-10, UC-11, UC-12, UC-14 (release), UC-16 (+ `DownstreamFinalized`), §6.1 (флаг отмены), §6.2; ACCEPTANCE A-DB-03, A-DB-08, A-UC-08, 09, 11, 13
* **Сделать:** `tallyho.engine.operations`: каждая операция принимает соединение пользователя, каскад по `parent_id` в порядке id, работа с outbox чанками; `cancel` — флаг + немедленная отмена неотправленных + ленивая при claim; `retry_failed(labels)` — CAS терминальный → sealed, чанками по `th_item_mark`, переоткрытие этапов от источников к получателям на корне; `retry_finalize` — сброс backoff; `release`.
* **DoD:** интеграция: rollback пользователя → состояние не изменилось; пауза → новые claim паркуются; resume → всё доделывается; отмена до `start_at` → `cancelled`, 0 dispatch; `retry_failed` на этапе с финализированным получателем → `DownstreamFinalized`; пользователь держит `FOR UPDATE` доменной строки + `pause` параллельно с финализацией → 0 дедлоков до пользователя.

#### T4.8 — Sweeper
* **Зависит:** T4.7
* **Док:** ARCHITECTURE UC-15 (все строки), §7.6, §10 (таблица восстановления), §11.4 (`th_expiry`), §15
* **Сделать:** `tallyho.engine.sweeper.Sweeper`: `expire_leases` (retry → outbox / исчерпано → `error("lease_expired")` / lease терминального → delete), `finalize_stuck` (sealed + pending 0 + `finalize_grace`; `hook_error` с backoff 1 с → 5 мин), `enforce_deadlines`, `seal_orphan_stages`, `reconcile_drift` (sealed, pending > 0, нет lease/outbox), `fold_stale_deltas`, `expire_unclaimed` (`th_expiry` → `error("expired")`), `retention` (деревья, от листьев к корню, чанками по 1 000, `release_required`). Каждый шаг — отдельная короткая транзакция, `SKIP LOCKED`, ограничение размера пачки.
* **DoD:** интеграция на каждый шаг: сценарий «сломали → один `sweep` → починилось»; retention не трогает батч без `release()` при `release_required`; после retention `view()` → `BatchPurged`.

#### T4.9 — Snapshotter
* **Зависит:** T4.8
* **Док:** ARCHITECTURE §7.4, UC-09, §9.4 (ETA), ACCEPTANCE I-08, A-UC-19
* **Сделать:** `tallyho.engine.snapshotter.Snapshotter.tick()`: расписание в памяти по partial-индексу, чтение счётчиков пачкой, пропуск без изменений, транзакция `on_progress` + CAS по `snap_seq` и state, EMA скорости для ETA.
* **DoD:** интеграция: снимок после финализации откатывается вместе с изменениями хука; `seq` строго растёт; без изменений счётчиков — 0 записей в БД.

#### T4.10 — Maintenance: лидерство, цикл, `run_maintenance_once`, `watch`
* **Зависит:** T4.9
* **Док:** ARCHITECTURE §3.2 (leader election), UC-13, §15 (`sweep_interval`, `watch_throttle`), ACCEPTANCE A-CH-07, A-UC-20
* **Сделать:** `tallyho.engine.maintenance.Maintenance`: лидер через `pg_try_advisory_lock` на выделенном соединении (потеря соединения = потеря лидерства), циклы relay scan / sweeper / snapshotter, корректная остановка; `run_maintenance_once()` для тестов; `NOTIFY th_progress` с троттлингом и `watch()` через `LISTEN` (финальное состояние не теряется).
* **DoD:** интеграция: два экземпляра — работает ровно один лидер; kill соединения лидера → второй берёт лидерство ≤ 2 × `sweep_interval`; `watch` не присылает чаще троттла и всегда присылает финал.

#### T4.11 — Чтение: `view`, `in_flight`, `items(label)`, `find`, `child`
* **Зависит:** T4.4
* **Док:** ARCHITECTURE UC-13, §9.3–9.4, §3.4 (`BatchHandle`), §7.6 (`BatchPurged`)
* **Сделать:** `tallyho.engine.reads`: сводка дерева одним-двумя запросами (счётчики + метрики + `th_feed` + `count(th_lease)`), сборка `BatchSummary`/`Progress` через T1.3; `in_flight(limit)` из `th_lease`; `items(label)` страницами по `th_item_mark` (keyset); `find(kind, key)`; `child(key)`.
* **DoD:** интеграция: число запросов `view()` не зависит от размера дерева (счётчик statement'ов); удалённый батч → `BatchPurged`.

### Ф5. runtime

#### T5.1 — `ItemContext`, `th.item.*`, `th.tracked`, `th.callback.current()`
* **Зависит:** T4.5, T4.7
* **Док:** ARCHITECTURE §3.4 (`ItemContext`), UC-03, UC-04, UC-08, §11.2 «Задача», §11.3 (служебный `_th`, `retry_verdict`); API.md §4.3, §5.1 (исторический)
* **Сделать:** `tallyho.runtime`: `ContextVar[ItemContext | None]`; модуль-фасад `item` (`id, spawn, spawn_call, sub_batch, expect, progress, incr, ok, skip, error, complete_in, cancelled, current`) — вне задачи `current()` = `None`, остальные — no-op или ошибка по настройке; `tracked(fn)` — `async`-обёртка с `functools.wraps`: извлечь `_th`, claim, heartbeat-таск, вызов, finish/release по `Runtime.retry_verdict`, сброс контекста; проверка «только `async def`»; `callback.current()` для колбэк-задач.
* **DoD:** юнит + интеграция: `_th` не попадает в функцию; дубль доставки → функция не вызвана; исключение с `RETRY` → `release`, с `FINAL` → `error("exhausted")`; `th.item.current()` вне задачи → `None`.

### Ф6. api

#### T6.1 — `Tallyho`, конфигурация, `install`, `migrate`
* **Зависит:** T5.1, T4.10, T4.11
* **Док:** ARCHITECTURE §3.4 (`Tallyho`), §11.1, §15 (все значения по умолчанию)
* **Сделать:** `tallyho.api`: `Settings` (frozen dataclass, значения §15, валидация), `Tallyho(engine, schema=, prefix=, hook_modules=, clock=, id_factory=, observer=, serializer=, **settings)`, `install(adapter)`, `migrate()`, `maintenance()`, `run_maintenance_once()`, декораторы хуков. `api` не импортирует `runtime`/`storage` напрямую (контракт import-linter) — связка через `engine`. Реэкспорт публичного API из `tallyho/__init__.py`.
* **DoD:** юнит: значения по умолчанию = таблица §15 (тест-таблица); неверная конфигурация → `ConfigurationError`.

#### T6.2 — `th.batch(...)` → `BatchBuilder`, `sub_batch`, `BatchHandle`
* **Зависит:** T6.1
* **Док:** ARCHITECTURE §11.2 (сводка), §12.4 (`schedule`), §13.2 (`start_import`), UC-01, UC-02
* **Сделать:** асинхронный контекст-менеджер: на выходе seal корня и этапов без `fed_by`; при исключении — ничего не пишется в чужую сессию сверх уже сделанного (rollback — забота пользователя), в своей — откат; `add/map/add_calls/sub_batch/expect/seal/handle`; `BatchHandle` — делегирование в engine (view/watch/wait/in_flight/операции).
* **DoD:** интеграция: примеры `schedule` из §12.4 и `start_import` из §13.2 выполняются как тесты с фейковым брокером.

#### T6.3 — `th.call(...)` с `ParamSpec` и типовые тесты
* **Зависит:** T6.1
* **Док:** API.md §2 п.1, ARCHITECTURE §11.2 «Вызовы», ACCEPTANCE A-NF-04
* **Сделать:** `call(fn: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> Call[P, R]` + `.opts(key=, weight=, queue=, **broker_opts)`; то же для `add`, `spawn`. Типовые тесты: файл `tests/typing/cases.py` с `assert_type` и негативными случаями, проверяемыми прогоном basedpyright в тесте (ожидаемые ошибки в конкретных строках).
* **DoD:** неверный аргумент в `th.call`/`batch.add`/`th.item.spawn` — ошибка basedpyright и mypy; корректные вызовы — без ошибок.

### Ф7. testing

#### T7.1 — `tallyho.testing`: `InlineBroker`, `FakeClock`, фикстуры
* **Зависит:** T6.2
* **Док:** ARCHITECTURE §12.6 «Тесты» (API `step`, `drain`, `kill_worker_after`, `duplicate_delivery_rate`, `seed`)
* **Сделать:** `InlineBroker` (адаптер `Dispatcher`+`Runtime`: очередь в памяти, выполнение через `tracked`, ретраи с `max_retries`, дубли доставки по seed, `step(n)`, `drain()`, эмуляция kill -9 — прерывание задачи без finish), `FakeClock` (`advance(**timedelta)`), pytest-плагин с фикстурами (`tallyho_env`), `run_maintenance_once`.
* **DoD:** тесты самого InlineBroker; все интеграционные тесты Ф4, где был самописный фейк, переведены на `InlineBroker` (если это упрощает их).

### Ф8. Адаптер flexiq

#### T8.0 — Спайк: проверка фактов о flexiq
* **Зависит:** T0.1
* **Док:** ARCHITECTURE §11.3 (таблица фактов), §16 «Открытые вопросы» п.1
* **Сделать:** установить `flexiq>=2.0,<3` (Windows или Linux-контейнер: `scripts/in-docker.sh`), поднять живой воркер `pool="thread"` на PG, проверить: `functools.wraps` не ломает имя задачи; kwarg `_th` проходит сериализацию и попадает в DLQ; `on_dead_letter` срабатывает при `retry_budget`; поведение `prefork` с async; сигнатуры `enqueue_many`, `current_job.retry_count`, `dead_letters_after`. Результат — `docs/plan/FLEXIQ_SPIKE.md`; расхождения с §11.3 — в отчёте для DECISIONS (пишет оркестратор).
* **DoD:** спайк-скрипты лежат в `tests/contract/flexiq/spike_*.py` (не в CI); каждый факт таблицы §11.3 отмечен «подтверждён / опровергнут / не проверен».
* Если flexiq недоступен ни в Windows, ни в контейнере → `blocked`, Ф8 целиком ждёт человека.

#### T8.1 — `FlexiqAdapter`: dispatch, `fq.task`, `retry_verdict`, DLQ
* **Зависит:** T8.0, T7.1
* **Док:** ARCHITECTURE §11.1, §11.3 (решения в правой колонке), §11.4 (опции); ACCEPTANCE §8
* **Сделать:** `tallyho.adapters.flexiq`: `FlexiqAdapter(queue)`, `fq.task(**opts)` = `queue.task(**opts)(th.tracked(fn))` с запретом `debounce*`/`batch=` и sync-функций; dispatch через `enqueue_many` группами по `task_name`, чанками по 1 000, в своём executor; `idempotency_key=f"th:{item_id}"`, если пользователь не задал свой; `metadata`/`notes` — байт в байт; `expires` → `th_expiry`; `depends_on` → `UnsupportedOption`; `retry_verdict` по конфигу задачи и `retry_count`; `on_dead_letter` → `call_soon_threadsafe` → finish; `reconcile_dead(since)` по курсору; `install` → ошибка для `prefork` и несовместимой версии.
* **DoD:** юнит на маппинг опций; импорт `flexiq` только в `tallyho.adapters.flexiq` (import-linter).

#### T8.2 — Контрактные тесты A-FQ-01…17
* **Зависит:** T8.1
* **Док:** ACCEPTANCE §8 (вся таблица)
* **Сделать:** `tests/contract/flexiq/` — по тесту на каждый A-FQ с живыми воркерами в отдельных процессах; маркер `integration` + `flexiq`; job в CI (Linux).
* **DoD:** все A-FQ зелёные на flexiq 2.0.x. Прогон на `master` flexiq — отметить `human` (нужен доступ к репозиторию/CI-матрица).

### Ф9. Сквозные примеры как тесты

#### T9.1 — Рассылки (ARCHITECTURE §12) на `InlineBroker`
* **Зависит:** T7.1
* **Док:** ARCHITECTURE §12.4, §12.6 (мок-провайдер, датасет, все 9 тестов)
* **Сделать:** `tests/examples/mailing/` — доменная модель, команды, хуки, задачи, мок-провайдер, датасет; все тесты из §12.6 (статус, числа `(9100, 600, 300, 40)`, breakdown, монотонность снимков, retention, падающий хук, пауза, авто-пауза, kill воркера).
* **DoD:** все тесты §12.6 зелёные без изменения ожидаемых чисел.

#### T9.2 — Конвейер парсинга (ARCHITECTURE §13) на `InlineBroker`
* **Зависит:** T7.1
* **Док:** ARCHITECTURE §13, DYNAMIC_WORKFLOWS §5, ACCEPTANCE A-UC-04, 05, 06, 15
* **Сделать:** `tests/examples/catalog/` — детерминированный генератор каталога, задачи `parse_page/parse_card/download_pdf`, хуки; тесты: параллельность этапов, каскад seal, пустой этап, упавший источник (`seal`/`cancel`), `max_depth`, `max_items`, прогресс по моментам t1–t5 из §13.3.
* **DoD:** числа совпадают с эталоном генератора; I-12/I-13 проверяются в тестах.

#### T9.3 — Исполняемые примеры из документации
* **Зависит:** T9.1, T9.2
* **Док:** ACCEPTANCE A-NF-07
* **Сделать:** тест, извлекающий python-блоки из README и помеченные блоки ARCHITECTURE, которые должны исполняться (маркер-комментарий), и прогоняющий их на `InlineBroker`.
* **DoD:** A-NF-07 зелёный для README и §12/§13.

### Ф10. Надёжность, наблюдаемость, CLI

#### T10.1 — Приёмка A-DB-01…12
* **Зависит:** T9.1
* **Док:** ACCEPTANCE §5
* **Сделать:** `tests/integration/acceptance_db/` — тест на каждый пункт. A-DB-11 (pgbouncer) — контейнер `edoburu/pgbouncer` в transaction mode, asyncpg `statement_cache_size=0` и psycopg 3. A-DB-12 — доменная таблица в другой схеме.
* **DoD:** все A-DB зелёные.

#### T10.2 — Стресс: дедлоки, конкурентные финализации, рандомизированные конвейеры
* **Зависит:** T9.2
* **Док:** ARCHITECTURE §14 «Дополнительно», COUNTERS §3.5, §4
* **Сделать:** стресс-тесты (маркер `slow`): `deadlock_timeout=100ms`, параллельные pause/cancel/финализации/несколько источников → `deadlocks = 0`; 1 000 прогонов конвейера со случайными пустыми этапами, падениями источников и «kill» воркеров (hypothesis stateful или seed-цикл) → нет open-этапа с терминальными источниками, ровно одна финализация на батч; 10% дублей доставки → 1 финализация.
* **DoD:** всё зелёное 3 прогона подряд с разными seed.

#### T10.3 — EXPLAIN-гард
* **Зависит:** T10.2
* **Док:** COUNTERS §4.2, ARCHITECTURE §14
* **Сделать:** фикстура «заполненная БД» (≥ 1M Items, генерация `generate_series`, маркер `slow`), реестр запросов горячего пути (каждый запрос storage регистрирует себя), тест `EXPLAIN (FORMAT JSON)` → нет `Seq Scan` по `th_item/th_batch/th_counter`.
* **DoD:** гард падает, если убрать любой индекс из §5.2 (проверить вручную один раз и записать в журнал).

#### T10.4 — Наблюдаемость и логи
* **Зависит:** T6.1
* **Док:** ACCEPTANCE A-NF-08, A-NF-09; ARCHITECTURE §7.3 (`th_hook_failures`), §7.5 (`th_hook_missing`)
* **Сделать:** события `Observer` во всех точках; `tallyho.observability.otel` (опционально, extra `otel`) — спаны create/claim/finish/finalize; метрики: `th_hook_failures`, `th_hook_missing`, лаг relay, размер буфера Completer, возраст самого старого lease, внутренние ретраи `40P01`. Логи без аргументов задач; ошибки хуков — с `batch_id`, `kind`, попыткой.
* **DoD:** тест-шпион `Observer` видит все события сценария §12; тест: в логах нет payload (подставить секрет в аргумент и проверить `caplog`).

#### T10.5 — CLI
* **Зависит:** T6.1
* **Док:** ARCHITECTURE §3.2 (maintenance отдельным процессом), §16 v1.x (`inspect`)
* **Сделать:** `tallyho migrate --dsn --schema`, `tallyho maintenance --dsn --schema --hook-module ...` (graceful SIGTERM), `tallyho inspect <batch_id|kind:key>` (дерево и прогресс).
* **DoD:** тесты CLI на PG; `sys.exit` только в `__main__`.

#### T10.6 — Мутационное тестирование CAS-запросов
* **Зависит:** T10.2
* **Док:** ACCEPTANCE A-NF-06
* **Сделать:** `mutmut` на модулях с CAS (finish, finalize, snapshot, claim, retry_failed); покрытие `engine`/`storage` ≥ 90% веток.
* **DoD:** 0 выживших мутантов в целевых функциях или обоснованные исключения в DECISIONS.

### Ф11. Приёмочный стенд (ACCEPTANCE §2–4, §6, §7, §9)

#### T11.1 — Эталонное приложение и генераторы
* **Зависит:** T8.2, T9.2
* **Док:** ACCEPTANCE §3 (всё), §3.2 (генератор сайта)
* **Сделать:** `tests/acceptance/app` — домены S1/S2/S3, общие правила задачи (`network()`, инъекция ошибок от seed), `hook_log`, фейковый сайт каталога на aiohttp (эталонная истина), фейковый почтовый провайдер с журналом; `docker compose` стенда (PG 16, toxiproxy, N воркеров flexiq, 2 реплики API).
* **DoD:** S1/S2/S3 на малом объёме проходят без хаоса.

#### T11.2 — Оракул инвариантов I-01…I-14
* **Зависит:** T11.1
* **Док:** ACCEPTANCE §4
* **Сделать:** `tests/acceptance/oracle.py` — каждая проверка отдельной функцией с числовым отчётом; ожидание `T_rec`.
* **DoD:** оракул ловит специально внесённые нарушения (по тесту на инвариант: сломали данные → красный).

#### T11.3 — Хаос-контроллер и сценарии A-CH-01…12
* **Зависит:** T11.2
* **Док:** ACCEPTANCE §6
* **Сделать:** контроллер (kill -9 воркеров, `docker kill`/`pg_ctl stop -m immediate`, toxiproxy toxics, libfaketime, SIGTERM, `requeue/replay/retry_dead`, долгая транзакция), расписание от seed, `uv run poe acceptance --seed N --scenario S --chaos A-CH-NN --duration 120`, журнал хаоса.
* **DoD:** каждый A-CH на S1/S2/S3 по 2 минуты запускается локально и даёт вердикт оракула; дефекты библиотеки помечены `xfail` с задачей-владельцем. Длинные прогоны (10/60 мин, 2 ч) — `human`.

#### T11.3b — Все A-CH зелёные без xfail
* **Зависит:** T11.3, Fix-6, Fix-8, Fix-11
* **Док:** ACCEPTANCE §6; журнал PROGRESS от 2026-10-02 (матрица волны 8)
* **Сделать:** снять все `xfail` в `tests/acceptance/test_chaos.py`, прогнать матрицу 12 × 3 на двух seed; оставшиеся красные ячейки — новые `Fix-N` с воспроизведением. Проверить Fix-8 на `LEASE_TTL=15 SWEEP_INTERVAL=1`. A-CH-05 и A-CH-09 помечены дефектом Fix-7 — перепроверить после исправления.
* **DoD:** `uv run poe acceptance --seed 1 --duration 120 --jobs 4` и то же с `--seed 2` — без xfailed и failed.

#### T11.4 — Сценарии A-UC-01…22 на стенде
* **Зависит:** T11.2, T13.6
* **Док:** ACCEPTANCE §7
* **Сделать:** по сценарию на каждый A-UC поверх стенда с оракулом.
* **DoD:** все A-UC зелёные на функциональном объёме.

#### T11.5 — Бенчмарк-харнесс A-PERF
* **Зависит:** T11.1
* **Док:** ACCEPTANCE §9, COUNTERS §4, ARCHITECTURE §14
* **Сделать:** генераторы нагрузки P-01…P-11, сбор p50/p99 по операциям, графики, отчёт; локальный «дымовой» прогон на малых объёмах.
* **DoD:** дымовой прогон формирует отчёт. Полные замеры на эталонном стенде — `human`.

### Исправления по итогам волны 8

Дефекты найдены хаос-стендом (T11.3) и прогоном примеров руководства (T12.1). Каждая задача: воспроизводящий тест, красный на старом коде, затем исправление; G2 обязателен. Если исправление меняет поведение, описанное в ARCHITECTURE, — сначала документ.

#### Fix-5 — Sweeper учитывает `max_retries` из декоратора задачи
* **Зависит:** —
* **Док:** ARCHITECTURE UC-15, §11.3, D-012; ACCEPTANCE A-CH-01
* **Сделать:** `Sweeper._max_retries` читает лимит только из `th_item.options`. Задача с `@fq.task(max_retries=3)` без `.opts(max_retries=…)` на первом истёкшем lease получает `error("lease_expired")` вместо переотправки. Эффективный лимит должен быть известен sweeper-у (записывать его в `th_item.options` при создании Item или спрашивать у адаптера).
* **DoD:** интеграционный тест: лимит только в декораторе, lease истёк → Item возвращён в outbox, `attempt + 1`; `poe acceptance --seed 1 --scenario S1 --chaos A-CH-01` без xfail.

#### Fix-6 — Движок периодически вызывает `reconcile_dead`
* **Зависит:** —
* **Док:** ARCHITECTURE §11.3, D-014; ACCEPTANCE A-CH-04, I-01, I-03, I-10
* **Сделать:** `Runtime.reconcile_dead` реализован в адаптерах, но engine его не вызывает. Если claim упал с `CompleterError` (PostgreSQL недоступен), flexiq отправляет джобу в DLQ, обработчик `JOB_DEAD` тоже не может записать итог — Item навсегда `active` без lease, outbox и джобы. Добавить проход maintenance: курсор сверки хранится в `th_meta`, мёртвые джобы завершают Items как `error("exhausted")`. После Fix-10 у `Maintenance` поле `relay` может быть `None`, а в процессе `th.install(None)` (CLI) адаптера нет: сверку выполняют только процессы с адаптером. Остаток Fix-7: при отказе PostgreSQL дольше суммы backoff ретраев flexiq джоба уходит в DLQ, не выполнившись.
* **DoD:** интеграционный тест с `InlineBroker`: потерянное событие DLQ → один проход maintenance завершает Item; A-CH-04 на S1 без зависших Items.

#### Fix-7 — Повторная доставка не оставляет Item без исполнителя
* **Зависит:** —
* **Док:** ARCHITECTURE UC-03, UC-04, §11.3 («чужой живой lease → успех»); ACCEPTANCE A-CH-10, A-FQ
* **Сделать:** повторная доставка (`requeue_job`, реап «мёртвого» воркера) получает `DUPLICATE` и закрывает джобу как успешную; исходное выполнение затем падает с повторяемой ошибкой, вердикт `RETRY` → `release` удаляет lease, а повторять уже некому: Item `active` без lease, outbox и джобы. Решить и записать в DECISIONS: `release` возвращает Item в outbox, если повтор брокером невозможен, либо sweeper подбирает `active` Items без lease, outbox и expiry. Там же: `CompleterError` на claim не должен подпадать под пользовательский `retry_on` и сразу уводить джобу в DLQ.
* **DoD:** интеграционный тест сценария «дубль → DUPLICATE → исходная попытка RETRY» завершает Item; A-CH-10 на S2 зелёный три прогона подряд.

#### Fix-8 — `complete_in` учитывает результат CAS
* **Зависит:** —
* **Док:** ARCHITECTURE UC-08, §10; ACCEPTANCE I-04, A-DB-01
* **Сделать:** `item.complete_in()` отбрасывает результат CAS (`runtime/context.py`). Если Item уже завершён (sweeper записал `lease_expired`), транзакция пользователя всё равно коммитит доменную строку. При CAS = 0 бросать ошибку (подкласс `TallyhoError`), чтобы транзакция пользователя откатилась.
* **DoD:** интеграционный тест: Item завершён sweeper-ом → `complete_in` бросает, доменной строки нет; A-CH-02 и A-CH-05 на S1 с `LEASE_TTL=15 SWEEP_INTERVAL=1` без нарушений I-04.

#### Fix-9 — Отменённый до старта батч не финализируется как `succeeded`
* **Зависит:** —
* **Док:** ARCHITECTURE §6.1 (флаг отмены, итог финализации), UC-12; ACCEPTANCE A-UC-11
* **Сделать:** сценарий: `async with th.batch(...)` в своей транзакции, сразу `handle.cancel()` до отправки задач, затем `handle.wait()` — примерно в половине прогонов `state=SUCCEEDED` при `progress.cancelled=4, ok=0`. Гипотеза (не проверена): фоновая финализация после seal читает строку батча до commit отмены, а счётчики — после. Сначала воспроизвести детерминированно, затем исправить выбор итога.
* **DoD:** детерминированный тест гонки «seal → финализация ↔ cancel»: итог всегда `cancelled`; 200 повторов сценария без `SUCCEEDED`.

#### Fix-10 — Быстрая отправка relay после commit в каждом процессе
* **Зависит:** —
* **Док:** ARCHITECTURE §3.2, UC-01, §6.3; T10.5 (CLI maintenance)
* **Сделать:** `Relay.run()` нигде не запускается, `kick()` только копит id, `flush_kicked()` вызывает лишь `InlineBroker`. В продакшне сообщения отправляет только `scan_once` лидера maintenance: задержка до `relay_grace + sweep_interval`. Запускать цикл relay в процессах, где установлен адаптер (жизненный цикл — вместе с `install`/закрытием, RUF006). CLI `tallyho maintenance` без брокера сообщения не отправляет (решение T10.5) — убедиться, что с fast-path установка не остаётся без отправки, и убрать ошибку в логе на каждом проходе.
* **DoD:** интеграционный тест с адаптером без maintenance: сообщение отправлено быстрее `relay_grace`; потерянный kick по-прежнему подбирает scan.

#### Fix-11 — Закрытие дожидается фоновых задач; SIGTERM возвращает удержанные Items
* **Зависит:** —
* **Док:** ARCHITECTURE §3.2, UC-04; ACCEPTANCE A-CH-08; AGENTS.md (RUF006)
* **Сделать:** фоновые задачи `_Facade._spawn_finalize` и `Operations._background` никто не дожидается: `DROP SCHEMA` в teardown тестов сталкивается с ними дедлоком (виновник — `tests/integration/api/test_batch.py::test_schedule_example_builds_and_seals_pipeline`), в логе воркера на SIGTERM — «Task was destroyed but it is pending», `Completer.close(requeue_held=True)` не вызывается. `Tallyho.aclose()` уже есть (Fix-10), но только останавливает relay: расширить `_Facade.close()` в `engine/assembly.py` — дождаться `_background`, задач `Operations` и вызвать `Completer.close(requeue_held=True)`; вызвать `aclose` из воркера стенда и фикстур. Хелпер `settle()` в `tests/integration/api/test_batch.py` заменить на `aclose`. `Relay.stop()` ждёт без таймаута — зависший `dispatch` задержит остановку.
* **DoD:** тест: после закрытия нет незавершённых задач библиотеки; прогон `tests/integration` на PostgreSQL с логом — 0 дедлоков с участием `DROP SCHEMA`; A-CH-08 оставляет 0 lease при `drain_timeout` по умолчанию.

#### Fix-12 — Запросы на соединении пользователя видят схему установки
* **Зависит:** —
* **Док:** ARCHITECTURE §11.1 (schema/prefix), UC-08; ACCEPTANCE A-NF-03, A-DB-12
* **Сделать:** по отчёту T12.1 `th.batch(session=)` и `item.complete_in(session)` выполняют запросы на соединении пользователя без имени схемы: без `schema_translate_map={None: schema}` или `search_path` получается `relation "th_batch" does not exist`. Сначала воспроизвести тестом (установка в не-public схеме, движок пользователя без translate map). Если подтверждается — подставлять схему на стороне библиотеки; если так задумано — описать требование в ARCHITECTURE §11.1.
* **DoD:** тест с пользовательской сессией без translate map и схемой, отличной от `search_path`, зелёный; страница установки в guide обновлена.

#### Fix-13 — Публичный API ↔ ARCHITECTURE §11
* **Зависит:** —
* **Док:** ARCHITECTURE §11.2, §11.4, §13.2, UC-02; guide
* **Сделать:** расхождения, найденные T12.1: `th.item` и `th.tracked` недоступны как атрибуты `Tallyho` (только `from tallyho import item, tracked, callback`); у `item.spawn` нет `opts=`; `into=` не принимает `BatchHandle`; `@fq.task(weight=2)` даёт `ConfigurationError`; `item.sub_batch` принимает `callbacks=`, а не `on_...=`; `CallbackContext.summary` всегда `None`; потоковое добавление UC-02 недостижимо через builder (выход из `async with` всегда делает seal). По каждому пункту: реализовать то, что обещает ARCHITECTURE, либо исправить документ с записью в DECISIONS. Затем обновить guide.
* **DoD:** типовые тесты (`tests/typing/cases.py`) и исполняемые примеры покрывают каждый пункт; в ARCHITECTURE нет API, которого нет в коде.

#### Fix-14 — Мелкие расхождения движка
* **Зависит:** —
* **Док:** ARCHITECTURE §7.3 (backoff хуков), §12.4 (`labels`), §3.2 (лидерство), D-024
* **Сделать:** (1) `Settings.hook_backoff_initial` проверяется, но в engine не передаётся; (2) `view.labels` и `view.metrics` (и поля `BatchSummary`) — один словарь: метки итога вперемешку с метриками `item.incr`; (3) advisory lock лидера maintenance и миграции не учитывает `prefix` — две установки в одной схеме делят лидера; (4) у корня конвейера `progress.found` равен числу под-батчей, хотя D-024 говорит о вычитании виртуальных Items; (5) `tree_cache.load` в `TaskRuntime._item` после успешного claim бросает сырые ошибки SQLAlchemy, а не подкласс `TallyhoError` (и они не попадают под расширенный `retry_on`).
* **DoD:** по тесту на каждый пункт.

#### Fix-15 — Дедлайн после явного `cancel()` не меняет итог на `failed`
* **Зависит:** —
* **Док:** ARCHITECTURE §6.1 (флаг и причина отмены), UC-12; D-011
* **Сделать:** `Operations.cancel` и проходы, выставляющие отмену (дедлайн, политика), перезаписывают `cancel_reason` без условия: дедлайн после явного `cancel()` сменит итог с `cancelled` на `failed(deadline)`. Первая причина должна выигрывать (условие `cancel_requested_at IS NULL`); записать правило в ARCHITECTURE §6.1.
* **DoD:** интеграционные тесты: `cancel()` → дедлайн → итог `cancelled`; дедлайн → `cancel()` → итог `failed`, причина `deadline`.

### Ф12. Документация и релиз

#### T12.1 — Пользовательская документация
* **Зависит:** T9.3, T13.6
* **Док:** ACCEPTANCE чек-лист §11 (ограничения v1), COUNTERS §3.6 (эксплуатация), §2 P10 (pgbouncer)
* **Сделать:** README (быстрый старт), `docs/guide/`: установка и миграции, батчи и конвейеры, хуки, эксплуатация PG (autovacuum, `backend_xmin`, pgbouncer), ограничения v1 (только PG, `pool="thread"`, несовместимые опции flexiq, мягкий `max_items`), CHANGELOG.
* **DoD:** примеры исполняются (T9.3).

#### T12.2 — CI: nightly и матрица
* **Зависит:** T11.4
* **Док:** ACCEPTANCE §11 (уровни PR / nightly / pre-release)
* **Сделать:** workflow `nightly.yml` (A-CH по 10 мин, P-01, P-04 1k×1k + история 5M), job для контрактных тестов flexiq (2.0.x + master), PR-уровень ≤ 15 мин.
* **DoD:** workflow валиден (`check-github-workflows`); запуск в GitHub — `human`.

#### T12.3 — Подписание релиза
* **Зависит:** всё
* **Сделать:** чек-лист ACCEPTANCE §11: pre-release прогон, 3 зелёных nightly, замеры на эталонном стенде, решение владельца о целевых числах; решение о слиянии миграций v1…vN в одну базовую (D-042).
* **Статус изначально:** `human`.

### Ф13. Расширения перед релизом: атрибуты и экспорт исходов Items

Обоснование, обзор библиотек и отложенные варианты — [V1_EXTENSIONS_PLAN.md](V1_EXTENSIONS_PLAN.md); решения — D-038…D-042. Фаза выполняется **до** Ф11 и Ф12: стенд, оракул и гайды должны сразу покрывать расширения.

Волны: T13.0 → {Fix-2, T13.1, T13.2, T13.5} → T13.3 → T13.4 → T13.6. Fix-3 найдена при выполнении T13.6.

#### T13.0 — Зафиксировать решения в документах
* **Зависит:** —
* **Док:** V1_EXTENSIONS_PLAN.md
* **Сделать:** ARCHITECTURE §1, §2, §3.4, §5.1–§5.2, §7.6, UC-14, UC-16, §11.2, §12.9, §15, §16; ACCEPTANCE I-14, A-UC-21/22, A-AT-01…12; DECISIONS D-038…D-042; карточки и строки Ф13.
* **DoD:** в ARCHITECTURE нет API, отсутствующего в карточках Ф13.

#### Fix-2 — `retry_failed()` сбрасывает `released_at`
* **Зависит:** T13.0
* **Док:** ARCHITECTURE §7.6, UC-14, UC-16; ACCEPTANCE I-14, A-UC-22
* **Сделать:** при переоткрытии дерева `released_at = NULL` у корня; `release()` нужно вызвать заново после новой финализации. Функции мутационного gate (D-037) не менять.
* **DoD:** интеграционный тест: финализация → `release()` → `retry_failed()` → финализация → retention не удаляет дерево до второго `release()`, после него удаляет.

#### Fix-3 — `InlineBroker.drain` дожидается после-коммитной работы Completer
* **Зависит:** —
* **Док:** ARCHITECTURE §9.2 (после-коммитные действия Completer), T7.1
* **Сделать:** Completer возвращает результат задаче сразу после commit, а оценка политики, финализация и её каскад идут следом (в цикле Completer или в фоновой задаче пути `complete_in`). `InlineBroker.drain`/`step` мог вернуться раньше, чем каскад дойдёт до корня и колбэк попадёт в outbox. `Completer.settled()` ждёт простоя; брокер вызывает его перед каждым проходом relay.
* **DoD:** один `drain()` доводит дерево с `complete_in` и исчерпанными ретраями до колбэка финализации; сценарии T13.6 стабильны.

#### Fix-4 — Редкий дедлок в стресс-файле и утечка статистики дедлоков между тестами
* **Зависит:** —
* **Док:** ARCHITECTURE §14 («0 дедлоков в стресс-тесте»), ACCEPTANCE A-DB-08; журнал PROGRESS от 2026-10-02
* **Сделать:** (1) `test_parallel_pause_cancel_sources_and_hooks_have_no_deadlocks` считает дедлоки по `pg_stat_database` всей БД, а статистика публикуется с задержкой: намеренный дедлок другого теста попадает в его окно. Считать дедлоки по своим соединениям (ловить `40P01` через callback `RetryPolicy`) или изолировать тест отдельной БД. (2) При прогоне одного `test_concurrency.py` с seed 186081763 прирост счётчика наблюдался 1 раз из 5 без намеренных дедлоков: найти запросы (PostgreSQL с `log_lock_waits`, прогон до воспроизведения) и устранить причину либо обосновать допустимость авто-повтора.
* **DoD:** 20 прогонов файла подряд без прироста счётчика, либо найденная причина с исправлением и тестом.

#### T13.1 — model: атрибуты и `memo`
* **Зависит:** T13.0
* **Док:** ARCHITECTURE §2, §5.1 (правила атрибутов), §11.2, §15; ACCEPTANCE A-AT-02, A-AT-03
* **Сделать:** `model/attributes.py`: одна функция нормализации для записи и фильтра (`str | int | bool`, `UUID → str`, `int` в пределах `bigint`), запрет префикса `tallyho.` и пустого ключа, лимиты через неизменяемый `AttributeLimits`, проверка `memo` (JSON-объект, размер); `InvalidAttributesError(ConfigurationError)` в `model/errors.py`. DTO `BatchInfo`, `BatchPage` и поля `attributes`/`memo` в `BatchView`, `attributes` в `BatchSummary` (замороженные словари, по умолчанию пустые).
* **DoD:** юнит-тесты границы каждого лимита и каждого запрещённого типа; `bool` не принимается за `int` и наоборот.

#### T13.2 — storage: схема v3
* **Зависит:** T13.0
* **Док:** ARCHITECTURE §5.1, §5.2; ACCEPTANCE A-AT-12; D-042
* **Сделать:** таблица `th_batch_attr(batch_id PK, attributes jsonb NOT NULL, memo jsonb NULL)` + GIN `jsonb_path_ops` по `attributes`; индекс `th_batch (kind, id) WHERE parent_id IS NULL`; встроенная миграция v3 и Alembic `upgrade(..., version=3)`; golden-DDL; `SCHEMA_VERSION = 3`.
* **DoD:** каталог после `migrate` совпадает с `create_all`; путь v2 → v3 на непустой БД; исторические golden v1 и v2 не изменились.

#### T13.3 — engine + api: запись и чтение атрибутов
* **Зависит:** T13.1, T13.2
* **Док:** ARCHITECTURE §5.1, §11.2; ACCEPTANCE A-AT-01, 04, 05, 10, 11
* **Сделать:** `th.batch(..., attributes=, memo=)` только для корня; лимиты — в `Settings` (§15); запись строки `th_batch_attr` в транзакции создания, только если есть атрибуты или `memo`, и только когда корень действительно создан (повтор `(kind, key)` — первый выигрывает); чтение — в том же statement дерева (`Reads._forest_statement`), значения корня раздаются всем узлам `BatchView`/`BatchSummary`; retention удаляет side-строку; атрибуты и `memo` не передаются в `Observer` и логи.
* **DoD:** rollback транзакции пользователя не оставляет строки; `on_finalized` и `on_progress` под-батча видят атрибуты корня; тест с секретом в атрибуте и `caplog`; число SQL-запросов `view()` и тика Snapshotter не выросло.

#### T13.4 — Листинг батчей
* **Зависит:** T13.3
* **Док:** ARCHITECTURE §11.2 («Листинг батчей»), §5.2; ACCEPTANCE A-AT-06, A-AT-07
* **Сделать:** `Reads.list_batches` и `th.list_batches(kinds=, states=, attributes=, created_after=, created_before=, limit=, cursor=)` → `BatchPage`; только корни, keyset по `id DESC`; непрозрачный курсор (испорченный — `ConfigurationError`); запросы зарегистрированы в `storage.hot_queries`.
* **DoD:** пагинация без пропусков и дублей при параллельном создании батчей; `{"n": 1}` не находит `{"n": "1"}`; EXPLAIN-гард зелёный (листинг по `kind` и по `attributes`).

#### T13.5 — `handle.items(states=, labels=)`
* **Зависит:** T13.0
* **Док:** ARCHITECTURE §11.2 («Чтение Items»), §15 (`items_scan_window`); ACCEPTANCE A-AT-08, A-AT-09; D-041
* **Сделать:** сигнатура `items(*, states: Collection[ItemState] | None = None, labels: Collection[str] | None = None)` в `Reads`, фасаде engine и `BatchHandle`; без фильтров — `ConfigurationError`; `labels=` — через `th_item_mark`; `states=` — окнами по `(batch_id, id)`, курсор сдвигается на последнюю строку окна и без совпадений; оба фильтра — пересечение; настройка `items_scan_window`. Обновить все вызовы `items(label=)` в коде, тестах и примерах.
* **DoD:** отменённые Items перечисляются; запрос окна в `storage.hot_queries`, EXPLAIN-гард без `Seq Scan`; `slow`-замер на батче в 1 млн Items с 0,1% совпадений: найдены все, ни один statement не читает больше окна, время — в журнал PROGRESS; `BatchPurged` для удалённого батча, как раньше.

#### T13.6 — Рецепт финального экспорта: пример, тесты, документация
* **Зависит:** Fix-2, T13.4, T13.5
* **Док:** ARCHITECTURE §12.9; ACCEPTANCE A-UC-21, A-UC-22
* **Сделать:** в `tests/examples/mailing` — отдельный сценарий на малом объёме (до 1 000 контактов) с таблицей `mailing_delivery`: задача пишет строку и вызывает `complete_in`; `on_finalized` ставит счётчики и `settling`; `settle_campaign` экспортирует `ERROR`/`CANCELLED` этапа `send`, закрывает остаток, ставит итоговый статус и вызывает `release()` корня одной транзакцией. Атрибуты и листинг — в том же сценарии. Маркированный smoke-блок в ARCHITECTURE §12.9 и упоминание в README.
* **DoD:** эталонный сценарий `(9100, 600, 300, 40)` не изменился; случаи: `exhausted`, отмена посреди разворачивания (строки без Items закрыты запросом по остатку), падение колбэка посередине и повтор, `retry_failed()` после settle с повторным экспортом; retention не удаляет дерево до `release()`; документационные тесты зелёные.
