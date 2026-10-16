# Управление ограниченными ресурсами — план имплементации

> Версия плана 1.0 · 2026-10-16 · к [ARCHITECTURE.md](../ARCHITECTURE.md) и [ACCEPTANCE.md](../ACCEPTANCE.md).
> Прогресс — [RESOURCES_PROGRESS.md](RESOURCES_PROGRESS.md). Решения по ходу — [DECISIONS.md](DECISIONS.md).
> **Протокол итерации, гейты и правила — [PLAN.md §0](PLAN.md#0-протокол-итерации-loop)**, здесь не дублируются.

## Источники

| Документ | Роль |
|---|---|
| [`../resource-management.md`](../resource-management.md) | исходный проектный документ: задача, словарь, разделение ответственности |
| [`../resource-management-tallyho.md`](../resource-management-tallyho.md) | конструкция доработок Tallyho (T1–T7) — **читать перед задачей**, в карточках ссылки на разделы |
| [`../resource-management-review.md`](../resource-management-review.md) | вердикты по претензиям со ссылками `файл:строка` и 12 пропущенных в документе вещей |
| [`../resource-management-research.md`](../resource-management-research.md) | опорные реализации: Oban, Sidekiq Ent, Temporal, Kueue, HPA |

Задачи вне этого репозитория (Identity Hub, capacity-governor) в план не входят:
`AccountLibrarry/docs/resource-management-plan.md` и
`capacity-governor/docs/resource-management-plan.md`.

## Что даёт набор

При нехватке дефицитного ресурса работа **ждёт**, а не проваливается, и объём
выпускаемой в брокер работы можно регулировать извне на ходу.

Контрольный замер — сквозной прогон domain-checker из §13 исходного документа
(20 доменов, кредиты 5 + 3 + 0): сейчас сгорает **10 ретраев** на 12 ждавших доменов,
после набора должно быть **0** при тех же 20 платных вызовах.

---

## 0. Порядок и зависимости

```
RM0.1 (relay: NULL ≠ 0)
  └─ RM1.1 (set_max_in_flight) ─┬─ RM1.2 (окно-aware распарковка)
                                 └─ RM6.1 (deadline_mode)
RM0.2 (ARCHITECTURE) ─┬─ RM2.1 (миграция) ── RM2.2 (item.defer) ─┬─ RM2.3 (потолки)
                      │                                          └─ RM2.4 (наблюдаемость)
                      └─ RM4.1 (th.demand)
RM3.1 (ленивые hook_modules) — независима
RM5.1 (решение «подпул = батч») — независима, только документы
RM7.1 (приёмка) — после RM1.2, RM2.3, RM2.4, RM4.1
```

**RM0.1 идёт первой.** Пока `(max_in_flight or 0)` в relay склеивает «окна нет» и
«окно ноль», разрешение нуля (RM1.1) внесёт регресс в батчи без окна.

`RM3.1` и `RM5.1` ни от чего не зависят — годятся в волну с чем угодно.

---

## 1. Карточки задач

### RM0.1 — relay: развести `NULL` и `0` в арифметике окна

**Док:** `../resource-management-tallyho.md` T2 («Две обязательные правки»), ARCHITECTURE §11.2.

`relay.py:645` считает `room = (rows[0].max_in_flight or 0) - used`. Выражение `or 0`
не различает `NULL` («окна нет») и `0` («окно закрыто»). Сейчас это безвредно, потому
что `0` запрещён валидацией; после RM1.1 станет багом.

Сделать: явная ветка `IS NULL` → окна нет, парковки нет; `0` → парковать всё. То же
проверить в проходе свободных мест `relay.py:827-838`.

**DoD:**
* тест: батч с `max_in_flight IS NULL` — записи не паркуются ни при какой загрузке;
* тест: батч с `max_in_flight = 0` (ставится напрямую в БД, до RM1.1) — паркуется всё;
* `uv run poe test-all` зелёный (трогаем `engine/`).

---

### RM0.2 — ARCHITECTURE и COUNTERS под `defer` и окно 0

**Док:** `../resource-management-tallyho.md` T1, T2; ARCHITECTURE §5.1, §5.2, §6.2, §9.4, §11.2.

По [AGENTS.md](../../AGENTS.md) документ правится **до** кода. Внести:

* §5.1 — колонки `th_outbox.defer_reason`, `th_item.defers`, `th_item.first_deferred_at`;
  явно отметить, что `defer` увеличивает `generation` (это возврат в outbox, UC-15);
* §5.2 — частичный индекс `th_outbox (batch_id) WHERE defer_reason IS NOT NULL`,
  частичный индекс `th_batch (kind) WHERE state IN (OPEN, SEALED)` для `demand()`;
* §6.2 — производное состояние `deferred` в таблице и на диаграмме Item;
* §9.4 — `Progress.deferred` как гейдж (считается как `in_flight`, не через счётчики);
* §11.2 — `set_max_in_flight`, `clear_max_in_flight`, `demand()`, семантика окна 0;
* COUNTERS.md — зафиксировать, что **новых колонок счётчиков нет** и почему
  (горячий путь группового коммита не трогаем).

**DoD:** `uv run poe check` зелёный (есть тесты на соответствие документации);
никаких изменений в `src/`.

---

### RM1.1 — `set_max_in_flight`, ноль разрешён

**Док:** `../resource-management-tallyho.md` T2. **Зависит:** RM0.1, RM0.2.

* `engine/producer.py:212` — минимум валидации `max_in_flight` с 1 на 0;
* `BatchHandle.set_max_in_flight(limit: int, *, session=None, reason: str | None = None)`
  — `limit >= 0`, абсолютное значение, идемпотентно; с `session=` участвует в
  транзакции вызывающего, пинок relay — после коммита;
* `BatchHandle.clear_max_in_flight()` — снять окно (`NULL`). Отдельным методом, чтобы
  не смешивать «нет окна» и «окно ноль» в одном аргументе;
* метод движка в `engine/public.py`; `reason` — в `th_batch.options` для наблюдаемости.

**DoD:**
* окно 0: новая работа паркуется, выполняющаяся доделывается;
* 0 → N: запаркованное уезжает на ближайшем проходе relay, не больше N;
* установка в транзакции пользователя: откат транзакции откатывает и окно;
* окно и пауза независимы: `resume()` не снимает окно, `set_max_in_flight` не снимает паузу;
* батч с окном 0 и всей работой в парковке не финализируется.

---

### RM1.2 — окно-aware распарковка

**Док:** `../resource-management-tallyho.md` T2 (правка 2), ARCHITECTURE §6.2. **Зависит:** RM1.1.

И пауза, и переполненное окно паркуют записи одинаково (`available_at = ∞`).
`resume()`, не знающий про окно, выпустит в брокер всё, что было запарковано окном.

Сделать: распарковка (в `resume()` и в проходах relay) выпускает не больше `room` и
только при `paused_at IS NULL`.

**DoD:**
* **инвариант под тест:** число строк `th_window` батча никогда не превышает его
  `max_in_flight` — при любой комбинации `pause`/`resume`/`set_max_in_flight`/завершений;
* тест: `pause()` при окне 2 → `resume()` → в брокере не больше 2;
* тест: N → 0 → N при живых задачах, инвариант держится;
* стресс-тест на конкурентные проходы relay (несколько процессов, `SKIP LOCKED`).

---

### RM2.1 — миграция под `defer`

**Док:** `../resource-management-tallyho.md` T1 («Хранение»). **Зависит:** RM0.2.

* `th_outbox` `+ defer_reason text NULL`;
* `th_item` `+ defers smallint NOT NULL DEFAULT 0`, `+ first_deferred_at timestamptz NULL`
  — без индексов, finish остаётся HOT;
* частичный индекс `th_outbox (batch_id) WHERE defer_reason IS NOT NULL`;
* опции батча `max_defers: int | None`, `max_deferred_for: timedelta | None` — в
  существующий `th_batch.options` jsonb, миграции не требуют.

Новых таблиц нет: «отложено сейчас» — гейдж по `th_outbox`, как `in_flight` по `th_lease`.

**DoD:** миграция ставится и откатывается; тест установки в схему; `th_counter` и
`th_counter_delta` не изменились.

---

### RM2.2 — `item.defer()`

**Док:** `../resource-management-tallyho.md` T1 («API», «Транзакция defer», «Путь брокера»).
**Зависит:** RM2.1, RM1.1.

```python
def defer(self, *, reason: str, delay: timedelta | None = None,
          until: datetime | None = None) -> None: ...
```

Одна транзакция: удалить `th_lease`, удалить `th_window`, вставить запись `th_outbox`
с `available_at` и `defer_reason`, `UPDATE th_item SET defers = defers + 1,
generation = generation + 1, first_deferred_at = coalesce(first_deferred_at, now())`,
вернуть запаркованные записи батча в объёме освободившегося места.
`th_item.state` остаётся `ACTIVE`, `attempt` **не меняется**, дельты счётчиков не пишутся.

Обрабатывается на **успешной** ветке `runtime/tracked.py`, не в `_settle_failure`:
брокер получает успех, попытка брокера не тратится.

**DoD:**
* `attempt` не изменился, брокер повтор не получил, записи в DLQ нет;
* `generation` вырос на 1;
* строка `th_window` удалена, запаркованный Item того же батча поехал;
* `until` наступил → Item снова `queued`; батч остался `SEALED`, не финализирован;
* `complete_in(session)`: домен и defer в одном коммите, при откате — ни того ни другого;
* `defer` + `cancel()` → Item `cancelled`; `defer` + `pause()` → не уехал раньше `until`;
* валидация: `delay` и `until` вместе, отрицательный `delay`, пустой `reason`,
  `reason` с первым символом U+001F — `ConfigurationError`;
* порядок итогов: `ok()` после `defer()` — `ConfigurationError`.

---

### RM2.3 — потолки отложений

**Док:** `../resource-management-tallyho.md` T1 («Потолки»). **Зависит:** RM2.2.

| Потолок | Где | Итог |
|---|---|---|
| `max_defers` | в транзакции defer | `error("deferred_too_long")` вместо defer |
| `max_deferred_for` | там же | то же |
| `max_deferred_for` у работы, которая так и не поехала | **sweeper** | то же |

Третья строка обязательна: при окне 0 Item не попадёт в воркер, `until` наступит, а
проверка внутри задачи не выполнится никогда. Нужен проход `engine/maintenance.py` по
`th_outbox WHERE defer_reason IS NOT NULL AND first_deferred_at < now() - max_deferred_for`.

Значения по умолчанию взять порядка Sidekiq Enterprise (~20 отложений / ~сутки),
обоснование — `../resource-management-research.md` §1.

**DoD:**
* `max_defers` исчерпан → `error("deferred_too_long")`, батч финализируется;
* sweeper закрывает Item при окне 0 и просроченном `max_deferred_for`;
* батч без потолков откладывает неограниченно (обратная совместимость).

---

### RM2.4 — наблюдаемость отложений

**Док:** `../resource-management-tallyho.md` T1 («Наблюдаемость»), ARCHITECTURE §9.4.
**Зависит:** RM2.2.

* `Progress.deferred: int` — гейдж;
* `BatchView.deferred_reasons: Mapping[str, int]` — разбивка по причинам;
* оба — одним `GROUP BY` в `view()`/`watch()`, рядом с запросом `in_flight`;
* событие наблюдателя `item_deferred(batch_id, item_id, reason, until)`;
* отдельного хука на `defer` нет: `on_progress` видит гейдж.

**DoD:** «ждут ресурса: 340» видно в `view()`; число SQL-запросов `view()` выросло не
больше чем на один; `on_progress` получает `deferred`.

---

### RM3.1 — ленивый импорт `hook_modules`

**Док:** `../resource-management-tallyho.md` T3. **Зависит:** —

`import_hook_modules(...)` — последний оператор `Tallyho.__init__`
(`api/client.py:265`), поэтому схема из [guide/hooks.md](../guide/hooks.md) (модуль
хуков делает `from app.tallyho_client import th`) падает circular import'ом.
Документация описывает нерабочий код.

Сделать: импорт лениво при первом обращении к реестру хуков (однократный флаг) +
явный `th.install_hooks()` для fail-fast на старте. Обёртку в `ConfigurationError`
сохранить. Обновить `guide/hooks.md`.

**DoD:**
* фикстура из двух настоящих модулей, воспроизводящая схему документации: клиент
  импортируется первым — хуки зарегистрированы, ошибки нет;
* неверное имя модуля падает на `install_hooks()`, а не при первой финализации;
* исполняемая проверка примера из документации (как T9.3).

---

### RM4.1 — `th.demand()`

**Док:** `../resource-management-tallyho.md` T4. **Зависит:** RM0.2, RM2.1.

```python
async def demand(self, kinds: Sequence[str], *,
                 attributes: Mapping[str, object] | None = None,
                 limit: int = 1000) -> list[BatchDemand]: ...
```

`BatchDemand`: `id`, `kind`, `key`, `attributes`, `pending`, `in_flight`, `deferred`,
`max_in_flight`, `paused`, плюс флаг `truncated` на выдаче при упоре в `limit`.

Один составной запрос с `LATERAL`: `pending` из `th_counter` + несвёрнутых
`th_counter_delta`, `in_flight` из `th_lease`, `deferred` из `th_outbox`. Обещать «один
дешёвый запрос» нельзя — стоимость O(активные батчи × слоты счётчиков), это в
докстринге. `kinds` обязателен. Частичный индекс `th_batch (kind) WHERE state IN
(OPEN, SEALED)` — существующий покрывает только корни, а потребителем бывает под-батч.

**DoD:**
* `pending`/`in_flight`/`deferred` совпадают с `view()` того же батча;
* несвёрнутые дельты учтены (пишем дельту, не вызываем свёртку, сверяем);
* `paused` и `max_in_flight` отражают текущее состояние, включая 0 и `NULL`;
* фильтр по `attributes` корня работает для под-батчей;
* **число SQL-запросов не зависит от числа батчей** (счётчик запросов в тесте);
* упор в `limit` выставляет `truncated`;
* EXPLAIN-гард (как T10.3) на новый запрос.

---

### RM5.1 — решение «подпул = батч» вместо `requires=`

**Док:** `../resource-management-tallyho.md` T5. **Зависит:** — (только документы)

§10.5 исходного документа предлагает `opts(requires={"region": "DE"})`. Отклонено:
метка легла бы в `th_item.options` (jsonb), а агрегат по ней нарушает инвариант
ARCHITECTURE §5.2 «на `th_item` нет индексов по изменяемым колонкам», ради которого
finish остаётся HOT.

Сделать: записать решение в [DECISIONS.md](DECISIONS.md) — подпул выражается
**отдельным под-батчем** с атрибутом, тогда `demand(attributes=…)` работает без нового
кода; снять пункт из roadmap ARCHITECTURE §16.

**DoD:** запись в DECISIONS.md с обоснованием и альтернативой; `src/` не изменён.

---

### RM6.1 — `deadline_mode="active"`

**Док:** `../resource-management-tallyho.md` T7; открытый вопрос §14.4 исходного
документа. **Зависит:** RM1.1.

Батч, час простоявший в окне 0 из-за нехватки ресурса, сейчас проваливается по
дедлайну, хотя работа не выполнялась.

Опция батча `deadline_mode: "wall" | "active"`, по умолчанию `"wall"` (обратная
совместимость). При `"active"` sweeper на каждом проходе сдвигает `deadline_at` вперёд
на длительность прохода, если батч в этот момент на паузе или в окне 0. Сдвиг
монотонный, новых колонок и аккумуляторов не нужно, проход по дедлайнам уже есть.

**DoD:** батч с `"active"`, простоявший в окне 0 дольше дедлайна, не провален;
батч с `"wall"` ведёт себя как раньше; работающий батч дедлайн не сдвигает.

---

### RM7.1 — приёмка

**Док:** [ACCEPTANCE.md](../ACCEPTANCE.md); §13 исходного документа.
**Зависит:** RM1.2, RM2.3, RM2.4, RM4.1.

Сценарий в приёмочное приложение (как T11.1): пул ресурсов с убывающей ёмкостью,
потребитель с динамическим окном, работа при исчерпании уходит в `defer`, после
пополнения продолжается.

**DoD:**
* **0 сгоревших ретраев** вместо 10 на сценарии «20 единиц работы, ёмкость 5 + 3 + 0»;
* ровно 20 платных вызовов, не больше одного параллельного с ресурса;
* работа завершилась 20 из 20 после пополнения;
* запись в ACCEPTANCE.md как новый критерий A-RM.

---

## 2. Что в этот план не входит

| Пункт | Почему |
|---|---|
| `requires=` как метка item'а | отклонено, см. RM5.1 |
| AIMD-лимит, резерв количества «оценка → факт» | Identity Hub, и отложены до провайдера, которому нужны |
| `eligible_count`, события ёмкости | Identity Hub: `AccountLibrarry/docs/resource-management-plan.md` |
| политики раздачи и пополнения, контур | `capacity-governor/docs/resource-management-plan.md`, фаза A — модуль в проекте-потребителе |
| FlexiQ: `max_concurrent` только при старте воркера, несовместимость `@task(batch=)` | проверено, справедливо, доработок не требует: потолок используется как статичный, пачки Tallyho нарезает сам |
