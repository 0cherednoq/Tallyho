# Хуки

## Зачем нужны хуки

Прогресс и итог батча обычно нужны в вашей таблице: `campaigns.sent`, `campaigns.status`.
На это есть причины:

1. Таблицы tallyho очищаются по retention, а итог кампании нужен навсегда.
2. Списки, сортировки и фильтры по вашей таблице не должны зависеть от служебных таблиц.
3. Доменный статус должен меняться в тот момент, когда батч завершился.

Привычные решения этого не дают. Задача-колбэк в брокере - это второй коммит: батч уже завершён, а
колбэк ещё в очереди или упал. `UPDATE campaigns SET sent = sent + 1` в каждой задаче создаёт
горячую строку в вашей таблице. Опрос `view()` теряет данные, если поллер отстал дольше retention.

**Транзакционный хук (tx-хук)** - ваша функция, которую tallyho выполняет внутри своей
транзакции на событии батча. Ваши изменения и изменение состояния батча коммитятся вместе или
вместе откатываются.

| Хук | Когда вызывается | Гарантия |
|---|---|---|
| `on_finalized(session, summary)` | батч переходит в терминальное состояние | ровно один успешный коммит, атомарно с финализацией. Хук упал - финализации нет, будет повтор |
| `on_progress(session, summary)` | не чаще `every` на батч и только если счётчики изменились | снимки монотонны по `summary.seq`; опоздавший снимок не перезапишет итог |
| `on_policy_breach(session, summary, breach)` | сработала [политика ошибок](batches/failure-policies.md) | атомарно с постановкой на паузу или провалом |

## Регистрация

Хуки регистрируются декораторами клиента на `kind` батча. Один хук каждого вида на `kind`; повторная
регистрация - `ConfigurationError`.

<!-- tallyho-noexec: раскладка по модулям вашего приложения; полный рабочий пример - ниже -->
```python
# app/tallyho_client.py
th = Tallyho(engine, schema="app", hook_modules=["app.mailing.hooks"])


# app/mailing/hooks.py
from app.tallyho_client import th


@th.on_finalized("campaign_deliveries")
async def save_result(session: AsyncSession, summary: BatchSummary) -> None: ...


@th.on_progress("campaign_deliveries", every=timedelta(seconds=2))
async def save_progress(session: AsyncSession, summary: BatchSummary) -> None: ...


@th.on_policy_breach("campaign_deliveries")
async def auto_pause(session: AsyncSession, summary: BatchSummary, breach: PolicyBreach) -> None: ...
```

:::{warning}
Хуки должны быть зарегистрированы в каждом процессе, который может финализировать батч: в
воркерах (там завершается последняя задача), в процессе maintenance (он подбирает пропущенное и
делает снимки прогресса) и в API-процессе (он финализирует, например, пустые батчи). Перечислите
модули с хуками в `hook_modules` - клиент импортирует их при создании. Для `tallyho maintenance`
это флаг `--hook-module`.
:::

От забытого импорта есть защита. При создании батча запоминается, какие хуки зарегистрированы для его
`kind`. Процесс, в котором нужного хука нет, батч не финализирует: пишет ошибку в лог, отправляет
наблюдателю событие `hook_missing` и оставляет батч процессу, где хук есть. Запись итога не
пропадёт незаметно.

Хук регистрируется на `kind` корня и получает сводку всего дерева. Под-батчи по умолчанию имеют
`kind` вида `<kind корня>.<key>`, поэтому хук корня не вызывается на каждом этапе; собственный хук
этапа регистрируйте на его `kind`.

## Сводка `BatchSummary`

| Поле | Значение |
|---|---|
| `id`, `kind`, `key` | идентификация батча |
| `state` | состояние: в `on_finalized` - терминальное, в остальных хуках - текущее |
| `progress` | счётчики и оценки, те же поля, что у [`view().progress`](batches/progress.md) |
| `labels` | число задач батча по меткам итога ([метки и метрики](batches/tasks.md)) |
| `metrics` | суммы `item.incr` по именам метрик |
| `children` | сводки под-батчей по ключу: `summary.children["send"]` |
| `seq` | монотонный номер снимка в пределах батча; у финализации он больше любого снимка прогресса |
| `reason` | причина запроса отмены: `cancel`, `deadline`, `fail_fast`, `policy` |
| `finished_at` | время финализации |
| `attributes` | [атрибуты](batches/attributes.md) корня |

Сводка неизменяема. В `on_finalized` числа окончательные и точные.

## Пример: статус, прогресс и авто-пауза

<!-- tallyho-noexec: фрагмент приложения: таблица reports принадлежит вашему проекту -->
```python
# app/hooks.py
KIND = "report_build"
ACTIVE = ("running", "paused")
FINAL = {
    BatchState.SUCCEEDED: "completed",
    BatchState.COMPLETED_WITH_ERRORS: "completed_with_errors",
    BatchState.FAILED: "failed",
    BatchState.CANCELLED: "cancelled",
}


@th.on_finalized(KIND)
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    # «Установить итог», а не «прибавить»: после retry_failed хук вызовется снова.
    await session.execute(
        update(reports)
        .where(reports.c.batch_id == summary.id, reports.c.status.in_(ACTIVE))
        .values(
            status=FINAL[summary.state],
            done=summary.progress.done,
            failed=summary.progress.error,
            progress=1.0,
            progress_seq=summary.seq,
        )
    )


@th.on_progress(KIND, every=timedelta(seconds=2))
async def save_progress(session: AsyncSession, summary: BatchSummary) -> None:
    await session.execute(
        update(reports)
        .where(reports.c.batch_id == summary.id, reports.c.progress_seq < summary.seq)  # монотонность
        .values(
            done=summary.progress.done,
            failed=summary.progress.error,
            progress=func.greatest(reports.c.progress, summary.progress.ratio or 0.0),  # не едет назад
            progress_seq=summary.seq,
        )
    )


@th.on_policy_breach(KIND)
async def auto_pause(session: AsyncSession, summary: BatchSummary, breach: PolicyBreach) -> None:
    await session.execute(
        update(reports)
        .where(reports.c.batch_id == summary.id, reports.c.status == "running")
        .values(status="paused", pause_reason=f"доля ошибок {breach.ratio:.0%}")
    )


# app/api.py
async def start_report(session: AsyncSession, report_id: int, sections: list[int]) -> None:
    policy = th.FailurePolicy.threshold(ratio=0.3, min_processed=6, action="pause")
    async with th.batch(KIND, key=f"report:{report_id}", failure_policy=policy, session=session) as batch:
        await batch.map(build_section, sections)
    await session.execute(
        insert(reports).values(id=report_id, batch_id=batch.handle.id, status="running")
    )
    # commit делает вызывающий: строка отчёта и батч появятся вместе


async def cancel_report(session: AsyncSession, report_id: int) -> None:
    report = await session.get(Report, report_id, with_for_update=True)  # сначала своя строка
    await th.handle(report.batch_id).cancel(session=session)  # потом tallyho, в той же транзакции


# строка reports по ходу отчёта из восьми разделов, два из которых не собрались:
# после трёх разделов       status="running",   done=3, progress=0.375
# политика сработала        status="paused",    pause_reason="доля ошибок 33%"
# оператор отменил отчёт    status="cancelled", done=8, failed=2    записал on_finalized
```

## Правила транзакции хука

При финализации tallyho открывает транзакцию, читает итоговые счётчики, вызывает ваш хук
и только потом переводит батч в терминальное состояние. Затем в той же транзакции ставятся
колбэк-задачи, и всё коммитится.

* Сессия хука - `AsyncSession` на соединении и в транзакции tallyho. Вызывать `commit()` и
  `rollback()` внутри хука нельзя: будет `HookTransactionError`, и финализация откатится.
* Соединение хука взято из движка, переданного в `Tallyho(engine, ...)`. Ваши таблицы хук
  находит так же, как остальной код на этом движке: по `search_path` или по
  `schema_translate_map` движка. Подробнее - [«Схема в ваших сессиях»](installation.md#схема-в-ваших-сессиях).
* Только работа с базой. HTTP-запросы, письма, обращения к брокеру из хука не делайте: они не
  откатятся вместе с транзакцией. Для них есть [колбэк-задачи](hooks/callbacks.md).
* Хук должен быть идемпотентным по смыслу. Пишите «установить итог», а не «прибавить к итогу».
  Два процесса могут начать финализацию одновременно: оба выполнят хук, но закоммитится ровно один,
  а изменения второго откатятся. Если `cancel()`, дедлайн или политика ошибок сработали, пока хук
  выполнялся, его изменения тоже откатятся, и хук будет вызван ещё раз - с тем итогом, который
  запишется (`summary.state`, `summary.reason`). После `retry_failed()` хук вызывается заново с
  новым итогом.
* Хук должен уложиться в `hook_timeout` (10 секунд по умолчанию). Ограничение действует и на
  время выполнения Python-кода, и на SQL-запросы внутри хука.
* Порядок блокировок - «сначала ваша строка, потом tallyho». Хук блокирует ваши строки до того,
  как tallyho меняет свою. Соблюдайте тот же порядок в API: сначала `SELECT … FOR UPDATE` своей
  строки, затем `handle.pause(session=...)`. Тогда хук и API-операция не образуют дедлок.
* Защищайте переход условием. `WHERE status IN (...)` в `on_finalized` не даст перезаписать
  статус, который уже изменил оператор. Батч может финализироваться и во время паузы,
  если на момент паузы оставались только выполняющиеся задачи: хук должен уметь закрыть сущность
  из статуса `paused`.

### `on_progress`

* Снимки делает процесс [maintenance](operations.md) - тот его экземпляр, который сейчас
  лидер. Без работающего maintenance `on_progress` не вызывается.
* `every` - минимальный интервал между снимками одного батча. Если счётчики не изменились, хук не
  вызывается и в базу ничего не пишется.
* Хук получает сводку корня со всеми под-батчами.
* Снимок, опоздавший к финализации, откатывается вместе с вашими изменениями и итог не
  перезаписывает. Дополнительная защита на вашей стороне - условие
  `WHERE progress_seq < :seq`, как в примере.
* `summary.progress.ratio` может немного уменьшиться, когда растёт оценка объёма. Храните максимум:
  `progress = GREATEST(progress, :ratio)`.
* Упавший `on_progress` финализацию не блокирует: снимок откатывается, и следующий будет сделан по
  расписанию.

### `on_policy_breach`

* Вызывается для политик `threshold` и `fail_fast` - в той же транзакции, что и постановка дерева
  на паузу (`action="pause"`) или запрос отмены (`action="fail"`).
* Третий аргумент - `PolicyBreach` (`tallyho.model.policy`): `batch_key` - ключ батча, где сработала
  политика; `labels` - метки фильтра политики (пустой список - считались все ошибки); `ratio` -
  фактическая доля; `action` - `"fail"` или `"pause"`.
* Хук ищется по `kind` батча, где сработала политика. Если там его нет, вызывается хук `kind` корня,
  а `breach.batch_key` говорит, где именно случилось.
* После `resume()` политика проверяется заново. Если доля ошибок всё ещё выше порога, батч снова
  встанет на паузу после следующей завершённой задачи: устраните причину или отмените батч.

```{toctree}
:hidden:

hooks/retry
hooks/callbacks
hooks/complete-in
hooks/retention
hooks/recipe
```
