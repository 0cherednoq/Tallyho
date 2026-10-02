# Хуки

[← Оглавление](README.md) · назад: [Батчи и конвейеры](batches.md) · далее: [Тестирование](testing.md)

## Зачем нужны хуки

Прогресс и итог батча обычно нужны в **вашей** таблице: `campaigns.sent`, `campaigns.status`.
Причин три:

1. Таблицы tallyho очищаются по retention, а итог кампании нужен навсегда.
2. Списки, сортировки и фильтры по вашей таблице не должны зависеть от служебных таблиц.
3. Доменный статус должен меняться ровно тогда, когда батч действительно завершился: не раньше и
   не позже.

Привычные решения этого не дают. Задача-колбэк в брокере — это второй коммит: батч уже завершён, а
колбэк ещё в очереди или упал. `UPDATE campaigns SET sent = sent + 1` в каждой задаче создаёт
горячую строку в вашей таблице. Опрос `view()` теряет данные, если поллер отстал дольше retention.

**Транзакционный хук (tx-хук)** — ваша функция, которую tallyho выполняет **внутри своей
транзакции** на событии батча. Ваши изменения и изменение состояния батча коммитятся вместе или
вместе откатываются.

| Хук | Когда вызывается | Гарантия |
|---|---|---|
| `on_finalized(session, summary)` | батч переходит в терминальное состояние | ровно один успешный коммит, атомарно с финализацией. Хук упал — финализации нет, будет повтор |
| `on_progress(session, summary)` | не чаще `every` на батч и только если счётчики изменились | снимки монотонны по `summary.seq`; опоздавший снимок не перезапишет итог |
| `on_policy_breach(session, summary, breach)` | сработала [политика ошибок](batches.md#политики-ошибок) | атомарно с постановкой на паузу или провалом |

## Регистрация

Хуки регистрируются декораторами клиента на `kind` батча. Один хук каждого вида на `kind`; повторная
регистрация — `ConfigurationError`.

<!-- tallyho-noexec: раскладка по модулям вашего приложения; полный рабочий пример — ниже -->
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

**Хуки должны быть зарегистрированы в каждом процессе, который может финализировать батч**: в
воркерах (там завершается последняя задача), в процессе maintenance (он подбирает пропущенное и
делает снимки прогресса) и в API-процессе (он финализирует, например, пустые батчи). Перечислите
модули с хуками в `hook_modules` — клиент импортирует их при создании. Для `tallyho maintenance`
это флаг `--hook-module`.

Защита от забытого импорта: при создании батча запоминается, какие хуки зарегистрированы для его
`kind`. Процесс, в котором нужного хука нет, батч **не финализирует**: пишет ошибку в лог, сообщает
событие `hook_missing` наблюдателю и оставляет батч процессу, где хук есть. Тихо пропустить запись
итога невозможно.

Хук регистрируется на `kind` корня и получает сводку всего дерева. Под-батчи по умолчанию имеют
`kind` вида `<kind корня>.<key>`, поэтому хук корня не вызывается на каждом этапе; собственный хук
этапа регистрируйте на его `kind`.

## Сводка `BatchSummary`

| Поле | Значение |
|---|---|
| `id`, `kind`, `key` | идентификация батча |
| `state` | состояние: в `on_finalized` — терминальное, в остальных хуках — текущее |
| `progress` | счётчики и оценки, те же поля, что у [`view().progress`](batches.md#чтение-прогресса) |
| `labels`, `metrics` | счётчики батча по именам: число задач по меткам итога и суммы `item.incr` ([общее пространство имён](batches.md#задача-и-её-итог)) |
| `children` | сводки под-батчей по ключу: `summary.children["send"]` |
| `seq` | монотонный номер снимка в пределах батча; у финализации он больше любого снимка прогресса |
| `reason` | причина запроса отмены: `cancel`, `deadline`, `fail_fast`, `policy` |
| `finished_at` | время финализации |
| `attributes` | [атрибуты](batches.md#атрибуты-memo-и-листинг) корня |

Сводка неизменяема. В `on_finalized` числа окончательные и точные.

## Пример: статус, прогресс и авто-пауза

<!-- tallyho-example: guide-hooks-domain -->
```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, Uuid, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.policy import PolicyBreach
from tallyho.model.states import BatchState
from tallyho.model.views import BatchSummary
from tallyho.testing import FakeClock, InlineBroker

# Движок приложения: имена таблиц без схемы — и ваших, и tallyho — ведут в схему приложения.
app_engine = engine.execution_options(schema_translate_map={None: schema})

# Доменная таблица приложения — в той же базе, что и таблицы tallyho.
reports = Table(
    "reports",
    MetaData(),
    Column("id", Integer, primary_key=True),
    Column("batch_id", Uuid),
    Column("status", String, nullable=False),
    Column("pause_reason", String),
    Column("done", Integer, nullable=False, default=0),
    Column("failed", Integer, nullable=False, default=0),
    Column("progress", Float, nullable=False, default=0.0),
    Column("progress_seq", Integer, nullable=False, default=0),
)
async with app_engine.begin() as connection:
    await connection.run_sync(reports.metadata.create_all)

clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
broker = InlineBroker()
th = Tallyho(app_engine, schema=schema, clock=clock)
th.install(broker.adapter)
await th.migrate()

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


async def build_section(section: int) -> None:
    if section >= 4:
        item.error("render_failed")


async def report() -> dict[str, object]:
    async with app_engine.connect() as connection:
        row = (await connection.execute(select(reports).where(reports.c.id == 1))).mappings().one()
    return dict(row)


try:
    # Доменная запись и батч создаются одной транзакцией приложения.
    policy = th.FailurePolicy.threshold(ratio=0.3, min_processed=6, action="pause")
    async with AsyncSession(app_engine) as session, session.begin():
        async with th.batch(KIND, key="report:1", failure_policy=policy, session=session) as batch:
            await batch.map(build_section, range(8))
        await session.execute(
            insert(reports).values(id=1, batch_id=batch.handle.id, status="running")
        )
    handle = batch.handle

    # Снимок прогресса: три задачи выполнены, прошло больше `every`.
    await broker.step(3)
    clock.advance(seconds=2)
    await th.run_maintenance_once()  # в продакшне это делает процесс maintenance
    snapshot = await report()
    assert (snapshot["status"], snapshot["done"]) == ("running", 3)

    # Политика ошибок ставит дерево на паузу и в той же транзакции вызывает on_policy_breach.
    await broker.drain()
    paused = await report()
    assert paused["status"] == "paused"
    assert paused["pause_reason"] == "доля ошибок 33%"
    assert (await handle.view()).paused

    # Оператор отменяет отчёт: сначала своя строка, затем tallyho — в одной транзакции.
    async with AsyncSession(app_engine) as session, session.begin():
        await session.execute(select(reports).where(reports.c.id == 1).with_for_update())
        await handle.cancel(session=session)
    assert (await handle.wait(timeout=30)).state is BatchState.CANCELLED

    final = await report()
    assert final["status"] == "cancelled"  # поставил on_finalized, атомарно с финализацией
    assert (final["done"], final["failed"]) == (8, 2)
finally:
    await broker.close()
```

## Правила транзакции хука

Финализация устроена так: tallyho открывает транзакцию, читает итоговые счётчики, вызывает ваш хук
и только потом переводит батч в терминальное состояние. Затем в той же транзакции ставятся
колбэк-задачи, и всё коммитится.

* **Сессия хука — `AsyncSession` на соединении и в транзакции tallyho.** Вызывать `commit()` и
  `rollback()` внутри хука нельзя: будет `HookTransactionError`, и финализация откатится.
* **Только работа с базой.** HTTP-запросы, письма, обращения к брокеру из хука не делайте: они не
  откатятся вместе с транзакцией. Для них есть [колбэк-задачи](#колбэк-задачи).
* **Хук должен быть идемпотентным по смыслу.** Пишите «установить итог», а не «прибавить к итогу».
  Два процесса могут начать финализацию одновременно: оба выполнят хук, но закоммитится ровно один,
  а изменения второго откатятся. После `retry_failed()` хук вызывается заново с новым итогом.
* **Хук должен уложиться в `hook_timeout`** (10 секунд по умолчанию). Ограничение действует и на
  время выполнения Python-кода, и на SQL-запросы внутри хука.
* **Порядок блокировок — «сначала ваша строка, потом tallyho».** Хук блокирует ваши строки до того,
  как tallyho меняет свою. Соблюдайте тот же порядок в API: сначала `SELECT … FOR UPDATE` своей
  строки, затем `handle.pause(session=...)`. Тогда хук и API-операция не образуют дедлок.
* **Защищайте переход условием.** `WHERE status IN (...)` в `on_finalized` не даст перезаписать
  статус, который уже изменил оператор. Учтите, что батч может финализироваться и во время паузы,
  если на момент паузы оставались только выполняющиеся задачи: хук должен уметь закрыть сущность
  из статуса `paused`.

### `on_progress`

* Снимки делает процесс [maintenance](operations.md#процессы) — тот его экземпляр, который сейчас
  лидер. Без работающего maintenance `on_progress` не вызывается.
* `every` — минимальный интервал между снимками одного батча. Если счётчики не изменились, хук не
  вызывается и в базу ничего не пишется.
* Хук получает сводку корня со всеми под-батчами.
* Снимок, опоздавший к финализации, откатывается вместе с вашими изменениями и итог не
  перезаписывает. Дополнительная защита на вашей стороне — условие
  `WHERE progress_seq < :seq`, как в примере.
* `summary.progress.ratio` может немного уменьшиться, когда растёт оценка объёма. Храните максимум:
  `progress = GREATEST(progress, :ratio)`.
* Упавший `on_progress` финализацию не блокирует: снимок откатывается, и следующий будет сделан по
  расписанию.

### `on_policy_breach`

* Вызывается для политик `threshold` и `fail_fast` — в той же транзакции, что и постановка дерева
  на паузу (`action="pause"`) или запрос отмены (`action="fail"`).
* Третий аргумент — `PolicyBreach` (`tallyho.model.policy`): `batch_key` — ключ батча, где сработала
  политика; `labels` — метки фильтра политики (пустой список — считались все ошибки); `ratio` —
  фактическая доля; `action` — `"fail"` или `"pause"`.
* Хук ищется по `kind` батча, где сработала политика. Если там его нет, вызывается хук `kind` корня,
  а `breach.batch_key` говорит, где именно случилось.
* После `resume()` политика проверяется заново. Если доля ошибок всё ещё выше порога, батч снова
  встанет на паузу после следующей завершённой задачи: устраните причину или отмените батч.

## Повтор упавшего хука

Если `on_finalized` бросил исключение или не уложился в таймаут:

1. вся транзакция финализации откатывается — и ваши изменения, и переход батча;
2. батч остаётся `SEALED`; число попыток и текст ошибки видны в `view.hook_attempts` и
   `view.hook_error`;
3. наблюдатель получает событие `hook_failed` (метрика `th_hook_failures`);
4. фоновые проверки повторяют финализацию с растущей паузой — от 1 секунды до 5 минут
   (`hook_backoff_max`).

Батч **не станет терминальным без успешного хука**: ваш статус и состояние батча не расходятся.
После исправления кода ничего делать не нужно — очередной повтор пройдёт. Чтобы не ждать,
вызовите `await handle.retry_finalize()`.

<!-- tallyho-example: guide-hooks-retry -->
```python
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.model.views import BatchSummary
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()

saved: list[BatchState] = []
bug = {"present": True}


@th.on_finalized("invoices")
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    if bug["present"]:
        raise LookupError("нет строки счёта")
    saved.append(summary.state)


async def issue_invoice(number: int) -> None: ...


try:
    async with th.batch("invoices", key="month:10") as batch:
        await batch.map(issue_invoice, range(3))
    await broker.drain()

    stuck = await batch.handle.view()
    assert stuck.state is BatchState.SEALED  # все задачи выполнены, но батч не финализирован
    assert stuck.progress.ok == 3
    assert stuck.hook_attempts >= 1
    assert "нет строки счёта" in (stuck.hook_error or "")
    assert saved == []

    bug["present"] = False  # код исправлен
    await batch.handle.retry_finalize()  # не ждать очередного повтора

    final = await batch.handle.wait(timeout=30)
    assert final.state is BatchState.SUCCEEDED
    assert saved == [BatchState.SUCCEEDED]  # хук закоммичен ровно один раз
finally:
    await broker.close()
```

## Колбэк-задачи

Всё, что нельзя делать в транзакции хука (уведомления, HTTP, долгий экспорт), выносится в
**колбэк-задачу** — обычную задачу брокера, которая ставится в очередь той же транзакцией, что и
финализация.

| Параметр `th.batch` / `sub_batch` | Когда ставится |
|---|---|
| `on_succeeded=th.call(...)` | батч завершился как `SUCCEEDED` |
| `on_completed_with_errors=th.call(...)` | `COMPLETED_WITH_ERRORS` |
| `on_failed=th.call(...)` | `FAILED` |
| `on_cancelled=th.call(...)` | `CANCELLED` |
| `on_finalized_task=th.call(...)` | любое терминальное состояние |

* Постановка — ровно один раз на финализацию, выполнение — at-least-once: колбэк может быть
  доставлен повторно, поэтому должен быть идемпотентным.
* Внутри колбэка `callback.current()` (`from tallyho import callback`) возвращает контекст с
  `callback_id` и `batch_id`. `callback_id` стабилен при повторной доставке — используйте его как
  ключ идемпотентности.
* Колбэк выполняется после коммита финализации и хука `on_finalized`: итог в вашей таблице уже есть.
* После `retry_failed()` и новой финализации колбэк ставится заново.

## Итог задачи в вашей транзакции

Если задача пишет в вашу таблицу и её запись должна быть атомарна с итогом задачи, завершите задачу
в своей транзакции:

<!-- tallyho-noexec: фрагмент задачи; engine и таблица deliveries принадлежат вашему приложению (исполняемая версия — в рецепте ниже) -->
```python
async def send_email(campaign_id: int, email: str) -> None:
    message_id = await mail_provider.send(to=email)
    async with engine.begin() as connection:
        await connection.execute(
            update(deliveries)
            .where(deliveries.c.campaign_id == campaign_id, deliveries.c.email == email)
            .values(status="sent", message_id=message_id)
        )
        item.ok("sent")
        await item.complete_in(connection)  # итог задачи — в этом же коммите
```

`item.complete_in(session)` принимает `AsyncSession` или `AsyncConnection`. Сначала задайте итог
(`ok/skip/error`) и все `spawn`, затем вызовите `complete_in`: он записывает то, что накоплено к
этому моменту. Если ваша транзакция откатилась, задача остаётся незавершённой и будет повторена
брокером.

## Retention и `release()`

Завершённые деревья удаляются фоновыми проверками.

| Настройка корня | Поведение |
|---|---|
| `retention=timedelta(days=14)` (по умолчанию) | дерево удаляется через 14 дней после завершения корня |
| `retention=None` | хранить вечно |
| `release_required=True` | удалять только после `handle.release()` **и** истечения `retention` |

* `release()` вызывается у корня и только после его завершения; раньше — `InvalidStateError`.
* `release()` относится к **последней** финализации. `retry_failed()` на любом узле дерева
  сбрасывает выданное разрешение: после новой финализации итоги задач другие, и их нужно забрать
  заново и снова вызвать `release()`.
* После удаления `handle.view()` и `handle.items()` бросают `BatchPurged`. Всё, что нужно навсегда,
  к этому моменту должно быть в ваших таблицах — для этого и существуют хуки.
* Ваши таблицы retention не затрагивает.

## Рецепт «строка на каждого получателя»

Задача: приложение ведёт строку на каждого получателя рассылки, и в неё должны попасть **все**
исходы — в том числе те, при которых код задачи не выполнялся или упал: исчерпанные ретраи
(`exhausted`), истёкшая аренда (`lease_expired`), истёкший срок (`expired`), отмена. Отдельного хука
на исход каждой задачи в v1 нет. Рецепт собирается из существующих механизмов в два шага:

1. **нормальный исход задача пишет сама** — своей строкой и итогом в одном коммите
   (`item.complete_in`);
2. **остальные исходы переносит колбэк финализации** — он читает `handle.items(states=...)`,
   обновляет строки и в той же транзакции вызывает `release()`.

<!-- tallyho-example: guide-hooks-release -->
```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import Column, MetaData, String, Table, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.errors import BatchPurged
from tallyho.model.states import ItemState
from tallyho.model.views import BatchSummary
from tallyho.testing import FakeClock, InlineBroker

# Движок приложения: имена таблиц без схемы — и ваших, и tallyho — ведут в схему приложения.
app_engine = engine.execution_options(schema_translate_map={None: schema})

metadata = MetaData()
issues = Table("issues", metadata, Column("key", String, primary_key=True), Column("status", String))
deliveries = Table(
    "deliveries",
    metadata,
    Column("email", String, primary_key=True),
    Column("status", String, nullable=False),
    Column("reason", String),
)
async with app_engine.begin() as connection:
    await connection.run_sync(metadata.create_all)

clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
broker = InlineBroker()
th = Tallyho(app_engine, schema=schema, clock=clock)
th.install(broker.adapter)
await th.migrate()

KIND = "issue_deliveries"
EXPORTED = {ItemState.ERROR: "failed", ItemState.CANCELLED: "cancelled"}


async def send(email: str) -> None:
    if email.endswith("@down.test"):
        raise ConnectionError("451 try again later")  # ретраи брокера, затем error("exhausted")
    async with app_engine.begin() as connection:  # строка получателя и итог задачи — один коммит
        await connection.execute(
            update(deliveries).where(deliveries.c.email == email).values(status="sent")
        )
        item.ok("sent")
        await item.complete_in(connection)


@th.on_finalized(KIND)
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    # Счётчики точные уже здесь; терминальный статус поставит колбэк после экспорта.
    await session.execute(update(issues).where(issues.c.key == summary.key).values(status="settling"))


async def settle(key: str) -> None:
    async with AsyncSession(app_engine) as session, session.begin():
        status = await session.scalar(select(issues.c.status).where(issues.c.key == key).with_for_update())
        if status != "settling":
            return  # повторная доставка колбэка
        handle = await th.find(KIND, key)
        async for entry in handle.items(states=set(EXPORTED)):
            await session.execute(
                update(deliveries)
                .where(deliveries.c.email == entry.key)
                .values(status=EXPORTED[entry.state], reason=entry.label)
            )
        await session.execute(  # получатели, которые так и не стали задачами
            update(deliveries)
            .where(deliveries.c.status == "pending")
            .values(status="cancelled", reason="not_dispatched")
        )
        await session.execute(update(issues).where(issues.c.key == key).values(status="done"))
        await handle.release(session=session)  # разрешение на удаление — в той же транзакции


async def statuses() -> dict[str, tuple[str, str | None]]:
    async with app_engine.connect() as connection:
        rows = await connection.execute(select(deliveries))
        return {row.email: (row.status, row.reason) for row in rows}


try:
    emails = ["ada@ok.test", "grace@ok.test", "later@down.test"]
    async with AsyncSession(app_engine) as session, session.begin():
        await session.execute(insert(issues).values(key="issue:1", status="running"))
        await session.execute(
            insert(deliveries), [{"email": email, "status": "pending"} for email in emails]
        )
        async with th.batch(
            KIND,
            key="issue:1",
            retention=timedelta(days=1),
            release_required=True,  # дерево ждёт экспорта
            on_finalized_task=th.call(settle, "issue:1"),
            session=session,
        ) as batch:
            await batch.add_calls(th.call(send, email).opts(key=email) for email in emails)

    await broker.drain()  # задачи → финализация → on_finalized → колбэк settle

    assert await statuses() == {
        "ada@ok.test": ("sent", None),
        "grace@ok.test": ("sent", None),
        "later@down.test": ("failed", "exhausted"),  # записал колбэк, а не задача
    }
    async with app_engine.connect() as connection:
        assert await connection.scalar(select(issues.c.status)) == "done"

    # Дерево освобождено и старше retention: фоновые проверки его удаляют.
    clock.advance(days=2)
    await th.run_maintenance_once()
    try:
        await batch.handle.view()
    except BatchPurged:
        purged = True
    else:
        purged = False
    assert purged
    assert (await statuses())["later@down.test"] == ("failed", "exhausted")  # ваши данные на месте
finally:
    await broker.close()
```

Условия, без которых рецепт некорректен:

* **Нормальный путь пишет строку в самой задаче**, через `complete_in`. Экспорт читает только
  `ERROR` и `CANCELLED`. Задача, которая после `retry_failed()` завершилась успешно, исправит свою
  строку сама, тем же кодом.
* **Последний шаг экспорта — запрос по остатку.** Получатели, которые так и не стали задачами
  (отмена посреди разворачивания аудитории, дубли по ключу, `skipped_by_limit`), в `items()` не
  появятся: их строки закрывает один `UPDATE … WHERE status = 'pending'`.
* **Счётчики ставит `on_finalized`**, а не колбэк: сводка уже содержит точные числа, и они атомарны
  с финализацией.
* **Терминальный доменный статус ставит колбэк.** Между финализацией и экспортом сущность находится
  в промежуточном статусе (`settling`), поэтому она не бывает «завершена, а строки ещё не
  обновлены».
* **Колбэк идемпотентен.** Экспорт, итоговый статус и `release()` — одна транзакция. Падение
  посередине оставляет `settling` и неосвобождённое дерево; повторная доставка безопасна. Если одна
  транзакция слишком велика, коммитьте экспорт чанками, а `release()` вызывайте в транзакции
  последнего.
* **`retry_failed()` повторяет цикл**: разрешение на удаление сбрасывается, `on_finalized` снова
  ставит `settling` и новый итог, колбэк экспортирует оставшиеся ошибки и снова вызывает
  `release()`.
* **Задачи этапа читайте у этапа.** В конвейере задачи лежат в под-батче, а не в корне:
  `send = await root.child("send")`, затем `send.items(...)`. `release()` при этом вызывается у корня.

Чего рецепт не даёт: исходы, возникшие без участия кода задачи, видны в ваших таблицах только после
финализации батча, а не по мере появления.

## Что дальше

* Проверить хуки в тестах — [Тестирование](testing.md).
* Где должны работать хуки и снимки прогресса — [Эксплуатация PostgreSQL](operations.md#процессы).
