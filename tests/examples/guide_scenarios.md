# Проверяемые сценарии руководства

Страницы сайта документации показывают код приложения без проверок. Здесь лежат сценарии,
которые подтверждают поведение, описанное на этих страницах: каждый блок выполняется в CI на
PostgreSQL со встроенным `InlineBroker` (`tests/examples/test_documentation.py`). В блоке уже
определены `engine` (`AsyncEngine`) и `schema` (имя пустой схемы).

Меняете поведение или пример на странице - поправьте и сценарий.

## readme-quickstart

Страница: `README.md`.

<!-- tallyho-example: readme-quickstart -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне - адаптер вашего брокера
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
seen: list[str] = []


async def greet(name: str) -> None:
    seen.append(name)
    item.ok("greeted")  # итог задачи с меткой


await th.migrate()  # создать таблицы
try:
    async with th.batch("readme.quickstart", key="demo") as batch:
        await batch.map(greet, ["Ada", "Grace"])  # по задаче на элемент

    assert await broker.drain(concurrency=2) == 2  # выполнить задачи
    view = await batch.handle.view()
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == 2
    assert dict(view.labels) == {"greeted": 2}
    assert sorted(seen) == ["Ada", "Grace"]
finally:
    await th.aclose()
```

## guide-batches-listing

Страница: `docs/guide/batches/attributes.md`.

<!-- tallyho-example: guide-batches-listing -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState, ItemState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def send(address: str) -> None:
    if address.endswith("@bounce.test"):
        item.error("hard_bounce", detail="550 user unknown")
    else:
        item.ok("sent", result={"message_id": f"msg:{address}"})


try:
    recipients = ["ada@ok.test", "grace@ok.test", "gone@bounce.test"]
    async with th.batch(
        "newsletter",
        key="issue:42",
        attributes={"tenant": "acme", "issue": 42},
        memo={"requested_by": "ops@example.test"},
    ) as batch:
        await batch.add_calls(th.call(send, address).opts(key=address) for address in recipients)
    await broker.drain()

    # Листинг корневых батчей: от новых к старым, постранично, без чтения счётчиков.
    page = await th.list_batches(
        kinds=["newsletter"],
        states=[BatchState.COMPLETED_WITH_ERRORS],
        attributes={"tenant": "acme"},
        limit=50,
    )
    assert [(info.key, info.state) for info in page.items] == [
        ("issue:42", BatchState.COMPLETED_WITH_ERRORS)
    ]
    assert page.next_cursor is None  # страниц больше нет

    view = await th.handle(page.items[0].id).view()  # за прогрессом - view()
    assert dict(view.attributes) == {"tenant": "acme", "issue": 42}
    assert view.memo == {"requested_by": "ops@example.test"}

    # Задачи батча по меткам: только помеченные (по умолчанию - ошибки).
    handle = await th.find("newsletter", "issue:42")
    bounced = [entry async for entry in handle.items(labels=["hard_bounce"])]
    assert [(entry.key, entry.label, entry.error) for entry in bounced] == [
        ("gone@bounce.test", "hard_bounce", "550 user unknown")
    ]

    # Задачи батча по состояниям: любые, включая успешные и отменённые.
    delivered = sorted([entry.key async for entry in handle.items(states={ItemState.OK})])
    assert delivered == ["ada@ok.test", "grace@ok.test"]
finally:
    await th.aclose()
```

## guide-batches-policy

Страница: `docs/guide/batches/failure-policies.md`.

<!-- tallyho-example: guide-batches-policy -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState, CancelReason
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def charge(order_id: int) -> None:
    if order_id % 2:
        item.error("declined")


try:
    policy = th.FailurePolicy.threshold(ratio=0.2, min_processed=4, labels=["declined"])
    async with th.batch("billing", key="run:1", failure_policy=policy) as batch:
        await batch.map(charge, range(10))

    await broker.drain()

    view = await batch.handle.view()
    assert view.state is BatchState.FAILED
    assert view.reason is CancelReason.POLICY  # почему батч провален
    assert view.progress.error >= 2
    assert view.progress.cancelled >= 1  # оставшиеся задачи не выполнялись
    assert view.progress.done == view.progress.found == 10
finally:
    await th.aclose()
```

## guide-batches-operations

Страница: `docs/guide/batches/operations.md`.

<!-- tallyho-example: guide-batches-operations -->
```python
from datetime import UTC, datetime, timedelta

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))  # управляемое время для примера
broker = InlineBroker()
th = Tallyho(engine, schema=schema, clock=clock, relay_grace=timedelta(0))
th.install(broker.adapter)
await th.migrate()

delivered: list[int] = []
glitches = {3}  # посылка 3 в первый раз не доставится


async def deliver(parcel: int) -> None:
    if parcel in glitches:
        glitches.discard(parcel)
        item.error("courier_unavailable")
        return
    delivered.append(parcel)


try:
    # Отложенный старт и перенос.
    start = clock.now() + timedelta(hours=1)
    async with th.batch("deliveries", key="route:1", start_at=start) as batch:
        await batch.map(deliver, range(6))
    handle = batch.handle
    assert await broker.drain() == 0  # до start_at в брокер ничего не уходит
    assert await handle.reschedule(start + timedelta(hours=1)) == 0
    clock.advance(hours=1)
    assert await broker.drain() == 0  # старт перенесён
    clock.advance(hours=1)
    await th.run_maintenance_once()  # срок наступил: фоновый проход отправил задачи в брокер

    # Пауза и продолжение.
    assert await broker.step(2) == 2  # выполнить две задачи
    await handle.pause()
    await broker.drain()  # сообщения из брокера откладываются, задачи не выполняются
    paused = await handle.view()
    assert paused.paused
    assert paused.progress.done == 2
    await handle.resume()
    await broker.drain()
    assert (await handle.view()).state is BatchState.COMPLETED_WITH_ERRORS

    # Повтор упавших.
    assert await handle.retry_failed(labels=["courier_unavailable"]) == 1
    await broker.drain()
    assert (await handle.view()).state is BatchState.SUCCEEDED
    assert sorted(delivered) == [0, 1, 2, 3, 4, 5]

    # Отмена посреди работы.
    async with th.batch("deliveries", key="route:2") as other:
        await other.map(deliver, range(10, 14))
    assert await broker.step(1) == 1  # одна посылка успела уехать
    await other.handle.cancel()
    await broker.drain()  # уже отправленные задачи отменяются, когда их получает воркер
    cancelled = await other.handle.view()
    assert cancelled.state is BatchState.CANCELLED
    assert (cancelled.progress.ok, cancelled.progress.cancelled) == (1, 3)
finally:
    await th.aclose()
```

## guide-batches-pipeline

Страница: `docs/guide/batches/pipelines.md`.

<!-- tallyho-example: guide-batches-pipeline -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()

CATALOG = {1: ["a", "b"], 2: ["b", "c"], 3: ["d"]}  # страница и карточки на ней


async def parse_page(page: int) -> None:
    if page == 1:  # число страниц становится известно после первой
        item.expect(len(CATALOG))
        for other in range(2, len(CATALOG) + 1):
            item.spawn(parse_page, other)  # в свой этап
    for card in CATALOG[page]:
        item.spawn(parse_card, card, into="cards", key=card)  # в следующий этап, с дедупликацией


async def parse_card(card: str) -> None:
    item.ok("parsed")


try:
    async with th.batch("catalog_parse", key="catalog:7", max_items=10_000) as root:
        pages = root.sub_batch("pages", max_depth=1)
        root.sub_batch("cards", fed_by=[pages], max_in_flight=2)
        await pages.add(parse_page, 1)
    # при выходе закрыты корень и pages; cards закроется сам, когда закончится pages

    await broker.drain()

    view = await root.handle.view()
    assert view.state is BatchState.SUCCEEDED
    # У корня каждый этап - одна задача; работа этапов - в children.
    assert (view.progress.found, view.progress.ok, view.progress.ratio) == (2, 2, 1.0)
    assert view.children["pages"].progress.found == 3
    cards = view.children["cards"].progress
    assert (cards.found, cards.ok, cards.duplicates) == (4, 4, 1)  # карточка "b" встретилась дважды
finally:
    await th.aclose()
```

## guide-batches-basics

Страница: `docs/guide/batches.md`.

<!-- tallyho-example: guide-batches-basics -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне - адаптер вашего брокера
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def resize(image_id: int) -> None:
    if image_id % 5 == 0:
        item.skip("already_resized")  # задача не понадобилась
        return
    if image_id == 7:
        item.error("corrupt_file", detail={"image_id": image_id})  # ошибка без исключения
        return
    item.incr("bytes_saved", 1024)  # своя метрика батча
    item.ok("resized")


try:
    async with th.batch("thumbnails", key="album:1") as batch:
        await batch.add(resize, 1)  # один вызов
        await batch.map(resize, range(2, 9))  # по вызову на каждый элемент
        await batch.add_calls([th.call(resize, 9).opts(weight=3), th.call(resize, 10)])
    # выход из блока: батч закрыт (seal), транзакция закоммичена, задачи ждут отправки в брокер

    await broker.drain()  # в тестах задачи выполняет InlineBroker

    view = await batch.handle.view()
    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    progress = view.progress
    assert (progress.found, progress.ok, progress.skip, progress.error) == (10, 7, 2, 1)
    labels = view.labels  # счётчики по меткам итога
    assert (labels["resized"], labels["already_resized"], labels["corrupt_file"]) == (7, 2, 1)
    assert view.metrics["bytes_saved"] == 7 * 1024  # сумма item.incr
    assert "bytes_saved" not in labels  # метрики в разбивку по меткам не попадают
    # Корневой батч всегда можно найти по (kind, key).
    again = await th.find("thumbnails", "album:1")
    assert again.id == batch.handle.id
finally:
    await th.aclose()
```

## guide-batches-streaming

Страница: `docs/guide/batches.md`.

<!-- tallyho-example: guide-batches-streaming -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()

SOURCE = [[1, 2, 3], [4, 5], [6]]  # порции, которые продюсер читает по очереди


async def import_row(row: int) -> None:
    item.ok("imported")


try:
    *chunks, last = SOURCE
    for chunk in chunks:
        async with th.batch("import", key="file:42", seal=False) as batch:  # своя транзакция
            await batch.map(import_row, chunk)
        await broker.drain()  # воркеры не ждут конца чтения

    view = await batch.handle.view()
    assert view.state is BatchState.OPEN  # всё добавленное сделано, но батч открыт
    assert (view.progress.found, view.progress.ok) == (5, 5)

    async with th.batch("import", key="file:42") as batch:  # последняя порция и seal
        await batch.map(import_row, last)
    await broker.drain()

    view = await batch.handle.view()
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.found == 6
finally:
    await th.aclose()
```

## guide-flexiq-call-options

Страница: `docs/guide/flexiq.md`.

<!-- tallyho-example: guide-flexiq-call-options -->
```python
import tempfile
from pathlib import Path

from flexiq import Queue

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter
from tallyho.model.errors import ConfigurationError, UnsupportedOption

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
    # Для примера хватает файловой очереди; в продакшне - Queue(backend="postgres", ...).
    queue = Queue(db_path=str(Path(directory) / "flexiq.db"))
    fq = FlexiqAdapter(queue)
    th = Tallyho(engine, schema=schema)
    th.install(fq)
    await th.migrate()

    @fq.task(max_retries=4, queue="mail")
    async def send_email(contact_id: int) -> None:
        item.ok("sent")

    rejected: list[str] = []
    for options in ({"depends_on": ["job-1"]}, {"debounce": 5}, {"no_such_option": True}):
        try:
            async with th.batch("campaign_deliveries", key="campaign:1") as batch:
                await batch.add_calls([th.call(send_email, 1).opts(**options)])
        except UnsupportedOption as error:
            rejected.append(error.option)  # транзакция батча откатилась, в базе ничего нет
    assert rejected == ["depends_on", "debounce", "no_such_option"]

    try:
        fq.task(batch=True)  # буферизация flexiq несовместима с учётом по одной задаче
    except UnsupportedOption as error:
        assert error.option == "batch"

    try:
        Tallyho(engine, schema=schema).install(FlexiqAdapter(queue, pool="prefork"))
    except ConfigurationError as error:
        assert "pool='thread'" in str(error)

    await th.aclose()
    await fq.close()
```

## guide-start-first-batch

Страница: `docs/guide/getting-started.md`.

<!-- tallyho-example: guide-start-first-batch -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне здесь адаптер вашего брокера
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()  # создать таблицы


async def send_email(address: str) -> None:
    if address.endswith("@bounce.test"):
        item.error("hard_bounce")  # ошибка без исключения и без ретраев
        return
    item.ok("sent")  # итог задачи с меткой


try:
    async with th.batch("newsletter", key="issue:1") as batch:
        await batch.map(send_email, ["ada@ok.test", "grace@ok.test", "gone@bounce.test"])
    # выход из блока: батч закрыт, транзакция закоммичена

    await broker.drain()  # выполнить задачи; в продакшне это делают воркеры

    view = await batch.handle.view()
    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    assert (view.progress.found, view.progress.ok, view.progress.error) == (3, 2, 1)
    assert dict(view.labels) == {"sent": 2, "hard_bounce": 1}
finally:
    await th.aclose()
```

## guide-hooks-lease-lost

Страница: `docs/guide/hooks/complete-in.md`.

<!-- tallyho-example: guide-hooks-lease-lost -->
```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import Column, MetaData, String, Table, insert, select

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

app_engine = engine.execution_options(schema_translate_map={None: schema})
metadata = MetaData()
deliveries = Table("deliveries", metadata, Column("email", String, primary_key=True))
async with app_engine.begin() as connection:
    await connection.run_sync(metadata.create_all)

clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))
broker = InlineBroker()
th = Tallyho(app_engine, schema=schema, clock=clock, lease_ttl=timedelta(seconds=60))
th.install(broker.adapter)
await th.migrate()

reached_the_end: list[str] = []


async def send(email: str) -> None:
    # Задача «зависла» дольше аренды: фоновые проверки завершили её как lease_expired.
    clock.advance(seconds=61)
    await th.run_maintenance_once()
    async with app_engine.begin() as connection:
        await connection.execute(insert(deliveries).values(email=email))
        item.ok("sent")
        await item.complete_in(connection)  # LeaseLostError: транзакция откатывается
    reached_the_end.append(email)


try:
    async with th.batch("issue", key="2026-10-01") as batch:
        await batch.add(send, "ada@example.com")
    await broker.drain()

    view = await batch.handle.wait(timeout=30)
    assert view.state is BatchState.COMPLETED_WITH_ERRORS
    assert (view.progress.ok, view.progress.error) == (0, 1)
    assert view.labels["lease_expired"] == 1  # итог фоновой проверки не изменился
    assert reached_the_end == []
    assert broker.dead_letters == ()  # для брокера попытка закончилась успехом
    async with app_engine.connect() as connection:
        assert (await connection.execute(select(deliveries))).all() == []  # строки «sent» нет
finally:
    await broker.close()
```

## guide-hooks-release

Страница: `docs/guide/hooks/recipe.md`.

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

# Движок приложения: ваши таблицы без схемы лежат в схеме приложения. Хуки получат сессию этого движка.
app_engine = engine.execution_options(schema_translate_map={None: schema})

metadata = MetaData()
issues = Table(
    "issues", metadata, Column("key", String, primary_key=True), Column("status", String)
)
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
    async with app_engine.begin() as connection:  # строка получателя и итог задачи - один коммит
        await connection.execute(
            update(deliveries).where(deliveries.c.email == email).values(status="sent")
        )
        item.ok("sent")
        await item.complete_in(connection)


@th.on_finalized(KIND)
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    # Счётчики точные уже здесь; терминальный статус поставит колбэк после экспорта.
    await session.execute(
        update(issues).where(issues.c.key == summary.key).values(status="settling")
    )


async def settle(key: str) -> None:
    async with AsyncSession(app_engine) as session, session.begin():
        status = await session.scalar(
            select(issues.c.status).where(issues.c.key == key).with_for_update()
        )
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
        await handle.release(session=session)  # разрешение на удаление - в той же транзакции


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

    await broker.drain()  # задачи, финализация, on_finalized, колбэк settle

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
    await th.aclose()
```

## guide-hooks-retry

Страница: `docs/guide/hooks/retry.md`.

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
    await th.aclose()
```

## guide-hooks-domain

Страница: `docs/guide/hooks.md`.

<!-- tallyho-example: guide-hooks-domain -->
```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Uuid,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho, item
from tallyho.model.policy import PolicyBreach
from tallyho.model.states import BatchState
from tallyho.model.views import BatchSummary
from tallyho.testing import FakeClock, InlineBroker

# Движок приложения: ваши таблицы без схемы лежат в схеме приложения. Хуки получат сессию этого движка.
app_engine = engine.execution_options(schema_translate_map={None: schema})

# Доменная таблица приложения - в той же базе, что и таблицы tallyho.
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
        .where(
            reports.c.batch_id == summary.id, reports.c.progress_seq < summary.seq
        )  # монотонность
        .values(
            done=summary.progress.done,
            failed=summary.progress.error,
            progress=func.greatest(
                reports.c.progress, summary.progress.ratio or 0.0
            ),  # не едет назад
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

    # Оператор отменяет отчёт: сначала своя строка, затем tallyho - в одной транзакции.
    async with AsyncSession(app_engine) as session, session.begin():
        await session.execute(select(reports).where(reports.c.id == 1).with_for_update())
        await handle.cancel(session=session)
    assert (await handle.wait(timeout=30)).state is BatchState.CANCELLED

    final = await report()
    assert final["status"] == "cancelled"  # поставил on_finalized, атомарно с финализацией
    assert (final["done"], final["failed"]) == (8, 2)
finally:
    await th.aclose()
```

## guide-install-session

Страница: `docs/guide/installation.md`.

<!-- tallyho-example: guide-install-session -->
```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)  # таблицы tallyho лежат в схеме schema
th.install(broker.adapter)
await th.migrate()


async def notify(order_id: int) -> None:
    assert order_id > 0


# Обычная сессия приложения: схема tallyho не входит в её search_path.
async with AsyncSession(engine) as session, session.begin():
    search_path = await session.scalar(text("SHOW search_path"))
    assert schema not in str(search_path)
    async with th.batch("orders", key="order:1", session=session) as batch:
        await batch.add(notify, 1)

await broker.drain()
assert (await batch.handle.view()).state is BatchState.SUCCEEDED
await broker.close()
```

## guide-install-migrate

Страница: `docs/guide/installation.md`.

<!-- tallyho-example: guide-install-migrate -->
```python
from sqlalchemy import text

from tallyho import Tallyho

th = Tallyho(engine, schema=schema, prefix="jobs_")  # префикс по умолчанию - "th_"
version = await th.migrate()  # создаёт схему и таблицы, возвращает версию схемы
assert version >= 1
assert await th.migrate() == version  # повторный вызов ничего не меняет

async with engine.connect() as connection:
    names = await connection.scalars(
        text("SELECT tablename FROM pg_tables WHERE schemaname = :schema"), {"schema": schema}
    )
    tables = set(names)
assert {"jobs_batch", "jobs_item", "jobs_outbox", "jobs_counter"} <= tables
```

## guide-operations-observer

Страница: `docs/guide/operations/observability.md`.

<!-- tallyho-example: guide-operations-observer -->
```python
from collections import Counter
from uuid import UUID

from tallyho import Tallyho, item
from tallyho.model.states import BatchState, ResultClass
from tallyho.protocols.observer import NullObserver
from tallyho.testing import InlineBroker


class Metrics(NullObserver):
    """Счётчики для вашей системы метрик (Prometheus, StatsD …)."""

    def __init__(self) -> None:
        self.items: Counter[tuple[str, str | None]] = Counter()
        self.finalized: list[tuple[str, str]] = []

    def item_finished(
        self, *, batch_id: UUID, item_id: UUID, result: ResultClass, label: str | None, attempt: int
    ) -> None:
        self.items[result.name.lower(), label] += 1

    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        self.finalized.append((kind, state.name.lower()))


metrics = Metrics()
broker = InlineBroker()
th = Tallyho(engine, schema=schema, observer=metrics)
th.install(broker.adapter)
await th.migrate()


async def convert(file_id: int) -> None:
    if file_id == 2:
        item.error("unsupported_format")
    else:
        item.ok("converted")


try:
    async with th.batch("conversions", key="upload:1") as batch:
        await batch.map(convert, range(4))
    await broker.drain()

    assert metrics.items == Counter({("ok", "converted"): 3, ("error", "unsupported_format"): 1})
    assert metrics.finalized == [("conversions", "completed_with_errors")]
finally:
    await th.aclose()
```

## guide-postgres-storage

Страница: `docs/guide/operations/postgres.md`.

<!-- tallyho-example: guide-postgres-storage -->
```python
from sqlalchemy import text

from tallyho import Tallyho

th = Tallyho(engine, schema=schema)
await th.migrate()

async with engine.connect() as connection:
    rows = await connection.execute(
        text(
            "SELECT c.relname, c.reloptions FROM pg_class AS c "
            "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema"
        ),
        {"schema": schema},
    )
    options = {name: set(values or ()) for name, values in rows}

assert "fillfactor=50" in options["th_counter"]
assert "autovacuum_vacuum_threshold=1000" in options["th_outbox"]
assert "fillfactor=85" in options["th_item"]
```

## guide-operations-maintenance

Страница: `docs/guide/operations.md`.

<!-- tallyho-example: guide-operations-maintenance -->
```python
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне - адаптер вашего брокера
th = Tallyho(
    engine,
    schema=schema,
    sweep_interval=timedelta(seconds=1),  # период фоновых проверок и страховочной отправки
)
th.install(broker.adapter)
await th.migrate()


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    runner = th.maintenance()
    task = asyncio.create_task(runner.run(), name="tallyho-maintenance")
    try:
        yield
    finally:
        runner.stop()  # мягкая остановка: текущий проход завершится
        await task
        await th.aclose()  # дождаться фоновых задач tallyho и остановить отправку


async def build(section: int) -> None: ...


try:
    async with lifespan():
        async with th.batch("report_build", key="daily") as batch:
            await batch.map(build, range(3))

        async with asyncio.timeout(30):
            while broker.pending < 3:  # отправлено сразу после коммита, без ожидания relay_grace
                await asyncio.sleep(0.05)

        await broker.drain()  # в продакшне задачи выполняют воркеры
        assert (await batch.handle.view()).state is BatchState.SUCCEEDED
finally:
    await th.aclose()  # повторный вызов ничего не делает
```

## guide-batches-streaming-close

Страница: `docs/guide/batches.md`. Пустой вход без `seal=False` закрывает батч.

<!-- tallyho-example: guide-batches-streaming-close -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()


async def import_row(row: int) -> None:
    item.ok("imported")


try:
    for chunk in ([1, 2, 3], [4, 5]):
        async with th.batch("import", key="file:7", seal=False) as batch:
            await batch.map(import_row, chunk)
    await broker.drain()
    assert (await batch.handle.view()).state is BatchState.OPEN

    async with th.batch("import", key="file:7") as batch:
        pass
    view = await batch.handle.wait(timeout=30)
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.found == 5
finally:
    await th.aclose()
```
