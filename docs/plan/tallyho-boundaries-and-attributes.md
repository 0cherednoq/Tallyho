# Границы Tallyho и пользовательские атрибуты

> **Перенесено в ARCHITECTURE (D-038, D-039).** Источник истины — [ARCHITECTURE.md](../ARCHITECTURE.md) §1, §2, §5.1, §11.2, §15, §16. Отличия принятого решения от этого предложения — [V1_EXTENSIONS_PLAN.md](V1_EXTENSIONS_PLAN.md) §1: side-таблица вместо колонок, только `str | int | bool`, без обязательного `app.*`; Read Model и operational API отложены.

Эта страница фиксирует архитектурное решение для разработчиков Tallyho и приложений,
которые интегрируют библиотеку с брокером задач. Tallyho остаётся техническим плагином учёта
выполнения. Библиотека не владеет предметными сущностями, бизнес-процессами и доменными
переходами, но позволяет сохранить неизменяемый пользовательский контекст Batch для поиска,
корреляции и диагностики.

## Решение

Tallyho отвечает за техническое выполнение:

- Batch, Item и их технические состояния;
- постановку через Outbox;
- lease, heartbeat, retry и восстановление после сбоев;
- прогресс, результаты, метки и пользовательские счётчики;
- pause, resume и cancel выполнения;
- политики ошибок и техническую финализацию;
- retention собственных данных;
- вызов транзакционных hooks приложения.

Tallyho не отвечает за:

- предметные Entity, например Order, Server, Proxy или Campaign;
- предметные Run, например CheckRun или ImportRun;
- доменные статусы и переходы между ними;
- права пользователя на бизнес-операцию;
- доменные события и историю предметного агрегата;
- долгосрочное хранение бизнес-данных;
- workflow replay, Signals, Updates и actor mailbox.

В частности, в Tallyho не добавляется отдельное поле `domain_state` или второй смысл поля
`state`. Поле `Batch.state` описывает только техническое состояние выполнения.

## Почему доменное состояние не хранится в Batch

Связь между предметной сущностью и Batch не обязана быть один к одному. Один Order может иметь
отдельные Batch для оплаты, резервирования, доставки и возврата. Один Batch также может
обрабатывать множество предметных сущностей.

```text
Order
├── payment Batch
├── stock reservation Batch
├── delivery Batch
└── refund Batch
```

Если каждый Batch хранит актуальный статус Order, появляется несколько конкурирующих источников
истины. Дополнительно retention может удалить Batch раньше, чем бизнес обязан удалить Order.
Поэтому актуальное доменное состояние хранит владеющий модуль приложения.

Технический результат и бизнес-результат также различаются. Успешно выполненная задача отмены
может дать следующую комбинацию:

```text
Batch.state = succeeded
Order.status = cancelled
```

Tallyho гарантирует техническое выполнение. Приложение интерпретирует результат и выполняет
доменный переход.

## Модель пользовательского контекста

Batch принимает два независимых вида пользовательского контекста.

| Поле | Изменяемость | Индексируется | Назначение |
| --- | --- | --- | --- |
| `state` | Меняет только Tallyho | Да | Технический lifecycle Batch |
| `attributes` | Неизменяемое | Да | Поиск, фильтрация и корреляция |
| `memo` | Неизменяемое | Нет | Контекст для отображения и диагностики |

### Индексируемые attributes

`attributes` содержат небольшой набор скалярных значений, заданных при создании Batch.
Tallyho хранит и индексирует значения, но не интерпретирует их бизнес-смысл.

```python
async with th.batch(
    "order.payment",
    key=f"payment:{payment_id}",
    attributes={
        "app.entity_type": "order",
        "app.entity_id": str(order_id),
        "app.customer_id": str(customer_id),
        "app.order_status_at_start": "awaiting_payment",
    },
) as batch:
    await batch.add(charge_payment, payment_id)
```

Значение `app.order_status_at_start` является историческим снимком. Оно не обязано меняться,
если Order позднее перейдёт в другой статус.

Приложение может использовать attributes в Query API:

```python
page = await th.batches.list(
    kind="order.payment",
    attributes={
        "app.entity_type": "order",
        "app.entity_id": str(order_id),
    },
    limit=100,
    cursor=cursor,
)
```

### Неиндексируемый memo

`memo` содержит диагностический контекст, по которому не требуется искать Batch.

```python
async with th.batch(
    "order.delivery",
    key=f"delivery:{delivery_id}",
    attributes={
        "app.entity_type": "order",
        "app.entity_id": str(order_id),
    },
    memo={
        "delivery_method": "courier",
        "order_status_at_start": "paid",
    },
):
    ...
```

`memo` не предназначен для секретов, полного снимка предметного агрегата или больших payload.

## Контракт attributes

Первая версия attributes следует ограничениям:

- значения задаются только при создании Batch;
- после создания Batch значения нельзя изменить или удалить;
- Tallyho не валидирует доменные переходы и не знает смысл ключей;
- ключи пользователя имеют namespace `app.*`;
- namespace `tallyho.*` зарезервирован библиотекой;
- разрешены только скалярные JSON-совместимые типы: `str`, `int`, `float`, `bool`, `UUID`,
  `datetime` и `None` после нормализации библиотекой;
- коллекции и вложенные объекты в attributes запрещены;
- количество ключей и общий размер ограничиваются конфигурацией;
- attributes удаляются вместе с Batch по его retention;
- секреты и чувствительные персональные данные хранить запрещено.

Рекомендуемые начальные ограничения:

| Ограничение | Значение по умолчанию |
| --- | ---: |
| Число attributes одного Batch | 32 |
| Длина ключа | 128 байт |
| Длина строкового значения | 512 байт |
| Общий сериализованный размер | 8 КиБ |

Точные значения являются частью конфигурации и могут быть скорректированы после нагрузочных
тестов. Проверка выполняется синхронно до записи Batch.

## Tenant как обычный атрибут

Tallyho не имеет встроенной модели Tenant. Multi-tenant приложение при необходимости записывает
идентификатор организационного контекста как пользовательский атрибут:

```python
attributes = {
    "app.tenant_id": str(tenant_id),
    "app.entity_type": "server",
    "app.entity_id": str(server_id),
}
```

Single-tenant приложение не добавляет этот ключ. Tallyho не проверяет доступ пользователя к
Tenant. Авторизацию и обязательный tenant-фильтр обеспечивает приложение.

## Доменный переход через транзакционный hook

Приложение обновляет предметный агрегат в транзакционном hook. Tallyho передаёт техническую
сводку, а доменный модуль решает, какой переход допустим.

```python
@th.on_finalized("order.payment")
async def payment_finished(
    session: AsyncSession,
    summary: BatchSummary,
) -> None:
    order_id = UUID(summary.attributes["app.entity_id"])
    order = await orders.get_for_update(session, order_id)

    if summary.labels.get("paid", 0) > 0:
        order.confirm_payment()
    else:
        order.reject_payment()
```

В этой модели:

- Tallyho гарантирует свою техническую финализацию и повтор hook по своей политике;
- приложение владеет `Order` и проверяет его инварианты;
- доменный статус не выводится библиотекой автоматически из `Batch.state`;
- доменные события создаёт агрегат приложения;
- Tallyho не становится workflow engine.

## Хранение и индексация в PostgreSQL

Начальная реализация может хранить контекст в двух JSONB-колонках `th_batch`:

```sql
attributes jsonb NOT NULL DEFAULT '{}',
memo jsonb NOT NULL DEFAULT '{}'
```

Для equality и containment-фильтров используется GIN:

```sql
CREATE INDEX ix_th_batch_attributes
ON th_batch
USING gin (attributes jsonb_path_ops);
```

Пример запроса:

```sql
SELECT *
FROM th_batch
WHERE attributes @> '{"app.entity_type":"order","app.entity_id":"42"}'::jsonb
ORDER BY created_at DESC, id DESC
LIMIT 100;
```

Обычные технические поля `kind`, `state`, `created_at` и `id` остаются отдельными колонками с
B-tree индексами. Tallyho не обещает эффективную сортировку или range-фильтры по произвольным
JSONB-атрибутам в первой версии.

## Query API

Публичный Query API должен поддерживать cursor pagination и сочетать технические фильтры с
attributes:

```python
page = await th.batches.list(
    kinds={"order.payment", "order.delivery"},
    states={BatchState.RUNNING, BatchState.FAILED},
    attributes={"app.entity_id": str(order_id)},
    created_after=started_at,
    limit=100,
    cursor=cursor,
)
```

Query API возвращает DTO библиотеки и не создаёт предметные Entity или Run.

## Расширенный SQLAlchemy Read Model

Высокоуровневый Query API является основным способом чтения, но не должен пытаться покрыть все
отчёты и административные выборки. Для кастомных запросов Tallyho предоставляет стабильный
read-only контракт в виде публичных SQLAlchemy `Table`-объектов.

```python
batch = th.read_model.batches

statement = (
    select(
        batch.c.id,
        batch.c.kind,
        batch.c.state,
        batch.c.created_at,
    )
    .where(
        batch.c.kind == "order.payment",
        batch.c.attributes.contains({"app.tenant_id": str(tenant_id)}),
    )
    .order_by(batch.c.created_at.desc(), batch.c.id.desc())
    .limit(100)
)

rows = (await session.execute(statement)).mappings().all()
```

Read Model нужен для сценариев, в которых универсального Query API недостаточно:

- пользовательские отчёты и агрегаты;
- административные выборки;
- соединение Batch с таблицами приложения;
- аналитика по attributes;
- диагностика выполнения;
- чтение через существующую SQLAlchemy session приложения.

### Публичная граница

Приложение не импортирует внутренние объекты из `tallyho.storage`. Такие импорты сделали бы
физическую структуру хранения случайным публичным API и помешали бы Tallyho менять таблицы,
счётчики, partitioning и реализацию Outbox.

Разрешённый контракт доступен через настроенный экземпляр:

```python
batch = th.read_model.batches
item = th.read_model.items
progress = th.read_model.batch_progress
```

Read Model принадлежит экземпляру `Tallyho`, потому что именно он знает фактические `schema` и
`prefix`. Это также позволяет одному процессу безопасно использовать несколько установок:

```python
orders = Tallyho(engine, schema="orders", prefix="th_")
mailing = Tallyho(engine, schema="mailing", prefix="jobs_")

orders_batch = orders.read_model.batches
mailing_batch = mailing.read_model.batches
```

Минимальный публичный объект:

```python
@dataclass(frozen=True, slots=True)
class ReadModel:
    batches: Table
    items: Table
    batch_progress: Table
    version: int
```

### Версионированные PostgreSQL views

SQLAlchemy Read Model описывает не внутренние write-таблицы, а стабильные PostgreSQL views:

```text
th_batch_read_v1
th_item_read_v1
th_batch_progress_v1
```

Физические таблицы и алгоритмы Tallyho могут меняться, пока представления сохраняют публичный
контракт. При несовместимом изменении библиотека создаёт следующую версию, например
`th_batch_read_v2`, и оставляет предыдущую на документированный период миграции.

SQLAlchemy объявляет колонки views явно в коде. Runtime reflection не используется, потому что он
добавляет I/O при запуске, ослабляет типизацию и обнаруживает несовместимую схему слишком поздно.

Пример публичного представления Batch:

```sql
CREATE VIEW th_batch_read_v1 AS
SELECT
    id,
    root_id,
    parent_id,
    kind,
    key,
    state,
    attributes,
    memo,
    created_at,
    start_at,
    deadline_at,
    paused_at,
    cancel_requested_at,
    finished_at,
    hook_attempts,
    hook_error
FROM th_batch;
```

Представление Item может экспортировать:

```text
id, batch_id, state, task_name, key, label, attempt, weight,
created_at, started_at, finished_at, result, error
```

Read Model не экспортирует по умолчанию:

- сериализованный payload задачи;
- fencing token и внутренний идентификатор claim;
- lease internals;
- содержимое Outbox и callback payload;
- данные, необходимые только Relay, Completer или Sweeper.

Для таких данных Tallyho предоставляет отдельный operational API, например `th.health()` или
`th.operations.stuck_batches()`, а не открывает write-модель.

### Только чтение

Read Model нельзя использовать для изменения данных:

```python
# Запрещено: обход state machine и инвариантов Tallyho.
await session.execute(update(th.read_model.batches).values(state="succeeded"))
```

Все изменения выполняются командами публичного API:

```python
await handle.pause()
await handle.resume()
await handle.cancel()
await handle.retry_failed()
```

PostgreSQL views по возможности должны быть не обновляемыми или иметь явный запрет записи.

### Соединение с таблицами приложения

Приложение может соединять read view Tallyho со своими таблицами в одной SQLAlchemy-сессии:

```python
batch = th.read_model.batches

statement = (
    select(
        orders.c.id,
        orders.c.status,
        batch.c.id.label("batch_id"),
        batch.c.state.label("batch_state"),
    )
    .join(batch, batch.c.id == orders.c.payment_batch_id)
    .where(orders.c.customer_id == customer_id)
)
```

Для часто выполняемых join приложение хранит `batch_id` в своей таблице. Соединение через
`attributes["app.entity_id"]` допустимо для редких диагностических запросов, но хуже
индексируется, требует преобразования типов и связывает SQL приложения с форматом JSONB.

Attributes предназначены прежде всего для поиска и корреляции:

```python
statement = select(batch).where(
    batch.c.attributes.contains(
        {
            "app.entity_type": "order",
            "app.entity_id": str(order_id),
        }
    )
)
```

### Результат кастомного запроса

Tallyho гарантирует колонки и их смысл, но не пытается преобразовать произвольный запрос в DTO.
Тип результата отчёта определяет приложение:

```python
@dataclass(frozen=True, slots=True)
class TenantBatchCount:
    tenant_id: str
    running: int


tenant = batch.c.attributes["app.tenant_id"].astext
statement = (
    select(
        tenant.label("tenant_id"),
        func.count().label("running"),
    )
    .where(batch.c.state == BatchState.RUNNING.value)
    .group_by(tenant)
)

result = [TenantBatchCount(**row) for row in (await session.execute(statement)).mappings()]
```

Таким образом, Tallyho предоставляет два уровня чтения:

```text
Типовой сценарий    → Query API → DTO библиотеки
Кастомный сценарий  → Read Model → SQLAlchemy RowMapping или DTO приложения
Операционная проверка → Operational API → health/diagnostic DTO
```

## Mutable search attributes отложены

Изменяемые search attributes могут быть полезны для административных проекций, но в первой
версии не входят в контракт. Они создают дополнительные требования:

- optimistic concurrency и версия attributes;
- атомарное обновление вместе с доменной транзакцией;
- явные операции `set` и `unset`;
- правила разрешения конфликтов;
- документированная возможность устаревшей проекции;
- отдельный аудит изменений при необходимости.

Если такое API понадобится, оно принимается отдельным архитектурным решением. Даже в этом случае
search attributes остаются денормализованной проекцией, а не источником истины доменной Entity.

## Критерий для будущих расширений

Новая возможность входит в ядро Tallyho, если она усиливает надёжность, наблюдаемость или
управление техническим выполнением задач и не требует знания предметного смысла данных.

Если возможность требует от Tallyho владеть бизнес-переходами, правами, Entity, предметной
историей или долговременным состоянием процесса, она остаётся в приложении или реализуется
отдельным необязательным расширением.
