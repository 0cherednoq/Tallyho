# Установка и миграции

## Требования

| Что | Версия |
|---|---|
| Python | 3.11 и новее |
| PostgreSQL | 14 и новее |
| SQLAlchemy | 2.1 и новее, только async (`AsyncEngine`, `AsyncSession`, `AsyncConnection`) |
| Драйвер | `asyncpg` от 0.29 или `psycopg` от 3.1 |

:::{important}
Таблицы tallyho и ваши доменные таблицы должны лежать в одной базе PostgreSQL. Схемы могут быть
разными. Иначе хуки и операции в вашей транзакции перестают быть атомарными.
:::

## Установка пакета

::::{tab-set}

:::{tab-item} uv
```bash
uv add "tallyho[asyncpg]"            # или tallyho[psycopg]
uv add "tallyho[asyncpg,flexiq]"     # с адаптером брокера flexiq
```
:::

:::{tab-item} poetry
```bash
poetry add "tallyho[asyncpg]"            # или tallyho[psycopg]
poetry add "tallyho[asyncpg,flexiq]"     # с адаптером брокера flexiq
```
:::

:::{tab-item} pip
```bash
pip install "tallyho[asyncpg]"            # или tallyho[psycopg]
pip install "tallyho[asyncpg,flexiq]"     # с адаптером брокера flexiq
```
:::

::::

| Extra | Что добавляет |
|---|---|
| `asyncpg` / `psycopg` | драйвер PostgreSQL; нужен один из двух |
| `flexiq` | адаптер брокера [flexiq](../integrations/flexiq.md) (`flexiq>=2.0,<3`) |
| `alembic` | Alembic для встраивания миграций в ваш проект |
| `testing` | `pytest-asyncio` для [pytest-фикстуры](../integrations/pytest.md) |
| `otel` | OpenTelemetry API для [наблюдаемости](operations/observability.md) |

## Клиент `Tallyho`

Клиент создаётся один раз на процесс поверх вашего `AsyncEngine`. Конструктор не обращается к БД.

<!-- tallyho-noexec: фрагмент конфигурации приложения: DSN и модуль с хуками есть только в вашем проекте -->
```python
from datetime import timedelta

from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho

engine = create_async_engine("postgresql+asyncpg://app:secret@db/app")
th = Tallyho(
    engine,
    schema="app",  # схема таблиц tallyho
    prefix="th_",  # префикс имён таблиц
    hook_modules=["app.mailing.hooks"],  # модули с tx-хуками, импортируются сразу
    retention=timedelta(days=30),  # любая настройка из таблицы ниже - именованным аргументом
)
th.install(adapter)  # адаптер брокера: FlexiqAdapter или InlineBroker().adapter в тестах
```

| Параметр | Значение |
|---|---|
| `engine` | ваш `AsyncEngine`; tallyho берёт из него соединения для собственных транзакций |
| `schema` | схема таблиц. `None` - схема из `search_path` |
| `prefix` | префикс имён таблиц, по умолчанию `"th_"` |
| `hook_modules` | модули с [tx-хуками](hooks.md#регистрация). Импортируются в конструкторе, в каждом процессе |
| `observer` | приёмник событий для метрик и трассировки - [наблюдаемость](operations/observability.md) |
| `clock` | источник времени; в тестах - [`FakeClock`](testing.md#fakeclock) |
| `serializer` | сериализатор аргументов задач для адаптеров без собственного кодека |
| `id_factory` | генератор идентификаторов (по умолчанию UUIDv7) |
| `**settings` | настройки из раздела [«Настройки»](../reference/settings.md) |

`th.install(adapter)` связывает клиент с брокером. Его вызывают один раз, до первого `th.batch(...)`,
`th.call(...)` и `th.maintenance()`; без `install` эти методы бросают `ConfigurationError`.
Для `th.migrate()` адаптер не нужен.

Процесс с адаптером сам [отправляет сообщения в брокер](operations.md#отправка-в-брокер): сразу
после коммита и ещё раз страховочным проходом.

:::{warning}
При остановке любого процесса вызывайте `await th.aclose()`. Он дожидается фоновой работы tallyho.
Что будет без него, описано на странице [Корректная остановка](operations/shutdown.md).
:::

Процессу, который брокера не знает и только обслуживает установку или читает прогресс, подходит
`th.install(None)`. В нём работают `th.maintenance()`, `th.handle(...)`, `th.find(...)` и
`th.list_batches(...)`; `th.batch(...)` и `th.call(...)` бросают `ConfigurationError`, а сообщения
такой процесс не отправляет. Так устроена команда `tallyho maintenance`.

## Схема и префикс

Все таблицы создаются в `schema` с именами `<prefix><имя>`: `th_batch`, `th_item`, `th_outbox`,
`th_counter` и так далее. Так tallyho живёт рядом с вашими таблицами и не пересекается с ними.

* Схему tallyho создаёт сам, если её ещё нет.
* Префикс нужен, чтобы имена не пересеклись с вашими таблицами. Две установки с разными
  префиксами в одной схеме независимы: у каждой свои таблицы и свой лидер фоновых проверок.
  Миграции установок одной схемы выполняются по очереди.
* `schema` и `prefix` должны совпадать во всех процессах одной установки: в API, в воркерах и
  в maintenance.
* Имена схемы и префикса проверяются при создании клиента и при миграции; недопустимое имя -
  `ConfigurationError`.

### Схема в ваших сессиях

Имя схемы записано в каждом запросе tallyho. Поэтому библиотека находит свои таблицы на любом
соединении этой базы: и в собственных транзакциях, и когда вы передаёте свою сессию или
соединение - в `th.batch(session=...)`, в операции `handle.pause(session=...)` и подобные, в
`item.complete_in(session)`. Настраивать `search_path` или `schema_translate_map` ради tallyho не
нужно, и настройки вашего соединения библиотека не меняет.

<!-- tallyho-noexec: фрагмент приложения: Order и notify принадлежат вашему проекту -->
```python
# Обычная сессия приложения: схема tallyho в её search_path не входит.
async with AsyncSession(engine) as session, session.begin():
    order = Order(customer_id=customer_id)
    session.add(order)
    await session.flush()
    async with th.batch("orders", key=f"order:{order.id}", session=session) as batch:
        await batch.add(notify, order.id)
# заказ и батч закоммичены вместе; таблицы tallyho библиотека нашла сама
```

Ваши таблицы tallyho не трогает. Где они лежат, определяет ваш движок:

* В ваших транзакциях всё работает как раньше: таблицы без схемы ищутся по `search_path` или
  по вашей `schema_translate_map`.
* В [tx-хуках](hooks.md) так же. Сессия хука работает на соединении движка, который вы
  передали в `Tallyho(engine, ...)`, с его настройками. Если ваши таблицы адресует
  `schema_translate_map`, передавайте в `Tallyho` движок с этим отображением:
  `Tallyho(engine.execution_options(schema_translate_map={None: "app"}), schema="app")`.
* `Tallyho(engine, schema=None)` описывает таблицы tallyho без схемы: их, как и ваши, ищет
  соединение. Тогда все движки, через которые вы вызываете tallyho, должны быть настроены
  одинаково.
* Если в вашей `schema_translate_map` есть ключ, равный имени схемы tallyho, отображение
  действует и на таблицы tallyho.

## Миграции

Есть три способа создать и обновить таблицы. Все три выполняют одни и те же операции, поэтому их
можно сочетать: например, Alembic в продакшне и `migrate()` в тестах.

### Встроенный `migrate()`

<!-- tallyho-noexec: фрагмент запуска приложения: engine создаётся в вашем проекте -->
```python
th = Tallyho(engine, schema="app", prefix="jobs_")  # префикс по умолчанию - "th_"
version = await th.migrate()  # создаёт схему и таблицы, возвращает версию схемы
await th.migrate()  # повторный вызов ничего не меняет и возвращает ту же версию

# в схеме app появились таблицы jobs_batch, jobs_item, jobs_outbox, jobs_counter и остальные
```

`migrate()` проводит всю миграцию одной транзакцией под advisory lock, поэтому его безопасно
вызывать при старте каждого процесса: параллельные вызовы выстроятся в очередь, а повторный ничего
не сделает. DDL ждёт чужие блокировки не дольше 5 секунд (`lock_timeout`) и при таймауте
откатывается целиком. Если схема в базе новее установленной библиотеки, `migrate()` бросает
`ConfigurationError`: сначала обновите пакет.

### Alembic

Если схемой базы управляет Alembic, миграции tallyho вызываются из ваших ревизий функцией
`tallyho.storage.alembic.upgrade`. Подключение и правила описаны на странице
[Alembic](../integrations/alembic.md).

### Командная строка

Те же миграции выполняет команда `tallyho migrate`, она ставится вместе с пакетом:

```bash
tallyho migrate --dsn postgresql+asyncpg://app:secret@db/app --schema app
# schema=app version=1
```

Все команды и их флаги собраны на странице [Командная строка](../reference/cli.md).

## Настройки

Сроки, размеры пачек и лимиты задаются именованными аргументами `Tallyho(...)`, например
`Tallyho(engine, schema="app", retention=timedelta(days=30))`. Полная таблица с умолчаниями - на
странице [Настройки](../reference/settings.md).
