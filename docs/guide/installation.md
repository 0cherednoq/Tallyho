# Установка и миграции

[← Оглавление](README.md) · далее: [Батчи и конвейеры](batches.md)

## Требования

| Что | Версия |
|---|---|
| Python | ≥ 3.11 |
| PostgreSQL | ≥ 14 |
| SQLAlchemy | ≥ 2.1, только async (`AsyncEngine`, `AsyncSession`, `AsyncConnection`) |
| Драйвер | `asyncpg` ≥ 0.29 или `psycopg` ≥ 3.1 |

Таблицы tallyho и ваши доменные таблицы должны лежать в **одной базе PostgreSQL** (схемы могут быть
разными): только так хуки и операции в вашей транзакции остаются атомарными.

## Установка пакета

```bash
pip install "tallyho[asyncpg]"            # или tallyho[psycopg]
pip install "tallyho[asyncpg,flexiq]"     # с адаптером брокера flexiq
```

| Extra | Что добавляет |
|---|---|
| `asyncpg` / `psycopg` | драйвер PostgreSQL; нужен один из двух |
| `flexiq` | адаптер брокера [flexiq](flexiq.md) (`flexiq>=2.0,<3`) |
| `alembic` | Alembic для встраивания миграций в ваш проект |
| `testing` | `pytest-asyncio` для [pytest-фикстуры](testing.md#pytest-фикстура) |
| `otel` | OpenTelemetry API для [наблюдаемости](operations.md#наблюдаемость) |

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
    retention=timedelta(days=30),  # любая настройка из таблицы ниже — именованным аргументом
)
th.install(adapter)  # адаптер брокера: FlexiqAdapter или InlineBroker().adapter в тестах
```

| Параметр | Значение |
|---|---|
| `engine` | ваш `AsyncEngine`; tallyho берёт из него соединения для собственных транзакций |
| `schema` | схема таблиц. `None` — схема из `search_path` |
| `prefix` | префикс имён таблиц, по умолчанию `"th_"` |
| `hook_modules` | модули с [tx-хуками](hooks.md#регистрация). Импортируются в конструкторе, **в каждом процессе** |
| `observer` | приёмник событий для метрик и трассировки — [наблюдаемость](operations.md#наблюдаемость) |
| `clock` | источник времени; в тестах — [`FakeClock`](testing.md#fakeclock) |
| `serializer` | сериализатор аргументов задач для адаптеров без собственного кодека |
| `id_factory` | генератор идентификаторов (по умолчанию UUIDv7) |
| `**settings` | настройки из раздела [«Настройки»](#настройки) |

`th.install(adapter)` связывает клиент с брокером. Его вызывают один раз, до первого `th.batch(...)`,
`th.call(...)` и `th.maintenance()`; без `install` эти методы бросают `ConfigurationError`.
Для `th.migrate()` адаптер не нужен.

Процесс с адаптером сам [отправляет сообщения в брокер](operations.md#отправка-в-брокер) — сразу
после коммита и страховочным проходом. При остановке любого процесса вызовите `await th.aclose()`:
он дожидается фоновой работы tallyho — см. [Корректная остановка](operations.md#корректная-остановка).

Процессу, который брокера не знает и только обслуживает установку или читает прогресс, подходит
`th.install(None)`. В нём работают `th.maintenance()`, `th.handle(...)`, `th.find(...)` и
`th.list_batches(...)`; `th.batch(...)` и `th.call(...)` бросают `ConfigurationError`, а сообщения
такой процесс не отправляет. Так устроена команда `tallyho maintenance`.

## Схема и префикс

Все таблицы создаются в `schema` с именами `<prefix><имя>`: `th_batch`, `th_item`, `th_outbox`,
`th_counter` и так далее. Так tallyho живёт рядом с вашими таблицами и не пересекается с ними.

* Схему tallyho создаёт сам, если её ещё нет.
* Префикс нужен, чтобы имена не пересеклись с вашими таблицами. **Одна схема — одна установка:**
  лидер фоновых проверок выбирается по схеме, поэтому две установки в одной схеме, даже с разными
  префиксами, мешали бы друг другу. Независимые установки разносите по схемам.
* `schema` и `prefix` должны совпадать во всех процессах одной установки: в API, в воркерах и
  в maintenance.
* Имена схемы и префикса проверяются при создании клиента и при миграции; недопустимое имя —
  `ConfigurationError`.

### Схема в ваших сессиях

В собственных транзакциях tallyho сам направляет запросы в `schema`. Но когда вы передаёте свою
сессию или соединение — в `th.batch(session=...)`, в операции `handle.pause(session=...)` и
подобные, в `item.complete_in(session)`, — запросы к таблицам tallyho выполняются **на вашем
соединении и без имени схемы**. Соединение должно находить эти таблицы само. Есть три способа:

| Способ | Как |
|---|---|
| Отображение схемы в движке | `app_engine = engine.execution_options(schema_translate_map={None: "app"})` и сессии поверх `app_engine`. Ваши таблицы, объявленные без схемы, при этом тоже адресуются в `app` |
| `search_path` | схема tallyho входит в `search_path` роли или базы: `ALTER ROLE app SET search_path = app, public` |
| Без схемы | `Tallyho(engine, schema=None)`: таблицы создаются и ищутся в схеме из `search_path` |

Иначе первый же вызов с `session=` завершится ошибкой PostgreSQL `relation "th_batch" does not exist`.

В [tx-хуках](hooks.md) действует то же отображение: сессия хука адресует таблицы без схемы в схему
tallyho. Доменные таблицы, лежащие в другой схеме, объявляйте с явным `schema=`.

## Миграции

Есть три способа создать и обновить таблицы. Все три выполняют одни и те же операции, поэтому их
можно сочетать: например, Alembic в продакшне и `migrate()` в тестах.

### Встроенный `migrate()`

<!-- tallyho-example: guide-install-migrate -->
```python
from sqlalchemy import text

from tallyho import Tallyho

th = Tallyho(engine, schema=schema, prefix="jobs_")  # префикс по умолчанию — "th_"
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

`migrate()` выполняет всю миграцию одной транзакцией под advisory lock, поэтому его безопасно
вызывать при старте каждого процесса: параллельные вызовы выстроятся в очередь, а повторный ничего
не сделает. DDL ждёт чужие блокировки не дольше 5 секунд (`lock_timeout`) и при таймауте
откатывается целиком. Если схема в базе новее установленной библиотеки, `migrate()` бросает
`ConfigurationError`: сначала обновите пакет.

### Alembic

Если схемой базы управляет Alembic, вызывайте миграции tallyho из своих ревизий. Нужен extra
`alembic`.

<!-- tallyho-noexec: файл ревизии выполняет Alembic внутри вашего проекта -->
```python
"""add tallyho tables"""

from alembic import op

from tallyho.storage.alembic import upgrade as tallyho_upgrade


def upgrade() -> None:
    tallyho_upgrade(op, version=1, schema="app")
```

Правила:

* **Одна ревизия — одна версия схемы tallyho.** Номер версии указывается явно, чтобы ревизия не
  меняла смысл при обновлении библиотеки. Версии идут подряд: `version=1`, в следующей ревизии
  `version=2`, затем `version=3` и `version=4`. Пропускать версии нельзя.
* Актуальную версию схемы возвращает `th.migrate()` и печатает `tallyho migrate`; на момент
  написания это 4. После обновления библиотеки сравните её с последней версией в своих ревизиях и
  допишите недостающие.
* Параметры `upgrade(op, *, version, schema, prefix="th_", lock_timeout=...)` должны совпадать с
  параметрами клиента `Tallyho`.
* Транзакцией и порядком управляет Alembic. Работает и offline-режим (`alembic upgrade --sql`).
* Ревизия записывает версию в служебную таблицу, поэтому `th.migrate()` после неё ничего не делает.
* Обратных миграций (`downgrade`) в v1 нет.

### Командная строка

Команда `tallyho` ставится вместе с пакетом (то же самое — `python -m tallyho.cli`).

```bash
tallyho migrate --dsn postgresql+asyncpg://app:secret@db/app --schema app
# schema=app version=4
```

| Команда | Что делает |
|---|---|
| `tallyho migrate --dsn DSN --schema SCHEMA` | создаёт или обновляет таблицы и печатает версию схемы |
| `tallyho maintenance --dsn DSN --schema SCHEMA [--hook-module MODULE ...] [--once]` | фоновые проверки отдельным процессом — см. [эксплуатацию](operations.md#maintenance-отдельным-процессом) |
| `tallyho inspect TARGET --dsn DSN --schema SCHEMA` | печатает дерево батча с прогрессом; `TARGET` — UUID батча или `kind:key` корня |
| `tallyho --version` | версия пакета |

`--dsn` — async-DSN SQLAlchemy (`postgresql+asyncpg://…` или `postgresql+psycopg://…`), `--schema`
обязателен. Команды работают с префиксом по умолчанию `th_`; установку с другим префиксом
мигрируйте через `th.migrate()` или Alembic.

Пример вывода `inspect` для конвейера из двух этапов (идентификаторы сокращены):

```text
catalog_parse key=catalog:7 id=01a0fbbd-… state=sealed done=0/2 found=2 queued=2 in_flight=0 errors=0 cancelled=0 progress=33.3%
  catalog_parse.cards key=cards id=01a0fbbd-… state=open done=2/6 found=2 queued=0 in_flight=0 errors=0 cancelled=0 progress=33.3%
  catalog_parse.pages key=pages id=01a0fbbd-… state=sealed done=1/3 found=3 queued=2 in_flight=0 errors=0 cancelled=0 progress=33.3%
```

`done=1/3` — завершено и ожидается всего, `?` на месте числа — оценки пока нет. В строке родителя
каждый под-батч считается одной задачей.

## Настройки

Настройки передаются именованными аргументами в `Tallyho(...)`. Неизвестное имя или недопустимое
значение — `ConfigurationError` при создании клиента.

| Настройка | По умолчанию | Смысл |
|---|---|---|
| `retention` | 14 дней | через сколько после завершения удалять дерево батча; `None` — хранить вечно. Можно переопределить в `th.batch(retention=...)` |
| `max_items` | `None` | лимит задач на дерево по умолчанию; переопределяется в `th.batch(max_items=...)` |
| `lease_ttl` / `heartbeat_every` | 60 с / 20 с | через сколько задача без признаков жизни считается потерянной и как часто воркер продлевает аренду |
| `completer_tick` / `completer_max_batch` | 20 мс / 500 | групповой коммит завершений: окно накопления и максимальный размер пачки |
| `completer_backpressure` | 10 000 | предел буфера завершений в процессе воркера |
| `relay_grace` / `relay_claim_ttl` | 5 с / 30 с | когда страховочная отправка подбирает неотправленные сообщения и на сколько их захватывает |
| `finalize_grace` | 30 с | через сколько фоновые проверки подбирают пропущенную финализацию |
| `sweep_interval` | 5 с | период фоновых проверок и страховочной отправки |
| `hook_timeout` | 10 с | предельное время одного tx-хука |
| `hook_backoff_max` | 5 мин | верхняя граница паузы между повторами упавшего хука |
| `snapshot_tick` | 500 мс | период цикла снимков прогресса; частота на батч задаётся `every` в хуке |
| `estimate_min_basis` / `estimate_min_share` | 20 / 0.05 | минимальная выборка для оценки ожидаемого объёма этапа |
| `eta_window` | 60 с | окно усреднения скорости для ETA |
| `lock_timeout` | 5 с | сколько транзакции tallyho ждут чужие блокировки |
| `close_timeout` | 10 с | сколько `th.aclose()` ждёт фоновую работу; что не успело — отменяется и восстанавливается фоновыми проверками |
| `watch_throttle` | 500 мс | не чаще одного уведомления `watch()` на батч |
| `counter_slots` | 8 | число слотов счётчиков на батч |
| `items_scan_window` | 5 000 | сколько строк читает один запрос `handle.items(states=...)` |
| `attributes_max_keys` | 32 | максимум атрибутов корня |
| `attributes_max_key_bytes` / `attributes_max_value_bytes` | 128 / 512 | длина ключа и строкового значения атрибута в UTF-8 |
| `attributes_max_bytes` / `memo_max_bytes` | 8 192 / 16 384 | размер всех атрибутов и `memo` в JSON |

Настройки, влияющие на учёт (`lease_ttl`, `counter_slots`, `relay_*`, `finalize_grace`), держите
одинаковыми во всех процессах установки.

## Что дальше

* Создать первый батч — [Батчи и конвейеры](batches.md).
* Где запускать фоновые проверки — [Эксплуатация PostgreSQL](operations.md#процессы).
