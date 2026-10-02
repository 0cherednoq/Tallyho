# Тестирование

[← Оглавление](README.md) · назад: [Хуки](hooks.md) · далее: [Адаптер flexiq](flexiq.md)

Пакет `tallyho.testing` позволяет проверять сценарии с батчами без брокера и без ожидания
реального времени:

| Инструмент | Назначение |
|---|---|
| `InlineBroker` | брокер в памяти: выполняет задачи в том же процессе, по шагам и детерминированно |
| `FakeClock` | часы, которые двигает тест: отложенный старт, аренда, retention, снимки прогресса |
| `th.run_maintenance_once()` | один проход фоновых проверок вместо долгоживущего процесса |
| фикстура `tallyho_env` | готовая установка для pytest |

PostgreSQL нужен настоящий: учёт построен на его транзакциях и блокировках. В тестах его удобно
поднимать через [testcontainers](https://testcontainers-python.readthedocs.io/) или брать готовый
сервер CI. Тесты изолируются схемами: одна схема — одна установка tallyho.

## `InlineBroker`

`InlineBroker` — полноценный адаптер брокера. Задачи проходят тот же путь, что и в продакшне:
запись в базу, отправка, захват воркером, выполнение, групповой коммит итогов, финализация, хуки и
колбэки. Отличие одно: очередь лежит в памяти, а выполнением управляет тест.

<!-- tallyho-example: guide-testing-broker -->
```python
from collections import Counter

from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

# Половина сообщений будет доставлена дважды; seed делает прогон воспроизводимым.
broker = InlineBroker(duplicate_delivery_rate=0.5, seed=7)
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
await th.migrate()

runs: Counter[int] = Counter()


async def notify(user_id: int) -> None:
    runs[user_id] += 1
    if user_id == 3 and runs[user_id] == 1:
        raise TimeoutError("провайдер не ответил")  # исключение → ретрай брокера
    item.ok("notified")


try:
    async with th.batch("notifications", key="digest:1") as batch:
        await batch.add_calls(th.call(notify, user_id).opts(max_retries=2) for user_id in range(6))

    assert await broker.step(2) == 2  # выполнить не больше двух доставок
    assert (await batch.handle.view()).progress.done >= 1

    delivered = await broker.drain()  # выполнять до полного простоя
    assert delivered + 2 == broker.deliveries
    assert broker.deliveries > 6  # дубли и ретрай тоже считаются доставками
    assert broker.pending == 0
    assert broker.dead_letters == ()

    view = await batch.handle.view()
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == 6
    # Дубли доставки не привели к повторному выполнению; задача 3 выполнялась дважды из-за ретрая.
    assert runs == Counter({0: 1, 1: 1, 2: 1, 3: 2, 4: 1, 5: 1})
finally:
    await broker.close()
```

| Член `InlineBroker` | Что делает |
|---|---|
| `InlineBroker(duplicate_delivery_rate=0.0, seed=None, serializer=None)` | `duplicate_delivery_rate` — доля сообщений, доставляемых дважды; `seed` — зерно генератора дублей |
| `broker.adapter` | то, что передаётся в `th.install(...)` |
| `await broker.step(n=1)` | выполнить не больше `n` доставок; возвращает число выполненных |
| `await broker.drain(concurrency=1)` | выполнять до простоя: пока есть сообщения и пока появляются новые. `concurrency` имитирует пул воркеров |
| `broker.pending` | сообщений в очереди |
| `broker.deliveries` | сколько доставок взято воркером всего, включая дубли |
| `broker.dead_letters` | сообщения, исчерпавшие попытки |
| `broker.kill_worker_after(n)` | на `n`-й следующей доставке «убить» воркер: задача захвачена, но не выполнена и не завершена |
| `await broker.close()` | дождаться внутренних операций и остановить воркер; вызывайте в конце теста |

Поведение, которое стоит знать:

* **Ретраи.** Исключение в задаче — повод для повторной доставки. Число повторов задаёт опция
  вызова `max_retries` (по умолчанию 0): `th.call(fn, ...).opts(max_retries=2)`. Когда попытки
  исчерпаны, задача получает итог `error("exhausted")`, а сообщение попадает в `dead_letters`.
* **Порядок.** `step` и `drain()` выполняют сообщения последовательно, в порядке очереди; каждая
  следующая задача стартует после того, как итог предыдущей записан и применены его последствия
  (политика ошибок, финализация, колбэки).
* **Колбэк-задачи** доставляются через ту же очередь и выполняются тем же `drain()`.
* **Функции задач** — обычные `async def`; оборачивать их декоратором не нужно. Брокер различает
  задачи по имени `модуль.имя_функции`, поэтому две разные функции с одинаковым полным именем в
  одном брокере — `ConfigurationError`.
* **Отложенные и возвращённые сообщения.** `step()` отправляет в очередь только то, что записано
  только что. Сообщения, срок которых наступил позже (отложенный старт, возврат задачи после
  истёкшей аренды), подбирают `drain()` и `th.run_maintenance_once()` — и только когда они старше
  `relay_grace` (5 секунд). В тестах с `FakeClock` задайте `relay_grace=timedelta(0)` или сдвигайте
  часы с запасом.
* **Что не ждёт `drain()`.** Финализация, которую запускает управляющая операция, когда в очереди
  нет задач (`retry_finalize()`, отмена батча на паузе, создание пустого батча), идёт в фоне.
  Дождитесь её явно: `await handle.wait(timeout=10)`.
* **Проверка устойчивости.** Запускайте важные сценарии с `duplicate_delivery_rate > 0`: так тест
  проверяет, что повторная доставка не выполняет задачу дважды и не ломает ваши хуки.

## `FakeClock`

Все сроки tallyho — отложенный старт, аренда задач, дедлайны, retention, интервалы снимков и
повторов хуков — отсчитываются от часов клиента. `FakeClock` передаётся в `Tallyho(clock=...)` и
двигается только вперёд.

<!-- tallyho-example: guide-testing-clock -->
```python
from datetime import UTC, datetime, timedelta

from tallyho import Tallyho
from tallyho.model.states import BatchState
from tallyho.testing import FakeClock, InlineBroker

clock = FakeClock(datetime(2026, 10, 1, 9, tzinfo=UTC))  # обязательно с часовым поясом
broker = InlineBroker()
th = Tallyho(
    engine,
    schema=schema,
    clock=clock,
    lease_ttl=timedelta(seconds=60),
    relay_grace=timedelta(0),  # отправлять отложенное точно в срок, без запаса
)
th.install(broker.adapter)
await th.migrate()

done: list[int] = []


async def work(number: int) -> None:
    done.append(number)


try:
    # Отложенный старт: до start_at задачи в брокер не уходят.
    async with th.batch("nightly", key="2026-10-01", start_at=clock.now() + timedelta(minutes=30)) as batch:
        await batch.add_calls(th.call(work, number).opts(max_retries=1) for number in range(3))
    assert await broker.drain() == 0
    clock.advance(minutes=30)

    # Падение воркера посреди задачи: аренда взята, итог не записан.
    broker.kill_worker_after(2)
    await broker.drain()
    stuck = await batch.handle.view()
    assert stuck.state is BatchState.SEALED
    assert (stuck.progress.ok, stuck.progress.in_flight) == (2, 1)

    # Аренда истекла: фоновые проверки возвращают задачу в очередь.
    clock.advance(seconds=61)
    await th.run_maintenance_once()
    await broker.drain()

    assert (await batch.handle.view()).state is BatchState.SUCCEEDED
    assert sorted(done) == [0, 1, 2]
finally:
    await broker.close()
```

| Член `FakeClock` | Что делает |
|---|---|
| `FakeClock(current)` | часы в заданный момент; `datetime` без часового пояса — `ConfigurationError` |
| `clock.now()` | текущий момент |
| `clock.advance(delta=None, **parts)` | сдвинуть вперёд: `advance(timedelta(seconds=1))` или `advance(hours=2, minutes=5)`; возвращает новое время. Отрицательный сдвиг — `ConfigurationError` |

Замечания:

* Задача, чья аренда истекла, возвращается в очередь, только если у неё остались попытки
  (`max_retries`); иначе она завершается с меткой `lease_expired`. Поэтому в примере выше задан
  `max_retries=1`.
* `th.run_maintenance_once()` выполняет один полный проход: страховочную отправку, все фоновые
  проверки (истёкшие аренды, пропущенные финализации, повтор упавших хуков, дедлайны, retention) и
  один цикл снимков прогресса. В тестах он заменяет [процесс maintenance](operations.md#процессы).
* Без `FakeClock` клиент использует время базы данных — так он работает в продакшне.

## pytest-фикстура

Плагин `tallyho.testing.pytest_plugin` даёт фикстуру `tallyho_env`: клиент с `InlineBroker` и
`FakeClock`, таблицы уже созданы, брокер закрывается после теста. Нужен extra `testing`
(`pytest-asyncio`).

Плагин ожидает от вашего проекта две фикстуры: `engine` (`AsyncEngine`) и `schema` (имя схемы для
этого теста).

<!-- tallyho-noexec: conftest.py и тест выполняет pytest вашего проекта; DSN и фикстуры схемы — ваши -->
```python
# conftest.py
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

pytest_plugins = ["tallyho.testing.pytest_plugin"]


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    value = create_async_engine(os.environ["TEST_DATABASE_URL"])
    try:
        yield value
    finally:
        await value.dispose()


@pytest_asyncio.fixture
async def schema(engine: AsyncEngine) -> AsyncIterator[str]:
    name = f"test_{uuid4().hex}"
    try:
        yield name  # схему создаст migrate() внутри tallyho_env
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))


# test_reports.py
import pytest

from tallyho.model.states import BatchState
from tallyho.testing import TallyhoTestEnv


async def build(section: int) -> None: ...


@pytest.mark.asyncio
async def test_report_is_built(tallyho_env: TallyhoTestEnv) -> None:
    async with tallyho_env.th.batch("report_build", key="report:1") as batch:
        await batch.map(build, range(3))
    await tallyho_env.drain()
    assert (await batch.handle.view()).state is BatchState.SUCCEEDED
```

| Член `TallyhoTestEnv` | Значение |
|---|---|
| `th`, `broker`, `clock`, `engine`, `schema` | установленный клиент, `InlineBroker`, `FakeClock`, движок и схема |
| `await env.step(n=1)`, `await env.drain()` | то же, что у брокера |
| `await env.run_maintenance_once()` | один проход фоновых проверок |
| `await env.close()` | остановить брокер; фикстура вызывает сама |

Хуки в таком тесте регистрируются на `tallyho_env.th` до создания батча. Если приложению нужна
своя сборка клиента (свои `hook_modules`, настройки, наблюдатель), напишите собственную фикстуру по
образцу: `FakeClock` → `InlineBroker` → `Tallyho(...)` → `install` → `migrate` → `yield` →
`broker.close()`.

## Что проверять

* **Итог в ваших таблицах**, а не только состояние батча: хук `on_finalized` — часть сценария.
* **Повторную доставку** (`duplicate_delivery_rate`) и **падение воркера** (`kill_worker_after`).
* **Падение хука**: батч остаётся `SEALED`, а после исправления финализируется — см.
  [повтор упавшего хука](hooks.md#повтор-упавшего-хука).
* **Удаление по retention**: после `clock.advance(...)` и `run_maintenance_once()` данные в ваших
  таблицах остаются на месте — см. [рецепт](hooks.md#рецепт-строка-на-каждого-получателя).
* **Реальный брокер** — отдельным небольшим набором сквозных тестов: `InlineBroker` не заменяет
  проверку конфигурации воркеров и опций брокера.

## Что дальше

* Подключить настоящий брокер — [Адаптер flexiq](flexiq.md).
