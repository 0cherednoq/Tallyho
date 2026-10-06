# Тестирование

## `InlineBroker`

`InlineBroker` - полноценный адаптер брокера. Задачи проходят тот же путь, что и в продакшне:
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
        raise TimeoutError("провайдер не ответил")  # исключение ведёт к ретраю брокера
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
    await th.aclose()
```

| Член `InlineBroker` | Что делает |
|---|---|
| `InlineBroker(duplicate_delivery_rate=0.0, seed=None, serializer=None, max_retries=0)` | `duplicate_delivery_rate` - доля сообщений, доставляемых дважды; `seed` - зерно генератора дублей; `max_retries` - число повторов для вызовов без своей опции `max_retries` |
| `broker.adapter` | то, что передаётся в `th.install(...)` |
| `await broker.step(n=1)` | выполнить не больше `n` доставок; возвращает число выполненных |
| `await broker.drain(concurrency=1)` | выполнять до простоя: пока есть сообщения и пока появляются новые. `concurrency` имитирует пул воркеров |
| `broker.pending` | сообщений в очереди |
| `broker.deliveries` | сколько доставок взято воркером всего, включая дубли |
| `broker.dead_letters` | сообщения, исчерпавшие попытки |
| `broker.kill_worker_after(n)` | на `n`-й следующей доставке «убить» воркер: задача захвачена, но не выполнена и не завершена |
| `await broker.close()` | остановить только воркер брокера. В конце теста вызывайте `await th.aclose()`: он останавливает и воркер, и остальную фоновую работу |

Поведение, которое стоит знать:

Ретраи
: Исключение в задаче - повод для повторной доставки. Число повторов задаёт опция
  вызова `max_retries`: `th.call(fn, ...).opts(max_retries=2)`. Для вызовов без этой опции действует
  `InlineBroker(max_retries=...)` (по умолчанию 0) - так в тесте изображается лимит, который в
  продакшне задан в декораторе задачи. Когда попытки исчерпаны, задача получает итог
  `error("exhausted")`, а сообщение попадает в `dead_letters`.

Сверка с DLQ
: `drain()` её не выполняет. Если в тесте сообщение попало в `dead_letters`, а
  итог задачи не записан, его запишет `await th.run_maintenance_once()` - так же, как
  [в продакшне](operations.md#сверка-с-dlq-брокера).

Порядок
: `step` и `drain()` выполняют сообщения последовательно, в порядке очереди; каждая
  следующая задача стартует после того, как итог предыдущей записан и применены его последствия
  (политика ошибок, финализация, колбэки).

Колбэк-задачи
: Доставляются через ту же очередь и выполняются тем же `drain()`.

Функции задач
: Это обычные `async def`, оборачивать их декоратором не нужно. Брокер различает
  задачи по имени `модуль.имя_функции`, поэтому две разные функции с одинаковым полным именем в
  одном брокере - `ConfigurationError`.

Никакой фоновой отправки
: С настоящим адаптером сообщения уходят в брокер сразу после
  коммита; с `InlineBroker` - только когда тест вызвал `step()` или `drain()`. Поэтому
  `broker.pending` и порядок выполнения не зависят от времени.

Отложенные и возвращённые сообщения
: `step()` отправляет в очередь только то, что записано
  только что. Сообщения, срок которых наступил позже (отложенный старт, возврат задачи после
  истёкшей аренды), подбирают `drain()` и `th.run_maintenance_once()` - и только когда они старше
  `relay_grace` (5 секунд). В тестах с `FakeClock` задайте `relay_grace=timedelta(0)` или сдвигайте
  часы с запасом.

Что не ждёт `drain()`
: Финализация, которую запускает управляющая операция, когда в очереди
  нет задач (`retry_finalize()`, отмена батча на паузе, создание пустого батча), идёт в фоне.
  Дождитесь её явно: `await handle.wait(timeout=10)`.

Проверка устойчивости
: Запускайте важные сценарии с ненулевым `duplicate_delivery_rate`: так тест
  проверяет, что повторная доставка не выполняет задачу дважды и не ломает ваши хуки.

## `FakeClock`

Все сроки tallyho - отложенный старт, аренда задач, дедлайны, retention, интервалы снимков и
повторов хуков - отсчитываются от часов клиента. `FakeClock` передаётся в `Tallyho(clock=...)` и
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
    await th.aclose()
```

| Член `FakeClock` | Что делает |
|---|---|
| `FakeClock(current)` | часы в заданный момент; `datetime` без часового пояса - `ConfigurationError` |
| `clock.now()` | текущий момент |
| `clock.advance(delta=None, **parts)` | сдвинуть вперёд: `advance(timedelta(seconds=1))` или `advance(hours=2, minutes=5)`; возвращает новое время. Отрицательный сдвиг - `ConfigurationError` |

Замечания:

* Задача, чья аренда истекла, возвращается в очередь, только если у неё остались попытки
  (`max_retries` вызова, иначе лимит брокера); иначе она завершается с меткой `lease_expired`.
  Каждый такой возврат тратит попытку. Поэтому в примере выше задан `max_retries=1`.
* `th.run_maintenance_once()` выполняет один полный проход: страховочную отправку, все фоновые
  проверки (истёкшие аренды, пропущенные финализации, повтор упавших хуков, дедлайны, retention) и
  один цикл снимков прогресса. В тестах он заменяет [процесс maintenance](operations.md).
* Без `FakeClock` клиент использует время базы данных - так он работает в продакшне.

## pytest

Готовая фикстура `tallyho_env` собирает всё перечисленное: клиент с `InlineBroker` и `FakeClock`,
созданные таблицы и закрытие установки после теста. Подключение описано на странице
[pytest](../integrations/pytest.md).

## Закрывайте установку до удаления схемы

После коммита tallyho продолжает работать в фоне: финализирует батч, публикует прогресс. Тест к
этому моменту может уже закончиться, и фикстура начнёт удалять схему. Фоновое чтение таблиц и
`DROP SCHEMA` блокируют друг друга, PostgreSQL обнаруживает дедлок и прерывает один из запросов -
тест падает в teardown, причём не каждый раз.

`await th.aclose()` дожидается всей фоновой работы, поэтому порядок в фикстуре такой: сначала
`aclose`, потом удаление схемы и `engine.dispose()`. `tallyho_env` делает это сама: фикстура
`schema` из вашего `conftest.py` завершается позже неё. Если тест создаёт клиент вручную, закройте
его в `finally`, как в примерах на этой странице.

Закрытой установкой пользоваться нельзя: запись бросает `ClosedError`. Читать состояние батча
(`handle.view()`) после `aclose` можно - удобно для итоговых проверок.

## Что проверять

* Итог в ваших таблицах, а не только состояние батча: хук `on_finalized` - часть сценария.
* Повторную доставку (`duplicate_delivery_rate`) и падение воркера (`kill_worker_after`).
* Падение хука: батч остаётся `SEALED`, а после исправления финализируется - см.
  [повтор упавшего хука](hooks/retry.md).
* Удаление по retention: после `clock.advance(...)` и `run_maintenance_once()` данные в ваших
  таблицах остаются на месте - см. [рецепт](hooks/recipe.md).
* Реальный брокер - отдельным небольшим набором сквозных тестов: `InlineBroker` не заменяет
  проверку конфигурации воркеров и опций брокера.
