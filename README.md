# tallyho

[![CI](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml/badge.svg)](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/tallyho.svg)](https://pypi.org/project/tallyho/)
[![Python](https://img.shields.io/pypi/pyversions/tallyho.svg)](https://pypi.org/project/tallyho/)

Async-библиотека для Python + PostgreSQL. Добавляет к любому брокеру задач групповой учёт
(батчи, вложенные батчи, прогресс, финализация ровно один раз), динамический fan-out,
конвейеры этапов и транзакционные хуки в доменные таблицы.

> Статус: pre-alpha, идёт реализация. Руководство — [docs/guide](docs/guide/README.md),
> архитектура — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Что это даёт

* **Батч как единица учёта.** Сколько задач найдено, сделано, упало, сколько осталось и когда
  закончится — одним запросом, для всего дерева под-батчей.
* **Финализация ровно один раз.** Итог батча записывается в вашу таблицу в той же транзакции, в
  которой батч становится завершённым: статус кампании и состояние батча не расходятся.
* **Динамический fan-out и конвейеры.** Задачи порождают задачи, этапы работают параллельно, и
  следующий этап сам закрывается, когда закончились его источники.
* **Управление группой.** Отложенный старт, пауза, отмена, повтор упавших, ограничение
  параллелизма, политики ошибок.
* **Ничего не зависает.** Падение воркера, брокера или сети не теряет задачи и не оставляет батч
  незавершённым.

Исполнение задач, ретраи и расписания остаются за брокером; бизнес-статусы — в ваших таблицах.

## Установка

Нужны Python ≥ 3.11 и PostgreSQL ≥ 14.

```bash
pip install "tallyho[asyncpg]"            # или tallyho[psycopg]
pip install "tallyho[asyncpg,flexiq]"     # с адаптером flexiq
```

## Быстрый старт

Ниже используется встроенный `InlineBroker`: пример действительно извлекается из README и
выполняется в CI на PostgreSQL. В нём уже определены `engine` (`AsyncEngine` SQLAlchemy) и `schema`
(имя схемы для таблиц tallyho).

<!-- tallyho-example: readme-quickstart -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()  # в продакшне — адаптер вашего брокера
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

Что здесь произошло:

1. `th.batch(kind, key=...)` создал батч и записал две задачи одной транзакцией; повторный вызов с
   тем же ключом второй батч не создаст.
2. При выходе из `async with` батч закрылся, а транзакция закоммитилась; после этого задачи
   отправляются в брокер.
3. Каждая задача записала свой итог; после последней батч финализировался — ровно один раз.
4. `handle.view()` вернул состояние и счётчики.

## Дальше

| Задача | Страница руководства |
|---|---|
| Поставить пакет, создать таблицы (`migrate`, Alembic, CLI) | [Установка и миграции](docs/guide/installation.md) |
| Под-батчи, конвейеры `fed_by`, `spawn`, политики ошибок, пауза и отмена, прогресс, атрибуты и листинг | [Батчи и конвейеры](docs/guide/batches.md) |
| Записать итог и прогресс в свои таблицы, retention и `release()` | [Хуки](docs/guide/hooks.md) |
| Тесты с `InlineBroker` и `FakeClock` | [Тестирование](docs/guide/testing.md) |
| Подключить брокер flexiq | [Адаптер flexiq](docs/guide/flexiq.md) |
| Процессы, autovacuum, pgbouncer, метрики | [Эксплуатация PostgreSQL](docs/guide/operations.md) |
| Что не входит в v1 | [Ограничения v1](docs/guide/limitations.md) |

## Разработка

Нужен [uv](https://docs.astral.sh/uv/) и (для интеграционных тестов) Docker.

```bash
uv sync --all-extras               # окружение + все инструменты
uv run pre-commit install          # хуки на commit и push
uv run poe check                   # всё, что проверяет CI (кроме интеграции)
uv run poe test-all                # + интеграционные тесты с PostgreSQL
```

Подробнее — [CONTRIBUTING.md](CONTRIBUTING.md).

## Лицензия

MIT
