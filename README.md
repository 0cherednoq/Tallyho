# tallyho

[![CI](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml/badge.svg)](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/tallyho.svg)](https://pypi.org/project/tallyho/)
[![Python](https://img.shields.io/pypi/pyversions/tallyho.svg)](https://pypi.org/project/tallyho/)

Async-библиотека для Python + PostgreSQL. Добавляет к любому брокеру задач групповой учёт
(батчи, вложенные батчи, прогресс, финализация ровно один раз), динамический fan-out,
конвейеры этапов и транзакционные хуки в доменные таблицы.

> Статус: pre-alpha, идёт реализация. Архитектура — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Установка

```bash
pip install "tallyho[asyncpg]"            # или tallyho[psycopg]
pip install "tallyho[asyncpg,flexiq]"     # с адаптером flexiq
```

## Быстрый старт

Ниже используется встроенный `InlineBroker`: пример действительно извлекается из README и
выполняется в CI на PostgreSQL.

<!-- tallyho-example: readme-quickstart -->
```python
from tallyho import Tallyho, item
from tallyho.model.states import BatchState
from tallyho.testing import InlineBroker

broker = InlineBroker()
th = Tallyho(engine, schema=schema)
th.install(broker.adapter)
seen: list[str] = []


async def greet(name: str) -> None:
    seen.append(name)
    item.ok("greeted")


await th.migrate()
try:
    async with th.batch("readme.quickstart", key="demo") as batch:
        await batch.map(greet, ["Ada", "Grace"])

    assert await broker.drain(concurrency=2) == 2
    view = await batch.handle.view()
    assert view.state is BatchState.SUCCEEDED
    assert view.progress.ok == 2
    assert sorted(seen) == ["Ada", "Grace"]
finally:
    await broker.close()
```

## Что ещё умеет

* **Атрибуты и листинг.** `th.batch(..., attributes={"tenant": "acme", "campaign_id": 42})` сохраняет
  неизменяемый контекст корня; `await th.list_batches(kinds=[...], attributes={...})` находит батчи
  по нему, постранично и без чтения счётчиков.
* **Исходы каждой задачи.** `handle.items(states={ItemState.ERROR, ItemState.CANCELLED})` перечисляет
  Items батча, включая отменённые и упавшие без участия кода задачи. Как перенести их в свою таблицу
  до удаления по retention — рецепт «строка на каждого получателя» в
  [ARCHITECTURE §12.9](docs/ARCHITECTURE.md#129-вариант-строка-на-каждого-получателя).

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
