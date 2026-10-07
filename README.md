# tallyho

[![CI](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml/badge.svg)](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg)](https://0cherednoq.github.io/Tallyho/)
[![PyPI](https://img.shields.io/pypi/v/tallyho.svg?cacheSeconds=300)](https://pypi.org/project/tallyho/)
[![Python](https://img.shields.io/pypi/pyversions/tallyho.svg?cacheSeconds=300)](https://pypi.org/project/tallyho/)

Async-библиотека для Python и PostgreSQL. Она добавляет к брокеру задач то, чего в нём обычно
нет: учёт группы задач (батчи, вложенные батчи, прогресс, финализация ровно один раз), задачи,
которые порождают задачи, конвейеры этапов и хуки, которые пишут итог в ваши таблицы той же
транзакцией.

> Статус: pre-alpha, версия 1.0 ещё не выпущена. Документация собирается в сайт из
> [docs/](docs/index.md) и публикуется на [GitHub Pages](https://0cherednoq.github.io/Tallyho/). Как библиотека
> устроена внутри, описано в [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Что это даёт

* Батч считает, сколько задач найдено, сделано, упало и сколько осталось. Один запрос отдаёт
  снимок всего дерева под-батчей вместе с оценкой времени до конца.
* Итог батча записывается в вашу таблицу в той же транзакции, в которой батч становится
  завершённым. Статус кампании и состояние батча не расходятся.
* Задачи порождают задачи, этапы конвейера работают параллельно, и следующий этап закрывается
  сам, когда закончились его источники.
* Группой можно управлять: отложенный старт, пауза, отмена, повтор упавших, ограничение
  параллелизма, политики ошибок.
* При падении воркера, брокера или сети задачи не теряются, и батч доходит до итога.

Исполнение задач, ретраи и расписания остаются за брокером, бизнес-статусы живут в ваших
таблицах.

## Установка

Нужны Python 3.11 или новее и PostgreSQL 14 или новее.

```bash
pip install "tallyho[asyncpg]"            # или tallyho[psycopg]
pip install "tallyho[asyncpg,flexiq]"     # с адаптером flexiq
```

## Быстрый старт

Скрипт ниже работает со встроенным `InlineBroker`: он выполняет задачи в том же процессе,
поэтому для знакомства хватит PostgreSQL и одного файла.

<!-- tallyho-noexec: самостоятельный скрипт: нужен ваш DSN; сценарий readme-quickstart выполняется в tests/examples/guide_scenarios.md -->
```python
# quickstart.py
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho, item
from tallyho.testing import InlineBroker

engine = create_async_engine("postgresql+asyncpg://app:secret@localhost/app")
broker = InlineBroker()  # выполняет задачи в этом же процессе; в продакшне здесь адаптер брокера
th = Tallyho(engine, schema="quickstart")
th.install(broker.adapter)


async def send_email(address: str) -> None:
    if address.endswith("@bounce.test"):
        item.error("hard_bounce")  # ошибка без исключения и без ретраев
        return
    item.ok("sent")  # итог задачи с меткой


async def main() -> None:
    await th.migrate()  # создать схему и таблицы
    async with th.batch("newsletter") as batch:
        await batch.map(send_email, ["ada@ok.test", "grace@ok.test", "gone@bounce.test"])
    # выход из блока: батч закрыт, транзакция закоммичена

    await broker.drain()  # выполнить задачи; в продакшне это делают воркеры

    view = await batch.handle.view()
    print(view.state.name, view.progress.found, view.progress.ok, view.progress.error)
    print(dict(view.labels))

    await th.aclose()
    await engine.dispose()


asyncio.run(main())

# COMPLETED_WITH_ERRORS 3 2 1
# {'sent': 2, 'hard_bounce': 1}
```

Что здесь произошло:

1. `th.batch(kind)` создал батч и записал три задачи одной транзакцией. С ключом,
   `th.batch(kind, key=...)`, повторный вызов второй батч не создаст.
2. При выходе из `async with` батч закрылся, а транзакция закоммитилась; после этого задачи
   отправляются в брокер.
3. Каждая задача записала свой итог; после последней батч финализировался - ровно один раз.
4. `handle.view()` вернул состояние и счётчики.

## Дальше

| Задача | Страница |
|---|---|
| Разобраться в терминах и гарантиях | [Основные понятия](docs/guide/concepts.md) |
| Посмотреть два приложения целиком на flexiq: проверка аккаунтов и экспорт почты | [Разбор на примерах](docs/guide/tutorial/overview.md) |
| Поставить пакет, создать таблицы (`migrate`, Alembic, CLI) | [Установка и миграции](docs/guide/installation.md) |
| Под-батчи, конвейеры `fed_by`, `spawn`, политики ошибок, пауза и отмена, прогресс, атрибуты и листинг | [Батчи](docs/guide/batches.md) |
| Записать итог и прогресс в свои таблицы, retention и `release()` | [Хуки](docs/guide/hooks.md) |
| Тесты с `InlineBroker` и `FakeClock` | [Тестирование](docs/guide/testing.md) |
| Подключить брокер flexiq | [Адаптер flexiq](docs/integrations/flexiq.md) |
| Процессы и maintenance, остановка, autovacuum, pgbouncer, метрики | [Процессы и maintenance](docs/guide/operations.md) |
| Что не входит в v1 | [Ограничения v1](docs/guide/limitations.md) |

Для ИИ-ассистентов в корне лежит [llms.txt](llms.txt): краткие правила работы с библиотекой и
карта документации со ссылками на файлы репозитория.

## Разработка

Нужен [uv](https://docs.astral.sh/uv/) и (для интеграционных тестов) Docker.

```bash
uv sync --all-extras               # окружение + все инструменты
uv run pre-commit install          # хуки на commit и push
uv run poe check                   # всё, что проверяет CI (кроме интеграции)
uv run poe test-all                # + интеграционные тесты с PostgreSQL
```

Сайт документации собирается командой `uv run --group docs poe docs`. Подробнее -
[CONTRIBUTING.md](CONTRIBUTING.md).

## Лицензия

MIT
