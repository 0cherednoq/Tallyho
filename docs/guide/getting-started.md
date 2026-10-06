# Быстрый старт

## Установка

Нужны:

* CPython 3.11 или новее;
* PostgreSQL 14 или новее;
* SQLAlchemy 2.1 в async-режиме и один из драйверов, `asyncpg` или `psycopg`.

::::{tab-set}

:::{tab-item} uv
```bash
uv add "tallyho[asyncpg]"
```
:::

:::{tab-item} poetry
```bash
poetry add "tallyho[asyncpg]"
```
:::

:::{tab-item} pip
```bash
pip install "tallyho[asyncpg]"
```
:::

::::

Драйвер ставится дополнением (extra), нужен один из двух:

* `tallyho[asyncpg]` для `asyncpg`;
* `tallyho[psycopg]` для `psycopg` 3.

Остальные дополнения подключают возможности:

* `tallyho[flexiq]` - адаптер брокера [flexiq](../integrations/flexiq.md);
* `tallyho[alembic]` - миграции tallyho внутри ваших ревизий [Alembic](../integrations/alembic.md);
* `tallyho[testing]` - [pytest-фикстура](../integrations/pytest.md) с готовой установкой;
* `tallyho[otel]` - наблюдатель для [OpenTelemetry](../integrations/opentelemetry.md).

:::{important}
Таблицы tallyho и ваши доменные таблицы должны лежать в одной базе PostgreSQL. Схемы могут
быть разными. Только так запись итога в вашу таблицу и завершение батча попадают в одну
транзакцию.
:::

:::{note}
Версия 1.0 ещё не выпущена. Пакет помечен как pre-alpha.
:::

## Первый батч

Батч - группа задач с общим учётом. Скрипт ниже рассылает три письма и печатает итог. В нём
работает встроенный `InlineBroker`: он выполняет задачи в том же процессе, поэтому для
знакомства хватит PostgreSQL и одного файла.

<!-- tallyho-noexec: самостоятельный скрипт: нужен ваш DSN; сценарий guide-start-first-batch выполняется в tests/examples/guide_scenarios.md -->
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

1. `th.batch(kind)` создал батч и записал три задачи одной транзакцией. В приложении батчу
   обычно дают ключ, `th.batch(kind, key="issue:1")`: повторный вызов с тем же ключом второй
   батч не создаст, и двойной клик по кнопке «запустить» рассылку не удвоит.
2. При выходе из `async with` батч закрылся, транзакция закоммитилась. Только после этого задачи
   уходят в брокер.
3. Каждая задача записала свой итог. После последней батч финализировался, ровно один
   раз.
4. `handle.view()` вернул состояние, счётчики и разбивку по меткам.

## Про примеры в этой документации

Дальше код показан так, как он выглядел бы в приложении: задачи объявлены через адаптер
[flexiq](../integrations/flexiq.md), батчи создаются в обработчиках API, итог пишется хуками в ваши таблицы.
Объекты `th`, `fq` и `engine` приходят из модуля приложения, он целиком показан в разделе
[Разбор на примерах](tutorial/overview.md). Имена вроде `mail`, `storage` или `reports`
обозначают ваш прикладной слой, его реализация для разговора о батчах не важна.

Что код вернёт или запишет, сказано в комментариях под ним. Числа в комментариях условные, а
поведение настоящее: на каждый пример в репозитории есть сценарий с проверками, который
выполняется в CI на PostgreSQL (`tests/examples/guide_scenarios.md`).

Страница [Тестирование](testing.md) устроена иначе. Там примеры и есть тесты, поэтому в них
остались `assert`.

## Итог в вашей таблице

Счётчики tallyho удаляются по сроку хранения, а итог рассылки нужен навсегда и в вашей таблице.
Для этого есть [хуки](hooks.md): функция, которую tallyho вызывает внутри транзакции
финализации.

<!-- tallyho-noexec: фрагмент использует таблицу campaigns вашего приложения; рабочий пример - на странице «Хуки» -->
```python
@th.on_finalized("newsletter")
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    await session.execute(
        update(campaigns)
        .where(campaigns.c.batch_id == summary.id)
        .values(status="done", sent=summary.progress.ok, failed=summary.progress.error)
    )
```

Если хук упал, батч не станет завершённым. Обратное тоже верно: завершённый батч означает, что
ваша строка обновлена.

## Настоящий брокер

`InlineBroker` годится для тестов и знакомства. В продакшне задачи исполняет брокер, tallyho
подключается к нему адаптером. Первый поддерживаемый брокер - flexiq:

<!-- tallyho-noexec: модуль приложения: нужен ваш DSN и процесс воркера flexiq -->
```python
from flexiq import Queue

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter

queue = Queue(backend="postgres", db_url="postgresql://app:secret@db/app", schema="flexiq")
th = Tallyho(engine, schema="app")
fq = FlexiqAdapter(queue)
th.install(fq)


@fq.task(max_retries=4, queue="mail")
async def send_email(address: str) -> None:
    ...
    item.ok("sent")
```

Код, который создаёт батчи, при этом не меняется. Подробности - на странице
[Адаптер flexiq](../integrations/flexiq.md).

## Для LLM-ассистентов

Если вы пишете код с ИИ-ассистентом, дайте ему документацию целиком. В корне сайта лежат два
файла: `llms.txt` с оглавлением и ссылками на Markdown-версии страниц и `llms-full.txt` со всей
документацией одним файлом. Кнопка в заголовке каждой страницы копирует её как Markdown.

## Куда дальше

| Задача | Страница |
|---|---|
| Разобраться в терминах и гарантиях | [Основные понятия](concepts.md) |
| Посмотреть два приложения целиком: проверка аккаунтов и экспорт почты | [Разбор на примерах](tutorial/overview.md) |
| Создать таблицы через `migrate()`, Alembic или CLI | [Установка и миграции](installation.md) |
| Под-батчи, конвейеры, политики ошибок, пауза и отмена | [Батчи](batches.md) |
| Записать итог и прогресс в свои таблицы | [Хуки](hooks.md) |
| Проверить сценарий без брокера | [Тестирование](testing.md) |
| Запустить в продакшне | [Процессы и maintenance](operations.md) |
| Узнать, чего в первой версии нет | [Ограничения v1](limitations.md) |
