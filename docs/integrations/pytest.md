# pytest

Плагин `tallyho.testing.pytest_plugin` даёт фикстуру `tallyho_env`: клиент с `InlineBroker` и
`FakeClock`, таблицы уже созданы, установка закрывается после теста.

## Установка

```bash
pip install "tallyho[asyncpg,testing]"
```

Дополнение `testing` ставит `pytest-asyncio`.

## Подключение

Плагин ожидает от вашего проекта две фикстуры: `engine` (`AsyncEngine`) и `schema` (имя схемы для
этого теста).

<!-- tallyho-noexec: conftest.py и тест выполняет pytest вашего проекта; DSN и фикстуры схемы - ваши -->
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

## Что есть в `tallyho_env`

| Член `TallyhoTestEnv` | Значение |
|---|---|
| `th`, `broker`, `clock`, `engine`, `schema` | установленный клиент, `InlineBroker`, `FakeClock`, движок и схема |
| `await env.step(n=1)`, `await env.drain()` | то же, что у брокера |
| `await env.run_maintenance_once()` | один проход фоновых проверок |
| `await env.close()` | закрыть установку (`th.aclose()`); фикстура вызывает сама |

## Своя фикстура

Хуки в тесте регистрируются на `tallyho_env.th` до создания батча. Если приложению нужна
своя сборка клиента (свои `hook_modules`, настройки, наблюдатель), напишите собственную фикстуру по
образцу. Порядок такой: `FakeClock`, `InlineBroker`, `Tallyho(...)`, `install`, `migrate`, `yield` и в
конце `th.aclose()`.

Как пользоваться `InlineBroker` и `FakeClock` в самих тестах, описано на странице
[Тестирование](../guide/testing.md).
