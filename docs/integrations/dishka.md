# Dishka: единица работы как зависимость

Фасад `item` удобен для скриптов: он находит текущую задачу через контекст выполнения и работает на
любой глубине стека. Слоистому приложению этого мало. Адаптер, который пишет
[закладку](../guide/batches/checkpoint.md) или итог в вашей транзакции, должен объявлять единицу
работы в конструкторе: тогда зависимость видно в типе, её подменяют в тестах, а вне задачи она
даёт ошибку, а не тишину.

Extra `tallyho[dishka]` даёт обёртку, которая открывает REQUEST-scope контейнера на каждую попытку
и кладёт в него текущий `WorkItem`.

## Установка

```bash
pip install 'tallyho[dishka]'
```

Обычный `import tallyho` dishka не требует и не импортирует: интеграция живёт в отдельном модуле
`tallyho.integrations.dishka`.

## Подключение

<!-- tallyho-noexec: модуль приложения: контейнер, интерактор и очередь принадлежат вашему проекту -->
```python
# app/wiring.py
from dishka import make_async_container

from tallyho import Tallyho
from tallyho.integrations.dishka import DishkaItemScope, TallyhoProvider

container = make_async_container(AppProvider(), TallyhoProvider())
th = Tallyho(engine, schema="app", middleware=[DishkaItemScope(container)])
```

* `TallyhoProvider` отдаёт `WorkItem` в REQUEST-scope. Он не создаёт объект, а берёт его из
  контекста scope, поэтому вне задачи его в контейнере нет.
* `DishkaItemScope` - это [`ItemMiddleware`](../guide/batches/tasks.md#обёртки-вокруг-задачи):
  обёртка вокруг вызова каждой отслеживаемой задачи. Scope закрывается и на успехе, и на
  исключении, и на отмене.
* Ставьте её первой в списке `middleware`, если хотите, чтобы REQUEST-scope охватывал остальные
  обёртки.

## Задача и интерактор

<!-- tallyho-noexec: модуль приложения: интерактор и доменные исключения принадлежат вашему проекту -->
```python
# app/tasks.py
from tallyho import Defer, Ok
from tallyho.integrations.dishka import FromDishka, inject


@fq.task(
    queue="checks",
    defer_on={NoCheckerAccount: lambda exc: Defer(reason=exc.reason, delay=exc.retry_after)},
)
@inject
async def check_domain_chunk(
    run_id: str, domains: list[str], interactor: FromDishka[CheckDomainChunk]
) -> Ok:
    checked = await interactor(CheckDomainChunkCommand(UUID(run_id), domains))
    return Ok("chunk", result={"checked": checked})
```

Интерактор не импортирует tallyho: он бросает свои доменные исключения, а их
[судьбу](../guide/batches/tasks.md#судьба-исключений) объявляет регистрация задачи. Закладку пишет
инфраструктурный адаптер, который получает `WorkItem` и сессию из контейнера:

<!-- tallyho-noexec: фрагмент приложения: порт ChunkProgress объявлен в вашем application-слое -->
```python
# app/adapters/progress.py
from sqlalchemy.ext.asyncio import AsyncSession

from tallyho import WorkItem


class TallyhoChunkProgress:
    """Прогресс пачки. WorkItem и сессия запроса приходят из контейнера."""

    def __init__(self, work: WorkItem, session: AsyncSession) -> None:
        self._work = work
        self._session = session

    def position(self) -> int:
        return self._work.checkpoint() or 0

    async def save(self, position: int) -> None:
        await self._work.checkpoint_in(self._session, position)
```

## Что учесть

* `@inject` ставится **под** декоратором задачи: сначала `@fq.task(...)`, затем `@inject`, затем
  функция. Так брокер регистрирует уже подготовленную функцию.
* `@inject` убирает `FromDishka`-параметры из видимой сигнатуры, поэтому
  `th.call(check_domain_chunk, run_id, domains)` типизируется без них. Взамен у такой задачи
  проверяется только тип результата: «сигнатура минус несколько параметров» в системе типов не
  выражается.
* Порядок аргументов задачи остаётся вашим: внедряемые параметры при вызове не передаются и в
  payload не попадают.
* Вне задачи (в скрипте, в юнит-тесте интерактора) scope не открыт, и разрешение `WorkItem` даёт
  понятную ошибку контейнера. Это сознательно: тихий no-op в DI прятал бы потерю итога.
* В юнит-тестах application-слоя подменяйте не `item`, а свой порт (`ChunkProgress`): tallyho там
  вообще не нужен.
