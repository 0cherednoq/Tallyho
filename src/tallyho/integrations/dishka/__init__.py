"""Интеграция единицы работы с Dishka: extra ``tallyho[dishka]`` (ARCHITECTURE §11.2).

Фасад ``item`` удобен для скриптов, но слоистому приложению нужна зависимость:
адаптер, который пишет закладку или итог, должен объявлять единицу работы в
конструкторе, а не достукиваться до неё из глубины стека. Эта интеграция
открывает REQUEST-scope на каждую попытку и кладёт в него текущий ``WorkItem``:

```python
container = make_async_container(AppProvider(), TallyhoProvider())
th = Tallyho(engine, schema="ledger", middleware=[DishkaItemScope(container)])


@fq.task(queue="checks", defer_on={NoCheckerAccount: to_defer})
@inject
async def check_domain_chunk(
    run_id: str, domains: list[str], interactor: FromDishka[CheckDomainChunk]
) -> Ok:
    checked = await interactor(CheckDomainChunkCommand(UUID(run_id), domains))
    return Ok("chunk", result={"checked": checked})
```

``@inject`` убирает ``FromDishka``-параметры из сигнатуры, поэтому
``th.call(check_domain_chunk, run_id, domains)`` типизируется без них, а адаптер
брокера видит задачу такой же, как без DI. Вне задачи scope не открыт, и
разрешение зависимости даёт понятную ошибку, а не тихий no-op.
"""

from __future__ import annotations

from dishka import FromDishka

from tallyho.integrations.dishka.provider import TallyhoProvider
from tallyho.integrations.dishka.scope import DishkaItemScope, current_container, inject

__all__ = [
    "DishkaItemScope",
    "FromDishka",
    "TallyhoProvider",
    "current_container",
    "inject",
]
