# Пауза, отмена, повтор

Все операции вызываются на `BatchHandle` и действуют на батч вместе с его под-батчами.

| Операция | Что делает |
|---|---|
| `await handle.reschedule(start_at)` | переносит старт ещё не отправленных задач; возвращает число уже отправленных, которых перенос не коснулся |
| `await handle.pause()` | пауза: новые задачи не отправляются, пришедшие из брокера откладываются без выполнения, выполняющиеся доделываются |
| `await handle.resume()` | снимает паузу |
| `await handle.cancel()` | запрос отмены: неотправленные задачи сразу `cancelled`, отправленные отменяются при получении воркером, выполняющиеся доделываются; итог - `CANCELLED` |
| `await handle.retry_failed(labels=None)` | возвращает ошибочные задачи в работу; возвращает их число |
| `await handle.retry_finalize()` | немедленно повторяет финализацию, если [хук упал](../hooks/retry.md) |
| `await handle.release()` | разрешает удалить дерево по [retention](../hooks/retention.md) |

Каждая операция принимает `session=` и тогда выполняется в вашей транзакции.

<!-- tallyho-noexec: фрагмент приложения: deliver и маршруты принадлежат вашему проекту -->
```python
# app/api.py
async def schedule_route(route_id: int, parcel_ids: list[int], start: datetime) -> None:
    async with th.batch("deliveries", key=f"route:{route_id}", start_at=start) as batch:
        await batch.map(deliver, parcel_ids)
    # до start в брокер ничего не уходит


async def postpone_route(route_id: int, start: datetime) -> int:
    handle = await th.find("deliveries", f"route:{route_id}")
    return await handle.reschedule(start)  # 0: перенос коснулся всех задач, ни одна ещё не отправлена


async def pause_route(route_id: int) -> None:
    handle = await th.find("deliveries", f"route:{route_id}")
    await handle.pause()
    view = await handle.view()
    print(view.paused, view.state.name, view.progress.done)  # True SEALED 2


async def resume_route(route_id: int) -> None:
    handle = await th.find("deliveries", f"route:{route_id}")
    await handle.resume()


async def retry_route(route_id: int) -> int:
    handle = await th.find("deliveries", f"route:{route_id}")
    return await handle.retry_failed(labels=["courier_unavailable"])  # 1: в работу вернулась одна задача


async def cancel_route(route_id: int) -> None:
    handle = await th.find("deliveries", f"route:{route_id}")
    await handle.cancel()
    view = await handle.wait(timeout=60)  # выполняющиеся задачи доделываются
    print(view.state.name, view.progress.ok, view.progress.cancelled)  # CANCELLED 1 3
```

Замечания:

* Пауза и отложенный старт - флаги, а не состояния. Батч на паузе остаётся `OPEN` или `SEALED`
  (`view.paused`, `view.paused_at`). Если на паузу нажали, когда оставались только выполняющиеся
  задачи, батч может финализироваться во время паузы - ваш хук должен это допускать.
* Отмена кооперативна. Уже выполняющаяся задача не прерывается: проверяйте `item.cancelled()` в
  долгих задачах и выходите сами.
* `retry_failed` возможен из `COMPLETED_WITH_ERRORS` и `FAILED`. Он переоткрывает батч, после
  выполнения задач батч финализируется заново, и `on_finalized` вызывается ещё раз с новым итогом.
  На корне он переоткрывает весь конвейер от источников к получателям. Для отдельного этапа,
  получатели которого уже финализированы, он бросает `DownstreamFinalized`: повторяйте с корня.
  Запрос отмены `retry_failed` не снимает: батч, проваленный по `deadline`, `fail_fast` или
  `policy`, после повтора снова завершится `FAILED` с той же причиной, а повторённые задачи
  отменятся при получении воркером. После `release()` повтор возможен только до истечения
  `retention`: дерево с истёкшим `retention` закрыто для операций - см.
  [Retention и `release()`](../hooks/retention.md).
* Дедлайн (`deadline=`) проверяют фоновые проверки: просроченный батч (корень или под-батч)
  вместе со своими под-батчами получает запрос отмены с причиной `deadline` и итог `FAILED`.
