# Политики ошибок

Политика решает, что делать с батчем, когда задачи завершаются с ошибкой. Фабрики доступны как
`th.FailurePolicy`:

| Политика | Поведение |
|---|---|
| `th.FailurePolicy.continue_()` | по умолчанию: ошибки не останавливают батч, итог - `COMPLETED_WITH_ERRORS` |
| `th.FailurePolicy.fail_fast()` | первая ошибка отменяет оставшиеся задачи, итог - `FAILED` |
| `th.FailurePolicy.threshold(ratio=, min_processed=0, labels=None, action="fail")` | когда доля ошибок превысила порог, выполняется `action` |

Как считается порог `threshold`:

* обработанные задачи - `ok + skip + error`; отменённые не учитываются;
* числитель - все ошибки или, если задан `labels=[...]`, только задачи с этими метками;
* политика срабатывает, когда обработано не меньше `min_processed` задач и доля **строго больше**
  `ratio`;
* `action="fail"` отменяет оставшиеся задачи и даёт итог `FAILED`; `action="pause"` ставит всё
  дерево на паузу и ждёт вашего решения. Оба действия атомарно вызывают хук
  [`on_policy_breach`](../hooks.md#on_policy_breach).

<!-- tallyho-noexec: фрагмент приложения: gateway принадлежит вашему проекту -->
```python
# app/tasks.py
@fq.task(max_retries=2, retry_on=[GatewayTimeout], queue="billing")
async def charge(order_id: int) -> None:
    try:
        await gateway.charge(order_id)
    except CardDeclined:
        item.error("declined")  # отказ банка: повторять бессмысленно


# app/api.py
async def start_billing_run(run_id: int, order_ids: list[int]) -> None:
    policy = th.FailurePolicy.threshold(ratio=0.2, min_processed=4, labels=["declined"])
    async with th.batch("billing", key=f"run:{run_id}", failure_policy=policy) as batch:
        await batch.map(charge, order_ids)


# десять заказов, каждый второй отклонён; после первых отказов политика срабатывает:
# view.state                      FAILED
# view.reason                     CancelReason.POLICY
# view.progress.error             2 или больше
# view.progress.cancelled         оставшиеся задачи не выполнялись
# view.progress.done == found     10
```

Провал по политике, дедлайну и `fail_fast` - не мгновенный переход, а запрос отмены: новые задачи
не принимаются, неотправленные сразу становятся `cancelled`, выполняющиеся доделываются, после чего
батч финализируется обычным путём, через хук `on_finalized`. Причину показывает `view.reason`:
`policy`, `fail_fast`, `deadline` или `cancel`.

## Первая причина выигрывает

Запрос отмены получает только батч, который ещё не отменяют.
Дедлайн, наступивший после `cancel()`, не превратит `CANCELLED` в `FAILED`, а `cancel()` после
дедлайна или `fail_fast` не превратит `FAILED` в `CANCELLED`: `view.reason` и `cancel_requested_at`
остаются от первого запроса. Остальное повторный запрос делает как обычно: проходит по под-батчам и
сразу отменяет их неотправленные задачи. Причина у каждого узла своя: под-батч, отменённый раньше
по своему дедлайну, после `cancel()` корня останется `FAILED` с причиной `deadline`, а корень станет
`CANCELLED`.
