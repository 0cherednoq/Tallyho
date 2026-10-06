# Повтор упавшего хука

Если `on_finalized` бросил исключение или не уложился в таймаут:

1. вся транзакция финализации откатывается - и ваши изменения, и переход батча;
2. батч остаётся `SEALED`; число попыток и текст ошибки видны в `view.hook_attempts` и
   `view.hook_error`;
3. наблюдатель получает событие `hook_failed` (метрика `th_hook_failures`);
4. фоновые проверки повторяют финализацию с растущей паузой: она удваивается после каждой неудачи
   от 1 секунды до 5 минут (`hook_backoff_initial`, `hook_backoff_max`).

Батч **не станет терминальным без успешного хука**: ваш статус и состояние батча не расходятся.
После исправления кода ничего делать не нужно - очередной повтор пройдёт. Чтобы не ждать,
вызовите `await handle.retry_finalize()`.

<!-- tallyho-noexec: фрагмент приложения: таблица invoice_runs принадлежит вашему проекту -->
```python
# app/hooks.py
@th.on_finalized("invoices")
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    await session.execute(
        update(invoice_runs)
        .where(invoice_runs.c.batch_id == summary.id)
        .values(status="done", issued=summary.progress.ok)  # колонки issued в таблице нет: хук падает
    )


# все три задачи выполнены, но батч не финализирован:
# view.state            SEALED
# view.progress.ok      3
# view.hook_attempts    1, затем растёт с каждым повтором
# view.hook_error       текст исключения из хука

# после исправления: дождаться очередного повтора или ускорить его
handle = await th.find("invoices", "month:10")
await handle.retry_finalize()
view = await handle.wait(timeout=30)
print(view.state.name)  # SUCCEEDED; хук закоммичен ровно один раз
```
