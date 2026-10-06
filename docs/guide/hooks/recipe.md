# Рецепт: строка на каждого получателя

Задача: приложение ведёт строку на каждого получателя рассылки, и в неё должны попасть все
исходы - в том числе те, при которых код задачи не выполнялся или упал: исчерпанные ретраи
(`exhausted`), истёкшая аренда (`lease_expired`), истёкший срок (`expired`), отмена. Отдельного хука
на исход каждой задачи в v1 нет. Рецепт собирается из существующих механизмов в два шага:

1. нормальный исход задача пишет сама: своей строкой и итогом в одном коммите
   (`item.complete_in`);
2. остальные исходы переносит колбэк финализации: он читает `handle.items(states=...)`,
   обновляет строки и в той же транзакции вызывает `release()`.

<!-- tallyho-noexec: фрагмент приложения: mail_provider и таблицы issues, deliveries принадлежат вашему проекту -->
```python
# app/tasks.py
KIND = "issue_deliveries"
EXPORTED = {ItemState.ERROR: "failed", ItemState.CANCELLED: "cancelled"}


@fq.task(max_retries=4, retry_on=[MailTemporaryError], queue="mail")
async def send(email: str) -> None:
    await mail_provider.send(to=email)  # сбой провайдера: ретраи flexiq, затем error("exhausted")
    async with engine.begin() as connection:  # строка получателя и итог задачи - один коммит
        await connection.execute(
            update(deliveries).where(deliveries.c.email == email).values(status="sent")
        )
        item.ok("sent")
        await item.complete_in(connection)


@fq.task(max_retries=5, queue="mail")
async def settle(key: str) -> None:
    async with AsyncSession(engine) as session, session.begin():
        status = await session.scalar(select(issues.c.status).where(issues.c.key == key).with_for_update())
        if status != "settling":
            return  # повторная доставка колбэка
        handle = await th.find(KIND, key)
        async for entry in handle.items(states=set(EXPORTED)):
            await session.execute(
                update(deliveries)
                .where(deliveries.c.email == entry.key)
                .values(status=EXPORTED[entry.state], reason=entry.label)
            )
        await session.execute(  # получатели, которые так и не стали задачами
            update(deliveries)
            .where(deliveries.c.status == "pending")
            .values(status="cancelled", reason="not_dispatched")
        )
        await session.execute(update(issues).where(issues.c.key == key).values(status="done"))
        await handle.release(session=session)  # разрешение на удаление - в той же транзакции


# app/hooks.py
@th.on_finalized(KIND)
async def save_result(session: AsyncSession, summary: BatchSummary) -> None:
    # Счётчики точные уже здесь; терминальный статус поставит колбэк после экспорта.
    await session.execute(
        update(issues)
        .where(issues.c.key == summary.key)
        .values(status="settling", sent=summary.progress.ok, failed=summary.progress.error)
    )


# app/api.py
async def start_issue(session: AsyncSession, key: str, emails: list[str]) -> None:
    await session.execute(insert(issues).values(key=key, status="running"))
    await session.execute(insert(deliveries), [{"email": email, "status": "pending"} for email in emails])
    async with th.batch(
        KIND,
        key=key,
        retention=timedelta(days=1),
        release_required=True,  # дерево ждёт экспорта
        on_finalized_task=th.call(settle, key),
        session=session,
    ) as batch:
        await batch.add_calls(th.call(send, email).opts(key=email) for email in emails)


# таблица deliveries после выпуска, в котором один адрес не принял письмо:
# ada@ok.test       sent
# grace@ok.test     sent
# later@down.test   failed      reason="exhausted"    записал колбэк, а не задача
# issues.status     "done"
#
# через retention после release() дерево удалено: handle.view() бросает BatchPurged,
# строки deliveries и issues остаются на месте
```

Условия, без которых рецепт некорректен:

* Нормальный путь пишет строку в самой задаче, через `complete_in`. Экспорт читает только
  `ERROR` и `CANCELLED`. Задача, которая после `retry_failed()` завершилась успешно, исправит свою
  строку сама, тем же кодом.
* Последний шаг экспорта - запрос по остатку. Получатели, которые так и не стали задачами
  (отмена посреди разворачивания аудитории, дубли по ключу, `skipped_by_limit`), в `items()` не
  появятся: их строки закрывает один `UPDATE … WHERE status = 'pending'`.
* Счётчики ставит `on_finalized`, а не колбэк: сводка уже содержит точные числа, и они атомарны
  с финализацией.
* Терминальный доменный статус ставит колбэк. Между финализацией и экспортом сущность находится
  в промежуточном статусе (`settling`), поэтому она не бывает «завершена, а строки ещё не
  обновлены».
* Колбэк идемпотентен. Экспорт, итоговый статус и `release()` - одна транзакция. Падение
  посередине оставляет `settling` и неосвобождённое дерево; повторная доставка безопасна. Если одна
  транзакция слишком велика, коммитьте экспорт чанками, а `release()` вызывайте в транзакции
  последнего.
* `retry_failed()` повторяет цикл: разрешение на удаление сбрасывается, `on_finalized` снова
  ставит `settling` и новый итог, колбэк экспортирует оставшиеся ошибки и снова вызывает
  `release()`. Повтор после `release()` возможен, только пока не истёк `retention`; позже
  `retry_failed()` бросает `BatchPurged`.
* Задачи этапа читайте у этапа. В конвейере задачи лежат в под-батче, а не в корне:
  `send = await root.child("send")`, затем `send.items(...)`. `release()` при этом вызывается у корня.

Чего рецепт не даёт: исходы, возникшие без участия кода задачи, видны в ваших таблицах только после
финализации батча, а не по мере появления.
