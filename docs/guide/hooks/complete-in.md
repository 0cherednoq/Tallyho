# Итог задачи в вашей транзакции

Если задача пишет в вашу таблицу и её запись должна быть атомарна с итогом задачи, завершите задачу
в своей транзакции:

<!-- tallyho-noexec: фрагмент задачи; engine и таблица deliveries принадлежат вашему приложению (исполняемая версия - в рецепте ниже) -->
```python
async def send_email(campaign_id: int, email: str) -> None:
    message_id = await mail_provider.send(to=email)
    async with engine.begin() as connection:
        await connection.execute(
            update(deliveries)
            .where(deliveries.c.campaign_id == campaign_id, deliveries.c.email == email)
            .values(status="sent", message_id=message_id)
        )
        item.ok("sent")
        await item.complete_in(connection)  # итог задачи - в этом же коммите
```

`item.complete_in(session)` принимает `AsyncSession` или `AsyncConnection`. Сначала задайте итог
(`ok/skip/error`) и все `spawn`, затем вызовите `complete_in`: он записывает то, что накоплено к
этому моменту. Если ваша транзакция откатилась, задача остаётся незавершённой и будет повторена
брокером.

## Если задача потеряла аренду

Задача может пережить свою аренду: зависла дольше `lease_ttl` без продления, батч отменили, после
сбоя её уже повторяет другой воркер. К моменту `complete_in` итог такой задачи записан без неё
(`lease_expired`, `cancelled`) или принадлежит другой попытке. Записать вашу строку в этот момент
значило бы получить доменный эффект у задачи, которая не считается успешной.

Поэтому `complete_in` проверяет, что задача ещё принадлежит этой попытке. Если нет, он ничего не
записывает и бросает `LeaseLostError`:

* не ловите её. Исключение должно выйти из блока транзакции: `async with engine.begin()` и
  `async with session.begin()` откатят ваши записи сами;
* это не ошибка задачи. Обёртка tallyho не записывает итог, не тратит попытку и возвращает
  брокеру успех: ни ретрая, ни DLQ не будет. Задачу доведёт тот, кому она теперь принадлежит, или
  она уже завершена;
* то же правило действует и без `complete_in`: если задача, потерявшая аренду, вернула результат
  или упала, обёртка не записывает итог и не отпускает чужую аренду, а брокеру отдаёт успех;
* побочные эффекты вне базы (отправленное письмо) к этому моменту уже случились - как и при любом
  повторе at-least-once, их идемпотентность остаётся на вас.

Вот как это выглядит, когда почтовый провайдер завис дольше аренды:

<!-- tallyho-noexec: фрагмент приложения: mail_provider и таблица deliveries принадлежат вашему проекту -->
```python
@fq.task(max_retries=3, retry_on=[MailTemporaryError], queue="mail")
async def send(email: str) -> None:
    message_id = await mail_provider.send(to=email)  # провайдер завис дольше lease_ttl
    async with engine.begin() as connection:
        await connection.execute(insert(deliveries).values(email=email, message_id=message_id))
        item.ok("sent")
        await item.complete_in(connection)  # LeaseLostError: блок откатывает вставку


# задачу, пока она висела, завершили фоновые проверки:
# view.labels                 {"lease_expired": 1}
# view.progress.ok, .error    0 1
# таблица deliveries          строки нет
# flexiq                      попытка закончилась успехом: ни ретрая, ни DLQ
```

Повторный вызов `complete_in` в той же задаче:

| Когда | Что происходит |
|---|---|
| после коммита вашей транзакции | ничего: задача уже завершена |
| в той же транзакции, пока первая запись в силе | ничего; итог и `spawn`, заданные после первого вызова, не записываются |
| после отката транзакции или savepoint | итог записывается заново - вместе с вашими новыми записями |
| в другой транзакции, пока первая открыта | `ConfigurationError`: задача завершается в одной транзакции |
