# Проверка аккаунтов

Есть список из ста почтовых аккаунтов. Нужно попробовать войти в каждый, записать вердикт рядом
с аккаунтом и в конце показать сводку: сколько живых, сколько с неверным паролем, сколько
заблокировано. Объём известен заранее, поэтому хватит одного батча.

## Задача

<!-- tallyho-noexec: фрагмент app/tasks.py; mail, accounts и исключения принадлежат вашему приложению -->
```python
# app/tasks.py
@fq.task(max_retries=4, retry_on=[MailTemporaryError], queue="check", timeout=60)
async def check_account(account_id: int) -> None:
    credentials = await accounts.credentials(account_id)
    try:
        await mail.login(credentials)
    except InvalidCredentials:
        verdict = "bad_password"
    except AccountLocked:
        verdict = "locked"
    else:
        verdict = "valid"

    async with engine.begin() as connection:
        await accounts.set_verdict(connection, account_id, verdict)
        item.ok(verdict)
        await item.complete_in(connection)  # вердикт и итог задачи одним коммитом
```

В задаче два вида исключений, и обходятся с ними по-разному.

`InvalidCredentials` и `AccountLocked` для чекера означают ответ: проверка состоялась, результат
отрицательный. Задача ловит их, записывает вердикт и завершается успешно, а метка
говорит, что именно выяснилось. По меткам tallyho ведёт счётчики, из них и получится сводка.

`MailTemporaryError` задача не ловит. Исключение, вышедшее из задачи, tallyho итогом не считает и
отдаёт решение flexiq. Тот повторит задачу до четырёх раз, а если не помогло, задача получит итог
`error` с меткой `"exhausted"`. Такой аккаунт остался непроверенным, и в батче это видно как
ошибка.

Отсюда правило: наружу из задачи выпускайте только то, что лечится повтором. Если выпустить
`InvalidCredentials`, flexiq честно отправит джобу в DLQ, а в батче вместо вердикта останется
безликое `"exhausted"`.

Последние четыре строки записывают вердикт. `item.complete_in(connection)` завершает задачу в
вашей транзакции, поэтому строка аккаунта и итог задачи коммитятся вместе. Не бывает аккаунта с
вердиктом, который батч считает непроверенным, и наоборот.

:::{tip}
`timeout` задавайте по реальной длительности задачи. Это ещё и срок, через который flexiq вернёт в
работу джобу, чей результат не удалось записать при отказе базы. Умолчание flexiq, 300 секунд, для
проверки логина великовато.
:::

## Запуск

<!-- tallyho-noexec: фрагмент app/api.py; таблица check_runs принадлежит вашему приложению -->
```python
# app/api.py
async def start_check(session: AsyncSession, list_id: int, account_ids: list[int]) -> None:
    async with th.batch(
        "account_check",
        key=f"list:{list_id}",
        max_in_flight=10,
        failure_policy=th.FailurePolicy.threshold(
            ratio=0.5, min_processed=20, labels=["exhausted"], action="pause"
        ),
        session=session,
    ) as batch:
        await batch.add_calls(
            th.call(check_account, account_id).opts(key=str(account_id)) for account_id in account_ids
        )
    await session.execute(
        insert(check_runs).values(list_id=list_id, batch_id=batch.handle.id, status="running")
    )
    # commit делает вызывающий: строка запуска и батч появятся вместе
```

Каждый параметр здесь закрывает одну неприятность.

`key=f"list:{list_id}"` делает запуск идемпотентным. Пользователь дважды нажал кнопку, запрос
повторился после таймаута, а батч всё равно один.

`.opts(key=...)` защищает от дублей внутри списка. Аккаунт, попавший в него дважды, станет одной
задачей, а повтор учтётся в счётчике `duplicates`. По этому ключу задачу потом легко найти.

`max_in_flight=10` держит не больше десяти логинов одновременно, сколько бы воркеров ни работало.

`failure_policy` страхует от лежащего почтового сервера. Если после двадцати проверок больше
половины закончились как `"exhausted"`, батч встаёт на паузу и ждёт человека, а не сжигает ретраи
на оставшихся аккаунтах. Подробнее о [политиках ошибок](../batches/failure-policies.md).

`session=session` помещает батч в вашу транзакцию. Строка `check_runs` и сто задач либо появятся
вместе, либо не появятся вовсе. В flexiq задачи уйдут только после коммита.

## Прогресс

<!-- tallyho-noexec: фрагмент app/api.py; th из app/tasks.py -->
```python
async def check_status(list_id: int) -> dict[str, object]:
    handle = await th.find("account_check", f"list:{list_id}")
    view = await handle.view()
    return {
        "state": view.state.name,
        "done": view.progress.done,
        "total": view.progress.found,
        "verdicts": dict(view.labels),
        "paused": view.paused,
    }


# посреди проверки:
# {"state": "SEALED", "done": 57, "total": 100, "paused": False,
#  "verdicts": {"valid": 41, "bad_password": 13, "locked": 3}}
```

Батч нашёлся по той же паре `kind` и `key`, с которой создавался. `view()` отдаёт согласованный
снимок: числа в нём относятся к одному моменту.

## Итог

Статус запуска живёт в вашей таблице. Менять его должен хук `on_finalized`, он выполняется внутри
транзакции, которая завершает батч.

<!-- tallyho-noexec: фрагмент app/hooks.py; таблица check_runs принадлежит вашему приложению -->
```python
# app/hooks.py
@th.on_finalized("account_check")
async def save_check_result(session: AsyncSession, summary: BatchSummary) -> None:
    await session.execute(
        update(check_runs)
        .where(check_runs.c.batch_id == summary.id)
        .values(
            status="done" if summary.state is BatchState.SUCCEEDED else "partial",
            valid=summary.labels.get("valid", 0),
            bad_password=summary.labels.get("bad_password", 0),
            locked=summary.labels.get("locked", 0),
            unchecked=summary.progress.error,
        )
    )


# после проверки в check_runs:
# status="partial", valid=71, bad_password=22, locked=6, unchecked=1
```

Гарантия двусторонняя. Если хук упал, батч не станет завершённым, а финализация повторится. Если
батч завершён, строка обновлена. Статус в вашей таблице и состояние батча не расходятся.

Числа в хуке окончательные. Писать их нужно как «установить», а не «прибавить»: после повтора
упавших хук вызовется ещё раз с новым итогом.

## Повтор непроверенных

Один аккаунт остался непроверенным: почтовый сервер не отвечал дольше, чем flexiq повторял задачу.
Батч завершился как `COMPLETED_WITH_ERRORS`. Это значит «все задачи завершены, часть с ошибкой»:
батч не завис, итог есть, хоть и неполный.

<!-- tallyho-noexec: фрагмент app/api.py; таблица check_runs принадлежит вашему приложению -->
```python
async def unchecked_accounts(list_id: int) -> list[str]:
    handle = await th.find("account_check", f"list:{list_id}")
    return [entry.key async for entry in handle.items(labels=["exhausted"])]


# ["4817"]


async def retry_unchecked(session: AsyncSession, list_id: int) -> int:
    run = await session.get(CheckRun, list_id, with_for_update=True)  # сначала своя строка
    retried = await th.handle(run.batch_id).retry_failed(labels=["exhausted"], session=session)
    run.status = "running"
    return retried


# 1
```

`retry_failed` возвращает в работу только ошибочные задачи. Девяносто девять проверенных аккаунтов
второй раз не трогаются. Когда повторённая задача завершится, батч финализируется заново и хук
запишет новый итог: `status="done", valid=72, unchecked=0`.

Порядок в `retry_unchecked` важен: сначала блокируется ваша строка, потом вызывается
tallyho. Хуки берут блокировки в том же порядке, поэтому API и хук не поймают дедлок.
