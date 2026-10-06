# Два примера

В этом разделе два приложения, собранных от начала до конца. Они независимы, читать можно в любом
порядке.

| Пример | Что показывает |
|---|---|
| [Проверка аккаунтов](checker.md) | один батч: сто задач, итог каждой, сводка, повтор упавших |
| [Экспорт почты](export.md) | конвейер: страницы ящика находят письма, письма находят вложения, и объём неизвестен до самого конца |

Оба примера работают на [flexiq](../../integrations/flexiq.md) и написаны так, как это выглядело бы в рабочем
проекте. Прикладной слой в них не реализован: почтовый клиент и хранилище представлены вызовами с
понятными именами, а что у них внутри, для разговора о батчах не важно.

## Прикладной слой

| Вызов | Что делает |
|---|---|
| `await mail.login(credentials)` | входит в ящик. Бросает `InvalidCredentials` или `AccountLocked` |
| `await mail.list_page(mailbox_id, cursor)` | возвращает страницу: идентификаторы писем и курсор следующей страницы |
| `await mail.fetch(mailbox_id, message_id)` | возвращает письмо со списком вложений. Бросает `MessageNotFound`, если письмо успели удалить |
| `await mail.download(mailbox_id, attachment_id)` | возвращает содержимое вложения |
| `storage.*`, `accounts.*` | ваше хранилище файлов и ваши таблицы |

Любой вызов `mail.*` при сетевом сбое бросает `MailTemporaryError`. Это единственное исключение,
после которого задачу имеет смысл повторять.

## Раскладка проекта

```text
app/
  tasks.py        клиент Tallyho, адаптер flexiq, задачи
  hooks.py        хуки: итог и прогресс в ваши таблицы
  api.py          запуск и статус из HTTP-обработчиков
  worker.py       точка входа воркера flexiq
```

## Клиент и адаптер

Модуль с клиентом и задачами импортируют и API, и воркеры, поэтому он один на всё приложение.

<!-- tallyho-noexec: модуль приложения: нужен ваш DSN, а задачи исполняет процесс воркера flexiq -->
```python
# app/tasks.py
from flexiq import Queue
from sqlalchemy.ext.asyncio import create_async_engine

from tallyho import Tallyho, item
from tallyho.adapters.flexiq import FlexiqAdapter

engine = create_async_engine("postgresql+asyncpg://app:secret@db/app")
queue = Queue(backend="postgres", db_url="postgresql://app:secret@db/app", schema="flexiq")

th = Tallyho(engine, schema="app", hook_modules=["app.hooks"])
fq = FlexiqAdapter(queue)
th.install(fq)  # до объявления задач: без этого @fq.task бросит ConfigurationError
```

`hook_modules` перечисляет модули с хуками. Клиент импортирует их сам, и хуки оказываются
зарегистрированы в каждом процессе: в API, в воркерах, в maintenance.

## Воркер

<!-- tallyho-noexec: точка входа процесса воркера; работает, пока его не остановят -->
```python
# app/worker.py
import asyncio

from app.tasks import queue, th

queue.run_worker(queues=["check", "mail", "files"], pool="thread")  # до SIGINT или SIGTERM
asyncio.run(th.aclose())  # дописать итоги и вернуть в очередь недоработавшие задачи
```

Пул только `thread`, и вторая строка обязательна. Почему, разобрано на странице
[Адаптер flexiq](../../integrations/flexiq.md#остановка).

Кроме воркеров нужен процесс maintenance. Он возвращает в очередь задачи умерших воркеров,
подбирает пропущенные финализации и делает снимки прогресса. Как его запускать, описано на
странице [Процессы и maintenance](../operations.md).
