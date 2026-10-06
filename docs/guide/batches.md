# Батчи

Батч - группа задач с общим учётом. Эта страница про то, как его создать и наполнить. Что
делают сами задачи, как устроены конвейеры и как батчем управлять, разобрано на вложенных
страницах.

## Создание батча

Батч создаётся конструктором `th.batch(...)`, который используют как `async with`:

<!-- tallyho-noexec: фрагмент приложения: images и thumbnails принадлежат вашему проекту -->
```python
# app/tasks.py
@fq.task(max_retries=3, retry_on=[StorageTimeout], queue="images")
async def resize(image_id: int) -> None:
    image = await images.get(image_id)
    if image.has_thumbnail:
        item.skip("already_resized")  # задача не понадобилась
        return
    try:
        saved_bytes = await thumbnails.build(image)
    except CorruptImage as error:
        item.error("corrupt_file", detail={"image_id": image_id, "reason": str(error)})
        return  # ошибка без исключения: ретраев не будет
    item.incr("bytes_saved", saved_bytes)  # своя метрика батча
    item.ok("resized")


# app/api.py
async def start_resize(album: Album) -> UUID:
    async with th.batch("thumbnails", key=f"album:{album.id}") as batch:
        await batch.add(resize, album.cover_id)  # один вызов
        await batch.map(resize, album.image_ids)  # по вызову на каждый элемент
        await batch.add_calls([th.call(resize, album.poster_id).opts(weight=3)])  # вызов с опциями
    # выход из блока: батч закрыт (seal), транзакция закоммичена, задачи уходят в брокер
    return batch.handle.id


async def resize_status(album_id: int) -> None:
    handle = await th.find("thumbnails", f"album:{album_id}")  # корень находится по (kind, key)
    view = await handle.view()
    print(view.state.name)
    print(view.progress.found, view.progress.ok, view.progress.skip, view.progress.error)
    print(dict(view.labels))
    print(view.metrics["bytes_saved"])


# COMPLETED_WITH_ERRORS
# 10 7 2 1
# {'resized': 7, 'already_resized': 2, 'corrupt_file': 1}
# 7168
```

* Всё, что добавлено внутри блока, записывается одной транзакцией. При
  исключении в блоке транзакция откатывается, и в брокер не уходит ничего. После коммита задачи
  сразу [отправляет в брокер](operations.md#отправка-в-брокер) тот же процесс.
* При успешном выходе из блока батч закрывается сам. `await batch.seal()`
  можно вызвать и раньше; после него `add` бросает `ConfigurationError`. Если объём работы заранее
  неизвестен, порождайте задачи из задач - см. [`spawn`](batches/pipelines.md#spawn-задачи-порождают-задачи). Если
  продюсер сам читает длинный источник порциями, добавляйте их
  [потоком](#потоковое-добавление) с `seal=False`.
* Создание идемпотентно по `(kind, key)`. Повторный `th.batch(kind, key=...)` с тем же ключом не
  создаёт второй батч, а возвращает существующий; его параметры, атрибуты и `memo` не меняются.
  Двойной клик по кнопке «запустить» не запустит рассылку дважды. Добавить задачи в уже закрытый
  батч при этом нельзя: `add` бросит `SealError`.
* Пустой батч финализируется сразу со статусом `SUCCEEDED`.
* `batch.handle` - ссылка на батч: по ней читают прогресс и управляют деревом. Сохраните
  `batch.handle.id` в своей таблице, чтобы потом получить ссылку через `th.handle(batch_id)`.

### В вашей транзакции

Передайте `session=` (`AsyncSession` или `AsyncConnection`), и батч будет создан в вашей
транзакции - атомарно с доменной записью. `commit` делаете вы; при откате в брокер не уходит ничего.

<!-- tallyho-noexec: фрагмент использует доменную модель Campaign вашего приложения -->
```python
async def schedule(session: AsyncSession, campaign_id: int) -> None:
    campaign = await session.get(Campaign, campaign_id, with_for_update=True)
    async with th.batch("campaign_deliveries", key=f"campaign:{campaign.id}", session=session) as batch:
        await batch.map(send_email, campaign.contact_ids)
    campaign.status = "running"
    campaign.batch_id = batch.handle.id
    # commit делает вызывающий: доменная запись и батч появятся вместе
```

Отправка в брокер и финализация после `seal` запускаются только после подтверждённого COMMIT. С
`AsyncSession` - сразу после коммита. У `AsyncConnection` нет события «после COMMIT», поэтому
tallyho опрашивает соединение из event loop: первые 5 мс на каждом проходе цикла, затем с паузами
до 50 мс. Раньше опроса работу запустит следующее обращение к соединению (новая транзакция). Внутри
самого `await connection.commit()` она не запускается: сразу после него задачи ещё не отправлены,
им нужен хотя бы один проход event loop. Ошибка COMMIT всё отменяет.

Подойдёт любая сессия или соединение той же базы: свои таблицы tallyho находит сам, настраивать
для него `search_path` не нужно - см. [«Схема в ваших сессиях»](installation.md#схема-в-ваших-сессиях).

Тот же параметр `session=` есть у всех управляющих операций (`pause`, `resume`, `cancel`,
`reschedule`, `retry_failed`, `release`).

:::{tip}
Порядок работы везде один: сначала ваша строка, потом вызов tallyho. Хуки блокируют строки в том
же порядке, поэтому дедлоков между вашим API и хуками не возникает.
:::

### Потоковое добавление

Когда продюсер читает длинный источник (файл, выгрузку, курсор чужого API) и не хочет держать одну
транзакцию на всё чтение, он добавляет задачи порциями: каждая порция - отдельный вход в
`th.batch(..., seal=False)` с тем же ключом. Выход из такого блока коммитит порцию, но батч
остаётся открытым, и задачи первых порций уже выполняются. Закрывает батч вход без `seal=False`
(в нём можно добавить последнюю порцию) или явный `await batch.seal()`.

<!-- tallyho-noexec: фрагмент приложения: source и import_row принадлежат вашему проекту -->
```python
async def import_file(file_id: int) -> None:
    key = f"file:{file_id}"
    async for rows in source.read_chunks(file_id, size=5_000):
        async with th.batch("import", key=key, seal=False) as batch:  # порция в своей транзакции
            await batch.map(import_row, rows)
        # порция закоммичена: воркеры разбирают её, пока читается следующая

    async with th.batch("import", key=key):  # вход без seal=False закрывает батч
        pass  # сюда же можно положить последнюю порцию


# пока файл читается:   view.state OPEN,   progress.found растёт от порции к порции
# после последнего входа: view.state SEALED, затем SUCCEEDED
```

* Ключ обязателен. Порции находят батч по `(kind, key)`; без `key` каждый вход создаст новый.
* Параметры задаёт первый вход. Колбэки, политика, `deadline`, атрибуты повторных входов не
  применяются - как при любом повторном `th.batch` с тем же ключом.
* `seal=False` действует на всё дерево блока: объявленные в нём под-батчи тоже остаются
  открытыми, закройте их `seal()`. Этапы с `fed_by` по-прежнему закрывает библиотека.
* Закрытый батч порций не принимает: после seal, финализации или отмены `add` бросает
  `SealError`.

:::{warning}
Незакрытый батч не завершится никогда. Если продюсер упал посреди чтения, батч останется
открытым: повторный запуск с тем же ключом может дочитать и закрыть его. На случай, когда этого
не произойдёт, задавайте `deadline`.
:::

### Параметры `th.batch`

| Параметр | Значение |
|---|---|
| `kind` | тип батча; обязательный |
| `key` | ключ идемпотентности; без него каждый вызов создаёт новый батч |
| `seal` | `True` по умолчанию; `False` - выход из блока не закрывает батч, см. [потоковое добавление](#потоковое-добавление) |
| `start_at` | отложенный старт: задачи уйдут в брокер не раньше этого момента |
| `deadline` | `datetime` или `timedelta`: если батч не завершился к сроку, он отменяется с итогом `FAILED` |
| `failure_policy` | [политика ошибок](batches/failure-policies.md) |
| `max_in_flight` | сколько задач этого батча одновременно находится в брокере и в работе |
| `expected_total` | заранее известный объём - для прогресса до закрытия батча |
| `max_items` | лимит задач на всё дерево; только у корня |
| `retention`, `release_required` | хранение завершённого дерева - см. [retention](hooks/retention.md); только у корня |
| `attributes`, `memo` | [контекст корреляции](batches/attributes.md); только у корня |
| `on_succeeded`, `on_completed_with_errors`, `on_failed`, `on_cancelled`, `on_finalized_task` | [колбэк-задачи](hooks/callbacks.md) на завершение |
| `session` | ваша сессия или соединение |

`max_in_flight` ограничивает один экземпляр батча. Общий лимит на тип задачи для всех батчей сразу -
настройка брокера.

```{toctree}
:hidden:

batches/tasks
batches/pipelines
batches/failure-policies
batches/operations
batches/progress
batches/attributes
```
