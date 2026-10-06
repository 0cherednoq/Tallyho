# Экспорт почты: прогресс

Выгрузка идёт час. Всё это время человеку у экрана нужен ответ на два вопроса: что происходит
на каждом этапе и далеко ли до конца. Первый вопрос простой, второй хитрее, потому что конца
никто не знает.

## Снимок дерева

`await handle.view()` возвращает согласованный снимок корня и всех этапов.

<!-- tallyho-noexec: фрагмент app/api.py; th из app/tasks.py -->
```python
# app/api.py
async def export_status(run_id: int) -> dict[str, object]:
    handle = await th.find("mail_export", f"run:{run_id}")
    view = await handle.view()
    return {
        "state": view.state.name,
        "ratio": view.progress.ratio,
        "stages": {
            key: {
                "state": stage.state.name,
                "found": stage.progress.found,
                "done": stage.progress.done,
                "errors": stage.progress.error,
                "expected": stage.progress.expected,
                "estimate": stage.progress.expected_is_estimate,
            }
            for key, stage in view.children.items()
        },
    }


# через десять минут после запуска:
# {"state": "SEALED", "ratio": 0.31, "stages": {
#   "pages":       {"state": "SEALED", "found": 214, "done": 190, "errors": 0, "expected": 214, "estimate": False},
#   "messages":    {"state": "OPEN", "found": 9480, "done": 5100, "errors": 2, "expected": 10700, "estimate": True},
#   "attachments": {"state": "OPEN", "found": 1310, "done": 840, "errors": 0, "expected": 2750, "estimate": True}}}
```

Числа корня и числа этапов отвечают на разные вопросы, и путать их не стоит.

| Где | Что считает |
|---|---|
| `view.progress.found`, `done` | этапы. У этого корня `found` всегда 3, а `done` растёт по мере того, как этапы финализируются |
| `view.progress.ratio` | долю работы по всему дереву, с учётом настоящих задач каждого этапа |
| `view.children["messages"].progress` | настоящие задачи этапа: письма |

Для полоски «вся выгрузка» берите `view.progress.ratio`. Для строк «страниц пройдено 190, писем
найдено 9 480» берите счётчики этапов.

## Сколько будет всего

У каждого этапа есть `found` и `done`. Третье число, `expected`, появляется не сразу, и у него три
состояния.

| Этап | `expected` | `expected_is_estimate` |
|---|---|---|
| закрыт | равно `found` | `False` |
| открыт, источники уже что-то сделали | оценка | `True` |
| открыт, оценить не на чем | `None` | `False` |

Оценка строится по тому, что уже известно. Если сто пройденных страниц дали пять тысяч писем, то
двести страниц дадут около десяти тысяч. Оценка появляется после двадцати завершённых задач
источника или пяти процентов его объёма, смотря что наступит раньше, и уточняется по ходу работы.

У `pages` есть особенность: этот этап наполняет сам себя. Продюсер закрыл его сразу, поэтому его
`expected` равен `found`, но `found` подрастает, пока задачи заказывают следующие страницы.
Окончательным число становится при финализации этапа, это видно по `progress.final`.

Отсюда правило для интерфейса. Пока `expected` равен `None`, показывайте только «найдено». Когда
он есть и это оценка, пишите «около». Процент рисуйте из `ratio`.

:::{note}
`ratio` может немного уменьшиться, когда оценка объёма выросла: нашёлся ящик на десять тысяч
писем. Библиотека отдаёт честное число. Чтобы полоска не ехала назад, храните у себя максимум.
:::

## Снимки в вашу таблицу

Опрашивать `view()` из каждого запроса пользователя не нужно. Пусть tallyho сам пишет прогресс в
вашу таблицу раз в пару секунд, а интерфейс читает её обычным запросом.

<!-- tallyho-noexec: фрагмент app/hooks.py; таблица export_stages принадлежит вашему приложению -->
```python
# app/hooks.py
async def write_stages(session: AsyncSession, summary: BatchSummary) -> None:
    for key, stage in summary.children.items():
        row = {
            "found": stage.progress.found,
            "done": stage.progress.done,
            "expected": stage.progress.expected,
            "seq": summary.seq,
        }
        await session.execute(
            pg_insert(export_stages)
            .values(batch_id=summary.id, stage=key, **row)
            .on_conflict_do_update(
                index_elements=["batch_id", "stage"],
                set_=row,
                where=export_stages.c.seq < summary.seq,  # опоздавший снимок не затрёт свежий
            )
        )


@th.on_progress("mail_export", every=timedelta(seconds=2))
async def save_progress(session: AsyncSession, summary: BatchSummary) -> None:
    await write_stages(session, summary)


# export_stages во время выгрузки:
# batch_id | stage       | found | done | expected | seq
# 0199...  | pages       |   214 |  190 |      214 |  41
# 0199...  | messages    |  9480 | 5100 |    10700 |  41
# 0199...  | attachments |  1310 |  840 |     2750 |  41
```

Хук регистрируется на `kind` корня и получает сводку всего дерева, поэтому одной функции хватает
на все этапы. Вызывает его процесс maintenance, и только если счётчики изменились.

`summary.seq` растёт с каждым снимком. Условие `seq < summary.seq` нужно на случай, когда два
снимка записываются не в том порядке, в котором сделаны.

Сводка в хуке несёт и `progress.eta`, оценку времени до конца. В разовом `view()` её нет: для
скорости нужно несколько замеров подряд.

После финализации снимки прекращаются. Последние, точные числа записывает хук `on_finalized`, о
нём [следующая страница](export-finalization.md). Удобно вызывать из него ту же `write_stages`.

## Живой поток

Для обновления без таблицы есть `handle.watch()`, асинхронный поток снимков. Его удобно завернуть
в SSE или печатать в консоль.

<!-- tallyho-noexec: фрагмент консольной утилиты; th из app/tasks.py -->
```python
async for view in handle.watch():  # заканчивается терминальным снимком
    files = view.children["attachments"].progress
    total = files.expected if files.expected is not None else "?"
    print(f"{view.progress.ratio or 0:.0%}  вложений {files.done} из {total}")

# 31%  вложений 840 из 2750
# 33%  вложений 905 из 2790
# ...
# 100%  вложений 3904 из 3904
```

`watch()` и `wait()` работают через `LISTEN` и `NOTIFY`. Через pgbouncer в режиме transaction
pooling уведомления не приходят, поэтому процессу с `watch()` нужно прямое подключение к
PostgreSQL.
