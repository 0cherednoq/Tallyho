# Динамические конвейеры: как это сделано у других и что берём

> Ресёрч 2026-09-30. Кейс: 24 страницы каталога → неизвестное число карточек → неизвестное число PDF → скачивание.
> Наш черновик: корневой батч с под-батчами-этапами, spawn в соседний открытый под-батч атомарно с завершением задачи, `fed_by` — автоматический seal этапа после финализации источников, прогресс «найдено / сделано / оценка».
>
> Пометки: **[V]** — проверено по документации или исходникам (ссылка), **[I]** — вывод.

## 1. Сводная таблица

| Система | Как добавляют работу по ходу | Как понимают «всё готово» | Следующий этап стартует до конца предыдущего? | Прогресс | Лимит параллельности на этап |
|---|---|---|---|---|---|
| **Oban Pro Workflow** | `add_graft` (заглушка, раскрывается в под-workflow), `append` из задачи, `add_many` | Зависимый ждёт весь под-workflow; graft закрыт, когда отработал grafter [V] | Нет, зависимость — барьер на весь шаг; конвейер только деревом graft на каждый элемент [I] | `status/1`: `total`, `counts` по состояниям, рекурсивный `subs`; оценки нет [V] | Нет, только очередь |
| **River Pro** | `WorkflowFromExisting` + `InsertManyTx` + `JobCompleteTx` в одной транзакции [V] | Нет понятия завершения workflow, только зависимости по задачам [V] | По задачам — да [V] | Нет API прогресса [V] | Нет |
| **Sidekiq Pro** | `batch.jobs {}` изнутри задачи батча, дочерние батчи [V] | Работающая задача ещё pending → батч не закроется; снаружи добавлять больше одного раза «не безопасно» [V] | Дочерние батчи — да, цепочки колбэков — барьер [V] | `total/pending/failures`; `total` растёт, оценки нет [V] | Нет |
| **BullMQ Flows** | `queue.add({parent})` + `moveToWaitingChildren` [V] | Родитель в `waiting-children`, пока множество зависимостей не пусто (Lua атомарно) [V] | Только дерево родитель → дети | `getDependenciesCount`: processed/unprocessed/failed [V] | Нет |
| **Hatchet** | `aio_run_many` из задачи, `child_key` для дедупа [V] | Родитель сам ждёт то, что породил [V] | Деревом — да | Дашборд, счётчиков в API нет [I] | CEL-ключи `ConcurrencyExpression` [V] |
| **Celery** | `replace`, `add_to_chord` (только Redis) [V] | Chord: `readycount == size + добавлено` [V] | Нет, chord — барьер [V] | Нет | Нет |
| **Airflow** | `.expand()` по выходу предыдущей задачи [V] | Число копий фиксируется один раз при раскрытии [V] | Нет (breadth-first); по элементу — только mapped task group, вложенное раскрытие в ней **запрещено** [V] | Грид по индексам, растущего итога нет | `max_active_tis_per_dagrun`; ловушка `..._per_dag` действует на все запуски [V] |
| **Dagster** | `DynamicOut` + `.map()` [V] | Ключи превращаются в шаги только после **успешного** завершения продюсера [V] | Нет: продюсер — барьер; узел не может зависеть от двух dynamic-выходов [V] | Нет агрегата [I] | `max_concurrent`, tag-лимиты [V] |
| **Prefect 3** | `.submit()/.map()` в обычном коде [V] | Пользовательский код дождался всех futures [V] | Да, по future [V] | Progress artifact — процент, который задаёт пользователь [V] | Tag-лимиты [V] |
| **Argo Workflows** | `withParam` по JSON-выходу шага [V] | Раскрытие после выхода продюсера [I] | Барьер; по элементу — через шаблон DAG на элемент [I] | **`N/M`, «M растёт при добавлении узла в граф»**; задача может сама писать `N/M` в `$ARGO_PROGRESS_FILE` [V] | `parallelism` на шаблон [V] |
| **Flyte** | `map_task`, `@dynamic` [V] | Список входа фиксирован [I] | Барьер [I] | — | `concurrency` на map task [V] |
| **Temporal** | Дочерние workflow, continue-as-new [V] | Логика кода | Как напишете | Query `{progress, in-flight IDs}` в sample «sliding window» [V] | Как напишете |
| **Crawlee / Apify** | `addRequest` с `uniqueKey` [V] | Обработчик (и его `enqueueLinks`) завершается **до** `markRequestHandled` → дети существуют раньше, чем родитель «сделан» [V] | Да, общая очередь | `Crawled N/total`, total растёт; ETA нет [V] | Только на весь краулер; per-label нет [V] |
| **Scrapy** | `engine.crawl` → scheduler + dupefilter [V] | `spider_is_idle`: scraper, downloader и scheduler пусты, проверка каждые 5 с [V] | Да, общая очередь | Логи «N pages/min», total нет [V] | `download_slot` как обходной путь [V] |

## 2. Что подтвердилось

1. **Атомарный spawn — проверенный инвариант.** Crawlee, Scrapy, Sidekiq, Celery `add_to_chord`, BullMQ, Oban graft держат группу открытой, пока жива задача, которая добавляет работу. River пишет расширение атомарно с завершением (`InsertManyTx` + `JobCompleteTx`). У нас то же самое, но в одной транзакции PostgreSQL, поэтому без эвристик вроде задержек Crawlee v1 (3 с, 10 с), purgatory у StormCrawler и idle-таймаутов scrapy-redis. Они там из-за нетранзакционного хранилища.
2. **Конвейер с этапами-соседями и автоматическим seal не делает никто.** Декларативные оркестраторы (Airflow, Dagster, Argo, Flyte) требуют завершения продюсера до раскрытия. Вложенное раскрытие у них запрещено (Airflow, Dagster) или идёт через «собрать и заново раскрыть». Очереди задач конвейерят только деревом «элемент → его дети». `fed_by` закрывает реальный пробел.
3. **«Найдено / сделано» вместо одного процента.** Так показывают Argo (`N/M`, M растёт), Spark (по этапам), Dask (полоска на префикс задачи), Beam (`workCompleted, workRemaining` — два числа, не доля).
4. **Оценки итога нет ни у кого из рассмотренных.** По сути это оценщик Кнута для размера дерева (1975): произведение наблюдаемых коэффициентов ветвления по уровням. Близкие идеи есть у оценщиков прогресса SQL-запросов (SIGMOD 2004). Показывать оценку можно, но с явной пометкой.
5. **Счётчики — в агрегатных строках, с починкой.** Oban 1.7 перешёл на таблицу счётчиков вместо агрегирующих запросов, Oban 1.8 добавил «repairs drifted counters». У нас это шардированные счётчики + reconcile.

## 3. На чём обжигались другие → что добавить нам

| Ловушка (где случилось) | Что делаем |
|---|---|
| Пустой шаг блокировал последующие: Oban 1.6.5 [V]. Пустые батчи легальны только с Sidekiq Pro 7.1 [V] | Этап, закрытый (seal) с 0 Items, финализируется сразу и каскадом закрывает этапы, которые он наполняет |
| Dagster не раскрывает dynamic-выход упавшего продюсера, и нижние шаги брошены [V] | Источник в любом терминальном состоянии закрывает этап, который он наполняет. Опция `on_feeder_failed="seal"` (по умолчанию) или `"cancel"` |
| Дедупликация без учёта: unique-отказ оставлял зависимых ждать несуществующую задачу — Oban (1.8.1 фикс фантомов), Sidekiq #2020 [V] | Дедуп внутри транзакции spawn **до** инкремента счётчиков: `ON CONFLICT DO NOTHING RETURNING`, считаем только вставленное. Метрика `duplicates` на под-батч, как `dupefilter/filtered` у Scrapy |
| Гонки преждевременного освобождения у graft: Oban 1.6.0-rc.4, 1.7.6 [V]; «добавлять снаружи больше одного раза не безопасно» — Sidekiq [V] | Правило: **в под-батч X можно добавлять только из задач самого X или из задач его источников `fed_by`**. Источник с живой задачей не финализирован, значит X не закрыт — блокировка строки X не нужна. Нарушение правила → ошибка при spawn |
| Счётчик «всего» смешивает попытки и уникальную работу: `scheduler/enqueued` у Scrapy, `requestsTotal` у Crawlee = processed [V] | `found` — только уникальные Items. Ретраи видны отдельно (`attempt`), в `found` не попадают |
| Воркер умер → задача «в работе» навсегда (без lock у Crawlee v1) [V] | Уже есть: lease + sweeper |
| `max_active_tis_per_dag` у Airflow неожиданно действует на все запуски [V] | Явный scope: `max_in_flight` — на этот экземпляр под-батча. Глобальный лимит на тип задачи — забота брокера (flexiq `max_concurrent`/`rate_limit`). Так и пишем в доке |
| Бесконечное разрастание: `max_map_length=1024` у Airflow, `maxRequestsPerCrawl`/`maxCrawlDepth` у Crawlee, лимиты истории Temporal [V] | `max_items` на дерево и опционально `max_depth` для самоподпитки (page → page). Сверх лимита — не ошибка, а счётчик `skipped_by_limit`, чтобы итоги оставались честными |
| Колбэк менял свой же батч — Sidekiq запрещает [V] | Запрет менять финализированный батч из его колбэка, кроме явного `retry_failed` |
| Порядок колбэков child/parent не гарантирован (Sidekiq, `complete`) [V] | Гарантия: `on_finalized` под-батча коммитится раньше финализации родителя (виртуальный Item завершается в той же транзакции) |

## 4. Что добавить в прогресс

| Идея (откуда) | Предложение |
|---|---|
| Счётчики по состояниям + рекурсивные `subs` (Oban `status/1`) | `view()` отдаёт по каждому под-батчу: `found`, `queued`, `in_flight`, `ok/skip/error/cancelled`, `duplicates`, `skipped_by_limit`, `final` (= sealed) |
| Оценка по коэффициентам ветвления (Кнут) | `expected` с полями `basis` (сколько задач-источников в выборке) и `is_estimate`. Не показываем, пока выборка меньше `min(20, 5%)` задач источника |
| ETA как время до опустошения, а не процент (Dataflow backlog seconds, tqdm EMA 0.3, rich 30 с) | `eta` = (ожидаемое − сделано) / EMA пропускной способности под-батча |
| Собственный прогресс задачи (`$ARGO_PROGRESS_FILE` у Argo) | `th.item.progress(n, of)` пишется в `th_lease` вместе с heartbeat — узкая таблица, лишних записей нет. Видно в `in_flight` (скачано 40 из 120 МБ) |
| Список задач в работе для отладки (Temporal sliding window) | `handle.in_flight(limit=)` — из `th_lease`, с возрастом lease и собственным прогрессом |
| Полоса, идущая назад, воспринимается плохо (UIST 2007 о неравномерном прогрессе; про откат прямого источника нет [I]) | Библиотека отдаёт честные числа; рецепт в доке: в домене `progress = GREATEST(progress, :p)` |

## 5. Итоговая модель для кейса

```python
async with th.batch(kind="catalog_parse", key=f"catalog:{cid}", max_items=200_000) as root:
    pages = root.sub_batch("pages", max_depth=1)                          # page 1 → pages 2..24
    cards = root.sub_batch("cards", fed_by=[pages], max_in_flight=100)
    pdfs  = root.sub_batch("pdfs",  fed_by=[cards], max_in_flight=50, on_feeder_failed="seal")
    await pages.add(parse_page, url, page=1)

@fq.task()
async def parse_page(url: str, page: int) -> None:
    html = await fetch(url, page)
    if page == 1:
        n = total_pages(html)
        th.item.expect(n)
        for p in range(2, n + 1):
            th.item.spawn(parse_page, url, p)
    for card in cards_of(html):
        th.item.spawn(parse_card, card.url, into="cards", key=normalize_url(card.url))

@fq.task(weight=2)
async def parse_card(url: str) -> None:
    for pdf in pdf_links(await fetch(url)):
        th.item.spawn(download_pdf, pdf, into="pdfs", key=normalize_url(pdf))

@fq.task(weight=4)
async def download_pdf(url: str) -> None:
    async for done, total in stream_download(url):
        th.item.progress(done, total)                    # видно в in_flight
```

```
parse_catalog                                    ≈73% (оценка) · ETA ~6 мин
  pages  24 / 24                ✓
  cards  600 / 712              ✓ · в работе 40/100 · дублей 18
  pdfs   1 300 / 1 540 найдено  · ≈1 827 ожидается (по 600 карточкам) · в работе 50/50 · ETA 5 мин
```
