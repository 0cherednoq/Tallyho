# RFC: недостающие lifecycle-хуки Tallyho v1

> **Отложено до v1.x (D-040).** В v1 этот RFC не реализуется: исходы Items переносит рецепт финального экспорта (ARCHITECTURE §12.9), обоснование — [V1_EXTENSIONS_PLAN.md](V1_EXTENSIONS_PLAN.md) §2. Документ сохранён как задел дизайна.

> Дата: 2026-10-01
> Ветка: `impl/v1`
> Статус: **предложение, не источник истины.** API из этого документа не существует, пока решение не перенесено в [ARCHITECTURE.md](../ARCHITECTURE.md) и [ACCEPTANCE.md](../ACCEPTANCE.md).

## Для кого этот документ

Документ предназначен для разработчика Tallyho, который проектирует публичный API v1. Цель: закрыть интеграцию с постоянными доменными таблицами приложения, не превращая Tallyho в workflow engine и не перенося бизнес-состояние в `th_*`.

## Решение

До v1 стоит добавить только два lifecycle-механизма:

1. `on_started` и парный `on_started_task` для фактического начала Batch.
2. Батчевый `on_terminal_items` для реальных терминальных Item, включая исходы, которые создала сама библиотека.

Не следует добавлять общие `before_task`, `after_task`, `on_attempt_started` и `on_attempt_finished`. Они вызываются на горячем пути, зависят от семантики брокера и дублируют код задачи или observability.

| Потребность приложения | Механизм |
| --- | --- |
| Атомарно сохранить бизнес-результат обычной задачи | Существующий `th.item.complete_in(session)` |
| Перевести Campaign из `scheduled` в `running`, когда работа реально началась | Новый `on_started` или `on_started_task` |
| Сохранить живые агрегированные счётчики | Существующий `on_progress` |
| Отразить `exhausted`, отмену и deadline в строках отдельных сущностей | Новый батчевый `on_terminal_items` |
| Установить окончательные счётчики и статус | Существующий `on_finalized` или `on_finalized_task` |
| Не удалить технические строки до переноса результата | Существующий `release_required=True` и `handle.release()` |

## Почему существующего API недостаточно

Существующие механизмы покрывают нормальный путь:

```text
задача
  -> изменить доменную сущность
  -> th.item.ok/error/skip(...)
  -> th.item.complete_in(session)
  -> один commit для домена и Tallyho Item
```

Остаются два пробела.

### Нет события фактического старта Batch

`start_at` означает «не публиковать раньше указанного времени», но не доказывает, что воркер начал работу. `on_progress` может прийти позже, а `on_finalized` приходит слишком поздно. Приложению приходится добавлять искусственный стартовый stage или переводить доменный статус первой пользовательской задачей.

### Нет проекции библиотечных терминальных исходов по отдельным Item

Пользовательская задача может атомарно вызвать `complete_in`. Однако некоторые исходы создаёт runtime Tallyho после выхода из пользовательского кода:

- исключение исчерпало retry и стало `error/exhausted`;
- Item отменён до выполнения;
- Item завершён по deadline или системной политике;
- Item завершён recovery-механизмом после сбоя воркера.

`on_progress` и `on_finalized` передают только агрегаты. По ним нельзя определить, какую строку `mailing_delivery` следует перевести в `failed` или `cancelled`.

## H1. Хук фактического старта Batch

### Публичный API

Транзакционный хук для приложений в той же PostgreSQL:

```python
@th.on_started("mailing.send")
async def mark_campaign_running(
    session: AsyncSession,
    summary: BatchSummary,
) -> None:
    await session.execute(
        update(MailingCampaign)
        .where(
            MailingCampaign.batch_id == summary.id,
            MailingCampaign.status == "scheduled",
        )
        .values(status="running", started_at=summary.started_at)
    )
```

Фоновый callback для application service, DI, другой БД или внешнего I/O:

```python
async with th.batch(
    "mailing.send",
    start_at=scheduled_at,
    on_started_task=th.call(start_campaign, campaign_id),
) as batch:
    ...
```

`BatchSummary` и `BatchView` получают поле, прочитанное из side-таблицы lifecycle:

```python
started_at: datetime | None
```

Строка и значение создаются только для Batch, у которого в неизменяемом `th_batch.hooks` объявлен `on_started` или `on_started_task`. Деревья без стартового механизма не выполняют запросов ради lifecycle и возвращают `started_at=None`.

### Точный момент старта

Batch считается начавшимся, когда отдельная транзакция стартового шлюза успешно завершилась перед claim первого реального Item в его поддереве. Это означает «работа допущена к claim», а не «lease уже выдан» и не «пользовательская функция уже начала выполняться».

Между commit стартового шлюза и claim остаётся crash-window: процесс может завершиться, пауза или отмена может победить в гонке. Поэтому строгий инвариант `started_at задан тогда и только тогда, когда уже был успешный claim` недостижим без общей транзакции и возврата к опасному порядку блокировок. `started_at` — монотонный факт успешно открытого шлюза; последующая доставка продолжит обычный claim.

Старт распространяется от Batch реального Item вверх по всей цепочке `parent_id` до корня. Это обязательно для конвейеров: у корня и промежуточных Batch могут быть только виртуальные Items, которые никогда не проходят broker claim.

```text
root (виртуальные Items)
  -> expand (реальный Item получил первый claim)
       => started: expand
       => started: root

  -> send (первый реальный Item получил claim позднее)
       => started: send
       => root уже started, повторного хука нет
```

Не считаются стартом:

- создание Batch;
- `seal()`;
- наступление `start_at`;
- запись Item в outbox;
- публикация сообщения брокеру;
- завершение или реактивация виртуального Item под-батча.

Виртуальный Item не является самостоятельным источником старта, но реальная работа его дочернего Batch запускает всех предков. Доменный статус `running` появляется только после того, как реальное вычисление готово получить воркера.

### Хранение старта без блокировки `th_batch`

Нельзя делать CAS через `UPDATE th_batch SET started_at = ...`. Claim и producer держат строки `th_batch FOR SHARE`; переход к эксклюзивной блокировке создаст ожидания до `lock_timeout` и риск дедлоков.

Старт хранится в отдельной узкой таблице:

```sql
CREATE TABLE th_batch_start (
    batch_id uuid PRIMARY KEY,
    started_at timestamptz,
    hook_attempts smallint NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    hook_error text
);
```

Перед первым claim отдельная транзакция стартового шлюза вставляет строки только для тех Batch цепочки, у которых `th_batch.hooks` требует `on_started` или `on_started_task`:

```sql
INSERT INTO th_batch_start (batch_id)
VALUES (...)
ON CONFLICT (batch_id) DO NOTHING;
```

Строки lifecycle блокируются в порядке `batch_id`. Транзакция не блокирует и не изменяет `th_batch` и `th_item`, поэтому пользовательский хук не выполняется после технических блокировок Tallyho и не обращает порядок «домен → Tallyho».

После успешного commit идентификатор Batch попадает в процессный кэш начавшихся Batch. Факт неизменяем, поэтому кэш не требует инвалидации. Если вся нужная цепочка уже известна процессу, обычный claim выполняется без lifecycle-запросов. Кэш является только оптимизацией: источником истины остаётся `th_batch_start`, а новый процесс один раз проверит БД.

### Транзакционная семантика

Для дерева, в чьей цепочке есть стартовые механизмы:

```text
получить TreeNode и неизменяемые hooks цепочки до claim

если цепочка не требует старта или целиком есть в process cache:
  перейти к обычному claim

BEGIN start gate                       # отдельная транзакция
  без блокировки проверить:
    Batch не paused/cancelled
    реальный Item ещё active и допущен по start_at/deadline
  иначе завершить gate и перейти к обычной классификации claim

  INSERT th_batch_start ... ON CONFLICT DO NOTHING
    только для узлов со стартовым hook/callback
  SELECT незапущенные строки th_batch_start FOR UPDATE
    в стабильном порядке batch_id

  если общий backoff ещё не истёк:
    записать парковку Item в outbox до next_attempt_at
    COMMIT без claim и без увеличения attempt
    вернуть PARKED

  для незапущенных узлов от root к leaf:
    SAVEPOINT node_start
      вызвать on_started, если объявлен
      создать outbox on_started_task, если объявлен
      UPDATE th_batch_start
        SET started_at=now(), hook_error=NULL, next_attempt_at=NULL
    RELEASE SAVEPOINT

    при ошибке узла:
      ROLLBACK TO SAVEPOINT node_start
      записать attempts/error/backoff этого узла
      записать парковку Item в outbox
      COMMIT уже успешно начатых предков и ошибки узла
      вернуть PARKED
COMMIT start gate

добавить успешно начатые batch_id в process cache
выполнить обычный claim в штатной транзакции Completer
```

Требования:

- стартовые хуки вызываются для текущего Batch и всех ещё не начатых предков;
- lifecycle старта запускается только по доставке реального Item, который предварительная проверка считает кандидатом на claim;
- два конкурентных первых воркера сериализуются на `th_batch_start`; хук выполняет только победитель;
- start gate не блокирует и не изменяет `th_batch` и `th_item`;
- хуки цепочки идут сверху вниз; дочерний узел не стартует раньше всех предков;
- savepoint одного узла атомарно объединяет его доменные изменения, `started_at` и callback outbox;
- ошибка дочернего узла не откатывает уже закоммиченный в этой транзакции старт предков;
- до успешного `on_started` пользовательская задача не начинается;
- повторная доставка или retry Item не вызывает хук повторно после успешного commit;
- `on_started_task` имеет стабильный `callback_id` и at-least-once доставку;
- пустой или отменённый до первого claim Batch не вызывает `on_started`;
- каждый под-батч со стартовым hook/callback имеет независимый `started_at`;
- Batch без стартового hook/callback не получает строку `th_batch_start` и lifecycle-запросы;
- start-хук корня срабатывает и для конвейера, где у корня нет реальных Items.

### Ошибка хука и парковка сообщения

Ошибка `on_started` не должна выходить из tracked wrapper как ошибка пользовательской задачи. Иначе брокер потратит retry, а постоянно сломанный хук уведёт все Items в `exhausted` до запуска бизнес-кода.

После rollback savepoint упавшего узла Tallyho в отдельной транзакции start gate:

1. Увеличивает `th_batch_start.hook_attempts` для упавшего Batch.
2. Записывает `hook_error` и `next_attempt_at` с экспоненциальным backoff.
3. Возвращает доставленный Item во внутренний outbox с `available_at=next_attempt_at`.
4. Не создаёт lease и не увеличивает `th_item.attempt`.
5. Возвращает runtime внутренний результат `PARKED`; пользовательская функция не вызывается, сообщение брокера подтверждается успешно.

Если процесс падает до commit start gate, at-least-once доставка брокера повторит вход. Если он падает после commit gate, но до обычного claim, `started_at` остаётся установленным, а следующая доставка продолжит claim без повтора успешного хука.

Backoff относится к Batch, а не к каждому Item. Пока он действует, другие доставки того же Batch паркуются до общего `next_attempt_at` без повторного вызова хука.

Если упал hook предка, паркуются реальные Items всего его поддерева. Ошибка hook листового Batch не блокирует независимый sibling, который не проходит через этот Batch. Медленный или сломанный start-hook не удерживает групповую транзакцию Completer и не задерживает её claim, heartbeat и finish.

В v1 входят оба варианта: `on_started` для короткой транзакционной проекции в той же PostgreSQL и `on_started_task` для application service, другой БД или внешнего I/O.

### Почему не использовать `on_progress`

`on_progress` предназначен для throttled-снимков. Его `every` допускает задержку, а первый снимок не является доказательством первого claim. Менять точное доменное состояние через эвристику `progress.in_flight > 0` нельзя.

## H2. Батчевый хук терминальных Items

### Назначение

`on_terminal_items` переносит в доменные таблицы результаты отдельных Items, которые нельзя надёжно записать из пользовательской задачи. Основной путь приложения по-прежнему использует `complete_in`; новый хук закрывает отмену, deadline, exhausted и recovery.

### Публичный API

```python
@th.on_terminal_items("mailing.send", batch_size=500)
async def project_delivery_outcomes(
    session: AsyncSession,
    items: Sequence[ItemView],
) -> None:
    await deliveries.apply_terminal_outcomes(session, items)
```

Хук получает существующий публичный `ItemView`, включая небольшие `result` и `error` из `th_item`. Он не получает аргументы задачи и полный payload. Приложение связывает реальный Item с доменной сущностью через безопасный `key`, например `delivery:{uuid}`. Виртуальные Items (`child_batch_id IS NOT NULL`) в этот хук не попадают: у них нет полезного доменного `key`, а итог ребёнка уже доставляет `on_finalized`.

`ItemView` собирается чтением `th_item` в момент проекции. Очередь не копирует `key`, `state`, `label`, `result`, `error`, `attempt` или `finished_at`. Схема `th_item` не меняется, отдельный `generation` не вводится.

### Почему хук батчевый

Вызов пользовательского кода на каждом завершении Item разрушит групповой commit Completer и добавит доменную задержку в горячий путь. `on_terminal_items` должен обрабатывать до `batch_size` событий одной транзакцией.

```text
Item становится terminal
  -> Tallyho атомарно добавляет ссылку в th_item_pending
  -> Item completion commit не ждёт пользовательский хук

Projector
  -> SELECT pending-ссылки FOR UPDATE SKIP LOCKED LIMIT batch_size
  -> JOIN th_item по item_id
  -> on_terminal_items(session, items)
  -> DELETE подтверждённых pending-ссылок
  -> COMMIT домена и подтверждений ссылок вместе
```

Минимальная side-таблица:

```sql
CREATE TABLE th_item_pending (
    batch_id uuid NOT NULL,
    item_id uuid NOT NULL,
    PRIMARY KEY (batch_id, item_id)
);
```

Таблица хранит только идентичность ожидающей проекции. Источник данных остаётся `th_item`. PK одновременно обслуживает выборку текущего Batch; второй индекс и служебный монотонный id не нужны. Projector не сортирует по `finished_at`; порядок бизнес-исходов разных Items не определён контрактом и не нужен для bulk-проекции.

### Все пути терминализации обязаны поставить проекцию

Реальный Item переходит `active -> terminal` не только в обычном `Completer.finish`. После исключения виртуальных Items остаются семь путей терминализации:

- обычный и scalar `finish`, в том числе `complete_in`;
- ленивую отмену при claim;
- явную отмену дерева;
- отмену политикой;
- исчерпание lease retries;
- отменённый lease;
- expiry Item без claim.

Нельзя размазывать `INSERT th_item_pending` вручную без общего контракта. Нужен один storage-примитив, который получает строки, действительно изменённые CAS `active -> terminal`, и вызывается каждым путём. Архитектурный тест должен перечислять все места терминализации или запрещать прямой terminal update вне этого примитива.

Решение о pending-ссылке принимает storage-примитив по неизменяемому `th_batch.hooks`, прочитанному из БД, а не по реестру текущего процесса. Поэтому процесс без импортированного hook-модуля не может молча потерять проекцию. Batch без флага `terminal_items` не получает строку pending и дополнительный projector barrier.

### Гарантии

- одна pending-ссылка соответствует реальному Item текущего Batch;
- автоматические broker retries не создают pending-ссылку, пока Item не стал терминальным;
- Batch не становится терминальным, пока его pending-ссылки не подтверждены; поэтому `retry_failed()` ещё недоступен и не может переоткрыть Item с ожидающей проекцией;
- после подтверждения проекций Batch может финализироваться, а позднейший `retry_failed()` снова использует освободившийся ключ `(batch_id, item_id)` для нового терминального исхода;
- Python-код хука может выполниться повторно после rollback;
- для каждой ссылки существует ровно один успешный commit доменной проекции и её подтверждения;
- ошибка хука не откатывает уже завершённый Item и не блокирует другие виды Batch;
- Batch с зарегистрированным `on_terminal_items` не финализируется, пока все его pending-ссылки не подтверждены;
- `on_finalized` всегда видит домен после применения всех terminal item projections;
- retention не удаляет Batch или Items, пока обязательная проекция не подтверждена;
- контракт не обещает порядок разных Items внутри пачки;
- несколько projector-процессов безопасно делят работу через `FOR UPDATE SKIP LOCKED`;
- неизвестный или отсутствующий зарегистрированный хук блокирует только соответствующую проекцию и финализацию, а не молча теряет ссылку.

При ошибке хука транзакция доменной проекции и удаления pending-ссылок откатывается. Pending-строки являются долговечным состоянием повтора; процессный backoff только ограничивает частоту следующих попыток. После рестарта задержка может сброситься и попытка произойдёт сразу — это безопасно. Ошибка наблюдаема через `Observer.hook_failed` и gauge отставания; отсутствующий обязательный модуль отмечается как `th_hook_missing` и оставляет barrier закрытым. В отличие от start gate, отдельная таблица ошибок Projector для корректности не нужна.

### Идемпотентность доменного обработчика

Несмотря на гарантию одного успешного commit, обработчик не должен прибавлять значения без защиты. Рекомендуемый доменный ключ:

```text
tallyho_item_id
```

Пример применения:

```sql
UPDATE mailing_delivery
SET
    status = :status,
    failure_code = :label,
    finished_at = :finished_at
WHERE tallyho_item_id = :item_id
  AND status NOT IN ('sent', 'failed', 'cancelled');
```

Для обычного `complete_in` доменная строка уже терминальна. Обработчик должен распознать совпадающий исход как no-op. Конфликтующий исход является ошибкой консистентности и не должен молча перезаписывать бизнес-результат. Если бизнесу требуется собственный номер повторного запуска, это доменное поле кампании или доставки, а не lifecycle-поле `th_item`.

### Взаимодействие с `on_progress` и `on_finalized`

Порядок для Batch с обоими новыми хуками:

```text
on_started
  -> пользовательские задачи и complete_in
  -> on_terminal_items пачками
  -> on_progress периодически
  -> все Items terminal
  -> th_item_pending для Batch пуста
  -> on_finalized
  -> Batch terminal
  -> retention
```

`on_progress` записывает абсолютный оперативный снимок. Его вызов не упорядочен относительно `on_started` и отдельных пачек `on_terminal_items`: счётчик `dispatched` может измениться до claim, а снимки throttled. `on_finalized` вызывается только после очистки `th_item_pending` и один раз пересчитывает или перезаписывает окончательный доменный итог. Ни один хук не должен делать `counter = counter + value` без version/seq-защиты.

## Что уже есть и не требует нового хука

### Успешный или ожидаемый бизнес-исход Item

Использовать:

```python
item.ok("sent")
await item.complete_in(session)
```

Отдельный `on_item_succeeded` не нужен.

### Прогресс и счётчики Batch

Использовать `on_progress(every=...)`, `summary.seq`, абсолютные значения и условие `domain.progress_seq < summary.seq`.

### Окончательный статус Batch

Использовать `on_finalized` для короткой DB-only логики или `on_finalized_task` для application service и внешнего I/O.

### Автоматическая пауза или провал по политике

Использовать существующий `on_policy_breach`.

### Ручные pause, resume, cancel и retry

Команда приложения сама инициирует операцию. Она может изменить доменную строку и вызвать `handle.pause/resume/cancel/retry_failed(session=session)` в одной транзакции. Lifecycle-хук здесь дублировал бы уже известное намерение.

### Seal и retention

Пользователь, который вызывает `seal`, уже знает об этом действии. Для защиты переноса результатов существуют `on_finalized`, `release_required` и `release`. `on_sealed` и `on_purged` для v1 не нужны.

## Какие хуки не добавлять в v1

| Предложение | Решение | Причина |
| --- | --- | --- |
| `before_task` / `after_task` | Не добавлять | Горячий путь, неясная транзакционность, легко поместить бизнес-логику не в тот слой |
| `on_attempt_started` | Не добавлять | Попытка является технической деталью брокера; для диагностики достаточно Observer и lease |
| `on_attempt_failed` | Не добавлять | До исчерпания retry это не доменный исход |
| `on_retry_scheduled` | Не добавлять | Зависит от broker adapter; относится к observability |
| `on_item_succeeded/error/skipped` отдельными API | Не добавлять | Один `on_terminal_items` плюс `state/label` расширяется без роста API |
| `on_sealed` | Не добавлять | Seal не означает начало или окончание работы |
| `on_purged` | Не добавлять | Бизнес-данные должны быть перенесены до retention |
| Автоматический `BatchState -> domain status` mapping | Не добавлять | Tallyho не владеет доменной state machine |

## Минимальный API v1 после решения

```python
# Существующие tx-хуки
th.on_progress(kind, every=...)
th.on_finalized(kind)
th.on_policy_breach(kind)

# Новые tx/projector-хуки
th.on_started(kind)
th.on_terminal_items(kind, batch_size=500)

# Существующие callback tasks
on_succeeded=th.call(...)
on_completed_with_errors=th.call(...)
on_failed=th.call(...)
on_cancelled=th.call(...)
on_finalized_task=th.call(...)

# Новый callback task
on_started_task=th.call(...)
```

## Критерии приёмки H1

1. Сто конкурентных claim одного Batch дают один успешный commit `on_started`.
2. Первый реальный Item листового stage запускает hook этого stage и всех не начатых предков, включая корень с одними виртуальными Items.
3. Первый Item следующего stage запускает только ещё не начатый stage и не повторяет hook корня.
4. Claim и параллельный producer с `th_batch FOR SHARE` не ждут эксклюзивного update `th_batch` ради lifecycle.
5. Ошибка `on_started` откатывает доменные изменения и не запускает пользовательскую задачу.
6. Ошибка хука возвращает Item во внутренний outbox с backoff, не увеличивает `th_item.attempt` и не бросает ошибку брокеру.
7. Десять доставок сломанного Batch вызывают hook не чаще общего Batch backoff и не превращаются в `exhausted`.
8. После исправления хука следующий claim повторяет старт без ручного изменения БД.
9. Batch с будущим `start_at` не вызывает хук раньше времени.
10. Пустой Batch финализируется без `on_started`.
11. Отмена до первого claim не вызывает `on_started`.
12. Retry и duplicate delivery после старта не вызывают второй успешный commit.
13. `on_started_task` записывается в outbox атомарно с успешным стартом и получает стабильный `callback_id`.
14. Недостающий обязательный hook module паркует Item и не приводит к тихому старту без доменной проекции.
15. Start gate работает в отдельной транзакции и не удерживает блокировки `th_batch`/`th_item` во время пользовательского хука.
16. Хуки цепочки выполняются от корня к листу; падение предка не оставляет потомка начатым.
17. Падение процесса после commit start gate и до claim не повторяет успешный hook; `started_at` означает открытый шлюз, а не выданный lease.
18. Batch без `on_started` и `on_started_task` не создаёт `th_batch_start`, не делает lifecycle-запросов и возвращает `started_at=None`.

## Критерии приёмки H2

1. Хук получает `ItemView` реальных Items со всеми состояниями и метками, включая `ok`, `skip`, `error`, `cancelled` и `exhausted`; виртуальные Items исключены.
2. Каждый из семи путей реального `active -> terminal` атомарно добавляет pending-ссылку через общий storage-примитив.
3. Исход из `complete_in` и его pending-ссылка коммитятся атомарно.
4. Pending-таблица не копирует `key`, `state`, `label`, `result`, `error` и `finished_at` из `th_item`.
5. Projector не зависит от сортировки по `finished_at`.
6. Ошибка хука откатывает доменную запись и удаление pending-ссылок, но не терминальный Item.
7. После рестарта projector повторяет неподтверждённую пачку.
8. Два projector-процесса не подтверждают одну ссылку двумя успешными commit.
9. Пока pending-ссылка существует, barrier не даёт Batch стать терминальным, поэтому `retry_failed()` недоступен.
10. После подтверждения проекции и последующего `retry_failed()` новый терминальный исход снова создаёт ссылку `(batch_id, item_id)`.
11. `on_finalized` не вызывается раньше очистки pending-ссылок Batch.
12. Retention не удаляет связанные Items при неподтверждённой проекции.
13. Batch без `on_terminal_items` не платит за pending INSERT, projector-запросы и barrier.
14. Пачка из 500 исходов допускает один bulk `UPDATE` доменной таблицы.
15. В переданном `ItemView` нет аргументов задачи, секретов и неограниченного payload; небольшие `result/error` доступны.
16. Отсутствующий hook module наблюдаем и не приводит к потере проекции.
17. Решение о создании ссылки принимает `th_batch.hooks` из БД, а не process-local registry.
18. Ошибка Projector откатывает пачку, включает process-local backoff и `Observer.hook_failed`; после рестарта pending-ссылки сохраняются.

## Порядок реализации

1. Принять или отклонить семантику H1 и H2 в `docs/ARCHITECTURE.md`.
2. Добавить сценарии H1/H2 и crash-points в `docs/ACCEPTANCE.md`.
3. Сначала прототипировать `th_batch_start`, ancestor propagation и парковку с backoff на конкурентных claim.
4. Затем реализовать минимальную pending-таблицу ссылок и barrier финализации.
5. Добавить публичные типы и декораторы только после проверки конкурентных прототипов.
6. Свести все семь terminal transition реальных Items к общему storage-примитиву или защитить их архитектурным тестом.
7. Расширить `tests/examples/mailing`: постоянные `mailing_delivery`, атомарный `complete_in`, библиотечный `exhausted`, отмена, progress, finalization и retention.
8. Измерить влияние H2 на Completer и retention benchmark. Batch без хука не должен получать дополнительный запрос на каждый Item.

## Принятые решения и оставшийся замер

1. В v1 нужны и `on_started`, и `on_started_task`.
2. `on_terminal_items` получает существующий `ItemView`, включая небольшие `result/error`.
3. `batch_size=500` — default; допустимый верхний предел нужно подтвердить benchmark-тестом до фиксации API.
4. Служебный монотонный id pending-таблицы не нужен; порядок между пачками не обещается.
5. Barrier H2 проверяет только текущий Batch. Каскад родителя останавливается естественно на виртуальном Item ребёнка.
6. `started_at` хранится только для Batch с `on_started` или `on_started_task`; остальные Batch не платят за этот механизм.
7. Виртуальные Items и `generation` не входят в H2.

До замера предела `batch_size` публичный API нельзя считать окончательно зафиксированным.
