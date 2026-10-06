# PostgreSQL

## Требования к базе

Одна база
: Таблицы tallyho и ваши таблицы, которые меняют хуки, лежат в одной базе
  PostgreSQL.

Только primary
: Движок, переданный в `Tallyho`, должен смотреть на primary. Решения о
  финализации по данным реплики приводят к ложному «ещё не готово» или к худшему.

Соединения
: tallyho берёт соединения из пула вашего `AsyncEngine`. Лидер maintenance
  постоянно удерживает одно соединение и берёт ещё на время проходов; учтите это в `pool_size`.

Время
: Все сроки (аренда, отложенный старт, дедлайны, retention) считаются по часам базы,
  поэтому расхождение часов между серверами приложения на учёт не влияет.

## Autovacuum

tallyho пишет много и коротко. Миграция сама задаёт параметры хранения для своих таблиц - менять
их вручную не нужно, достаточно не выключать autovacuum.

| Таблицы | Что задаёт миграция | Зачем |
|---|---|---|
| `th_counter`, `th_metric` | `fillfactor=50`, `autovacuum_vacuum_scale_factor=0`, `autovacuum_vacuum_threshold=1000` | строки счётчиков обновляются постоянно; свободное место на странице даёт обновления без записи в индексы (HOT), а порог по числу мёртвых строк запускает очистку рано |
| `th_outbox`, `th_lease`, `th_counter_delta`, `th_window` | `autovacuum_vacuum_scale_factor=0`, `autovacuum_vacuum_threshold=1000` | таблицы «вставили и удалили»: их размер должен соответствовать текущей работе, а не истории |
| `th_item` | `fillfactor=85` | единственное обновление задачи (запись итога) не трогает индексы |

Убедиться, что параметры на месте, можно запросом к каталогу PostgreSQL:

```sql
SELECT c.relname, c.reloptions
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'app' AND c.relname IN ('th_counter', 'th_outbox', 'th_item');

--   relname   |                                  reloptions
-- ------------+-------------------------------------------------------------------------------
--  th_counter | {fillfactor=50,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=1000}
--  th_outbox  | {autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=1000}
--  th_item    | {fillfactor=85}
```

Что проверить на своей стороне:

* autovacuum включён, и воркеров autovacuum хватает (`autovacuum_max_workers`): при высокой нагрузке
  частые очистки маленьких таблиц tallyho не должны ждать очистки ваших больших таблиц;
* очистка действительно проходит:

```sql
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = 'app' AND relname LIKE 'th\_%'
ORDER BY n_dead_tup DESC;
```

Если `n_dead_tup` у `th_counter` или `th_outbox` стабильно на порядки больше `n_live_tup`, очистке
что-то мешает - чаще всего длинная транзакция (следующий раздел).

## Длинные транзакции и `backend_xmin`

Главный враг любых горячих таблиц, не только tallyho, - транзакция, которая долго остаётся
открытой где угодно в кластере. PostgreSQL не может убрать старые версии строк, пока они видны
хотя бы одной живой транзакции. Счётчики обновляются тысячи раз в секунду; если горизонт очистки
стоит, страницы счётчиков распухают, цепочки версий удлиняются, и каждое чтение и обновление
становится медленнее. Корректность при этом не страдает - падает скорость.

Что держит горизонт:

* сессии `idle in transaction` (забытый `BEGIN`, зависший обработчик запроса);
* долгие аналитические запросы и `pg_dump` на primary;
* реплика с `hot_standby_feedback = on`, на которой идёт долгий запрос;
* незавершённые подготовленные транзакции (`pg_prepared_xacts`) и отставшие слоты репликации.

Как найти:

```sql
SELECT pid, usename, application_name, state,
       now() - xact_start AS xact_age,
       age(backend_xmin)  AS xmin_age,
       left(query, 80)    AS query
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC
LIMIT 10;
```

Что настроить:

* `idle_in_transaction_session_timeout` - например, 60 секунд для роли приложения: забытые
  транзакции будут закрыты сами;
* мониторинг и алерт на возраст самой старой транзакции (`max(now() - xact_start)` и
  `max(age(backend_xmin))` из запроса выше);
* тяжёлую аналитику и `pg_dump` - на реплику без `hot_standby_feedback` либо в окно низкой нагрузки;
* в своих задачах не держите транзакцию открытой на время сетевых вызовов: открыли, записали,
  закоммитили.

Собственные транзакции tallyho короткие. Они ставят `lock_timeout` (5 секунд по умолчанию) и
`statement_timeout` и сами повторяются при дедлоке, конфликте сериализации и таймауте блокировки.
Если повторы не помогли, операция бросает `ConcurrentModification`. Хук ограничен `hook_timeout`.

### Уровень изоляции вашей транзакции

`session=` во всех операциях и `item.complete_in(session)` работают при `READ COMMITTED` и
`REPEATABLE READ`. Завершение задачи в вашей транзакции не трогает горячие строки счётчиков,
поэтому не ждёт чужих блокировок и не даёт ошибок сериализации.

## pgbouncer

tallyho работает через pgbouncer в режиме **transaction pooling** с обоими драйверами. Нужно
отключить кэш подготовленных выражений на стороне драйвера, иначе получите
`prepared statement does not exist`:

<!-- tallyho-noexec: фрагмент конфигурации: нужен запущенный pgbouncer -->
```python
from sqlalchemy.ext.asyncio import create_async_engine

# asyncpg
engine = create_async_engine(
    "postgresql+asyncpg://app:secret@pgbouncer:6432/app",
    connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0},
)

# psycopg 3
engine = create_async_engine(
    "postgresql+psycopg://app:secret@pgbouncer:6432/app",
    connect_args={"prepare_threshold": None},
)
```

Через transaction pooling проверены: создание батчей в вашей сессии, выполнение и завершение задач,
финализация с хуками, `pause`/`resume`.

Двум возможностям нужна сессия, а не транзакция, поэтому им дайте прямое подключение к
PostgreSQL (или пул pgbouncer в режиме session):

| Что | Почему |
|---|---|
| процесс maintenance | лидер удерживает advisory lock уровня сессии; через transaction pooling блокировка «уезжает» на чужое серверное соединение, и выбор лидера перестаёт работать |
| `handle.watch()` и `handle.wait()` | используют `LISTEN`/`NOTIFY`; через transaction pooling уведомления не приходят, и обновления приходят только по таймауту опроса |

Проще всего завести для процесса maintenance отдельный `AsyncEngine` с прямым DSN. Остальные
процессы (API, воркеры) могут ходить через pgbouncer.

## Retention

Завершённые деревья удаляет лидер maintenance - порциями, не блокируя работу. Правила и
`release()` описаны в разделе [Retention и `release()`](../hooks/retention.md). Если
`retention=None`, таблицы растут без ограничений: это допустимо, но следите за размером `th_item`.
