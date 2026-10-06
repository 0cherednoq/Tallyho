# pgbouncer

tallyho работает через pgbouncer в режиме transaction pooling с обоими драйверами. Двум
возможностям нужно прямое подключение, о них ниже.

## Подключение

Отключите кэш подготовленных выражений на стороне драйвера, иначе получите
`prepared statement does not exist`.

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
финализация с хуками, `pause` и `resume`.

## Чему нужно прямое подключение

Двум возможностям нужна сессия, а не транзакция. Им дайте прямое подключение к PostgreSQL или пул
pgbouncer в режиме session.

| Что | Почему |
|---|---|
| процесс maintenance | лидер удерживает advisory lock уровня сессии; через transaction pooling блокировка «уезжает» на чужое серверное соединение, и выбор лидера перестаёт работать |
| `handle.watch()` и `handle.wait()` | используют `LISTEN`/`NOTIFY`; через transaction pooling уведомления не приходят, и обновления приходят только по таймауту опроса |

Проще всего завести для процесса maintenance отдельный `AsyncEngine` с прямым DSN. Остальные
процессы, API и воркеры, могут ходить через pgbouncer.
