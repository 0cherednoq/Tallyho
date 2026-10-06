# Командная строка

Команда `tallyho` ставится вместе с пакетом (то же самое - `python -m tallyho.cli`).

```bash
tallyho migrate --dsn postgresql+asyncpg://app:secret@db/app --schema app
# schema=app version=1
```

| Команда | Что делает |
|---|---|
| `tallyho migrate --dsn DSN --schema SCHEMA` | создаёт или обновляет таблицы и печатает версию схемы |
| `tallyho maintenance --dsn DSN --schema SCHEMA [--hook-module MODULE ...] [--once]` | фоновые проверки отдельным процессом - см. [эксплуатацию](../guide/operations.md#maintenance-отдельным-процессом) |
| `tallyho inspect TARGET --dsn DSN --schema SCHEMA` | печатает дерево батча с прогрессом; `TARGET` - UUID батча или `kind:key` корня |
| `tallyho --version` | версия пакета |

`--dsn` - async-DSN SQLAlchemy (`postgresql+asyncpg://…` или `postgresql+psycopg://…`), `--schema`
обязателен. Команды работают с префиксом по умолчанию `th_`; установку с другим префиксом
мигрируйте через `th.migrate()` или Alembic.

Пример вывода `inspect` для конвейера из двух этапов (идентификаторы сокращены):

```text
catalog_parse key=catalog:7 id=01a0fbbd-… state=sealed done=0/2 found=2 queued=2 in_flight=0 errors=0 cancelled=0 progress=33.3%
  catalog_parse.cards key=cards id=01a0fbbd-… state=open done=2/6 found=2 queued=0 in_flight=0 errors=0 cancelled=0 progress=33.3%
  catalog_parse.pages key=pages id=01a0fbbd-… state=sealed done=1/3 found=3 queued=2 in_flight=0 errors=0 cancelled=0 progress=33.3%
```

`done=1/3` - завершено и ожидается всего, `?` на месте числа - оценки пока нет. В строке родителя
каждый под-батч считается одной задачей.
