# Alembic

Если схемой базы управляет Alembic, вызывайте миграции tallyho из своих ревизий. Тогда таблицы
библиотеки создаются и обновляются тем же `alembic upgrade`, что и ваши.

## Установка

```bash
pip install "tallyho[asyncpg,alembic]"
```

## Подключение

<!-- tallyho-noexec: файл ревизии выполняет Alembic внутри вашего проекта -->
```python
"""add tallyho tables"""

from alembic import op

from tallyho.storage.alembic import upgrade as tallyho_upgrade


def upgrade() -> None:
    tallyho_upgrade(op, version=1, schema="app")
```

## Правила

* Одна ревизия - одна версия схемы tallyho. Номер версии указывается явно, чтобы ревизия не
  меняла смысл при обновлении библиотеки. Версии идут подряд, пропускать их нельзя: когда
  выйдет `version=2`, для неё понадобится следующая ревизия.
* Актуальную версию схемы возвращает `th.migrate()` и печатает `tallyho migrate`. Сейчас это 1.
  После обновления библиотеки сравните её с последней версией в своих ревизиях и допишите
  недостающие.
* Параметры `upgrade(op, *, version, schema, prefix="th_", lock_timeout=...)` должны совпадать с
  параметрами клиента `Tallyho`.
* Транзакцией и порядком управляет Alembic. Работает и offline-режим (`alembic upgrade --sql`).
* Ревизия записывает версию в служебную таблицу, поэтому `th.migrate()` после неё ничего не делает.
* Обратных миграций (`downgrade`) в v1 нет.

Остальные способы создать таблицы, `th.migrate()` и команда `tallyho migrate`, описаны на странице
[Установка и миграции](../guide/installation.md#миграции).
