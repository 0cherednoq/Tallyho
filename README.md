# tallyho

[![CI](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml/badge.svg)](https://github.com/0cherednoq/tallyho/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/tallyho.svg)](https://pypi.org/project/tallyho/)
[![Python](https://img.shields.io/pypi/pyversions/tallyho.svg)](https://pypi.org/project/tallyho/)

Async-библиотека для Python + PostgreSQL. Добавляет к любому брокеру задач групповой учёт
(батчи, вложенные батчи, прогресс, финализация ровно один раз), динамический fan-out,
конвейеры этапов и транзакционные хуки в доменные таблицы.

> Статус: pre-alpha, идёт реализация. Архитектура — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Установка

```bash
pip install "tallyho[asyncpg]"            # или tallyho[psycopg]
pip install "tallyho[asyncpg,flexiq]"     # с адаптером flexiq
```

## Разработка

Нужен [uv](https://docs.astral.sh/uv/) и (для интеграционных тестов) Docker.

```bash
uv sync --all-extras               # окружение + все инструменты
uv run pre-commit install          # хуки на commit и push
uv run poe check                   # всё, что проверяет CI (кроме интеграции)
uv run poe test-all                # + интеграционные тесты с PostgreSQL
```

Подробнее — [CONTRIBUTING.md](CONTRIBUTING.md).

## Лицензия

MIT
