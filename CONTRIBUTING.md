# Разработка tallyho

## Окружение

```bash
uv sync --all-extras
uv run pre-commit install
```

## Команды (`uv run poe <task>`)

| Задача | Что делает |
|---|---|
| `fmt` | `ruff format` + `ruff check --fix` |
| `lint` | проверка стиля и линт без исправлений |
| `types` | `mypy` (strict+) и `basedpyright` (режим `all`) |
| `imports` | контракты архитектуры `import-linter` |
| `deps` | `deptry`: зависимости объявлены и используются |
| `test` | юнит- и архитектурные тесты |
| `test-all` | всё, включая PostgreSQL (Docker или `TALLYHO_TEST_DSN`) |
| `check` | `lint` → `types` → `imports` → `deps` → `test` |
| `check-all` | то же, но с `test-all`: PostgreSQL и покрытие ≥ 95% |

## Что проверяется и где

| Проверка | pre-commit | pre-push | CI |
|---|---|---|---|
| ruff format / ruff check (`ALL` + preview) | ✓ | | ✓ |
| mypy, basedpyright | ✓ | | ✓ |
| import-linter, deptry | ✓ | | ✓ |
| unit + architecture tests | | ✓ | ✓ |
| integration (PG 14/16/17), покрытие ≥ 95% | | | ✓ |
| сборка wheel/sdist, `twine check` | | | ✓ |

## Структура

```
src/tallyho/
  model/       нижний слой: состояния, сводки, ошибки (без БД)
  protocols/   Dispatcher, Runtime, Serializer, Clock, Observer
  storage/     таблицы, запросы, миграции (SQLAlchemy Core)
  hooks/       реестр tx-хуков
  engine/      Completer, Relay, Sweeper, Finalizer, Snapshotter
  runtime/     tracked, ItemContext
  api/         Tallyho, BatchBuilder, BatchHandle
  adapters/    адаптеры брокеров (flexiq)
  testing/     InlineBroker, FakeClock — для тестов пользователей
  cli/         командная строка
tests/
  unit/          быстрые тесты без БД
  architecture/  AST-проверки соглашений
  integration/   PostgreSQL
  helpers/       общие хелперы тестов (импорт: `tests.helpers.*`)
```

## Интеграционные тесты

* Фикстуры в `tests/integration/conftest.py`: `engine`, `schema` (пустая схема,
  уникальная на тест, после теста — `DROP SCHEMA ... CASCADE`), `connection`
  (`AsyncConnection`), `session` (`AsyncSession`). Таблицы теста создавайте только
  в `schema`.
* `tests/helpers/db.py`: `deadlock_count` (`pg_stat_database.deadlocks`, сравнивать
  разницу до/после), `held_locks` (`pg_locks`).
* Параллельно: `uv run pytest -n 4`. Без `TALLYHO_TEST_DSN` каждый xdist-воркер
  поднимает свой контейнер PostgreSQL, с ним — все делят одну БД.
* Долгие тесты помечаются `@pytest.mark.slow`; пропустить их: `-m "not slow"`.

Слои и запреты импортов описаны в `[tool.importlinter]` в `pyproject.toml`
и соответствуют [docs/ARCHITECTURE.md §3.3](docs/ARCHITECTURE.md).

## Релиз

1. Обновить `version` в `pyproject.toml` (`uv version --bump minor`) и `CHANGELOG.md`.
2. Тег `vX.Y.Z` → workflow `release.yml` публикует на PyPI через Trusted Publishing.
