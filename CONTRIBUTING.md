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
| `docs` | сайт документации в `docs/_build/html`; нужна группа `docs` (`uv sync --all-extras --group docs`) |
| `bench` | бенчмарки A-PERF: `--id P-01[,P-04,...]` или `all`, `--profile smoke\|nightly\|full`; отчёт в `.work-tmp/bench/` ([docs/benchmarks](docs/benchmarks/README.md)) |

## Что проверяется и где

| Проверка | pre-commit | pre-push | CI |
|---|---|---|---|
| ruff format / ruff check (`ALL` + preview) | ✓ | | ✓ |
| mypy, basedpyright | ✓ | | ✓ |
| import-linter, deptry | ✓ | | ✓ |
| unit + architecture tests | | ✓ | ✓ |
| integration (PG 14/16/17), покрытие ≥ 95% | | | ✓ |
| сборка wheel/sdist, `twine check` | | | ✓ |

## CI (ACCEPTANCE §11)

**PR** — `.github/workflows/ci.yml`, бюджет ≤ 15 минут:

* lint, типы, import-linter, deptry, pre-commit;
* полный набор на Python 3.11 × PostgreSQL 16 разбит на четыре параллельные части (`engine`,
  `examples`, `storage` = storage + stress, `rest` — всё остальное); job `coverage` объединяет
  данные частей (`coverage combine`) и проверяет ≥ 95%;
* короткий compatibility smoke на Python 3.12/3.13/3.14 × PostgreSQL 16, Python 3.11 × PG 14
  и Python 3.14 × PG 17;
* `poe test-flexiq` (flexiq из `uv.lock`) и `tests/acceptance -m flexiq` — эталонное приложение
  и юнит-тесты стенда;
* A-UC-01/02/04/21/22 на compose-стенде (`--scale 0.1`), три параллельных job.

**Nightly** — `.github/workflows/nightly.yml`, по расписанию и вручную (`workflow_dispatch` с
`seed` и `duration`), бюджет ≤ 4 часа:

* A-CH: 12 отказов × S1/S2/S3, по ячейке на runner, окно хаоса 600 с;
* A-UC-01…22 на функциональном объёме — четыре части по два стенда;
* полный набор тестов на дополнительных комбинациях Python/PostgreSQL из compatibility smoke;
* контракты flexiq на последнем патче 2.0.x и на master (Python 3.11 и 3.13); источник master —
  переменные репозитория `FLEXIQ_GIT_URL` / `FLEXIQ_GIT_REF`, по умолчанию
  `https://github.com/ByteVeda/flexiq` @ `master`, Python SDK в `sdks/python`;
* P-01 и P-04 — `poe bench --id P-NN --profile nightly` (см. [docs/benchmarks](docs/benchmarks/README.md)),
  отчёт `.work-tmp/bench/` — артефакт запуска.

Seed nightly по умолчанию — дата запуска `YYYYMMDD`; он и команда воспроизведения печатаются
в лог и в сводку запуска. `.work-tmp/acceptance/**` (журнал хаоса, `oracle.json`,
`containers.log`) прикладывается к запуску артефактом.

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
benchmarks/      харнесс A-PERF P-01…P-11 (`poe bench`, docs/benchmarks)
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

## Документация

Сайт собирается Sphinx с темой [Shibuya](https://shibuya.lepture.com/) из страниц в MyST Markdown.

```bash
uv sync --all-extras --group docs
uv run poe docs                               # HTML в docs/_build/html, предупреждение = ошибка
python -m http.server -d docs/_build/html     # посмотреть локально
```

| Что | Где |
|---|---|
| главная и оглавление | `docs/index.md` |
| руководство | `docs/guide/` |
| интеграции (flexiq, Alembic, pytest, метрики, pgbouncer) | `docs/integrations/` |
| архитектура для пользователя | `docs/architecture/` |
| справочник настроек, CLI, ошибок | `docs/reference/` |
| справочник API из докстрингов | `docs/reference/api/` |
| конфигурация, стили, логотип | `docs/conf.py`, `docs/_static/` |
| таблицы схемы из кода (страница «Хранилище») | `docs/_ext/tallyho_schema.py` |

Остальные файлы в `docs/` (ARCHITECTURE, ACCEPTANCE, `plan/`, `benchmarks/`) - внутренние документы
проекта, на сайт они не попадают: список страниц сайта задаёт `include_patterns` в `docs/conf.py`.

Правила для страниц:

* Примеры на страницах - код приложения без проверок: задачи через `@fq.task`, батчи в
  обработчиках API, хуки в свои таблицы. Что код вернёт или запишет, пишется комментарием под ним.
  `assert` остаётся только на странице «Тестирование».
* Каждый блок `python` помечен `<!-- tallyho-noexec: причина -->` либо, если он выполняется в CI,
  `<!-- tallyho-example: имя -->`. Новую страницу добавьте в манифест
  `tests/examples/test_documentation.py`.
* Поведение, которое описывает пример, подтверждает сценарий с проверками в
  `tests/examples/guide_scenarios.md` (выполняется в CI на PostgreSQL, имя сценария - в том же
  манифесте). Сценарии учебного раздела лежат в `tests/examples/tutorial/`. Меняете пример на
  странице - поправьте и сценарий.
* Ссылки между страницами относительные, на файл `.md`, с якорем как на GitHub. Их проверяют и тест,
  и сборка сайта.
* Предупреждения, вкладки и карточки пишутся директивами с двоеточиями (`:::{warning}`,
  `::::{tab-set}`), диаграммы - блоком `mermaid`.

Публикует сайт `.github/workflows/docs.yml`: на PR только сборка, после push в `main` - выкладка на
GitHub Pages. В настройках репозитория один раз включите Settings, Pages, Source: GitHub Actions.

## Релиз

1. Обновить `version` в `pyproject.toml` (`uv version --bump minor`) и `CHANGELOG.md`.
2. Тег `vX.Y.Z` → workflow `release.yml` публикует на PyPI через Trusted Publishing.
