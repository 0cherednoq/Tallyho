# Правила для LLM-агентов

Этот репозиторий пишется в основном LLM. Правила ниже обязательны.

## Источник истины

- Архитектура — `docs/ARCHITECTURE.md`. При расхождении с другими документами прав он.
- Критерии приёмки — `docs/ACCEPTANCE.md`.
- Не выдумывай API, которого нет в ARCHITECTURE. Если нужно отступить — сначала обнови документ.

## Перед тем как сказать «готово»

```bash
uv run poe fmt
uv run poe check        # должен быть зелёным целиком
```

Если трогал `storage/` или `engine/` — ещё `uv run poe test-all`.

## Линтеры не отключаются

- Запрещено ослаблять конфиг в `pyproject.toml` (ruff/mypy/basedpyright/import-linter/coverage),
  чтобы пройти проверку. Исправляй код.
- Точечное подавление — только с именем правила и причиной, иначе упадёт
  `tests/architecture/test_conventions.py`. `# noqa` запрещён:
  - `# ruff: ignore[blind-except]  # причина`
  - `# type: ignore[attr-defined]  # причина`
  - `# pyright: ignore[reportAny]  # причина`

## Ошибки

- Всё, что бросает библиотека, — подкласс `tallyho.model.errors.TallyhoError`.
  Встроенные исключения напрямую — только `TypeError` и `NotImplementedError`.
- `except Exception` / `BaseException` без повторного `raise` запрещён (ruff BLE001).
  То же для `contextlib.suppress(Exception)`.
- Всегда `raise NewError(...) from exc` внутри `except`.
- Сообщение исключения — в переменную или в сам класс, не f-строкой в `raise` (ruff EM, TRY003).
- Не глотай `asyncio.CancelledError`. Любая задача, созданная через `create_task`, хранится
  и дожидается (ruff RUF006).

## Типы

- `Any` запрещён (mypy `disallow_any_explicit`, basedpyright `reportAny`, ruff TID251).
  Используй `object`, `TypeVar`, `ParamSpec`, `Protocol`.
- Публичные функции полностью аннотированы; `from __future__ import annotations` в каждом файле.
- Переопределения помечаются `@override` (`typing_extensions.override` на 3.11).
- Каждый модуль объявляет `__all__`.

## Архитектура

- Слои: `model` → `protocols` → `storage | hooks` → `engine` → `runtime | api` → `adapters | cli | testing`.
  Нижний слой не импортирует верхний. Проверяет `lint-imports`.
- Время и ID — только через протоколы `Clock` / `IdFactory`, чтобы тесты были детерминированы.
- Библиотека не настраивает логирование, не вызывает `sys.exit`, не читает окружение молча.

## Тесты

- Новый код — с тестами; покрытие веток ≥ 95%.
- Юнит-тесты без БД в `tests/unit/`, с PostgreSQL — в `tests/integration/`.
- Тесты независимы от порядка (`pytest-randomly`), предупреждения = ошибки.
