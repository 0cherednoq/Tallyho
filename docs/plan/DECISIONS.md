# tallyho v1 — журнал решений

> Решения, принятые при реализации поверх [ARCHITECTURE.md](../ARCHITECTURE.md). Новые записи — в конец.
> Статусы: `ACCEPTED` — действует; `ACCEPTED (автономно, пересмотреть)` — принято циклом без человека; `SUPERSEDED by D-NNN`.

## D-001 · Иерархия документов · ACCEPTED
При расхождениях прав `ARCHITECTURE.md` v2.1. `ACCEPTANCE.md` задаёт критерии тестов. `DESIGN.md`, `API.md`, `COUNTERS.md` — исторический ресёрч: брать из них только то, что не противоречит ARCHITECTURE (в частности, **нет** SQLite, Flow/Run, бизнес-статусов, `bw_status_history`).

## D-002 · Время · ACCEPTED (уточнить в T1.4)
Все сроки (`lease_until`, `available_at`, `start_at`, `deadline_at`, retention, backoff хуков) считаются по времени БД (ARCHITECTURE §10, ACCEPTANCE A-CH-09). В движке нет `datetime.now()`.
Для тестов `FakeClock` должен подменять «сейчас» в SQL. Предлагаемая механика: `Clock.now() -> datetime | None`; `None` → storage использует `now()` БД, значение → storage биндит его параметром. `protocols` не импортирует SQLAlchemy (import-linter). Интервалы в процессе (тик Completer, heartbeat) — `asyncio` loop time.

## D-003 · Покрытие и гейты · ACCEPTED
`fail_under = 95` в `pyproject.toml` нельзя выполнить юнит-тестами: код storage/engine покрывается интеграционными тестами с PostgreSQL. Поэтому `poe test` (unit, быстрый гейт pre-push) идёт без покрытия, а `poe test-all` (unit + integration) — с покрытием 95%. Порог не снижается.

## D-004 · Соединения в storage/engine · ACCEPTED
Функции `storage` и `engine` принимают `AsyncConnection`. `AsyncSession` пользователя разворачивается через `await session.connection()` на границе (`tallyho.storage.tx.resolve_connection`). Мы никогда не делаем commit/rollback чужой транзакции.

## D-005 · Коды состояний (smallint) · ACCEPTED
* `th_batch.state`: `open=0, sealed=1, finalizing=2, succeeded=10, completed_with_errors=11, failed=12, cancelled=13` (терминальные ≥ 10 — удобно для partial-индексов и `state < 10`). `finalizing` в БД не сохраняется (существует только внутри транзакции, ARCHITECTURE §6.1), код зарезервирован.
* `th_item.state`: `active=0, ok=10, skip=11, error=12, cancelled=13`.
* `th_outbox.kind`: `item=0, callback=1`.
* `th_batch.on_feeder_failed`: `seal=0, cancel=1`.
Значения фиксируются снимок-тестом в T1.1: менять только миграцией.

## D-006 · Сериализация payload · ACCEPTED (проверить в T8.0)
`th_item.payload` хранит аргументы вызова для (пере)отправки relay'ем. Кодек — `Serializer` адаптера, по умолчанию `JsonSerializer`. Для flexiq, где аргументы сериализует сам брокер (cloudpickle/msgpack/cbor), payload должен позволять восстановить ровно те объекты, что передал пользователь (A-FQ-01). Если JSON этого не обеспечивает, адаптер предоставляет свой кодек — решить по итогам спайка T8.0.

## D-007 · Ветка и коммиты · ACCEPTED
Работа в ветке `impl/v1` (pre-commit `no-commit-to-branch` запрещает `main`). Коммиты небольшие и зелёные, несколько на задачу; сообщения на русском, формат — PLAN §0.6. Push и PR — только по просьбе человека.

## D-008 · Автономность · ACCEPTED
Цикл работает без ограничения по времени, пока библиотека не доделана, и не ждёт человека: неясности решает сам с записью `ACCEPTED (автономно, пересмотреть)` в этом файле. Статус `OPEN` не используется. Правила — PLAN §0.4.
