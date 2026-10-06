# Справочник API

Справочник собран из докстрингов пакета. Публичный API - всё, что реэкспортирует модуль
`tallyho`, и модули, перечисленные на этих страницах. Остальное считается внутренним и может
измениться без предупреждения.

| Раздел | Что внутри |
|---|---|
| [Клиент и батчи](api/client.md) | `Tallyho`, `Settings`, `BatchBuilder`, `BatchHandle`, `Call` |
| [Внутри задачи](api/runtime.md) | фасады `item` и `callback`, декоратор `tracked` |
| [Модель](api/model.md) | состояния, снимки и сводки, политики ошибок, исключения |
| [Тестирование](api/testing.md) | `InlineBroker`, `FakeClock`, `TallyhoTestEnv`, pytest-фикстура |
| [Интеграции и расширение](api/extensions.md) | адаптер flexiq, наблюдатели, Alembic, протоколы для своих реализаций |

```{toctree}
:hidden:

api/client
api/runtime
api/model
api/testing
api/extensions
```
