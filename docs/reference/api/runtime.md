# Внутри задачи

`item` и `callback` - модульные объекты: `from tallyho import item, callback`. Они находят
текущую задачу через контекст выполнения, поэтому передавать их параметром не нужно. Правила
итога описаны на странице [Задача и её итог](../../guide/batches/tasks.md).

## Итог задачи

Итог попытки - возвращаемое значение задачи. Все четыре типа заморожены и сравнимы, поэтому
задачу проверяют обычным `assert await task(...) == Skip("duplicate")`.

```{eval-rst}
.. automodule:: tallyho.model.outcomes
   :members:
```

## `WorkItem`

Единица работы одной попытки: её отдают `item.current()` и `item.require()`, её же получают
зависимости [через контейнер](../../integrations/dishka.md). `ItemContext` - прежнее имя этого
класса, алиас сохраняется на один минорный выпуск.

```{eval-rst}
.. autoclass:: tallyho.WorkItem
   :members: id, batch_id, attempt, defers, checkpoint, checkpoint_in, progress, incr, ok, skip,
             error, defer, adopt, complete_in, cancelled, spawn, spawn_call, expect, sub_batch
```

## Судьба исключений

`defer_on`, `error_on` и `skip_on` разбираются при регистрации задачи; адаптер брокера собирает из
них политику и передаёт её обёртке.

```{eval-rst}
.. autofunction:: tallyho.runtime.exceptions.build_exception_policy

.. autoclass:: tallyho.runtime.exceptions.ExceptionPolicy
   :members:
```

## Обёртки вокруг задачи

```{eval-rst}
.. autoclass:: tallyho.ItemMiddleware
   :members:
```

## `item`

```{eval-rst}
.. autoclass:: tallyho.runtime.context.ItemFacade
   :members:
```

## `callback`

```{eval-rst}
.. autoclass:: tallyho.runtime.context.CallbackFacade
   :members:

.. autoclass:: tallyho.runtime.CallbackContext
   :members:
```

## `tracked`

```{eval-rst}
.. autofunction:: tallyho.tracked
```
