# Внутри задачи

`item` и `callback` - модульные объекты: `from tallyho import item, callback`. Они находят
текущую задачу через контекст выполнения, поэтому передавать их параметром не нужно. Правила
итога описаны на странице [Задача и её итог](../../guide/batches/tasks.md).

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
