# Модель

Неизменяемые значения, которые библиотека отдаёт наружу. Все лежат в пакете `tallyho.model`.

## Состояния

```{eval-rst}
.. automodule:: tallyho.model.states
   :members:
   :undoc-members:
```

## Снимки и сводки

```{eval-rst}
.. automodule:: tallyho.model.views
   :members:
```

## Политики ошибок

```{eval-rst}
.. autoclass:: tallyho.model.policy.FailurePolicy
   :members:

.. autoclass:: tallyho.model.policy.PolicyBreach
   :members:

.. autoclass:: tallyho.model.policy.PolicyAction
   :members:
   :undoc-members:
```

## Исключения

Когда какое исключение бросается, описано в таблице на странице [Ошибки](../errors.md).

```{eval-rst}
.. automodule:: tallyho.model.errors
   :members:
```
