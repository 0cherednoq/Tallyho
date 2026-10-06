# Интеграции и расширение

## Адаптер flexiq

Нужно дополнение `tallyho[flexiq]`. Руководство - [Адаптер flexiq](../../guide/flexiq.md).

```{eval-rst}
.. autoclass:: tallyho.adapters.flexiq.FlexiqAdapter
   :members:
```

## Наблюдатели

Руководство - [Наблюдаемость](../../guide/operations/observability.md).

```{eval-rst}
.. autoclass:: tallyho.protocols.observer.Observer
   :members:

.. autoclass:: tallyho.protocols.observer.NullObserver

.. autoclass:: tallyho.observability.otel.OpenTelemetryObserver
```

## Alembic и maintenance

```{eval-rst}
.. autofunction:: tallyho.storage.alembic.upgrade

.. autofunction:: tallyho.cli.app.serve_maintenance
```

## Часы, идентификаторы, сериализация

Эти протоколы принимает конструктор `Tallyho`: `clock=`, `id_factory=`, `serializer=`.

```{eval-rst}
.. autoclass:: tallyho.protocols.clock.Clock
   :members:

.. autoclass:: tallyho.protocols.clock.SystemClock

.. autoclass:: tallyho.protocols.ids.IdFactory
   :members:

.. autoclass:: tallyho.protocols.ids.UuidV7Factory

.. autoclass:: tallyho.protocols.serialization.Serializer
   :members:

.. autoclass:: tallyho.protocols.serialization.JsonSerializer
```

## Свой адаптер брокера

Адаптер реализует протоколы из `tallyho.protocols.broker`. `Dispatcher` отправляет сообщения в
брокер, `Runtime` связывает выполнение задачи с учётом tallyho.

```{eval-rst}
.. automodule:: tallyho.protocols.broker
   :members:
```
