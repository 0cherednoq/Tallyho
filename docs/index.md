---
layout: landing
---

# tallyho

<img class="hero-logo light-only" src="_static/images/logo-light.svg" alt="tallyho">
<img class="hero-logo dark-only" src="_static/images/logo-dark.svg" alt="tallyho">

```{rst-class} lead
```

Учёт групп задач поверх вашего брокера: батчи, прогресс, конвейеры этапов и итог, который
записывается в вашу таблицу одной транзакцией с завершением батча.

:::{container} buttons

[Быстрый старт](guide/getting-started.md)
[Разбор на примерах](guide/tutorial/overview.md)

:::

```{rubric} Что умеет
```

::::{grid} 1 1 2 3
:class-row: surface
:padding: 0
:gutter: 2

:::{grid-item-card} {octicon}`number` Батч как единица учёта
:link: guide/batches/progress
:link-type: doc

Сколько задач найдено, сделано, упало и сколько осталось. Один вызов `view()` отдаёт
согласованный снимок всего дерева под-батчей, с оценкой объёма и времени до конца.
:::

:::{grid-item-card} {octicon}`check-circle` Финализация ровно один раз
:link: guide/hooks
:link-type: doc

Ваш хук выполняется внутри транзакции, которая завершает батч. Статус кампании в вашей
таблице и состояние батча меняются вместе или вместе откатываются.
:::

:::{grid-item-card} {octicon}`git-branch` Задачи порождают задачи
:link: guide/batches/pipelines
:link-type: doc

Страница каталога находит карточки, карточка находит картинки, и заранее никто не знает,
сколько их будет. Этапы работают параллельно, а следующий закрывается сам, когда
закончились его источники.
:::

:::{grid-item-card} {octicon}`stop` Управление группой
:link: guide/batches/operations
:link-type: doc

Отложенный старт, пауза, отмена, повтор упавших задач, ограничение параллелизма и
политики ошибок. Любую операцию можно выполнить в вашей транзакции.
:::

:::{grid-item-card} {octicon}`shield-check` При сбоях задачи не теряются
:link: guide/concepts
:link-type: doc

Если упал воркер, брокер или сеть, задача вернётся в очередь, пропущенную финализацию
подберут фоновые проверки, и батч дойдёт до итога.
:::

:::{grid-item-card} {octicon}`beaker` Тесты без брокера
:link: guide/testing
:link-type: doc

`InlineBroker` выполняет задачи в процессе теста по шагам, `FakeClock` двигает время.
Повторную доставку и падение воркера можно воспроизвести одной строкой.
:::

::::

```{rubric} Что остаётся за вами
```

tallyho хранит только техническое состояние группы задач. Исполнение, ретраи, расписания и
rate limit делает брокер. Бизнес-статусы и бизнес-данные живут в ваших таблицах: библиотека
сообщает вам точные факты о батче, а что они значат для кампании или заказа, решаете вы.

Нужны Python 3.11 или новее, PostgreSQL 14 или новее и SQLAlchemy 2.0+ в async-режиме. Первый
поддерживаемый брокер - [flexiq](integrations/flexiq.md).

```{toctree}
:caption: Начало работы
:hidden:

guide/getting-started
guide/concepts
guide/installation
```

```{toctree}
:caption: Разбор на примерах
:hidden:

guide/tutorial/overview
guide/tutorial/checker
guide/tutorial/export
guide/tutorial/export-progress
guide/tutorial/export-finalization
```

```{toctree}
:caption: Руководство
:hidden:

guide/batches
guide/hooks
guide/testing
```

```{toctree}
:caption: Интеграции
:hidden:

integrations/index
```

```{toctree}
:caption: Эксплуатация
:hidden:

guide/operations
guide/operations/shutdown
guide/operations/postgres
guide/operations/observability
guide/limitations
```

```{toctree}
:caption: Архитектура
:hidden:

architecture/index
```

```{toctree}
:caption: Справочник
:hidden:

reference/settings
reference/cli
reference/errors
reference/api
```
