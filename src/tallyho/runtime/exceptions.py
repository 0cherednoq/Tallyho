"""Судьба исключения задачи, объявленная при её регистрации (ARCHITECTURE UC-18).

«Что это исключение значит для учёта работы» — знание об одном типе исключения,
а не об одном вызове. Поэтому оно объявляется опциями регистрации задачи
(``defer_on``, ``error_on``, ``skip_on``), а не повторяется в теле каждой задачи:

```python
@fq.task(
    queue="checks",
    retry_on=[ProviderUnavailable],
    defer_on={NoCheckerAccount: lambda e: Defer(reason=e.reason, delay=e.retry_after)},
    error_on={InvalidDomainName: "invalid_domain"},
    skip_on={DomainAlreadyChecked: "duplicate"},
)
async def check_domain_chunk(run_id: str, domains: list[str]) -> Ok: ...
```

Модуль чистый: он только разбирает и применяет правила, записью итога занимается
обёртка ``tracked``. Адаптер брокера разбирает опции при регистрации, поэтому
неоднозначная конфигурация падает до первого вызова, а не в проде.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from tallyho.model.errors import ConfigurationError
from tallyho.model.outcomes import Defer, Error, Skip

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from tallyho.model.outcomes import Outcome

__all__ = ["ExceptionPolicy", "build_exception_policy"]

_BAD_RULES = "defer_on / error_on / skip_on принимают словарь {класс исключения: правило}"
_BAD_KEY = "ключом правила должен быть класс исключения"
_DEFER_VALUE = "значение defer_on — Defer или функция exc -> Defer"
_LABEL_VALUE = "значение error_on / skip_on — метка строкой, готовый итог или функция exc -> итог"
_WRONG_KIND = "функция правила вернула итог другого класса"


@dataclass(frozen=True, slots=True)
class ExceptionPolicy:
    """Правила перевода исключений задачи в итог попытки.

    Пустая политика (``not policy``) означает, что у задачи правил нет и
    обёртка идёт прежним путём: вердикт брокера, ретрай или ``exhausted``.
    """

    rules: Mapping[type[BaseException], Callable[[BaseException], Outcome]]

    def __bool__(self) -> bool:
        """Проверить, объявлены ли правила.

        Returns:
            ``True``, если есть хотя бы одно правило.
        """
        return bool(self.rules)

    def outcome_for(self, exc: BaseException) -> Outcome | None:
        """Найти итог для исключения по ближайшему классу в MRO.

        Выигрывает самый конкретный объявленный класс: правило для подкласса
        сильнее правила для его базы, а порядок объявления ни на что не влияет.

        Args:
            exc: Исключение, вышедшее из пользовательской задачи.

        Returns:
            Итог попытки или ``None``, если правила для этого исключения нет.
        """
        if not self.rules:
            return None
        for klass in type(exc).__mro__:
            rule = self.rules.get(cast("type[BaseException]", klass))
            if rule is not None:
                return rule(exc)
        return None


def build_exception_policy(
    *,
    defer_on: object = None,
    error_on: object = None,
    skip_on: object = None,
    retry_on: tuple[type[BaseException], ...] = (),
) -> ExceptionPolicy:
    """Проверить и собрать правила на этапе регистрации задачи.

    Args:
        defer_on: ``{Exc: Defer(...)}`` или ``{Exc: exc -> Defer}``.
        error_on: ``{Exc: "метка"}``, ``{Exc: Error(...)}`` или функция.
        skip_on: то же для ``Skip``.
        retry_on: классы, которые брокер повторяет; пересечение с правилами
            запрещено.

    Returns:
        Собранная политика; пустая, если правил нет.

    Raises:
        ConfigurationError: неверная форма правила, один класс в двух опциях
            или класс одновременно в правилах и в ``retry_on``.
    """
    rules: dict[type[BaseException], Callable[[BaseException], Outcome]] = {}
    sources: dict[type[BaseException], str] = {}
    for name, value, kind in (
        ("defer_on", defer_on, Defer),
        ("error_on", error_on, Error),
        ("skip_on", skip_on, Skip),
    ):
        for klass, rule in _parse(value, kind).items():
            if klass in sources:
                message = (
                    f"{klass.__name__} указан и в {sources[klass]}, и в {name}: "
                    "у исключения одна судьба"
                )
                raise ConfigurationError(message)
            sources[klass] = name
            rules[klass] = rule
    for klass, source in sources.items():
        if any(klass is retried for retried in retry_on):
            message = (
                f"{klass.__name__} указан и в retry_on, и в {source}: у исключения одна судьба"
            )
            raise ConfigurationError(message)
    return ExceptionPolicy(rules=MappingProxyType(rules))


def _parse(
    value: object, kind: type[Outcome]
) -> dict[type[BaseException], Callable[[BaseException], Outcome]]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(_BAD_RULES)
    parsed: dict[type[BaseException], Callable[[BaseException], Outcome]] = {}
    for key, rule in cast("Mapping[object, object]", value).items():
        if not isinstance(key, type) or not issubclass(key, BaseException):
            raise ConfigurationError(_BAD_KEY)
        parsed[key] = _rule(rule, kind)
    return parsed


def _rule(rule: object, kind: type[Outcome]) -> Callable[[BaseException], Outcome]:
    if isinstance(rule, kind):
        ready = rule
        return lambda _exc: ready
    if isinstance(rule, str) and kind is not Defer:
        label = rule
        maker = cast("Callable[[str], Outcome]", kind)
        return lambda _exc: maker(label)
    if callable(rule):
        mapper = cast("Callable[[BaseException], object]", rule)

        def apply(exc: BaseException) -> Outcome:
            produced = mapper(exc)
            if not isinstance(produced, kind):
                raise ConfigurationError(_WRONG_KIND)
            return produced

        return apply
    raise ConfigurationError(_DEFER_VALUE if kind is Defer else _LABEL_VALUE)
