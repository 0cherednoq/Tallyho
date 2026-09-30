"""Реестр транзакционных хуков (ARCHITECTURE §7.2, §7.5).

Хуки регистрируются декораторами на ``kind`` батча. При создании батча в
``th_batch.hooks`` пишется :meth:`HookRegistry.required_hooks`; процесс, у
которого нужного хука нет, батч не финализирует (:meth:`HookRegistry.ensure`).

Хуки выполняются внутри транзакции tallyho: сессия привязана к нашему
соединению, ``commit()`` / ``rollback()`` в хуке запрещены (§7.3).
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, TypeVar

from tallyho.model.errors import ConfigurationError, HookMissingError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from sqlalchemy.ext.asyncio import AsyncSession

    from tallyho.model.policy import PolicyBreach
    from tallyho.model.views import BatchSummary

__all__ = [
    "FinalizedHook",
    "HookName",
    "HookRegistry",
    "PolicyBreachHook",
    "ProgressHook",
    "ProgressRegistration",
    "import_hook_modules",
]


class HookName(StrEnum):
    """Имя хука в ``th_batch.hooks``.

    ``progress`` совпадает с предикатом partial-индекса Snapshotter'а (§5.2).
    """

    FINALIZED = "finalized"
    PROGRESS = "progress"
    POLICY_BREACH = "policy_breach"

    @property
    def decorator(self) -> str:
        """Имя декоратора регистрации: ``on_finalized`` и т. п."""
        return f"on_{self.value}"


class FinalizedHook(Protocol):
    """``on_finalized(session, summary)`` — атомарно с финализацией батча."""

    def __call__(self, session: AsyncSession, summary: BatchSummary, /) -> Awaitable[None]:
        """Перенести итог батча в доменные таблицы."""
        ...


class ProgressHook(Protocol):
    """``on_progress(session, summary)`` — снимок прогресса не чаще ``every``."""

    def __call__(self, session: AsyncSession, summary: BatchSummary, /) -> Awaitable[None]:
        """Перенести снимок прогресса в доменные таблицы."""
        ...


class PolicyBreachHook(Protocol):
    """``on_policy_breach(session, summary, breach)`` — атомарно с паузой или провалом."""

    def __call__(
        self, session: AsyncSession, summary: BatchSummary, breach: PolicyBreach, /
    ) -> Awaitable[None]:
        """Отразить срабатывание политики ошибок в домене."""
        ...


FinalizedT = TypeVar("FinalizedT", bound=FinalizedHook)
ProgressT = TypeVar("ProgressT", bound=ProgressHook)
PolicyBreachT = TypeVar("PolicyBreachT", bound=PolicyBreachHook)


@dataclass(frozen=True, slots=True)
class ProgressRegistration:
    """Зарегистрированный ``on_progress`` и его период ``every``."""

    hook: ProgressHook
    every: timedelta


_DECORATORS: dict[str, str] = {name.value: name.decorator for name in HookName}
_DUPLICATE = "tx-хук уже зарегистрирован"
_EMPTY_KIND = "kind хука должен быть непустой строкой"
_BAD_EVERY = "every у on_progress должен быть положительным timedelta"


class HookRegistry:
    """Хуки процесса по ``kind``; один хук каждого вида на ``kind``."""

    def __init__(self) -> None:
        """Пустой реестр."""
        self._finalized: dict[str, FinalizedHook] = {}
        self._progress: dict[str, ProgressRegistration] = {}
        self._breach: dict[str, PolicyBreachHook] = {}

    # --- регистрация -------------------------------------------------------

    def on_finalized(self, kind: str) -> Callable[[FinalizedT], FinalizedT]:
        """Декоратор: хук финализации батчей ``kind``.

        Returns:
            Декоратор, который регистрирует хук и возвращает его без изменений.
        """
        self._check_new(kind, HookName.FINALIZED, registered=kind in self._finalized)

        def register(hook: FinalizedT) -> FinalizedT:
            self._check_new(kind, HookName.FINALIZED, registered=kind in self._finalized)
            self._finalized[kind] = hook
            return hook

        return register

    def on_progress(self, kind: str, every: timedelta) -> Callable[[ProgressT], ProgressT]:
        """Декоратор: снимки прогресса батчей ``kind`` не чаще ``every``.

        Returns:
            Декоратор, который регистрирует хук и возвращает его без изменений.

        Raises:
            ConfigurationError: ``every`` не положительный ``timedelta``, пустой
                ``kind`` или хук для ``kind`` уже есть.
        """
        if every <= timedelta(0):
            raise ConfigurationError(_BAD_EVERY)
        self._check_new(kind, HookName.PROGRESS, registered=kind in self._progress)

        def register(hook: ProgressT) -> ProgressT:
            self._check_new(kind, HookName.PROGRESS, registered=kind in self._progress)
            self._progress[kind] = ProgressRegistration(hook, every)
            return hook

        return register

    def on_policy_breach(self, kind: str) -> Callable[[PolicyBreachT], PolicyBreachT]:
        """Декоратор: хук срабатывания политики ошибок для ``kind``.

        Returns:
            Декоратор, который регистрирует хук и возвращает его без изменений.
        """
        self._check_new(kind, HookName.POLICY_BREACH, registered=kind in self._breach)

        def register(hook: PolicyBreachT) -> PolicyBreachT:
            self._check_new(kind, HookName.POLICY_BREACH, registered=kind in self._breach)
            self._breach[kind] = hook
            return hook

        return register

    # --- поиск -------------------------------------------------------------

    def required_hooks(self, kind: str) -> tuple[str, ...]:
        """Хуки, которые требуются батчу ``kind``.

        Returns:
            Имена для ``th_batch.hooks`` в порядке :class:`HookName`.
        """
        present = {
            HookName.FINALIZED: kind in self._finalized,
            HookName.PROGRESS: kind in self._progress,
            HookName.POLICY_BREACH: kind in self._breach,
        }
        return tuple(name.value for name in HookName if present[name])

    def finalized(self, kind: str) -> FinalizedHook | None:
        """Хук финализации ``kind``.

        Returns:
            Зарегистрированный ``on_finalized`` или ``None``.
        """
        return self._finalized.get(kind)

    def progress(self, kind: str) -> ProgressRegistration | None:
        """Хук снимков прогресса ``kind``.

        Returns:
            ``on_progress`` вместе с ``every`` или ``None``.
        """
        return self._progress.get(kind)

    def policy_breach(self, kind: str, *, root_kind: str | None = None) -> PolicyBreachHook | None:
        """``on_policy_breach`` для ``kind``, иначе для ``root_kind`` корня (§12.4).

        Где именно сработала политика, хук корня узнаёт из ``breach.batch_key``.

        Returns:
            Хук батча, иначе хук корня, иначе ``None``.
        """
        hook = self._breach.get(kind)
        if hook is None and root_kind is not None:
            hook = self._breach.get(root_kind)
        return hook

    def ensure(self, kind: str, required: Iterable[str]) -> None:
        """Проверить, что все хуки из ``th_batch.hooks`` есть в процессе.

        Неизвестное имя хука тоже считается отсутствующим: его записал процесс
        с более новой версией библиотеки.

        Raises:
            HookMissingError: первого недостающего хука.
        """
        present = set(self.required_hooks(kind))
        for name in required:
            if name not in present:
                raise HookMissingError(kind, _DECORATORS.get(name, name))

    @staticmethod
    def _check_new(kind: str, name: HookName, *, registered: bool) -> None:
        if not kind:
            raise ConfigurationError(_EMPTY_KIND)
        if registered:
            message = f"{_DUPLICATE}: {name.decorator} для kind={kind!r}"
            raise ConfigurationError(message)


def import_hook_modules(modules: Iterable[str]) -> None:
    """Импортировать ``hook_modules``: их декораторы регистрируют хуки (§7.5).

    Повторный импорт берётся из ``sys.modules`` и хуки не дублирует.

    Raises:
        ConfigurationError: модуль или его зависимость не импортируется
            (``ImportError``); прочие исключения модуля пробрасываются как есть.
    """
    for name in modules:
        try:
            importlib.import_module(name)
        except ImportError as exc:
            message = f"не удалось импортировать hook_modules {name!r}: {exc}"
            raise ConfigurationError(message) from exc
