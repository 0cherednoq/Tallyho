"""Политики ошибок батча (ARCHITECTURE §11.2, UC-11, §12.4).

* ``FailurePolicy.continue_()`` — ошибки не останавливают батч (по умолчанию);
* ``FailurePolicy.fail_fast()`` — первая ошибка → запрос отмены с причиной
  ``fail_fast``, итог ``failed``;
* ``FailurePolicy.threshold(ratio=, min_processed=, labels=, action=)`` — доля
  ошибок (или Items с метками ``labels``) среди обработанных превысила ``ratio``
  после ``min_processed`` обработанных → ``action``: ``"fail"`` (запрос отмены
  с причиной ``policy``, итог ``failed``) или ``"pause"`` (пауза дерева).

Политика — чистая функция от счётчиков: :meth:`FailurePolicy.evaluate` ничего
не знает о БД. Что делать с вердиктом, решает движок (T4.6).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Literal, Protocol, cast

from tallyho.model.errors import ConfigurationError
from tallyho.model.states import CancelReason

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "FailurePolicy",
    "OutcomeCounts",
    "PolicyAction",
    "PolicyBreach",
    "PolicyKind",
    "PolicyVerdict",
]


class PolicyKind(StrEnum):
    """Вид политики ошибок."""

    CONTINUE = "continue"
    FAIL_FAST = "fail_fast"
    THRESHOLD = "threshold"


class PolicyAction(StrEnum):
    """Что делать при срабатывании политики."""

    FAIL = "fail"
    PAUSE = "pause"


class OutcomeCounts(Protocol):
    """Счётчики завершённых Items по классам итога; им удовлетворяет ``Progress``."""

    @property
    def ok(self) -> int:
        """Завершены с ``ok``."""
        ...

    @property
    def skip(self) -> int:
        """Завершены с ``skip``."""
        ...

    @property
    def error(self) -> int:
        """Завершены с ``error``."""
        ...


@dataclass(frozen=True, slots=True, kw_only=True)
class PolicyBreach:
    """Срабатывание политики — третий аргумент хука ``on_policy_breach`` (§12.4).

    ``batch_key`` — ключ батча, где сработала политика (``None`` у корня без
    ключа); ``labels`` — метки фильтра политики (пусто — считались все ошибки);
    ``ratio`` — фактическая доля на момент срабатывания.
    """

    batch_key: str | None
    labels: list[str]
    ratio: float
    action: PolicyAction


@dataclass(frozen=True, slots=True, kw_only=True)
class PolicyVerdict:
    """Результат :meth:`FailurePolicy.evaluate`.

    ``action`` — ``None``, если политика не сработала. ``processed`` —
    обработанные Items (``ok + skip + error``), ``failed`` — из них ошибки
    (или Items с метками фильтра), ``ratio = failed / processed``.
    ``reason`` — причина запроса отмены при ``action == FAIL``.
    """

    action: PolicyAction | None
    ratio: float
    processed: int
    failed: int
    reason: CancelReason | None = None

    @property
    def breached(self) -> bool:
        """Политика сработала."""
        return self.action is not None

    def breach(self, *, batch_key: str | None, labels: Iterable[str]) -> PolicyBreach:
        """Описание срабатывания для ``on_policy_breach``.

        Returns:
            :class:`PolicyBreach` с долей и действием этого вердикта.

        Raises:
            ConfigurationError: политика не сработала.
        """
        if self.action is None:
            message = "политика не сработала: описывать нечего"
            raise ConfigurationError(message)
        return PolicyBreach(
            batch_key=batch_key,
            labels=list(labels),
            ratio=self.ratio,
            action=self.action,
        )


def _check_ratio(ratio: object) -> float:
    if isinstance(ratio, bool) or not isinstance(ratio, int | float):
        message = f"ratio должен быть числом, получено {ratio!r}"
        raise ConfigurationError(message)
    if not (math.isfinite(ratio) and 0 <= ratio <= 1):
        message = f"ratio должен быть в [0, 1], получено {ratio!r}"
        raise ConfigurationError(message)
    return float(ratio)


def _check_min_processed(min_processed: object) -> int:
    if isinstance(min_processed, bool) or not isinstance(min_processed, int) or min_processed < 0:
        message = f"min_processed должен быть целым >= 0, получено {min_processed!r}"
        raise ConfigurationError(message)
    return min_processed


def _check_labels(labels: object) -> tuple[str, ...] | None:
    if labels is None:
        return None
    if isinstance(labels, str | bytes) or not isinstance(labels, list | tuple | set | frozenset):
        message = f"labels должен быть списком строк, получено {labels!r}"
        raise ConfigurationError(message)
    items = cast("Iterable[object]", labels)  # элементы проверяются ниже
    result: list[str] = []
    for label in items:
        if not isinstance(label, str) or not label:
            message = f"метка должна быть непустой строкой, получено {label!r}"
            raise ConfigurationError(message)
        result.append(label)
    if not result:
        message = "labels не может быть пустым: None — все ошибки"
        raise ConfigurationError(message)
    return tuple(dict.fromkeys(result))


def _check_action(action: object) -> PolicyAction:
    if not isinstance(action, str) or action not in {a.value for a in PolicyAction}:
        message = f"action должен быть 'fail' или 'pause', получено {action!r}"
        raise ConfigurationError(message)
    return PolicyAction(action)


@dataclass(frozen=True, slots=True)
class FailurePolicy:
    """Политика ошибок батча. Создаётся фабриками, а не конструктором.

    ``labels`` — фильтр меток (``None`` — считаются все ошибки).
    """

    kind: PolicyKind
    ratio: float = 0.0
    min_processed: int = 0
    labels: tuple[str, ...] | None = None
    action: PolicyAction = PolicyAction.FAIL

    @classmethod
    def continue_(cls) -> FailurePolicy:
        """Ошибки не останавливают батч; итог — ``completed_with_errors``.

        Returns:
            Политика ``continue``.
        """
        return cls(PolicyKind.CONTINUE)

    @classmethod
    def fail_fast(cls) -> FailurePolicy:
        """Первая ошибка → запрос отмены с причиной ``fail_fast``.

        Returns:
            Политика ``fail_fast``.
        """
        return cls(PolicyKind.FAIL_FAST)

    @classmethod
    def threshold(
        cls,
        *,
        ratio: float,
        min_processed: int = 0,
        labels: Iterable[str] | None = None,
        action: Literal["fail", "pause"] = "fail",
    ) -> FailurePolicy:
        """Порог доли ошибок среди обработанных Items.

        Срабатывает, когда обработано не меньше ``min_processed`` Items и доля
        ошибок (или Items с метками ``labels``) строго больше ``ratio``.

        Неверные параметры (``ratio`` вне ``[0, 1]``, ``min_processed < 0``,
        пустой ``labels``, неизвестный ``action``) → ``ConfigurationError``.

        Returns:
            Политика ``threshold``.
        """
        return cls(
            PolicyKind.THRESHOLD,
            ratio=_check_ratio(ratio),
            min_processed=_check_min_processed(min_processed),
            labels=_check_labels(
                labels if labels is None or isinstance(labels, str) else tuple(labels)
            ),
            action=_check_action(action),
        )

    def evaluate(self, counts: OutcomeCounts, labels: Mapping[str, int]) -> PolicyVerdict:
        """Проверить политику на текущих счётчиках батча.

        ``labels`` — число Items по меткам итога (``th_metric``). Отменённые
        Items обработанными не считаются.

        Returns:
            Вердикт: сработала ли политика и с каким действием.
        """
        processed = counts.ok + counts.skip + counts.error
        if self.labels is None:
            failed = counts.error
        else:
            failed = sum(labels.get(label, 0) for label in self.labels)
        ratio = failed / processed if processed else 0.0
        action: PolicyAction | None = None
        reason: CancelReason | None = None
        if self.kind is PolicyKind.FAIL_FAST and failed > 0:
            action, reason = PolicyAction.FAIL, CancelReason.FAIL_FAST
        elif (
            self.kind is PolicyKind.THRESHOLD
            and processed >= self.min_processed
            and ratio > self.ratio
        ):
            action = self.action
            reason = CancelReason.POLICY if action is PolicyAction.FAIL else None
        return PolicyVerdict(
            action=action, ratio=ratio, processed=processed, failed=failed, reason=reason
        )

    def to_json(self) -> dict[str, object]:
        """Представление для ``th_batch.options`` (jsonb).

        Returns:
            Словарь из JSON-совместимых значений.
        """
        data: dict[str, object] = {"kind": self.kind.value}
        if self.kind is PolicyKind.THRESHOLD:
            data |= {
                "ratio": self.ratio,
                "min_processed": self.min_processed,
                "labels": None if self.labels is None else list(self.labels),
                "action": self.action.value,
            }
        return data

    @classmethod
    def from_json(cls, data: Mapping[str, object]) -> FailurePolicy:
        """Восстановить политику из :meth:`to_json`.

        Returns:
            Политика, равная исходной.

        Raises:
            ConfigurationError: данные не описывают политику.
        """
        kind = data.get("kind")
        if kind == PolicyKind.CONTINUE:
            return cls.continue_()
        if kind == PolicyKind.FAIL_FAST:
            return cls.fail_fast()
        if kind != PolicyKind.THRESHOLD:
            message = f"неизвестный вид политики: {kind!r}"
            raise ConfigurationError(message)
        return cls(
            PolicyKind.THRESHOLD,
            ratio=_check_ratio(data.get("ratio")),
            min_processed=_check_min_processed(data.get("min_processed", 0)),
            labels=_check_labels(data.get("labels")),
            action=_check_action(data.get("action", PolicyAction.FAIL.value)),
        )
