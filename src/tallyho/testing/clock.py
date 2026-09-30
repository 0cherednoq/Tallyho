"""Управляемые часы для детерминированных тестов."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, final

from typing_extensions import override

from tallyho.model.errors import ConfigurationError
from tallyho.protocols.clock import Clock

if TYPE_CHECKING:
    from datetime import datetime

__all__ = ["FakeClock"]

_AWARE_REQUIRED = "FakeClock требует datetime с часовым поясом"
_FORWARD_ONLY = "FakeClock можно двигать только вперёд"


@final
class FakeClock(Clock):
    """Часы, чьё календарное и монотонное время двигает сам тест."""

    def __init__(self, current: datetime) -> None:
        """Создать часы в заданном aware-моменте.

        Raises:
            ConfigurationError: ``current`` не содержит часовой пояс.
        """
        if current.tzinfo is None or current.utcoffset() is None:
            raise ConfigurationError(_AWARE_REQUIRED)
        self._current = current
        self._monotonic = 0.0

    @override
    def now(self) -> datetime:
        """Вернуть текущее управляемое время.

        Returns:
            Установленный тестом aware-момент.
        """
        return self._current

    @override
    def monotonic(self) -> float:
        """Вернуть число секунд, пройденных через :meth:`advance`.

        Returns:
            Неубывающее тестовое время в секундах.
        """
        return self._monotonic

    def advance(self, delta: timedelta | None = None, **parts: float) -> datetime:
        """Сдвинуть часы вперёд на ``delta`` и/или аргументы ``timedelta``.

        Примеры: ``advance(timedelta(seconds=1))`` и ``advance(hours=2)``.

        Returns:
            Новое календарное время.

        Raises:
            ConfigurationError: сдвиг отрицательный или содержит неверные части.
        """
        try:
            change = (delta or timedelta()) + timedelta(**parts)
        except (TypeError, OverflowError) as exc:
            raise ConfigurationError(str(exc)) from exc
        if change < timedelta():
            raise ConfigurationError(_FORWARD_ONLY)
        self._current += change
        self._monotonic += change.total_seconds()
        return self._current
