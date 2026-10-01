"""Атрибуты и ``memo`` корневого батча: нормализация и лимиты (ARCHITECTURE §5.1, §15).

``attributes`` — неизменяемые пары «ключ → ``str | int | bool``» для корреляции
и поиска, ``memo`` — произвольный JSON-объект для диагностики. Одна и та же
:func:`normalize_attributes` готовит словарь и к записи, и к фильтру листинга:
containment jsonb строг к типу JSON, поэтому неявных приведений нет — иначе
фильтр молча ничего бы не находил (D-038).

Тексты ошибок называют ключ и нарушенное правило, но не содержат значений:
значения атрибутов и ``memo`` могут быть секретами.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, TypeAlias, cast
from uuid import UUID

from tallyho.model.errors import ConfigurationError, InvalidAttributesError

__all__ = [
    "AttributeLimits",
    "AttributeValue",
    "normalize_attributes",
    "normalize_memo",
]

AttributeValue: TypeAlias = str | int | bool
"""Значение атрибута после нормализации (``UUID`` уже превращён в строку)."""

_RESERVED_PREFIX: Final = "tallyho."
_BIGINT_MIN: Final = -(2**63)
_BIGINT_MAX: Final = 2**63 - 1
_KEY_PREVIEW_CHARS: Final = 64
# NUL внутри JSON-строки: `\u0000` после чётного числа обратных слэшей.
_ESCAPED_NUL: Final = re.compile(r"(?<!\\)(?:\\\\)*\\u0000")
# Суррогаты в UTF-8 не кодируются. Ищем их сами, а не ловим UnicodeEncodeError:
# исключение кодека хранит всю строку и унесло бы значение в ``__cause__``.
_SURROGATE: Final = re.compile(r"[\ud800-\udfff]")


@dataclass(frozen=True, slots=True, kw_only=True)
class AttributeLimits:
    """Лимиты атрибутов и ``memo`` корня (ARCHITECTURE §15).

    ``max_keys`` — число атрибутов; ``max_key_bytes`` / ``max_value_bytes`` —
    длина ключа и строкового значения в UTF-8; ``max_bytes`` — размер всего
    словаря атрибутов в компактном JSON; ``memo_max_bytes`` — размер ``memo``
    в компактном JSON.
    """

    max_keys: int = 32
    max_key_bytes: int = 128
    max_value_bytes: int = 512
    max_bytes: int = 8192
    memo_max_bytes: int = 16384

    def __post_init__(self) -> None:
        """Проверить лимиты (``ConfigurationError``, если не целое >= 1)."""
        for name, value in (
            ("max_keys", self.max_keys),
            ("max_key_bytes", self.max_key_bytes),
            ("max_value_bytes", self.max_value_bytes),
            ("max_bytes", self.max_bytes),
            ("memo_max_bytes", self.memo_max_bytes),
        ):
            _check_limit(name, value)


def _check_limit(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        message = f"{name} должен быть целым >= 1, получено {value!r}"
        raise ConfigurationError(message)


_DEFAULT_LIMITS: Final = AttributeLimits()


def normalize_attributes(
    raw: Mapping[str, object] | None,
    *,
    limits: AttributeLimits = _DEFAULT_LIMITS,
) -> dict[str, AttributeValue]:
    """Проверить и нормализовать атрибуты — и для записи, и для фильтра листинга.

    ``None`` → пустой словарь. Значения ``str``, ``int`` (в пределах ``bigint``)
    и ``bool`` сохраняют тип, ``UUID`` превращается в строку. ``bool`` и ``int``
    не смешиваются: ``True`` остаётся ``True``, ``1`` остаётся ``1``. Подклассы
    ``str`` и ``int`` (например, ``StrEnum``) приводятся к базовому типу.

    Returns:
        Новый словарь из обычных ``str``, ``int`` и ``bool``.

    Raises:
        InvalidAttributesError: ``raw`` — не ``Mapping``; ключ не строка, пуст,
            начинается с ``tallyho.`` или длиннее лимита; значение другого типа
            (``float``, ``None``, ``datetime``, ``bytes``, коллекции), число вне
            ``bigint`` или строка длиннее лимита; строка содержит суррогат или
            NUL; превышено число ключей или размер словаря в JSON.
    """
    if raw is None:
        return {}
    mapping = _as_mapping(raw, "attributes")
    if len(mapping) > limits.max_keys:
        message = f"attributes: ключей {len(mapping)}, это больше лимита {limits.max_keys}"
        raise InvalidAttributesError(message)
    result: dict[str, AttributeValue] = {}
    for raw_key, raw_value in mapping.items():
        key = _normalize_key(raw_key, limits)
        result[key] = _normalize_value(key, raw_value, limits)
    size = len(_compact_json(result).encode("utf-8"))
    if size > limits.max_bytes:
        message = f"attributes: размер в JSON {size} байт больше лимита {limits.max_bytes}"
        raise InvalidAttributesError(message)
    return result


def normalize_memo(
    raw: Mapping[str, object] | None,
    *,
    limits: AttributeLimits = _DEFAULT_LIMITS,
) -> dict[str, object] | None:
    """Проверить ``memo`` и вернуть его глубокую копию из обычных JSON-типов.

    ``None`` → ``None``. ``memo`` — JSON-объект: ``Mapping`` со строковыми
    ключами и строго JSON-сериализуемым содержимым (``NaN`` и бесконечности
    запрещены).

    Returns:
        Копия ``memo`` после ``dumps``/``loads`` либо ``None``.

    Raises:
        InvalidAttributesError: ``raw`` — не ``Mapping``, ключ верхнего уровня
            не строка, содержимое не сериализуется в JSON, строка содержит
            суррогат или NUL, размер больше ``memo_max_bytes``.
    """
    if raw is None:
        return None
    mapping = _as_mapping(raw, "memo")
    for key in mapping:
        if not isinstance(key, str):
            message = f"memo: ключи должны быть строками, получен {type(key).__name__}"
            raise InvalidAttributesError(message)
    try:
        text = json.dumps(dict(mapping), ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError, RecursionError) as exc:
        message = "memo: содержимое не сериализуется в JSON"
        raise InvalidAttributesError(message) from exc
    if _SURROGATE.search(text) is not None:
        message = "memo: строка не кодируется в UTF-8"
        raise InvalidAttributesError(message)
    if _ESCAPED_NUL.search(text) is not None:
        message = "memo: строка содержит символ NUL, jsonb его не хранит"
        raise InvalidAttributesError(message)
    size = len(text.encode("utf-8"))
    if size > limits.memo_max_bytes:
        message = f"memo: размер в JSON {size} байт больше лимита {limits.memo_max_bytes}"
        raise InvalidAttributesError(message)
    return cast("dict[str, object]", json.loads(text))


def _as_mapping(raw: object, what: str) -> Mapping[object, object]:
    if not isinstance(raw, Mapping):
        message = f"{what} должен быть словарём (Mapping), получен {type(raw).__name__}"
        raise InvalidAttributesError(message)
    return cast("Mapping[object, object]", raw)  # ключи и значения проверяются ниже


def _compact_json(value: Mapping[str, AttributeValue]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _plain_str(text: str) -> str:
    """Привести подкласс ``str`` (``StrEnum``, ``str``-миксин ``Enum``) к ``str``.

    ``str(member)`` и f-строка у ``Enum`` с миксином вернули бы имя члена,
    а не значение, поэтому строка пересобирается через байты; ``surrogatepass``
    сохраняет одиночные суррогаты — их отклоняет :func:`_utf8_size`.

    Returns:
        Обычная строка с тем же содержимым.
    """
    return text.encode("utf-8", "surrogatepass").decode("utf-8", "surrogatepass")


def _show_key(key: str) -> str:
    """Ключ для текста ошибки: длинный обрезается.

    Returns:
        ``repr`` ключа или его начала.
    """
    if len(key) <= _KEY_PREVIEW_CHARS:
        return repr(key)
    return f"{key[:_KEY_PREVIEW_CHARS]!r}…"


def _utf8_size(text: str, what: str) -> int:
    """Длина строки в UTF-8.

    Returns:
        Число байт.

    Raises:
        InvalidAttributesError: строка не кодируется в UTF-8 (суррогат)
            или содержит NUL, который jsonb не хранит.
    """
    if _SURROGATE.search(text) is not None:
        message = f"{what}: строка не кодируется в UTF-8"
        raise InvalidAttributesError(message)
    if "\x00" in text:
        message = f"{what}: строка содержит символ NUL, jsonb его не хранит"
        raise InvalidAttributesError(message)
    return len(text.encode("utf-8"))


def _normalize_key(key: object, limits: AttributeLimits) -> str:
    if not isinstance(key, str):
        message = f"ключ атрибута должен быть строкой, получен {type(key).__name__}"
        raise InvalidAttributesError(message)
    name = _plain_str(key)
    if not name:
        message = "ключ атрибута не может быть пустым"
        raise InvalidAttributesError(message)
    label = _show_key(name)
    if name.startswith(_RESERVED_PREFIX):
        message = f"ключ атрибута {label}: префикс {_RESERVED_PREFIX!r} зарезервирован за tallyho"
        raise InvalidAttributesError(message)
    size = _utf8_size(name, f"ключ атрибута {label}")
    if size > limits.max_key_bytes:
        message = (
            f"ключ атрибута {label}: длина {size} байт в UTF-8 больше лимита {limits.max_key_bytes}"
        )
        raise InvalidAttributesError(message)
    return name


def _normalize_value(key: str, value: object, limits: AttributeLimits) -> AttributeValue:
    label = _show_key(key)
    if isinstance(value, bool):  # раньше int: bool — подкласс int
        return value
    if isinstance(value, int):
        number = int(value)  # подкласс int (IntEnum) → обычное число
        if not _BIGINT_MIN <= number <= _BIGINT_MAX:
            message = f"атрибут {label}: целое значение выходит за пределы bigint"
            raise InvalidAttributesError(message)
        return number
    if isinstance(value, str | UUID):
        text = str(value) if isinstance(value, UUID) else _plain_str(value)
        size = _utf8_size(text, f"атрибут {label}")
        if size > limits.max_value_bytes:
            message = (
                f"атрибут {label}: длина значения {size} байт в UTF-8 "
                f"больше лимита {limits.max_value_bytes}"
            )
            raise InvalidAttributesError(message)
        return text
    message = (
        f"атрибут {label}: тип {type(value).__name__} не поддерживается, "
        "допустимы str, int, bool и UUID"
    )
    raise InvalidAttributesError(message)
