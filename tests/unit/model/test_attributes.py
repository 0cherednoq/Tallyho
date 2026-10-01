"""Атрибуты и memo батча: нормализация, запрещённые типы, границы лимитов (A-AT-02, A-AT-03)."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest

from tallyho.model.attributes import AttributeLimits, normalize_attributes, normalize_memo
from tallyho.model.errors import ConfigurationError, InvalidAttributesError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tallyho.model.attributes import AttributeValue

BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1
SECRET = "s3cr3t-t0ken"  # ruff: ignore[hardcoded-password-string]  # маркер для проверки текста ошибок
LIMIT_FIELDS = ("max_keys", "max_key_bytes", "max_value_bytes", "max_bytes", "memo_max_bytes")


class Env(StrEnum):
    PROD = "prod"


class Tier(str, Enum):  # ruff: ignore[replace-str-enum]  # проверяется именно str-миксин Enum
    GOLD = "gold"


class Priority(IntEnum):
    HIGH = 3


def _raw(value: object) -> Mapping[str, object]:
    """Передать в нормализацию то, что не проходит по аннотации."""
    return cast("Mapping[str, object]", value)


def _json_size(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _rejected(raw: object, *, limits: AttributeLimits | None = None) -> InvalidAttributesError:
    with pytest.raises(InvalidAttributesError) as exc_info:
        _ = normalize_attributes(_raw(raw), limits=limits or AttributeLimits())
    return exc_info.value


def _memo_rejected(raw: object, *, limits: AttributeLimits | None = None) -> InvalidAttributesError:
    with pytest.raises(InvalidAttributesError) as exc_info:
        _ = normalize_memo(_raw(raw), limits=limits or AttributeLimits())
    return exc_info.value


# --- AttributeLimits ---------------------------------------------------------


def test_limits_defaults_match_architecture() -> None:
    limits = AttributeLimits()
    assert dataclasses.asdict(limits) == {
        "max_keys": 32,
        "max_key_bytes": 128,
        "max_value_bytes": 512,
        "max_bytes": 8192,
        "memo_max_bytes": 16384,
    }


def test_limits_are_frozen() -> None:
    limits = AttributeLimits()
    name = "max_keys"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(limits, name, 1)


@pytest.mark.parametrize("field", LIMIT_FIELDS)
def test_limit_of_one_is_accepted(field: str) -> None:
    limits = dataclasses.replace(AttributeLimits(), **{field: 1})
    assert dataclasses.asdict(limits)[field] == 1


@pytest.mark.parametrize("field", LIMIT_FIELDS)
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "8", None], ids=repr)
def test_limit_must_be_positive_int(field: str, bad: object) -> None:
    with pytest.raises(ConfigurationError, match=field) as exc_info:
        dataclasses.replace(AttributeLimits(), **{field: cast("int", bad)})
    assert not isinstance(exc_info.value, InvalidAttributesError)


# --- normalize_attributes: типы ----------------------------------------------


def test_none_and_empty_become_empty_dict() -> None:
    assert normalize_attributes(None) == {}
    assert normalize_attributes({}) == {}


def test_supported_types_are_kept() -> None:
    raw: dict[str, object] = {"tenant": "acme", "campaign_id": 42, "dry_run": False}
    result = normalize_attributes(raw)
    assert result == {"tenant": "acme", "campaign_id": 42, "dry_run": False}
    assert result is not raw
    assert {key: type(value) for key, value in result.items()} == {
        "tenant": str,
        "campaign_id": int,
        "dry_run": bool,
    }


def test_bool_and_int_are_not_mixed() -> None:
    result = normalize_attributes({"t": True, "f": False, "one": 1, "zero": 0})
    assert result["t"] is True
    assert result["f"] is False
    assert type(result["one"]) is int
    assert type(result["zero"]) is int


def test_uuid_becomes_string() -> None:
    value = UUID("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
    result = normalize_attributes({"order": value})
    assert result == {"order": "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"}
    assert type(result["order"]) is str


def test_str_and_int_subclasses_become_plain() -> None:
    raw: dict[str, object] = {Env.PROD: Env.PROD, "tier": Tier.GOLD, "priority": Priority.HIGH}
    result = normalize_attributes(raw)
    assert result == {"prod": "prod", "tier": "gold", "priority": 3}
    assert [type(key) for key in result] == [str, str, str]
    assert [type(value) for value in result.values()] == [str, str, int]


def test_mapping_proxy_is_accepted_and_input_untouched() -> None:
    source: dict[str, object] = {"order": UUID(int=7)}
    result = normalize_attributes(MappingProxyType(source))
    assert result == {"order": str(UUID(int=7))}
    assert source == {"order": UUID(int=7)}


@pytest.mark.parametrize("value", [BIGINT_MIN, BIGINT_MAX, 0, -1])
def test_int_within_bigint_is_accepted(value: int) -> None:
    assert normalize_attributes({"n": value}) == {"n": value}


@pytest.mark.parametrize("value", [BIGINT_MIN - 1, BIGINT_MAX + 1])
def test_int_outside_bigint_is_rejected(value: int) -> None:
    exc = _rejected({"n": value})
    assert "'n'" in str(exc)
    assert "bigint" in str(exc)
    assert str(value) not in str(exc)


@pytest.mark.parametrize(
    "value",
    [
        1.5,
        float("nan"),
        None,
        datetime(2026, 10, 1, tzinfo=UTC),
        date(2026, 10, 1),
        b"bytes",
        bytearray(b"bytes"),
        Decimal(1),
        ["a"],
        ("a",),
        {"a": 1},
        {"a"},
        frozenset({"a"}),
        object(),
    ],
    ids=lambda value: type(value).__name__,
)
def test_forbidden_value_type_is_rejected(value: object) -> None:
    exc = _rejected({"field": value})
    assert "'field'" in str(exc)
    assert type(value).__name__ in str(exc)
    assert exc.__cause__ is None


@pytest.mark.parametrize(
    "raw", [[("a", 1)], "a=1", 5, {"a"}, object()], ids=lambda raw: type(raw).__name__
)
def test_non_mapping_is_rejected(raw: object) -> None:
    exc = _rejected(raw)
    assert type(raw).__name__ in str(exc)


# --- normalize_attributes: ключи ---------------------------------------------


@pytest.mark.parametrize("key", ["tallyho.", "tallyho.kind", "tallyho.a.b"])
def test_reserved_prefix_is_rejected(key: str) -> None:
    exc = _rejected({key: 1})
    assert repr(key) in str(exc)
    assert "зарезервирован" in str(exc)


@pytest.mark.parametrize("key", ["tallyho", "tallyhox", "app.tallyho.kind", "Tallyho.kind", "."])
def test_similar_keys_are_allowed(key: str) -> None:
    assert normalize_attributes({key: 1}) == {key: 1}


def test_empty_key_is_rejected() -> None:
    assert "пуст" in str(_rejected({"": 1}))


@pytest.mark.parametrize("key", [1, None, b"k", UUID(int=1), ("a",), True], ids=repr)
def test_non_string_key_is_rejected(key: object) -> None:
    exc = _rejected({key: "value"})
    assert type(key).__name__ in str(exc)


def test_long_key_is_truncated_in_message() -> None:
    key = "k" * 5000
    exc = _rejected({key: 1})
    assert "k" * 64 in str(exc)
    assert "k" * 65 not in str(exc)


# --- normalize_attributes: лимиты --------------------------------------------


def test_max_keys_boundary() -> None:
    at_limit: dict[str, object] = {f"k{i}": i for i in range(32)}
    assert len(normalize_attributes(at_limit)) == 32
    exc = _rejected(at_limit | {"extra": 1})
    assert "33" in str(exc)
    assert "32" in str(exc)


def test_max_keys_custom_limit() -> None:
    limits = AttributeLimits(max_keys=1)
    assert normalize_attributes({"a": 1}, limits=limits) == {"a": 1}
    _rejected({"a": 1, "b": 2}, limits=limits)


@pytest.mark.parametrize(
    ("at_limit", "over_limit"),
    [
        ("k" * 128, "k" * 129),
        ("ж" * 64, "ж" * 64 + "k"),  # 65 символов, но 129 байт
        ("😀" * 32, "😀" * 32 + "k"),
    ],
    ids=["ascii", "cyrillic", "emoji"],
)
def test_key_bytes_boundary(at_limit: str, over_limit: str) -> None:
    assert len(at_limit.encode()) == 128
    assert normalize_attributes({at_limit: 1}) == {at_limit: 1}
    exc = _rejected({over_limit: 1})
    assert "129" in str(exc)
    assert "128" in str(exc)


@pytest.mark.parametrize(
    ("at_limit", "over_limit"),
    [
        ("v" * 512, "v" * 513),
        ("ж" * 256, "ж" * 256 + "v"),  # 257 символов, но 513 байт
        ("😀" * 128, "😀" * 128 + "v"),
    ],
    ids=["ascii", "cyrillic", "emoji"],
)
def test_value_bytes_boundary(at_limit: str, over_limit: str) -> None:
    assert len(at_limit.encode()) == 512
    assert normalize_attributes({"note": at_limit}) == {"note": at_limit}
    exc = _rejected({"note": over_limit})
    assert "'note'" in str(exc)
    assert "513" in str(exc)
    assert "512" in str(exc)


def test_value_limit_applies_to_uuid_string() -> None:
    value = UUID(int=1)
    assert normalize_attributes({"id": value}, limits=AttributeLimits(max_value_bytes=36))
    _rejected({"id": value}, limits=AttributeLimits(max_value_bytes=35))


def test_value_limit_does_not_apply_to_numbers() -> None:
    limits = AttributeLimits(max_value_bytes=1)
    assert normalize_attributes({"n": BIGINT_MAX, "b": False}, limits=limits) == {
        "n": BIGINT_MAX,
        "b": False,
    }


def _sized_attributes(last_value_bytes: int, fill: str = "v") -> dict[str, object]:
    """16 строковых атрибутов: 15 по 512 байт и последний заданной длины.

    Компактный JSON: скобки (2) + запятые (15) + 16 записей ``"kNN":"…"``
    (8 байт обвязки каждая) + значения = 145 + 15 * 512 + ``last_value_bytes``.
    """
    width = len(fill.encode())
    attributes: dict[str, object] = {f"k{i:02d}": fill * (512 // width) for i in range(15)}
    attributes["k15"] = fill * (last_value_bytes // width) + "v" * (last_value_bytes % width)
    return attributes


@pytest.mark.parametrize("fill", ["v", "ж", "😀"], ids=["ascii", "cyrillic", "emoji"])
def test_total_bytes_boundary(fill: str) -> None:
    at_limit = _sized_attributes(8192 - 145 - 15 * 512, fill)
    assert _json_size(at_limit) == 8192
    assert normalize_attributes(at_limit) == at_limit
    over_limit = _sized_attributes(8192 - 145 - 15 * 512 + 1, fill)
    exc = _rejected(over_limit)
    assert "8193" in str(exc)
    assert "8192" in str(exc)


def test_total_bytes_counts_normalized_values() -> None:
    raw: dict[str, object] = {"id": UUID(int=1), "ok": True, "n": 12}
    normalized: dict[str, AttributeValue] = {"id": str(UUID(int=1)), "ok": True, "n": 12}
    size = _json_size(normalized)
    assert normalize_attributes(raw, limits=AttributeLimits(max_bytes=size)) == normalized
    _rejected(raw, limits=AttributeLimits(max_bytes=size - 1))


# --- normalize_attributes: строки, которые не хранит jsonb -------------------


@pytest.mark.parametrize("text", ["\ud800", "a\udfffb", "\ud83d" + "\ude00"], ids=ascii)
def test_surrogate_is_rejected_in_key_and_value(text: str) -> None:
    value_exc = _rejected({"field": text})
    assert "'field'" in str(value_exc)
    assert "UTF-8" in str(value_exc)
    assert value_exc.__cause__ is None
    assert "UTF-8" in str(_rejected({text: 1}))


def test_nul_is_rejected_in_key_and_value() -> None:
    assert "NUL" in str(_rejected({"field": "a\x00b"}))
    assert "NUL" in str(_rejected({"a\x00b": 1}))


# --- normalize_attributes: значения не попадают в текст ошибки ----------------


@pytest.mark.parametrize(
    "value",
    [
        SECRET * 100,
        [SECRET],
        {SECRET: SECRET},
        (SECRET,),
        SECRET.encode(),
        f"{SECRET}\x00",
        f"{SECRET}\ud800",
    ],
    ids=["too-long", "list", "dict", "tuple", "bytes", "nul", "surrogate"],
)
def test_error_message_names_key_but_not_value(value: object) -> None:
    exc = _rejected({"api_token": value})
    assert "'api_token'" in str(exc)
    assert SECRET not in str(exc)
    assert SECRET not in repr(exc)
    assert exc.__cause__ is None


def test_error_message_hides_number_and_float() -> None:
    assert "31337" not in str(_rejected({"pin": 31337.25}))
    assert str(2**70) not in str(_rejected({"pin": 2**70}))


def test_total_size_error_hides_values() -> None:
    exc = _rejected({"a": SECRET, "b": SECRET}, limits=AttributeLimits(max_bytes=16))
    assert SECRET not in str(exc)


# --- normalize_memo ----------------------------------------------------------


def test_memo_none_stays_none() -> None:
    assert normalize_memo(None) is None


def test_memo_empty_object_is_kept() -> None:
    assert normalize_memo({}) == {}


def test_memo_is_deep_copied_into_plain_json_types() -> None:
    nested: dict[str, object] = {"ids": [1, 2], "pair": (1, "a")}
    source: dict[str, object] = {"request": nested, "ok": True, "ratio": 0.5, "none": None}
    result = normalize_memo(MappingProxyType(source))
    assert result == {
        "request": {"ids": [1, 2], "pair": [1, "a"]},
        "ok": True,
        "ratio": 0.5,
        "none": None,
    }
    assert type(result) is dict
    copied = cast("dict[str, list[int]]", result["request"])
    copied["ids"].append(3)
    assert nested["ids"] == [1, 2]
    cast("list[int]", nested["ids"]).append(4)
    assert copied["ids"] == [1, 2, 3]


@pytest.mark.parametrize(
    "raw", [["a"], "text", 5, ("a", 1), {"a"}], ids=lambda raw: type(raw).__name__
)
def test_memo_non_mapping_is_rejected(raw: object) -> None:
    exc = _memo_rejected(raw)
    assert type(raw).__name__ in str(exc)


@pytest.mark.parametrize("key", [1, None, 1.5, True, ("a",)], ids=repr)
def test_memo_non_string_key_is_rejected(key: object) -> None:
    exc = _memo_rejected({"ok": 1, key: 2})
    assert type(key).__name__ in str(exc)


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), float("-inf"), [1, {"x": float("nan")}]], ids=repr
)
def test_memo_nan_and_infinity_are_rejected(value: object) -> None:
    exc = _memo_rejected({"value": value})
    assert isinstance(exc.__cause__, ValueError)


@pytest.mark.parametrize(
    "value",
    [
        object(),
        datetime(2026, 10, 1, tzinfo=UTC),
        UUID(int=1),
        {"a"},
        b"bytes",
        Decimal(1),
        MappingProxyType({"a": 1}),
        {("a", "b"): 1},
    ],
    ids=lambda value: type(value).__name__,
)
def test_memo_unserializable_is_rejected(value: object) -> None:
    exc = _memo_rejected({"value": value})
    assert isinstance(exc.__cause__, TypeError)


def test_memo_circular_reference_is_rejected() -> None:
    loop: dict[str, object] = {}
    loop["self"] = loop
    exc = _memo_rejected(loop)
    assert isinstance(exc.__cause__, ValueError)


@pytest.mark.parametrize("fill", ["v", "ж", "😀"], ids=["ascii", "cyrillic", "emoji"])
def test_memo_size_boundary(fill: str) -> None:
    # `{"k":"…"}` — 8 байт обвязки.
    payload = fill * ((16384 - 8) // len(fill.encode()))
    at_limit = {"k": payload}
    assert _json_size(at_limit) == 16384
    assert normalize_memo(at_limit) == at_limit
    exc = _memo_rejected({"k": payload + "v"})
    assert "16385" in str(exc)
    assert "16384" in str(exc)


def test_memo_custom_limit_is_independent_of_attribute_limits() -> None:
    limits = AttributeLimits(max_bytes=1, max_keys=1, max_value_bytes=1, memo_max_bytes=14)
    memo: dict[str, object] = {"a": 1, "b": 22}
    assert _json_size(memo) == 14
    assert normalize_memo(memo, limits=limits) == memo
    _memo_rejected({"a": 1, "b": 222}, limits=limits)


@pytest.mark.parametrize(
    "memo",
    [{"k": "a\x00b"}, {"k": ["x", {"deep": "\x00"}]}, {"k\x00": 1}, {"k": "\\\x00"}],
    ids=["value", "nested", "key", "after-backslash"],
)
def test_memo_nul_is_rejected(memo: Mapping[str, object]) -> None:
    exc = _memo_rejected(memo)
    assert "NUL" in str(exc)


@pytest.mark.parametrize("text", ["\\u0000", "\\\\u0000", "u0000", "\\x00"], ids=ascii)
def test_memo_text_that_looks_like_escaped_nul_is_kept(text: str) -> None:
    assert normalize_memo({"k": text}) == {"k": text}


def test_memo_surrogate_is_rejected() -> None:
    exc = _memo_rejected({"k": [f"{SECRET}\ud800"]})
    assert "UTF-8" in str(exc)
    assert exc.__cause__ is None


@pytest.mark.parametrize(
    "memo",
    [
        {"token": SECRET * 2000},
        {"token": SECRET, "bad": float("nan")},
        {"token": SECRET, "bad": object()},
        {"token": f"{SECRET}\x00"},
        {"token": f"{SECRET}\ud800"},
    ],
    ids=["too-long", "nan", "unserializable", "nul", "surrogate"],
)
def test_memo_error_message_hides_content(memo: Mapping[str, object]) -> None:
    exc = _memo_rejected(memo)
    assert SECRET not in str(exc)
    assert SECRET not in repr(exc.__cause__)
