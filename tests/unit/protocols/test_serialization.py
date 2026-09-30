"""JsonSerializer и SerializerCodec: round-trip, ошибки, проверка протоколов."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tallyho.model.errors import TallyhoError
from tallyho.protocols.serialization import (
    JsonSerializer,
    PayloadCodec,
    SerializationError,
    Serializer,
    SerializerCodec,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tallyho.protocols.serialization import CallArgs

JSON_SCALARS = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text()
)
JSON_VALUES = st.recursive(
    JSON_SCALARS,
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)


@given(JSON_VALUES)
def test_json_round_trip(value: object) -> None:
    serializer = JsonSerializer()
    assert serializer.loads(serializer.dumps(value)) == value


def test_json_is_compact_utf8() -> None:
    assert JsonSerializer().dumps({"k": ["юникод ✓", 1]}) == '{"k":["юникод ✓",1]}'.encode()


@pytest.mark.parametrize(
    "value",
    [b"\x00", datetime(2026, 10, 1, tzinfo=UTC), {1, 2}, math.nan, math.inf],
    ids=["bytes", "datetime", "set", "nan", "inf"],
)
def test_json_rejects_unrepresentable_values(value: object) -> None:
    with pytest.raises(SerializationError):
        JsonSerializer().dumps(value)


@pytest.mark.parametrize("data", [b"{", b"\xff\xfe", b""], ids=["truncated", "not-utf8", "empty"])
def test_json_rejects_garbage(data: bytes) -> None:
    with pytest.raises(SerializationError) as info:
        JsonSerializer().loads(data)
    assert isinstance(info.value, TallyhoError)


@given(
    st.lists(JSON_VALUES, max_size=5),
    st.dictionaries(st.text(), JSON_VALUES, max_size=5),
)
def test_codec_round_trip(args: list[object], kwargs: dict[str, object]) -> None:
    codec = SerializerCodec()
    decoded = codec.decode("app.task", codec.encode("app.task", tuple(args), kwargs))
    assert decoded == (tuple(args), kwargs)


def test_codec_restores_positional_tuple() -> None:
    codec = SerializerCodec()
    args, kwargs = codec.decode("t", codec.encode("t", (1, "a"), {"flag": True}))
    assert args == (1, "a")
    assert isinstance(args, tuple)
    assert kwargs == {"flag": True}


class _ReprSerializer:
    """Фейк Serializer: хранит значение в памяти и отдаёт его ключ."""

    def __init__(self) -> None:
        self.stored: list[object] = []

    def dumps(self, value: object) -> bytes:
        self.stored.append(value)
        return str(len(self.stored) - 1).encode()

    def loads(self, data: bytes) -> object:
        return self.stored[int(data)]


def test_codec_uses_given_serializer() -> None:
    serializer = _ReprSerializer()
    codec = SerializerCodec(serializer)
    moment = datetime(2026, 10, 1, tzinfo=UTC)
    assert codec.decode("t", codec.encode("t", (moment,), {"at": moment})) == (
        (moment,),
        {"at": moment},
    )
    assert codec.serializer is serializer


@pytest.mark.parametrize(
    "stored",
    [[1, 2], {"args": [1]}, {"args": 1, "kwargs": {}}, {"args": [], "kwargs": []}],
    ids=["list", "no-kwargs", "args-not-list", "kwargs-not-dict"],
)
def test_codec_rejects_foreign_payload(stored: object) -> None:
    codec = SerializerCodec()
    with pytest.raises(SerializationError):
        codec.decode("t", JsonSerializer().dumps(stored))


class _TaggedCodec:
    """Фейк PayloadCodec: байты зависят от имени задачи, как у flexiq."""

    def encode(
        self, task_name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> bytes:
        return f"{task_name}|{len(args)}|{len(kwargs)}".encode()

    def decode(self, task_name: str, data: bytes) -> CallArgs:
        return (task_name,), {"raw": data}


def test_codec_fake_sees_task_name() -> None:
    codec: PayloadCodec = _TaggedCodec()
    data = codec.encode("app.send", (1,), {})
    assert codec.decode("app.send", data) == (("app.send",), {"raw": b"app.send|1|0"})


def test_protocols_accept_implementations() -> None:
    serializers: list[Serializer] = [JsonSerializer(), _ReprSerializer()]
    codecs: list[PayloadCodec] = [SerializerCodec(), _TaggedCodec()]
    assert all(isinstance(item, Serializer) for item in serializers)
    assert all(isinstance(item, PayloadCodec) for item in codecs)
    assert not isinstance(JsonSerializer(), PayloadCodec)
    assert not isinstance(SerializerCodec(), Serializer)
