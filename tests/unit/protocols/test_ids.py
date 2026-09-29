"""UUIDv7: версия и вариант, метка времени, монотонность, порядок сортировки."""

from __future__ import annotations

import itertools
import uuid

from hypothesis import given
from hypothesis import strategies as st

from tallyho.protocols.ids import IdFactory, UuidV7Factory

MS = 1_000_000
T0_MS = 1_790_000_000_000  # 2026-09-21


class _ManualTime:
    def __init__(self, ms: int) -> None:
        self.ms: int = ms

    def __call__(self) -> int:
        return self.ms * MS + 123


def _factory(clock: _ManualTime) -> UuidV7Factory:
    return UuidV7Factory(time_ns=clock)


def _timestamp_ms(value: uuid.UUID) -> int:
    return value.int >> 80


def test_version_and_variant() -> None:
    value = _factory(_ManualTime(T0_MS)).new_id()
    assert value.version == 7
    assert value.variant == uuid.RFC_4122


def test_timestamp_is_unix_milliseconds() -> None:
    assert _timestamp_ms(_factory(_ManualTime(T0_MS)).new_id()) == T0_MS


def test_monotonic_within_one_millisecond() -> None:
    factory = _factory(_ManualTime(T0_MS))
    ids = [factory.new_id() for _ in range(10_000)]
    assert all(a < b for a, b in itertools.pairwise(ids))
    assert {_timestamp_ms(value) for value in ids} == {T0_MS}


def test_sort_order_equals_generation_order_across_milliseconds() -> None:
    clock = _ManualTime(T0_MS)
    factory = _factory(clock)
    ids: list[uuid.UUID] = []
    for step in range(50):
        clock.ms = T0_MS + step // 7
        ids.append(factory.new_id())
    assert sorted(ids) == ids
    assert sorted(ids, key=lambda value: value.bytes) == ids
    assert sorted(ids, key=str) == ids


def test_clock_going_back_keeps_order() -> None:
    clock = _ManualTime(T0_MS)
    factory = _factory(clock)
    first = factory.new_id()
    clock.ms = T0_MS - 5_000
    second = factory.new_id()
    assert second > first
    assert _timestamp_ms(second) == T0_MS


def test_counter_overflow_moves_timestamp_forward() -> None:
    factory = UuidV7Factory(time_ns=_ManualTime(T0_MS), urandom=lambda n: b"\xff" * n)
    first = factory.new_id()
    # Дойти до переполнения честно — 2**41 вызовов, поэтому ставим счётчик напрямую.
    factory._counter = (1 << 42) - 1  # ruff: ignore[private-member-access]  # см. комментарий выше
    second = factory.new_id()
    assert second > first
    assert _timestamp_ms(second) == T0_MS + 1


def test_fresh_counter_leaves_headroom() -> None:
    value = UuidV7Factory(time_ns=_ManualTime(T0_MS), urandom=lambda n: b"\xff" * n).new_id()
    counter = (value.int >> 64 & 0xFFF) << 30 | (value.int >> 32 & ((1 << 30) - 1))
    assert counter == (1 << 41) - 1
    assert value.version == 7
    assert value.variant == uuid.RFC_4122


def test_default_factory_is_monotonic() -> None:
    factory = UuidV7Factory()
    ids = [factory.new_id() for _ in range(2_000)]
    assert all(value.version == 7 for value in ids)
    assert sorted(ids) == ids
    assert len(set(ids)) == len(ids)


def test_factory_satisfies_protocol() -> None:
    factory: IdFactory = UuidV7Factory()
    assert isinstance(factory, IdFactory)
    assert not isinstance(object(), IdFactory)


@given(st.lists(st.integers(min_value=0, max_value=(1 << 48) - 2), min_size=1, max_size=50))
def test_ids_strictly_increase_for_any_clock_sequence(times_ms: list[int]) -> None:
    clock = _ManualTime(times_ms[0])
    factory = _factory(clock)
    ids: list[uuid.UUID] = []
    for ms in times_ms:
        clock.ms = ms
        ids.append(factory.new_id())
    assert all(a < b for a, b in itertools.pairwise(ids))
    assert all(value.version == 7 and value.variant == uuid.RFC_4122 for value in ids)
