"""Продюсер без БД: параметры батча и хранимые колбэки."""

from __future__ import annotations

import pytest

from tallyho.engine.producer import CallbackName, RootSpec, StoredCallback
from tallyho.model.errors import ConfigurationError


def test_stored_callback_round_trip() -> None:
    callback = StoredCallback(
        task_name="notify", payload=b"\x00\xffargs", queue="mail", options={"priority": 5}
    )
    data = callback.to_json()
    assert data == {
        "task_name": "notify",
        "payload": "AP9hcmdz",
        "queue": "mail",
        "options": {"priority": 5},
    }
    assert StoredCallback.from_json(data) == callback


def test_stored_callback_defaults() -> None:
    callback = StoredCallback.from_json({"task_name": "t", "payload": "", "options": {}})
    assert callback == StoredCallback(task_name="t", payload=b"")


@pytest.mark.parametrize(
    "data",
    [
        {"payload": "", "options": {}},
        {"task_name": "t", "payload": 1, "options": {}},
        {"task_name": "t", "payload": "", "queue": 1, "options": {}},
        {"task_name": "t", "payload": "", "options": []},
        {"task_name": "t", "payload": "не base64", "options": {}},
    ],
)
def test_stored_callback_rejects_broken_json(data: dict[str, object]) -> None:
    with pytest.raises(ConfigurationError, match="колбэка"):
        _ = StoredCallback.from_json(data)


def test_stored_callback_options_must_be_json() -> None:
    callback = StoredCallback(task_name="t", payload=b"", options={"at": object()})
    with pytest.raises(ConfigurationError, match="JSON"):
        _ = callback.to_json()


def test_callback_names_match_architecture() -> None:
    assert [name.value for name in CallbackName] == [
        "on_succeeded",
        "on_completed_with_errors",
        "on_failed",
        "on_cancelled",
        "on_finalized_task",
    ]


def test_root_spec_requires_kind() -> None:
    with pytest.raises(ConfigurationError, match="kind"):
        _ = RootSpec(kind="")


def test_root_spec_rejects_bad_limits() -> None:
    with pytest.raises(ConfigurationError, match="max_in_flight"):
        _ = RootSpec(kind="k", max_in_flight=0)
    with pytest.raises(ConfigurationError, match="expected_total"):
        _ = RootSpec(kind="k", expected_total=-1)
    with pytest.raises(ConfigurationError, match="max_items"):
        _ = RootSpec(kind="k", max_items=0)
    with pytest.raises(ConfigurationError, match="max_items"):
        _ = RootSpec(kind="k", max_items=True)


def test_root_spec_accepts_limits() -> None:
    spec = RootSpec(kind="k", max_in_flight=1, expected_total=0, max_items=1)
    assert (spec.max_in_flight, spec.expected_total, spec.max_items) == (1, 0, 1)
