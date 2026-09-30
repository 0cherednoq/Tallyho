"""TaskCall: опции, неизменяемость, проверки."""

from __future__ import annotations

from typing import cast

import pytest

from tallyho.model.calls import TaskCall
from tallyho.model.errors import ConfigurationError


def _opaque(value: object) -> object:
    return value


def test_defaults() -> None:
    call = TaskCall(task_name="app.send_email")
    assert call.args == ()
    assert call.kwargs == {}
    assert (call.key, call.weight, call.queue) == (None, 1, None)
    assert call.options == {}


def test_opts_returns_new_call() -> None:
    call = TaskCall(task_name="t", args=(1, 2), kwargs={"mailbox_id": 3})
    tuned = call.opts(key="a@b.c", weight=5, queue="mail", priority=7)
    assert tuned is not call
    assert (tuned.key, tuned.weight, tuned.queue) == ("a@b.c", 5, "mail")
    assert tuned.options == {"priority": 7}
    assert (tuned.args, tuned.kwargs) == ((1, 2), {"mailbox_id": 3})
    assert (call.key, call.weight, call.queue, call.options) == (None, 1, None, {})


def test_opts_keeps_unset_and_merges_options() -> None:
    call = TaskCall(task_name="t").opts(key="k", queue="q", priority=1, max_retries=2)
    again = call.opts(priority=9)
    assert (again.key, again.queue) == ("k", "q")
    assert again.options == {"priority": 9, "max_retries": 2}


def test_mappings_are_read_only_copies() -> None:
    kwargs: dict[str, object] = {"a": 1}
    args = cast("tuple[object, ...]", _opaque([1]))  # список вместо кортежа
    call = TaskCall(task_name="t", kwargs=kwargs, args=args)
    kwargs["a"] = 2
    assert call.kwargs == {"a": 1}
    assert call.args == (1,)
    with pytest.raises(TypeError):
        cast("dict[str, object]", call.kwargs)["a"] = 3
    with pytest.raises(TypeError):
        cast("dict[str, object]", call.options)["x"] = 3


def test_equality_by_value() -> None:
    assert TaskCall(task_name="t", args=(1,)).opts(key="k") == TaskCall(
        task_name="t", args=(1,), key="k"
    )


def test_empty_task_name() -> None:
    with pytest.raises(ConfigurationError, match="task_name"):
        TaskCall(task_name="")


@pytest.mark.parametrize("weight", [0, -1, True, 1.5])
def test_invalid_weight(weight: object) -> None:
    with pytest.raises(ConfigurationError, match="weight"):
        TaskCall(task_name="t", weight=cast("int", weight))
    with pytest.raises(ConfigurationError, match="weight"):
        TaskCall(task_name="t").opts(weight=cast("int", weight))
