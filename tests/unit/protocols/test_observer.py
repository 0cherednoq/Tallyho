"""Observer: NullObserver молчит, наследник переопределяет только нужные события."""

from __future__ import annotations

from uuid import UUID

from typing_extensions import override

from tallyho.model.states import BatchState, ResultClass
from tallyho.protocols.observer import NullObserver, Observer

BATCH = UUID("01920000-0000-7000-8000-000000000002")
ITEM = UUID("01920000-0000-7000-8000-000000000001")


def _emit_all(observer: Observer) -> None:
    observer.item_finished(
        batch_id=BATCH, item_id=ITEM, result=ResultClass.OK, label="sent", attempt=1
    )
    observer.batch_finalized(batch_id=BATCH, kind="mailing", state=BatchState.SUCCEEDED)
    observer.hook_failed(
        batch_id=BATCH, kind="mailing", hook="on_finalized", attempt=2, error=RuntimeError("x")
    )
    observer.hook_missing(batch_id=BATCH, kind="mailing", hook="on_finalized")
    observer.relay_dispatched(messages=10, duration=0.5)
    observer.completer_flush(items=3, duration=0.01)


class _FlushCounter(NullObserver):
    def __init__(self) -> None:
        self.flushed: int = 0

    @override
    def completer_flush(self, *, items: int, duration: float) -> None:
        self.flushed += items


def test_null_observer_accepts_every_event() -> None:
    observer = NullObserver()
    assert isinstance(observer, Observer)
    _emit_all(observer)


def test_subclass_overrides_only_needed_events() -> None:
    observer = _FlushCounter()
    _emit_all(observer)
    assert observer.flushed == 3


def test_protocol_rejects_object_without_events() -> None:
    assert not isinstance(object(), Observer)
