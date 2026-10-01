"""OpenTelemetry implementation of the engine Observer protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

from opentelemetry import metrics, trace
from typing_extensions import override

from tallyho.protocols.observer import NullObserver

if TYPE_CHECKING:
    from uuid import UUID

    from opentelemetry.metrics import Meter
    from opentelemetry.trace import Tracer

    from tallyho.model.states import BatchState, ResultClass

__all__ = ["OpenTelemetryObserver"]


@final
class OpenTelemetryObserver(NullObserver):
    """Emit lifecycle spans and operational measurements without task payloads."""

    def __init__(self, *, tracer: Tracer | None = None, meter: Meter | None = None) -> None:
        """Use the global providers unless an explicit tracer or meter is supplied."""
        self._tracer = tracer or trace.get_tracer("tallyho")
        selected_meter = meter or metrics.get_meter("tallyho")
        self._hook_failures = selected_meter.create_counter("th_hook_failures")
        self._hook_missing = selected_meter.create_counter("th_hook_missing")
        self._relay_lag = selected_meter.create_histogram("th_relay_lag", unit="s")
        self._completer_buffer = selected_meter.create_histogram("th_completer_buffer_size")
        self._oldest_lease = selected_meter.create_histogram("th_oldest_lease_age", unit="s")
        self._transaction_retries = selected_meter.create_counter("th_transaction_retries")

    def _span(self, name: str, attributes: dict[str, str | int]) -> None:
        with self._tracer.start_as_current_span(name) as span:
            span.set_attributes(attributes)

    @override
    def batch_created(self, *, batch_id: UUID, kind: str) -> None:
        self._span(
            "tallyho.create", {"tallyho.batch.id": str(batch_id), "tallyho.batch.kind": kind}
        )

    @override
    def item_claimed(self, *, batch_id: UUID, item_id: UUID, attempt: int) -> None:
        self._span(
            "tallyho.claim",
            {
                "tallyho.batch.id": str(batch_id),
                "tallyho.item.id": str(item_id),
                "tallyho.item.attempt": attempt,
            },
        )

    @override
    def item_finished(
        self,
        *,
        batch_id: UUID,
        item_id: UUID,
        result: ResultClass,
        label: str | None,
        attempt: int,
    ) -> None:
        attributes: dict[str, str | int] = {
            "tallyho.batch.id": str(batch_id),
            "tallyho.item.id": str(item_id),
            "tallyho.item.result": result.name.lower(),
            "tallyho.item.attempt": attempt,
        }
        if label is not None:
            attributes["tallyho.item.label"] = label
        self._span("tallyho.finish", attributes)

    @override
    def batch_finalized(self, *, batch_id: UUID, kind: str, state: BatchState) -> None:
        self._span(
            "tallyho.finalize",
            {
                "tallyho.batch.id": str(batch_id),
                "tallyho.batch.kind": kind,
                "tallyho.batch.state": state.name.lower(),
            },
        )

    @override
    def hook_failed(
        self, *, batch_id: UUID, kind: str, hook: str, attempt: int, error: BaseException
    ) -> None:
        del batch_id, error
        self._hook_failures.add(
            1, {"tallyho.batch.kind": kind, "tallyho.hook": hook, "attempt": attempt}
        )

    @override
    def hook_missing(self, *, batch_id: UUID, kind: str, hook: str) -> None:
        del batch_id
        self._hook_missing.add(1, {"tallyho.batch.kind": kind, "tallyho.hook": hook})

    @override
    def relay_lag(self, *, seconds: float) -> None:
        self._relay_lag.record(seconds)

    @override
    def completer_buffer(self, *, items: int) -> None:
        self._completer_buffer.record(items)

    @override
    def oldest_lease(self, *, seconds: float) -> None:
        self._oldest_lease.record(seconds)

    @override
    def transaction_retry(self, *, sqlstate: str) -> None:
        if sqlstate == "40P01":
            self._transaction_retries.add(1, {"db.system": "postgresql", "db.sqlstate": sqlstate})
