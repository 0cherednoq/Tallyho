"""OpenTelemetry Observer emits the documented spans and metrics."""

from __future__ import annotations

from uuid import UUID

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tallyho.model.states import BatchState, ResultClass
from tallyho.observability.otel import OpenTelemetryObserver

BATCH = UUID("01920000-0000-7000-8000-000000000002")
ITEM = UUID("01920000-0000-7000-8000-000000000001")


def test_emits_lifecycle_spans_and_operational_metrics() -> None:
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    metrics = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metrics])
    observer = OpenTelemetryObserver(
        tracer=tracer_provider.get_tracer("test"),
        meter=meter_provider.get_meter("test"),
    )

    observer.batch_created(batch_id=BATCH, kind="mailing")
    observer.item_claimed(batch_id=BATCH, item_id=ITEM, attempt=1)
    observer.item_finished(
        batch_id=BATCH,
        item_id=ITEM,
        result=ResultClass.OK,
        label="sent",
        attempt=1,
    )
    observer.batch_finalized(batch_id=BATCH, kind="mailing", state=BatchState.SUCCEEDED)
    observer.hook_failed(
        batch_id=BATCH,
        kind="mailing",
        hook="on_finalized",
        attempt=2,
        error=RuntimeError("secret must not be exported"),
    )
    observer.hook_missing(batch_id=BATCH, kind="mailing", hook="on_progress")
    observer.relay_lag(seconds=0.4)
    observer.completer_buffer(items=7)
    observer.oldest_lease(seconds=3.0)
    observer.transaction_retry(sqlstate="40001")
    observer.transaction_retry(sqlstate="40P01")

    assert [span.name for span in spans.get_finished_spans()] == [
        "tallyho.create",
        "tallyho.claim",
        "tallyho.finish",
        "tallyho.finalize",
    ]
    data = metrics.get_metrics_data()
    assert data is not None
    names = {
        metric.name
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    assert names == {
        "th_hook_failures",
        "th_hook_missing",
        "th_relay_lag",
        "th_completer_buffer_size",
        "th_oldest_lease_age",
        "th_transaction_retries",
    }
