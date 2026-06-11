"""
Centralised telemetry: OpenTelemetry distributed tracing.

Active only when OTEL_EXPORTER_OTLP_ENDPOINT is set in the environment.
Exports spans to Jaeger / Grafana Tempo via OTLP gRPC.
When the env var is not set the tracer is a no-op (zero overhead).
"""
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ── OpenTelemetry tracer ──────────────────────────────────────────────────────

def setup_tracer(service_name: str) -> trace.Tracer:
    """
    Call once at service startup.
    If OTEL_EXPORTER_OTLP_ENDPOINT is set, spans are exported via OTLP gRPC.
    Otherwise the tracer is a no-op (zero overhead).
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint:
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)
