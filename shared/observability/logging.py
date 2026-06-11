"""
Centralised logging configuration for all services.
Emits structured JSON — one object per line — so logs are machine-parseable
by CloudWatch, Datadog, Grafana Loki, or any log aggregator.

trace_id / conversation_id / event_type are injected automatically from
shared.trace_context (ContextVar) so every log line carries full context
without the caller having to pass them explicitly.
"""
import json
import logging
import time

from shared.observability.trace import _trace_id, _conversation_id, _event_type

_EXTRA_FIELDS = ("stage", "latency_ms", "message_id", "offset", "partition", "topic")


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log: dict = {
            "ts":              time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level":           record.levelname,
            "logger":          record.name,
            "msg":             record.getMessage(),
            "trace_id":        _trace_id.get("-"),
            "conversation_id": _conversation_id.get("-"),
            "event_type":      _event_type.get("-"),
        }
        for key in _EXTRA_FIELDS:
            if hasattr(record, key):
                log[key] = getattr(record, key)
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log)


def setup_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
