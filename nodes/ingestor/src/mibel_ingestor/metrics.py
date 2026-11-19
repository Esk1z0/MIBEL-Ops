"""Prometheus metrics, scraped by the manager's Prometheus over the VPN (ADR-05).

Low cardinality on purpose (labels: source, dataset, status/outcome/rule). Process CPU and memory come
from prometheus_client's default process collector. Bind address: ``metrics.host`` or the
``MIBEL_METRICS_HOST`` environment variable (the VPN address is never written in the repository).

Label values must be plain strings: pass ``enum.value``, never the enum member itself.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --------------------------------------------------------------------------- constants
PREFIX = "mibel_ingestor"
JOB_DURATION_BUCKETS = (0.25, 1, 2.5, 5, 10, 30, 60, 120)

JOB_RUNS = Counter(f"{PREFIX}_job_runs_total", "Job executions", ["source", "dataset", "status"])
JOB_SECONDS = Histogram(
    f"{PREFIX}_job_duration_seconds",
    "Job wall time",
    ["source", "dataset"],
    buckets=JOB_DURATION_BUCKETS,
)
# outcome: parsed | unchanged | sent | rejected
OBSERVATIONS = Counter(f"{PREFIX}_observations_total", "Observations per outcome", ["source", "dataset", "outcome"])
GATE_VIOLATIONS = Counter(f"{PREFIX}_gate_violations_total", "Edge gate violations", ["rule", "mode"])
LAST_SUCCESS = Gauge(f"{PREFIX}_last_success_timestamp_seconds", "Last successful job", ["source", "dataset"])
BUFFER_PENDING = Gauge(f"{PREFIX}_buffer_pending_messages", "Messages waiting for delivery")
BUFFER_DROPPED = Counter(f"{PREFIX}_buffer_dropped_total", "Undelivered messages dropped by TTL or size")
DELIVERIES = Counter(f"{PREFIX}_deliveries_total", "Delivery attempts per message", ["status"])


# --------------------------------------------------------------------------- public API
def start_metrics_server(host: str, port: int) -> None:
    """Expose ``/metrics`` on ``host:port`` in a background thread."""
    start_http_server(port, addr=host)
