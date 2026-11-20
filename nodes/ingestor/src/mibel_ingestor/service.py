"""Composition root: builds the adapters from the settings and wires them into the core (ADR-05).

::

    Settings ──▶ Store (SQLite) · Sink · Gates · Connectors (+ HttpClient each)
                       └──────────────▶ Pipeline ──▶ Scheduler

Also offers the three ways of running the node: the service loop, one pass of every job, and a
resumable backfill.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

from .connectors import Connector, available_sources, build_connector
from .gates import Gates
from .http import HttpClient
from .pipeline import JobReport, Pipeline
from .scheduler import Scheduler
from .settings import ConfigError, Settings, SourceSettings
from .sinks import Sink, build_sink
from .store import Store


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


log = logging.getLogger("mibel_ingestor.service")


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


@dataclass
class SourceStatus:
    """Whether a source runs on this node, and why."""

    name: str
    enabled: bool
    reason: str
    datasets: list[str]


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def describe_sources(settings: Settings) -> list[SourceStatus]:
    """List every known or configured source with its status (``mibel-ingestor sources``)."""
    statuses = []
    for name in sorted(set(available_sources()) | set(settings.sources)):
        source_settings = settings.sources.get(name)
        if source_settings is None:
            statuses.append(SourceStatus(name, False, "not configured", []))
            continue
        statuses.append(SourceStatus(
            name=name,
            enabled=source_settings.enabled,
            reason=_status_reason(source_settings),
            datasets=list(source_settings.datasets),
        ))
    return statuses


class Service:
    """The assembled node: store, sink, gates, connectors, pipeline and scheduler."""

    def __init__(
        self,
        settings: Settings,
        *,
        sink: Sink | None = None,
        store: Store | None = None,
        only: set[str] | None = None,
    ) -> None:
        """Build every adapter.

        Args:
            sink: replaces the configured sink (tests and benchmarks).
            store: replaces the SQLite file (tests use ``Store(":memory:")``).
            only: run just these sources, even if disabled in the configuration.

        Raises:
            ConfigError: no source would run.
        """
        self.settings = settings
        self.store = store or Store(settings.buffer.path)
        self.sink = sink or build_sink(settings.sink, settings.node_id)
        self.gates = Gates(settings.quality, settings.quality_profile)
        self.connectors = self._build_connectors(only)
        if not self.connectors:
            raise ConfigError("no source enabled")
        self.pipeline = Pipeline(settings, self.store, self.gates, self.connectors)
        self.scheduler = Scheduler(settings, self.pipeline, self.store, self.sink)

    def run_forever(self) -> None:
        """Run the service loop until a stop is requested."""
        log.info("ingestor started", extra={
            "mode": self.settings.mode.value,
            "node": self.settings.node_id,
            "sources": sorted(self.connectors),
            "sink": self.settings.sink.type.value,
        })
        self.scheduler.run_forever()

    def run_once(self, source: str | None = None, dataset: str | None = None) -> list[JobReport]:
        """Run every job once (optionally one source or dataset), then deliver everything produced."""
        reports = []
        for source_name, connector in self.connectors.items():
            if source and source_name != source:
                continue
            for dataset_name in connector.datasets:
                if dataset and dataset_name != dataset:
                    continue
                reports.append(self.pipeline.run(source_name, dataset_name))
        self._deliver_all()
        return reports

    def backfill(self, source: str, dataset: str, start: date, end: date) -> list[JobReport]:
        """Download history day by day in chunks of ``backfill_chunk_days``.

        Resumable: completed days are recorded and skipped on the next call.

        Raises:
            ConfigError: the source or dataset is unknown or disabled.
        """
        connector = self.connectors.get(source)
        if connector is None or dataset not in connector.datasets:
            raise ConfigError(f"unknown or disabled source/dataset: {source}/{dataset}")
        chunk_days = int(connector.datasets[dataset].options.get("backfill_chunk_days", 1))
        reports = []
        day = start
        while day <= end:
            day_count = min(chunk_days, (end - day).days + 1)
            chunk = [day + timedelta(days=i) for i in range(day_count)]
            day += timedelta(days=day_count)
            if self._is_chunk_done(source, dataset, chunk):
                continue
            report = self.pipeline.run(source, dataset, self.pipeline.day_window(chunk[0], day_count))
            reports.append(report)
            if report.succeeded:
                self._mark_chunk_done(source, dataset, chunk)
            self._deliver_all()
        return reports

    def close(self) -> None:
        """Flush the sink and release connections and the database."""
        self.sink.close()
        for connector in self.connectors.values():
            connector.http.close()
        self.store.close()

    # --- private ---
    def _build_connectors(self, only: set[str] | None) -> dict[str, Connector]:
        """Build the connectors that should run: ``only`` if given, else every enabled source."""
        connectors = {}
        for name, source_settings in self.settings.sources.items():
            if not self._should_run(name, source_settings, only):
                continue
            connectors[name] = build_connector(source_settings, self._new_http_client, self.settings.timezone)
        return connectors

    @staticmethod
    def _should_run(name: str, source_settings: SourceSettings, only: set[str] | None) -> bool:
        """Whether a source is selected and has at least one enabled dataset."""
        if not source_settings.datasets:
            return False
        if only:
            return name in only
        return source_settings.enabled

    def _new_http_client(self, min_interval: float) -> HttpClient:
        """Create the HTTP client of one source."""
        return HttpClient(self.settings.http, min_interval=min_interval)

    def _is_chunk_done(self, source: str, dataset: str, chunk: list[date]) -> bool:
        """Whether every day of a backfill chunk was already completed."""
        return all(self.store.is_backfill_done(source, dataset, day.isoformat()) for day in chunk)

    def _mark_chunk_done(self, source: str, dataset: str, chunk: list[date]) -> None:
        """Record every day of a backfill chunk as completed."""
        for day in chunk:
            self.store.mark_backfill_done(source, dataset, day.isoformat())

    def _deliver_all(self) -> None:
        """Deliver until the outbox has nothing due."""
        while self.scheduler.deliver_once():
            pass


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _status_reason(source_settings: SourceSettings) -> str:
    """Explain why a configured source is enabled or not."""
    if source_settings.enabled:
        return "enabled"
    if source_settings.options.get("enabled") == "auto":
        return "missing secret(s): " + ", ".join(source_settings.options.get("secrets", []))
    return "disabled in configuration"
