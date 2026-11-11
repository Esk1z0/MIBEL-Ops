"""Composition root: builds the adapters from the settings and wires them into the core."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

from . import connectors, sinks
from .connectors import Connector
from .gates import Gates
from .http import HttpClient
from .pipeline import JobReport, Pipeline
from .scheduler import Scheduler
from .settings import ConfigError, Settings
from .store import Store

log = logging.getLogger("mibel_ingestor.service")


@dataclass
class SourceStatus:
    name: str
    enabled: bool
    reason: str
    datasets: list[str]


def source_status(settings: Settings) -> list[SourceStatus]:
    out = []
    for name in sorted(set(connectors.available()) | set(settings.sources)):
        cfg = settings.sources.get(name)
        if cfg is None:
            out.append(SourceStatus(name, False, "not configured", []))
            continue
        reason = "enabled" if cfg.enabled else (
            "missing secret(s): " + ", ".join(cfg.raw.get("secrets", [])) if cfg.raw.get("enabled") == "auto"
            else "disabled in configuration")
        out.append(SourceStatus(name, cfg.enabled, reason, list(cfg.datasets)))
    return out


class Service:
    def __init__(self, settings: Settings, *, sink: sinks.Sink | None = None, store: Store | None = None,
                 only: set[str] | None = None) -> None:
        self.s = settings
        self.store = store or Store(settings.buffer.path)
        self.sink = sink or sinks.build(settings.sink, settings.node_id)
        self.gates = Gates(settings.quality, settings.quality_profile)
        self.connectors: dict[str, Connector] = {}
        for name, cfg in settings.sources.items():
            if (only and name not in only) or (not only and not cfg.enabled) or not cfg.datasets:
                continue
            self.connectors[name] = connectors.build(
                cfg, lambda interval: HttpClient(settings.http, min_interval=interval), settings.timezone)
        if not self.connectors:
            raise ConfigError("no source enabled")
        self.pipeline = Pipeline(settings, self.store, self.gates, self.connectors)
        self.scheduler = Scheduler(settings, self.pipeline, self.store, self.sink)

    def run_forever(self) -> None:
        log.info("ingestor started", extra={"mode": self.s.mode, "node": self.s.node_id,
                                            "sources": sorted(self.connectors), "sink": self.s.sink.type})
        self.scheduler.run_forever()

    def run_once(self, source: str | None = None, dataset: str | None = None) -> list[JobReport]:
        reports = []
        for name, conn in self.connectors.items():
            if source and name != source:
                continue
            for ds in conn.datasets:
                if dataset and ds != dataset:
                    continue
                reports.append(self.pipeline.run(name, ds))
        while self.scheduler.deliver_once():
            pass
        return reports

    def backfill(self, source: str, dataset: str, start: date, end: date) -> list[JobReport]:
        """Resumable: completed days are recorded and skipped on the next call."""
        conn = self.connectors.get(source)
        if conn is None or dataset not in conn.datasets:
            raise ConfigError(f"unknown or disabled source/dataset: {source}/{dataset}")
        chunk = int(conn.datasets[dataset].raw.get("backfill_chunk_days", 1))
        reports, day = [], start
        while day <= end:
            days = min(chunk, (end - day).days + 1)
            todo = [day + timedelta(days=i) for i in range(days)]
            if all(self.store.backfill_is_done(source, dataset, d.isoformat()) for d in todo):
                day += timedelta(days=days)
                continue
            report = self.pipeline.run(source, dataset, self.pipeline.day_window(day, days))
            reports.append(report)
            if report.succeeded:
                for d in todo:
                    self.store.backfill_mark(source, dataset, d.isoformat())
            while self.scheduler.deliver_once():
                pass
            day += timedelta(days=days)
        return reports

    def close(self) -> None:
        self.sink.close()
        for conn in self.connectors.values():
            conn.http.close()
        self.store.close()
