"""The single processing path, identical for live capture and backfill (only the window and the
``mode`` tag change):

    requests → download → parse → keep window → change detection (live) → available_at
             → edge gates → batches + lineage → outbox      (rejected → quarantine + DLQ)

Stage timings (wall and CPU) are measured with cheap counters so the benchmark can compare
platforms without a profiler.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from datetime import time as dtime
from typing import Any

from . import __version__, metrics
from .connectors import Connector, Dataset
from .gates import Context, Gates
from .http import FetchError
from .model import (
    UTC,
    ContractError,
    Mode,
    NotAvailable,
    Observation,
    ParseResult,
    Window,
    build_batch,
    build_dlq,
    iso,
    utc_now,
)
from .settings import Settings, duration
from .store import Store

log = logging.getLogger("mibel_ingestor.pipeline")
CHUNK_ROWS = 2000            # rows per Kafka message (keeps messages well below 1 MB)


@dataclass
class JobReport:
    source: str
    dataset: str
    mode: str
    status: str = "ok"       # ok | not_available | contract_error | fetch_error | error
    error: str = ""
    requests: int = 0
    not_available: int = 0
    parsed: int = 0
    unchanged: int = 0
    sent: int = 0
    rejected: int = 0
    messages: int = 0
    bytes: int = 0
    stages: dict[str, list[float]] = field(default_factory=lambda: defaultdict(lambda: [0.0, 0.0]))

    @property
    def succeeded(self) -> bool:
        return self.status in ("ok", "not_available")

    def as_dict(self) -> dict[str, Any]:
        out = {k: v for k, v in vars(self).items() if k != "stages"}
        out["stages"] = {k: {"wall_s": round(w, 6), "cpu_s": round(c, 6)} for k, (w, c) in self.stages.items()}
        return out


@contextmanager
def timed(report: JobReport, stage: str) -> Iterator[None]:
    w0, c0 = time.perf_counter(), time.process_time()
    try:
        yield
    finally:
        acc = report.stages[stage]
        acc[0] += time.perf_counter() - w0
        acc[1] += time.process_time() - c0


def floor_time(ts: datetime, seconds: float) -> datetime:
    step = max(60.0, min(seconds, 3600.0))
    epoch = ts.timestamp()
    return datetime.fromtimestamp(epoch - epoch % step, UTC)


class Pipeline:
    def __init__(self, settings: Settings, store: Store, gates: Gates, connectors: dict[str, Connector],
                 clock: Callable[[], datetime] = utc_now) -> None:
        self.s = settings
        self.store = store
        self.gates = gates
        self.connectors = connectors
        self.clock = clock

    # -- windows ----------------------------------------------------------------------------
    def live_window(self, ds: Dataset, now: datetime | None = None) -> Window:
        now = now or self.clock()
        return Window(floor_time(now - timedelta(seconds=ds.window_back), ds.resolution),
                      now + timedelta(seconds=ds.window_ahead), Mode.LIVE)

    def day_window(self, first: date, days: int) -> Window:
        tz = self.s.timezone
        start = datetime.combine(first, dtime(0), tz).astimezone(UTC)
        end = datetime.combine(first + timedelta(days=days), dtime(0), tz).astimezone(UTC)
        return Window(start, end, Mode.BACKFILL)

    # -- available_at estimate for backfill (conservative: never earlier than the real one) --
    def estimate_available(self, ds: Dataset, obs: Observation) -> datetime:
        pub = ds.publication
        if "at" in pub:
            hh, mm = (int(x) for x in str(pub["at"]).split(":"))
            offset = timedelta(days=int(pub.get("day_offset", 0)))
            local_day = obs.event_time.astimezone(self.s.timezone).date() + offset
            return datetime.combine(local_day, dtime(hh, mm), self.s.timezone).astimezone(UTC)
        lag = duration(pub["lag"]) if "lag" in pub else ds.resolution
        return obs.event_time + timedelta(seconds=lag)

    # -- the job ----------------------------------------------------------------------------
    def run(self, source: str, dataset: str, window: Window | None = None) -> JobReport:
        conn = self.connectors[source]
        ds = conn.datasets[dataset]
        window = window or self.live_window(ds)
        report = JobReport(source, dataset, window.mode.value)
        started = time.perf_counter()
        try:
            self._run(conn, ds, window, report)
        except ContractError as exc:
            report.status, report.error = "contract_error", str(exc)[:300]
            self._enqueue_dlq(source, dataset, "DG-E01", str(exc)[:300], [], 0)
        except NotImplementedError as exc:
            report.status, report.error = "error", str(exc)[:300]
        except Exception as exc:   # one failing source never stops the others
            report.status = "fetch_error" if isinstance(exc, FetchError) else "error"
            report.error = f"{type(exc).__name__}: {exc}"[:300]
        elapsed = time.perf_counter() - started
        metrics.JOB_RUNS.labels(source, dataset, report.status).inc()
        metrics.JOB_SECONDS.labels(source, dataset).observe(elapsed)
        if report.succeeded:
            metrics.LAST_SUCCESS.labels(source, dataset).set(time.time())
        level = logging.INFO if report.succeeded else logging.WARNING
        log.log(level, "job finished", extra={"source": source, "dataset": dataset, "mode": report.mode,
                                               "status": report.status, "error": report.error,
                                               "parsed": report.parsed, "sent": report.sent,
                                               "rejected": report.rejected, "seconds": round(elapsed, 3)})
        return report

    def _run(self, conn: Connector, ds: Dataset, window: Window, report: JobReport) -> None:
        source = conn.name
        fetched_at = self.clock()
        reqs = conn.requests(ds, window)
        report.requests = len(reqs)
        parsed = ParseResult()
        for req in reqs:
            try:
                with timed(report, "download"):
                    payload = conn.download(req)
            except NotAvailable:
                report.not_available += 1
                continue
            report.bytes += len(payload)
            with timed(report, "parse"):
                parsed.extend(conn.parse(ds, req, payload))
        if reqs and report.not_available == len(reqs):
            report.status = "not_available"
            return

        with timed(report, "detect"):
            inside = [o for o in parsed.observations if window.start <= o.event_time < window.end]
            parsed.step("outside_window", len(parsed.observations) - len(inside))
            report.parsed = len(inside)
            if window.mode is Mode.LIVE:
                fresh = self.store.changed(source, inside)
                report.unchanged = len(inside) - len(fresh)
                provenance = "first_seen"
            else:
                fresh = inside
                provenance = "estimated"
            for o in fresh:
                if o.available_at is not None:
                    provenance = "source"
                elif window.mode is Mode.LIVE:
                    o.available_at = fetched_at
                else:
                    o.available_at = self.estimate_available(ds, o)

        with timed(report, "gates"):
            ctx = Context(now=fetched_at, tz=self.s.timezone, nature=ds.nature, resolution=ds.resolution,
                          capacities=self.store.capacities())
            result = self.gates.evaluate(fresh, ctx)
        report.sent, report.rejected = len(result.accepted), len(result.rejected)
        for rule, n in result.warned.items():
            metrics.GATE_VIOLATIONS.labels(rule, "warn").inc(n)
        for rule, n in result.blocked.items():
            metrics.GATE_VIOLATIONS.labels(rule, "block").inc(n)

        lineage = {
            "connector": f"{source}@{conn.version}", "ingestor_version": __version__,
            "fetched_at": iso(fetched_at), "window": [iso(window.start), iso(window.end)],
            "requests": [r.meta for r in reqs], "not_available": report.not_available,
            "steps": parsed.steps, "available_at": provenance,
            "gates": result.summary(self.gates.profile, self.gates.version),
            "counts": {"parsed": report.parsed, "unchanged": report.unchanged,
                       "sent": report.sent, "rejected": report.rejected},
        }
        with timed(report, "serialize"):
            topic, key = f"{self.s.sink.topic_prefix}.{source}", f"{source}.{ds.name}"
            messages = []
            for i in range(0, len(result.accepted), CHUNK_ROWS):
                chunk = result.accepted[i:i + CHUNK_ROWS]
                payload = build_batch(source=source, dataset=ds.name, nature=ds.nature, mode=window.mode,
                                      ingestor=self.s.node_id, observations=chunk,
                                      lineage={**lineage, "chunk": i // CHUNK_ROWS}, created_at=fetched_at)
                messages.append((topic, key, payload))
            self.store.enqueue(messages)
            report.messages = len(messages)
            if result.rejected:
                self.store.quarantine(source, ds.name, result.rejected)
                by_rule: dict[str, list[Observation]] = defaultdict(list)
                for o, rule in result.rejected:
                    by_rule[rule].append(o)
                for rule, items in by_rule.items():
                    self._enqueue_dlq(source, ds.name, rule, "edge gate failed", items, len(items))
            if window.mode is Mode.LIVE:
                self.store.remember(source, fresh)
            if ds.raw.get("role") == "capacity":
                prefix = str(ds.raw.get("variable_prefix", ""))
                latest: dict[str, tuple[datetime, float]] = {}
                for o in result.accepted:
                    if o.variable not in latest or o.event_time > latest[o.variable][0]:
                        latest[o.variable] = (o.event_time, o.value)
                self.store.set_capacity({v.removeprefix(prefix): val for v, (_, val) in latest.items()})

        metrics.OBSERVATIONS.labels(source, ds.name, "parsed").inc(report.parsed)
        metrics.OBSERVATIONS.labels(source, ds.name, "unchanged").inc(report.unchanged)
        metrics.OBSERVATIONS.labels(source, ds.name, "sent").inc(report.sent)
        metrics.OBSERVATIONS.labels(source, ds.name, "rejected").inc(report.rejected)

    def _enqueue_dlq(self, source: str, dataset: str, rule: str, reason: str,
                     samples: list[Observation], count: int) -> None:
        payload = build_dlq(source=source, dataset=dataset, ingestor=self.s.node_id, rule=rule, reason=reason,
                            samples=samples, count=count, created_at=self.clock())
        self.store.enqueue([(self.s.sink.dlq_topic, f"{source}.{dataset}", payload)])
