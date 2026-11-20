"""The single processing path of a job, identical for live capture and backfill.

Only the window and the ``mode`` tag change between both (ADR-05 §5.2)::

    requests → download → parse → keep window → change detection (live only) → available_at
             → edge gates → batches + lineage → outbox          (rejected → quarantine + DLQ)

``available_at`` (when a value became known) is taken from the source when it states it, set to the
fetch time in live mode, or estimated conservatively from the dataset's ``publication`` in backfill.

Stage timings (wall and CPU) are measured with cheap counters, so the benchmark can compare platforms
without a profiler.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from datetime import time as wall_time
from enum import Enum
from typing import Any

from . import __version__, metrics
from .connectors import Connector, Dataset
from .gates import Context, GateResult, Gates
from .http import FetchError
from .model import (
    UTC,
    CaptureMode,
    ContractError,
    NotAvailable,
    Observation,
    OutgoingMessage,
    ParseResult,
    Rejection,
    Request,
    Window,
    build_batch,
    build_dlq,
    to_iso,
    utc_now,
)
from .settings import Settings, parse_duration
from .store import Store


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


log = logging.getLogger("mibel_ingestor.pipeline")
# Rows per Kafka message: keeps messages well below Kafka's 1 MB default limit
CHUNK_ROWS = 2000
MAX_ERROR_CHARS = 300
CAPACITY_ROLE = "capacity"
# Live windows start on a boundary between 1 min and 1 h, so consecutive polls ask for the same range
MIN_FLOOR_SECONDS = 60.0
MAX_FLOOR_SECONDS = 3600.0
STRUCTURE_RULE = "DG-E01"


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class JobStatus(str, Enum):
    """Outcome of one job run."""

    OK = "ok"
    NOT_AVAILABLE = "not_available"
    CONTRACT_ERROR = "contract_error"
    FETCH_ERROR = "fetch_error"
    ERROR = "error"


class Stage(str, Enum):
    """Measured stages of a job."""

    DOWNLOAD = "download"
    PARSE = "parse"
    DETECT = "detect"
    GATES = "gates"
    SERIALIZE = "serialize"


class AvailableAtSource(str, Enum):
    """Where the ``available_at`` of a batch comes from (recorded in the lineage)."""

    SOURCE = "source"
    FIRST_SEEN = "first_seen"
    ESTIMATED = "estimated"


@dataclass
class StageTiming:
    """Accumulated wall and CPU seconds of one stage."""

    wall_s: float = 0.0
    cpu_s: float = 0.0


@dataclass
class JobReport:
    """What happened in one job run: status, counts per outcome and stage timings."""

    source: str
    dataset: str
    mode: str
    status: JobStatus = JobStatus.OK
    error: str = ""
    requests: int = 0
    not_available: int = 0
    parsed: int = 0
    unchanged: int = 0
    sent: int = 0
    rejected: int = 0
    messages: int = 0
    bytes: int = 0
    stages: dict[str, StageTiming] = field(default_factory=lambda: defaultdict(StageTiming))

    @property
    def succeeded(self) -> bool:
        """Whether the run counts as a success (nothing published yet is not a failure)."""
        return self.status in (JobStatus.OK, JobStatus.NOT_AVAILABLE)

    def fail(self, status: JobStatus, error: str) -> None:
        """Mark the run as failed with a short error message."""
        self.status = status
        self.error = error[:MAX_ERROR_CHARS]

    def as_dict(self) -> dict[str, Any]:
        """Return the report as plain JSON-ready values (used by ``once`` and the benchmark)."""
        report = {name: value for name, value in vars(self).items() if name != "stages"}
        report["status"] = self.status.value
        report["stages"] = {
            stage: {"wall_s": round(timing.wall_s, 6), "cpu_s": round(timing.cpu_s, 6)}
            for stage, timing in self.stages.items()
        }
        return report


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


@contextmanager
def measure_stage(report: JobReport, stage: Stage) -> Iterator[None]:
    """Add the wall and CPU time of the enclosed block to ``report.stages[stage]``."""
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    try:
        yield
    finally:
        timing = report.stages[stage.value]
        timing.wall_s += time.perf_counter() - wall_start
        timing.cpu_s += time.process_time() - cpu_start


def floor_to_step(moment: datetime, seconds: float) -> datetime:
    """Round a moment down to a multiple of ``seconds`` (clamped between 1 min and 1 h), in UTC."""
    step = max(MIN_FLOOR_SECONDS, min(seconds, MAX_FLOOR_SECONDS))
    epoch_seconds = moment.timestamp()
    return datetime.fromtimestamp(epoch_seconds - epoch_seconds % step, UTC)


class Pipeline:
    """Runs jobs: one (source, dataset) over one window, from download to outbox."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        gates: Gates,
        connectors: dict[str, Connector],
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings
        self.store = store
        self.gates = gates
        self.connectors = connectors
        self.clock = clock

    # --- windows ---
    def live_window(self, dataset: Dataset, now: datetime | None = None) -> Window:
        """Return the live window ``[now - back, now + ahead)``, its start floored to the resolution."""
        now = now or self.clock()
        start = floor_to_step(now - timedelta(seconds=dataset.window_back), dataset.resolution)
        end = now + timedelta(seconds=dataset.window_ahead)
        return Window(start, end, CaptureMode.LIVE)

    def day_window(self, first_day: date, day_count: int) -> Window:
        """Return a backfill window covering whole local days, starting at local midnight."""
        local_timezone = self.settings.timezone
        start = datetime.combine(first_day, wall_time(0), local_timezone).astimezone(UTC)
        end = datetime.combine(first_day + timedelta(days=day_count), wall_time(0), local_timezone).astimezone(UTC)
        return Window(start, end, CaptureMode.BACKFILL)

    # --- available_at ---
    def estimate_available_at(self, dataset: Dataset, observation: Observation) -> datetime:
        """Estimate when a backfilled observation was first published.

        The estimate is conservative (never earlier than the real publication), so point-in-time
        features never see a value before it existed. ``publication`` is either ``{at: "HH:MM",
        day_offset: N}`` (published at a local time, N days after the period's day) or ``{lag: "1h"}``;
        without it, one resolution step after the period start is assumed.
        """
        publication = dataset.publication
        if "at" in publication:
            hour, minute = (int(part) for part in str(publication["at"]).split(":"))
            day_offset = timedelta(days=int(publication.get("day_offset", 0)))
            local_day = observation.event_time.astimezone(self.settings.timezone).date() + day_offset
            published = datetime.combine(local_day, wall_time(hour, minute), self.settings.timezone)
            return published.astimezone(UTC)
        lag = parse_duration(publication["lag"]) if "lag" in publication else dataset.resolution
        return observation.event_time + timedelta(seconds=lag)

    # --- the job ---
    def run(self, source: str, dataset_name: str, window: Window | None = None) -> JobReport:
        """Run one job and return its report; errors are caught so one source never stops the others."""
        connector = self.connectors[source]
        dataset = connector.datasets[dataset_name]
        window = window or self.live_window(dataset)
        report = JobReport(source, dataset_name, window.mode.value)
        started = time.perf_counter()
        try:
            self._execute(connector, dataset, window, report)
        except ContractError as exc:
            report.fail(JobStatus.CONTRACT_ERROR, str(exc))
            self._enqueue_dlq(source, dataset_name, STRUCTURE_RULE, report.error, samples=[], count=0)
        except NotImplementedError as exc:
            report.fail(JobStatus.ERROR, str(exc))
        except FetchError as exc:
            report.fail(JobStatus.FETCH_ERROR, f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # any other bug or surprise in one source must not stop the service
            report.fail(JobStatus.ERROR, f"{type(exc).__name__}: {exc}")
        elapsed = time.perf_counter() - started
        _record_job_metrics(report, elapsed)
        _log_report(report, elapsed)
        return report

    # --- job steps ---
    def _execute(self, connector: Connector, dataset: Dataset, window: Window, report: JobReport) -> None:
        """Run every step of a job, filling ``report`` as it goes."""
        source = connector.name
        fetched_at = self.clock()
        requests = connector.requests(dataset, window)
        report.requests = len(requests)
        parsed = self._download_and_parse(connector, dataset, requests, report)
        if requests and report.not_available == len(requests):
            report.status = JobStatus.NOT_AVAILABLE
            return

        with measure_stage(report, Stage.DETECT):
            fresh = self._select_fresh(source, window, parsed, report)
            available_at_source = self._assign_available_at(dataset, window, fresh, fetched_at)

        with measure_stage(report, Stage.GATES):
            gate_result = self._apply_gates(dataset, fresh, fetched_at)
        report.sent = len(gate_result.accepted)
        report.rejected = len(gate_result.rejected)

        lineage = self._build_lineage(
            connector=connector,
            window=window,
            requests=requests,
            parsed=parsed,
            available_at_source=available_at_source,
            gate_result=gate_result,
            report=report,
            fetched_at=fetched_at,
        )
        with measure_stage(report, Stage.SERIALIZE):
            report.messages = self._enqueue_batches(source, dataset, window, gate_result.accepted, lineage, fetched_at)
            self._enqueue_rejections(source, dataset.name, gate_result.rejected)
            if window.is_live:
                self.store.remember_sent(source, fresh)
            if dataset.options.get("role") == CAPACITY_ROLE:
                self._update_capacities(dataset, gate_result.accepted)
        _count_observations(report)

    def _download_and_parse(
        self,
        connector: Connector,
        dataset: Dataset,
        requests: list[Request],
        report: JobReport,
    ) -> ParseResult:
        """Download and parse every request; unpublished resources are counted, not treated as errors."""
        parsed = ParseResult()
        for request in requests:
            try:
                with measure_stage(report, Stage.DOWNLOAD):
                    payload = connector.download(request)
            except NotAvailable:
                report.not_available += 1
                continue
            report.bytes += len(payload)
            with measure_stage(report, Stage.PARSE):
                parsed.extend(connector.parse(dataset, request, payload))
        return parsed

    def _select_fresh(self, source: str, window: Window, parsed: ParseResult, report: JobReport) -> list[Observation]:
        """Keep the observations inside the window and, in live mode, only those that changed."""
        # plain comparisons (no method call per observation): this runs on every poll, measured by bench.py
        start = window.start
        end = window.end
        inside = [observation for observation in parsed.observations if start <= observation.event_time < end]
        parsed.count_step("outside_window", len(parsed.observations) - len(inside))
        report.parsed = len(inside)
        if not window.is_live:
            return inside
        fresh = self.store.filter_changed(source, inside)
        report.unchanged = len(inside) - len(fresh)
        return fresh

    def _assign_available_at(
        self,
        dataset: Dataset,
        window: Window,
        observations: list[Observation],
        fetched_at: datetime,
    ) -> AvailableAtSource:
        """Fill ``available_at`` where the source did not state it, and return its provenance."""
        provenance = AvailableAtSource.FIRST_SEEN if window.is_live else AvailableAtSource.ESTIMATED
        for observation in observations:
            if observation.available_at is not None:
                provenance = AvailableAtSource.SOURCE
            elif window.is_live:
                observation.available_at = fetched_at
            else:
                observation.available_at = self.estimate_available_at(dataset, observation)
        return provenance

    def _apply_gates(self, dataset: Dataset, observations: list[Observation], fetched_at: datetime) -> GateResult:
        """Evaluate the edge gates and count their violations in the metrics."""
        context = Context(
            now=fetched_at,
            timezone=self.settings.timezone,
            nature=dataset.nature,
            resolution=dataset.resolution,
            capacities=self.store.load_capacities(),
        )
        gate_result = self.gates.evaluate(observations, context)
        for rule, count in gate_result.warned.items():
            metrics.GATE_VIOLATIONS.labels(rule, "warn").inc(count)
        for rule, count in gate_result.blocked.items():
            metrics.GATE_VIOLATIONS.labels(rule, "block").inc(count)
        return gate_result

    def _build_lineage(
        self,
        connector: Connector,
        window: Window,
        requests: list[Request],
        parsed: ParseResult,
        available_at_source: AvailableAtSource,
        gate_result: GateResult,
        report: JobReport,
        fetched_at: datetime,
    ) -> dict[str, Any]:
        """Return the lineage shared by every batch of the job: what was asked and what was done."""
        return {
            "connector": f"{connector.name}@{connector.version}",
            "ingestor_version": __version__,
            "fetched_at": to_iso(fetched_at),
            "window": [to_iso(window.start), to_iso(window.end)],
            "requests": [request.meta for request in requests],
            "not_available": report.not_available,
            "steps": parsed.steps,
            "available_at": available_at_source.value,
            "gates": gate_result.summary(self.gates.profile, self.gates.version),
            "counts": {
                "parsed": report.parsed,
                "unchanged": report.unchanged,
                "sent": report.sent,
                "rejected": report.rejected,
            },
        }

    def _enqueue_batches(
        self,
        source: str,
        dataset: Dataset,
        window: Window,
        accepted: list[Observation],
        lineage: dict[str, Any],
        fetched_at: datetime,
    ) -> int:
        """Serialize the accepted observations in chunks of ``CHUNK_ROWS`` and store them in the outbox."""
        topic = f"{self.settings.sink.topic_prefix}.{source}"
        key = f"{source}.{dataset.name}"
        messages = []
        for chunk_index, first_row in enumerate(range(0, len(accepted), CHUNK_ROWS)):
            payload = build_batch(
                source=source,
                dataset=dataset.name,
                nature=dataset.nature,
                mode=window.mode,
                ingestor=self.settings.node_id,
                observations=accepted[first_row:first_row + CHUNK_ROWS],
                lineage={**lineage, "chunk": chunk_index},
                created_at=fetched_at,
            )
            messages.append(OutgoingMessage(topic, key, payload))
        self.store.enqueue(messages)
        return len(messages)

    def _enqueue_rejections(self, source: str, dataset_name: str, rejections: list[Rejection]) -> None:
        """Quarantine rejected observations and send one DLQ message per blocking rule."""
        if not rejections:
            return
        self.store.quarantine(source, dataset_name, rejections)
        observations_by_rule: dict[str, list[Observation]] = defaultdict(list)
        for rejection in rejections:
            observations_by_rule[rejection.rule].append(rejection.observation)
        for rule, observations in observations_by_rule.items():
            self._enqueue_dlq(source, dataset_name, rule, "edge gate failed", observations, len(observations))

    def _update_capacities(self, dataset: Dataset, accepted: list[Observation]) -> None:
        """Store the latest installed capacity per technology from a ``role: capacity`` dataset (DG-E11)."""
        prefix = str(dataset.options.get("variable_prefix", ""))
        latest: dict[str, Observation] = {}
        for observation in accepted:
            current = latest.get(observation.variable)
            if current is None or observation.event_time > current.event_time:
                latest[observation.variable] = observation
        megawatts_by_technology = {
            variable.removeprefix(prefix): observation.value for variable, observation in latest.items()
        }
        self.store.set_capacities(megawatts_by_technology)

    def _enqueue_dlq(
        self,
        source: str,
        dataset_name: str,
        rule: str,
        reason: str,
        samples: list[Observation],
        count: int,
    ) -> None:
        """Store a DLQ message explaining why observations (or a whole payload) were rejected."""
        payload = build_dlq(
            source=source,
            dataset=dataset_name,
            ingestor=self.settings.node_id,
            rule=rule,
            reason=reason,
            samples=samples,
            count=count,
            created_at=self.clock(),
        )
        self.store.enqueue([OutgoingMessage(self.settings.sink.dlq_topic, f"{source}.{dataset_name}", payload)])


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _count_observations(report: JobReport) -> None:
    """Add the job's observation counts per outcome to the metrics."""
    outcomes = {
        "parsed": report.parsed,
        "unchanged": report.unchanged,
        "sent": report.sent,
        "rejected": report.rejected,
    }
    for outcome, count in outcomes.items():
        metrics.OBSERVATIONS.labels(report.source, report.dataset, outcome).inc(count)


def _record_job_metrics(report: JobReport, elapsed: float) -> None:
    """Record the run, its duration and, on success, the time of the last success."""
    metrics.JOB_RUNS.labels(report.source, report.dataset, report.status.value).inc()
    metrics.JOB_SECONDS.labels(report.source, report.dataset).observe(elapsed)
    if report.succeeded:
        metrics.LAST_SUCCESS.labels(report.source, report.dataset).set(time.time())


def _log_report(report: JobReport, elapsed: float) -> None:
    """Log one line per job: INFO on success, WARNING on failure."""
    level = logging.INFO if report.succeeded else logging.WARNING
    log.log(level, "job finished", extra={
        "source": report.source,
        "dataset": report.dataset,
        "mode": report.mode,
        "status": report.status.value,
        "error": report.error,
        "parsed": report.parsed,
        "sent": report.sent,
        "rejected": report.rejected,
        "seconds": round(elapsed, 3),
    })
