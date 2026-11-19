"""In-process scheduler and deliverer (ADR-05 §5.2).

- Each (source, dataset) job has its own cadence ``every`` + ``offset``, aligned to local wall-clock time
  (e.g. every 1 h at minute 7). Jobs are independent: a slow or failing source never delays another
  one, and a job never overlaps with itself.
- Failures: HTTP retries are bounded per run (``http.retries``). After a failed run the job is retried
  with exponential backoff (``retry_on_failure.first`` … ``max``), never later than its next regular
  slot, and it never stops retrying.
- A small thread pool (``service.workers``) runs the jobs; one extra thread drains the outbox.

::

    main loop ──every ≤30 s──▶ due jobs ──▶ thread pool ──▶ Pipeline.run ──▶ outbox
        │                                                                     │
        └──every 60 s──▶ housekeeping (purge, metrics)      deliverer thread ◀┘──▶ sink
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from . import metrics
from .model import utc_now
from .pipeline import JobReport, Pipeline
from .settings import Settings
from .sinks import Sink
from .store import PendingMessage, Store

# --------------------------------------------------------------------------- constants
log = logging.getLogger("mibel_ingestor.scheduler")
HOUSEKEEPING_EVERY = timedelta(seconds=60)
# The main loop wakes at least every 30 s and at most every 0.2 s
MIN_SLEEP_S = 0.2
MAX_SLEEP_S = 30.0
# Upper bound of the delivery backoff when the sink keeps failing
MAX_DELIVERY_DELAY_S = 300.0
# Days searched for the next slot: today, tomorrow and a safety margin
SLOT_SEARCH_DAYS = 3
MAX_ERROR_CHARS = 200


# --------------------------------------------------------------------------- types
@dataclass
class Job:
    """One scheduled (source, dataset) with its cadence, next run and consecutive failures."""

    source: str
    dataset: str
    every: float
    offset: float
    next_run: datetime
    failures: int = 0
    future: Future[JobReport] | None = field(default=None, repr=False)

    @property
    def is_running(self) -> bool:
        """Whether the job is currently executing on the pool."""
        return self.future is not None and not self.future.done()


# --------------------------------------------------------------------------- public API
def find_next_slot(after: datetime, every: float, offset: float, local_timezone: ZoneInfo) -> datetime:
    """Return the first slot strictly after ``after``: local midnight + offset + k·every.

    Slots follow the local wall clock, so an hourly job keeps its minute across DST changes.

    Raises:
        ValueError: no slot exists in the next days (inconsistent ``every``/``offset``).
    """
    local_after = after.astimezone(local_timezone)
    day = local_after.date()
    for _ in range(SLOT_SEARCH_DAYS):
        slot = _first_slot_of_day_after(day, every, offset, local_timezone, local_after)
        if slot is not None:
            return slot.astimezone(after.tzinfo)
        day += timedelta(days=1)
    raise ValueError("no slot found (check schedule every/offset)")


class Scheduler:
    """Runs every job on its cadence and delivers the outbox to the sink."""

    def __init__(
        self,
        settings: Settings,
        pipeline: Pipeline,
        store: Store,
        sink: Sink,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.settings = settings
        self.pipeline = pipeline
        self.store = store
        self.sink = sink
        self.clock = clock
        self.stop_requested = threading.Event()
        # every job runs once at start-up, then follows its schedule
        self.jobs = self._build_jobs(first_run=clock())

    def request_stop(self) -> None:
        """Ask the main loop to finish (called from SIGTERM/SIGINT handlers)."""
        self.stop_requested.set()

    # --- main loop ---
    def run_forever(self) -> None:
        """Run jobs and housekeeping until a stop is requested, then deliver what is left."""
        deliverer = threading.Thread(target=self._deliver_forever, name="deliverer", daemon=True)
        deliverer.start()
        next_housekeeping = self.clock()
        with ThreadPoolExecutor(max_workers=self.settings.workers, thread_name_prefix="job") as pool:
            while not self.stop_requested.is_set():
                now = self.clock()
                self._submit_due_jobs(pool, now)
                if now >= next_housekeeping:
                    self.run_housekeeping()
                    next_housekeeping = now + HOUSEKEEPING_EVERY
                self.stop_requested.wait(self._seconds_until_next_event(next_housekeeping))
            log.info("stopping: waiting for running jobs")
        deliverer.join(timeout=self.settings.buffer.flush_every + 5)
        self.deliver_once()  # last attempt before exit

    # --- delivery ---
    def deliver_once(self) -> int:
        """Send one batch of due messages and return how many were delivered."""
        due = self.store.fetch_due(self.settings.buffer.batch)
        if not due:
            return 0
        results = self._send(due)
        delivered_ids = []
        failed = []
        for pending, was_delivered in zip(due, results, strict=True):
            if was_delivered:
                delivered_ids.append(pending.id)
            else:
                failed.append(pending)
        self.store.mark_delivered(delivered_ids)
        self.store.mark_failed(failed, first_delay=self.settings.buffer.flush_every, max_delay=MAX_DELIVERY_DELAY_S)
        metrics.DELIVERIES.labels("ok").inc(len(delivered_ids))
        metrics.DELIVERIES.labels("failed").inc(len(failed))
        return len(delivered_ids)

    # --- housekeeping ---
    def run_housekeeping(self) -> None:
        """Apply the buffer life limits and refresh the buffer metrics."""
        buffer = self.settings.buffer
        dropped = self.store.purge(
            pending_ttl=buffer.pending_ttl,
            seen_ttl=buffer.seen_ttl,
            quarantine_ttl=buffer.quarantine_ttl,
            max_bytes=buffer.max_bytes,
        )
        if dropped:
            metrics.BUFFER_DROPPED.inc(dropped)
            log.warning("undelivered messages dropped by buffer limits", extra={"dropped": dropped})
        metrics.BUFFER_PENDING.set(self.store.count_pending())

    # --- private ---
    def _build_jobs(self, first_run: datetime) -> list[Job]:
        """Create one job per dataset of every connector, all due at ``first_run``."""
        jobs = []
        for source, connector in self.pipeline.connectors.items():
            for dataset_name, dataset in connector.datasets.items():
                jobs.append(Job(
                    source=source,
                    dataset=dataset_name,
                    every=dataset.schedule_every,
                    offset=dataset.schedule_offset,
                    next_run=first_run,
                ))
        return jobs

    def _submit_due_jobs(self, pool: ThreadPoolExecutor, now: datetime) -> None:
        """Submit every job whose time has come and that is not already running."""
        for job in self.jobs:
            if not job.is_running and job.next_run <= now:
                job.future = pool.submit(self._run_job, job)

    def _run_job(self, job: Job) -> JobReport:
        """Run a job on the pool and schedule its next run."""
        report = self.pipeline.run(job.source, job.dataset)
        self._schedule_next_run(job, report)
        return report

    def _schedule_next_run(self, job: Job, report: JobReport) -> None:
        """Set the next run: the regular slot after a success, a backoff retry after a failure.

        The retry delay doubles with each consecutive failure (``retry_first`` … ``retry_max``) but is
        never later than the regular slot, so a failing job keeps trying forever.
        """
        now = self.clock()
        regular_slot = find_next_slot(now, job.every, job.offset, self.settings.timezone)
        if report.succeeded:
            job.failures = 0
            job.next_run = regular_slot
            return
        job.failures += 1
        delay = min(self.settings.retry_max, self.settings.retry_first * 2 ** (job.failures - 1))
        job.next_run = min(regular_slot, now + timedelta(seconds=delay))

    def _seconds_until_next_event(self, next_housekeeping: datetime) -> float:
        """Return how long the main loop may sleep before a job or housekeeping is due."""
        upcoming = [job.next_run for job in self.jobs if not job.is_running]
        upcoming.append(next_housekeeping)
        seconds = (min(upcoming) - self.clock()).total_seconds()
        return min(max(seconds, MIN_SLEEP_S), MAX_SLEEP_S)

    def _send(self, due: list[PendingMessage]) -> list[bool]:
        """Send messages to the sink; an exception marks the whole batch as not delivered."""
        try:
            return self.sink.send([pending.message for pending in due])
        except Exception as exc:  # a sink failure must never stop the deliverer thread
            log.warning("delivery failed", extra={"error": f"{type(exc).__name__}: {exc}"[:MAX_ERROR_CHARS]})
            return [False] * len(due)

    def _deliver_forever(self) -> None:
        """Deliver continuously; wait ``flush_every`` whenever the outbox is (almost) empty."""
        while not self.stop_requested.is_set():
            delivered = self.deliver_once()
            if delivered < self.settings.buffer.batch:
                self.stop_requested.wait(self.settings.buffer.flush_every)


# --------------------------------------------------------------------------- private helpers
def _first_slot_of_day_after(
    day: date,
    every: float,
    offset: float,
    local_timezone: ZoneInfo,
    local_after: datetime,
) -> datetime | None:
    """Return the first slot of ``day`` (local) strictly after ``local_after``, or None if there is none.

    Slots are built on the naive wall clock (midnight + offset + k·every) and only then given the
    timezone, which keeps them on the same wall-clock minute across DST changes.
    """
    midnight = datetime.combine(day, time(0))
    step = 0
    while True:
        wall_clock = midnight + timedelta(seconds=offset + step * every)
        if wall_clock.date() != midnight.date():
            return None
        slot = wall_clock.replace(tzinfo=local_timezone)
        if slot > local_after:
            return slot
        step += 1
