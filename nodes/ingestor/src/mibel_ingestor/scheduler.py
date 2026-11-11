"""In-process scheduler and deliverer (ADR-05).

- Each (source, dataset) job has its own cadence ``every`` + ``offset``, aligned to local wall-clock
  time (e.g. every 1h at minute 7). Jobs are independent: a slow or failing source never delays
  another one, and a job never overlaps with itself.
- Failures: HTTP retries are bounded per run (``http.retries``); after a failed run the job is retried
  with exponential backoff (``retry_on_failure.first`` … ``max``), never later than its next regular
  slot, and it never stops retrying.
- A small thread pool (``service.workers``) runs the jobs; one extra thread drains the outbox.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from . import metrics
from .model import utc_now
from .pipeline import JobReport, Pipeline
from .settings import Settings
from .sinks import Sink
from .store import Store

log = logging.getLogger("mibel_ingestor.scheduler")


def next_slot(after: datetime, every: float, offset: float, tz: ZoneInfo) -> datetime:
    """First slot strictly after ``after``: local midnight + offset + k·every (wall-clock, DST-aware)."""
    local = after.astimezone(tz)
    day = local.date()
    for _ in range(3):                                    # today, tomorrow (and a safety margin)
        midnight = datetime.combine(day, time(0))         # naive wall clock
        k = 0
        while True:
            wall = midnight + timedelta(seconds=offset + k * every)
            if wall.date() != day:
                break
            slot = wall.replace(tzinfo=tz)
            if slot > local:
                return slot.astimezone(after.tzinfo)
            k += 1
        day += timedelta(days=1)
    raise ValueError("no slot found (check schedule every/offset)")


@dataclass
class Job:
    source: str
    dataset: str
    every: float
    offset: float
    next_run: datetime
    failures: int = 0
    future: Future[JobReport] | None = field(default=None, repr=False)

    @property
    def running(self) -> bool:
        return self.future is not None and not self.future.done()


class Scheduler:
    def __init__(self, settings: Settings, pipeline: Pipeline, store: Store, sink: Sink,
                 clock: Callable[[], datetime] = utc_now) -> None:
        self.s = settings
        self.pipeline = pipeline
        self.store = store
        self.sink = sink
        self.clock = clock
        self.stop = threading.Event()
        now = clock()
        self.jobs = [Job(src, name, ds.schedule_every, ds.schedule_offset,
                         next_slot(now - timedelta(seconds=ds.schedule_every), ds.schedule_every,
                                   ds.schedule_offset, settings.timezone))
                     for src, conn in pipeline.connectors.items() for name, ds in conn.datasets.items()]
        for job in self.jobs:                 # run once at start-up, then follow the schedule
            job.next_run = now

    # -- jobs -------------------------------------------------------------------------------
    def _finished(self, job: Job, report: JobReport) -> None:
        now = self.clock()
        regular = next_slot(now, job.every, job.offset, self.s.timezone)
        if report.succeeded:
            job.failures = 0
            job.next_run = regular
        else:
            job.failures += 1
            delay = min(self.s.retry_max, self.s.retry_first * 2 ** (job.failures - 1))
            job.next_run = min(regular, now + timedelta(seconds=delay))

    def _submit(self, pool: ThreadPoolExecutor, job: Job) -> None:
        def task() -> JobReport:
            report = self.pipeline.run(job.source, job.dataset)
            self._finished(job, report)
            return report
        job.future = pool.submit(task)

    # -- delivery ---------------------------------------------------------------------------
    def deliver_once(self) -> int:
        due = self.store.due(self.s.buffer.batch)
        if not due:
            return 0
        try:
            results = self.sink.send([(p.topic, p.key, p.payload) for p in due])
        except Exception as exc:
            log.warning("delivery failed", extra={"error": f"{type(exc).__name__}: {exc}"[:200]})
            results = [False] * len(due)
        ok = [p.id for p, r in zip(due, results, strict=True) if r]
        bad = [p for p, r in zip(due, results, strict=True) if not r]
        self.store.delivered(ok)
        self.store.failed(bad, base=self.s.buffer.flush_every, cap=300.0)
        metrics.DELIVERIES.labels("ok").inc(len(ok))
        metrics.DELIVERIES.labels("failed").inc(len(bad))
        return len(ok)

    def _deliverer(self) -> None:
        while not self.stop.is_set():
            sent = self.deliver_once()
            if sent < self.s.buffer.batch:
                self.stop.wait(self.s.buffer.flush_every)

    def housekeeping(self) -> None:
        b = self.s.buffer
        dropped = self.store.purge(pending_ttl=b.pending_ttl, seen_ttl=b.seen_ttl,
                                   quarantine_ttl=b.quarantine_ttl, max_bytes=b.max_bytes)
        if dropped:
            metrics.BUFFER_DROPPED.inc(dropped)
            log.warning("undelivered messages dropped by buffer limits", extra={"dropped": dropped})
        metrics.BUFFER_PENDING.set(self.store.pending())

    # -- main loop --------------------------------------------------------------------------
    def run_forever(self) -> None:
        deliverer = threading.Thread(target=self._deliverer, name="deliverer", daemon=True)
        deliverer.start()
        next_housekeeping = self.clock()
        with ThreadPoolExecutor(max_workers=self.s.workers, thread_name_prefix="job") as pool:
            while not self.stop.is_set():
                now = self.clock()
                for job in self.jobs:
                    if not job.running and job.next_run <= now:
                        self._submit(pool, job)
                if now >= next_housekeeping:
                    self.housekeeping()
                    next_housekeeping = now + timedelta(seconds=60)
                waiting = [j.next_run for j in self.jobs if not j.running] + [next_housekeeping]
                sleep = (min(waiting) - self.clock()).total_seconds()
                self.stop.wait(min(max(sleep, 0.2), 30.0))
            log.info("stopping: waiting for running jobs")
        deliverer.join(timeout=self.s.buffer.flush_every + 5)
        self.deliver_once()                                    # last attempt before exit
