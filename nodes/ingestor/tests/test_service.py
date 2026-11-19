"""Service wiring without network: retries, delivery with a broken sink, resumable backfill."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from mibel_ingestor.model import OutgoingMessage, Request
from mibel_ingestor.pipeline import JobReport, JobStatus
from mibel_ingestor.service import Service, describe_sources
from mibel_ingestor.settings import Settings
from mibel_ingestor.sinks import NullSink, Sink
from mibel_ingestor.store import Store

import synthetic
from conftest import MADRID


class BrokenSink:
    """A sink whose broker is always down."""

    def __init__(self) -> None:
        self.calls = 0

    def send(self, messages: list[OutgoingMessage]) -> list[bool]:
        """Fail every delivery."""
        self.calls += 1
        raise ConnectionError("broker down")

    def close(self) -> None:
        """Nothing to release."""


def serve_synthetic_omie(request: Request) -> bytes:
    """Return a synthetic OMIE file for the requested day instead of downloading it."""
    day = date.fromisoformat(str(request.meta["day"]))
    if request.params["parents"] == "precios_pibcic":
        return synthetic.omie_continuous(day)
    return synthetic.omie_marginal(day)


def omie_service(settings: Settings, sink: Sink | None = None) -> Service:
    """A service running only OMIE, with synthetic downloads and an in-memory store."""
    service = Service(settings, sink=sink or NullSink(), store=Store(":memory:"), only={"omie"})
    service.connectors["omie"].download = serve_synthetic_omie  # type: ignore[method-assign]
    return service


def test_failed_job_retries_with_backoff_before_next_slot(settings: Settings) -> None:
    service = omie_service(settings)
    job = service.scheduler.jobs[0]
    failed = JobReport("omie", job.dataset, "live", status=JobStatus.FETCH_ERROR)
    before = datetime.now(MADRID)
    service.scheduler._schedule_next_run(job, failed)
    first_retry = job.next_run
    service.scheduler._schedule_next_run(job, failed)
    assert job.failures == 2
    assert first_retry - before <= timedelta(seconds=settings.retry_first + 5)
    assert job.next_run - before <= timedelta(seconds=2 * settings.retry_first + 5)
    service.scheduler._schedule_next_run(job, JobReport("omie", job.dataset, "live"))
    assert job.failures == 0


def test_messages_survive_a_broken_sink(settings: Settings) -> None:
    sink = BrokenSink()
    service = omie_service(settings, sink)
    reports = service.run_once("omie", "day_ahead")
    assert reports[0].status is JobStatus.OK
    assert sink.calls == 1
    assert service.store.count_pending() == reports[0].messages > 0  # kept for the next delivery


def test_backfill_is_resumable(settings: Settings) -> None:
    service = omie_service(settings)
    first_run = service.backfill("omie", "day_ahead", date(2026, 1, 1), date(2026, 1, 3))
    assert len(first_run) == 3
    assert all(report.sent == 192 and report.mode == "backfill" for report in first_run)
    second_run = service.backfill("omie", "day_ahead", date(2026, 1, 1), date(2026, 1, 4))
    assert len(second_run) == 1  # only the new day


def test_describe_sources_explains_disabled_sources(settings: Settings) -> None:
    statuses = {status.name: status for status in describe_sources(settings)}
    assert not statuses["entsoe"].enabled
    assert "ENTSOE_TOKEN" in statuses["entsoe"].reason
    assert statuses["omie"].enabled
