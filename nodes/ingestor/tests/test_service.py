"""Service wiring without network: retries, delivery with a broken sink, resumable backfill."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from mibel_ingestor.model import Request
from mibel_ingestor.pipeline import JobReport
from mibel_ingestor.service import Service, source_status
from mibel_ingestor.settings import Settings
from mibel_ingestor.store import Store

import synthetic
from conftest import TZ


class BrokenSink:
    def __init__(self) -> None:
        self.calls = 0

    def send(self, messages: list[tuple[str, str, bytes]]) -> list[bool]:
        self.calls += 1
        raise ConnectionError("broker down")

    def close(self) -> None:
        pass


def omie_service(settings: Settings, sink: object | None = None) -> Service:
    from mibel_ingestor.sinks import NullSink
    svc = Service(settings, sink=sink or NullSink(), store=Store(":memory:"), only={"omie"})  # type: ignore[arg-type]
    conn = svc.connectors["omie"]

    def fake_download(req: Request) -> bytes:       # serve synthetic files instead of the network
        day = date.fromisoformat(str(req.meta["day"]))
        if req.params["parents"] == "precios_pibcic":
            return synthetic.omie_continuous(day)
        return synthetic.omie_marginal(day)
    conn.download = fake_download                   # type: ignore[method-assign]
    return svc


def test_failed_job_retries_with_backoff_before_next_slot(settings: Settings) -> None:
    svc = omie_service(settings)
    job = svc.scheduler.jobs[0]
    failed = JobReport("omie", job.dataset, "live", status="fetch_error")
    before = datetime.now(TZ)
    svc.scheduler._finished(job, failed)
    first = job.next_run
    svc.scheduler._finished(job, failed)
    assert job.failures == 2
    assert first - before <= timedelta(seconds=settings.retry_first + 5)
    assert job.next_run - before <= timedelta(seconds=2 * settings.retry_first + 5)
    svc.scheduler._finished(job, JobReport("omie", job.dataset, "live"))
    assert job.failures == 0


def test_messages_survive_a_broken_sink(settings: Settings) -> None:
    sink = BrokenSink()
    svc = omie_service(settings, sink)
    reports = svc.run_once("omie", "day_ahead")
    assert reports[0].status == "ok" and sink.calls == 1
    assert svc.store.pending() == reports[0].messages > 0          # kept for the next delivery


def test_backfill_is_resumable(settings: Settings) -> None:
    svc = omie_service(settings)
    first = svc.backfill("omie", "day_ahead", date(2026, 1, 1), date(2026, 1, 3))
    assert len(first) == 3 and all(r.sent == 192 and r.mode == "backfill" for r in first)
    assert svc.backfill("omie", "day_ahead", date(2026, 1, 1), date(2026, 1, 4)).__len__() == 1


def test_source_status_explains_disabled_sources(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    status = {s.name: s for s in source_status(settings)}
    assert not status["entsoe"].enabled and "ENTSOE_TOKEN" in status["entsoe"].reason
    assert status["omie"].enabled
