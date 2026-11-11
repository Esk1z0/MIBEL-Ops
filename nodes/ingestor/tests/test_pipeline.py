"""End-to-end through the core with a fake connector, an in-memory store and no network."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import pytest

from mibel_ingestor.connectors import Connector, Dataset
from mibel_ingestor.gates import Gates
from mibel_ingestor.http import HttpClient
from mibel_ingestor.model import UTC, ContractError, Mode, NotAvailable, ParseResult, Request, Window
from mibel_ingestor.pipeline import Pipeline
from mibel_ingestor.settings import Settings, SourceSettings
from mibel_ingestor.store import Store

from conftest import TZ

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


class Fake(Connector):
    name = "fake"

    def __init__(self, settings: Settings, values: list[float], behaviour: str = "ok") -> None:
        raw = {"datasets": {"demand": {"nature": "observation", "resolution": "15m",
                                       "window": {"back": "2h", "ahead": "0s"}, "publication": {"lag": "10m"}}}}
        super().__init__(SourceSettings("fake", True, raw, raw["datasets"]), HttpClient(settings.http), TZ)
        self.values = values
        self.behaviour = behaviour

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        return [Request("https://example.invalid", meta={"n": 1})]

    def download(self, req: Request) -> bytes:
        if self.behaviour == "missing":
            raise NotAvailable("not yet")
        return b"payload"

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        if self.behaviour == "broken":
            raise ContractError("bad structure")
        out = ParseResult()
        for i, v in enumerate(self.values):
            out.observations.append(self.obs("demand_real", "ES", NOW - timedelta(minutes=15 * (i + 1)), v, "MW"))
        return out


def pipeline(settings: Settings, values: list[float], behaviour: str = "ok") -> tuple[Pipeline, Store]:
    store = Store(":memory:")
    p = Pipeline(settings, store, Gates(settings.quality, "production"),
                 {"fake": Fake(settings, values, behaviour)}, clock=lambda: NOW)
    return p, store


def messages(store: Store) -> list[tuple[str, dict[str, Any]]]:
    return [(m.topic, json.loads(m.payload)) for m in store.due(100)]


def test_batch_contract_and_lineage(settings: Settings) -> None:
    p, store = pipeline(settings, [30000, 31000])
    report = p.run("fake", "demand")
    assert report.status == "ok" and report.sent == 2
    ((topic, msg),) = messages(store)
    assert topic == "mibel.raw.fake"
    assert msg["contract"] == "mibel.observations/1" and msg["mode"] == "live" and msg["nature"] == "observation"
    assert msg["fields"] == ["id", "variable", "node", "event_time", "available_at", "value", "unit"]
    assert msg["rows"][0][3] == "2026-11-11T11:45:00Z" and msg["rows"][0][4] == "2026-11-11T12:00:00Z"
    assert msg["lineage"]["gates"]["profile"] == "production"
    assert msg["lineage"]["available_at"] == "first_seen"


def test_unchanged_values_are_not_sent_again(settings: Settings) -> None:
    p, store = pipeline(settings, [30000, 31000])
    p.run("fake", "demand")
    store.delivered([m.id for m in store.due(100)])
    report = p.run("fake", "demand")
    assert report.unchanged == 2 and report.sent == 0 and not store.due(100)
    p.connectors["fake"].values = [30000, 31500]                   # type: ignore[attr-defined]
    assert p.run("fake", "demand").sent == 1                     # only the revision travels


def test_ids_are_deterministic(settings: Settings) -> None:
    p1, s1 = pipeline(settings, [30000])
    p2, s2 = pipeline(settings, [30000])
    p1.run("fake", "demand")
    p2.run("fake", "demand")
    assert messages(s1)[0][1]["rows"][0][0] == messages(s2)[0][1]["rows"][0][0]


def test_rejected_values_go_to_quarantine_and_dlq(settings: Settings) -> None:
    p, store = pipeline(settings, [30000, 5])                      # 5 MW: outside DG-E10
    report = p.run("fake", "demand")
    assert report.sent == 1 and report.rejected == 1
    dlq = [m for t, m in messages(store) if t == "mibel.dlq"]
    assert dlq[0]["rule"] == "DG-E10" and dlq[0]["count"] == 1


def test_contract_error_reports_dg_e01(settings: Settings) -> None:
    p, store = pipeline(settings, [], behaviour="broken")
    report = p.run("fake", "demand")
    assert report.status == "contract_error" and not report.succeeded
    assert messages(store)[0][1]["rule"] == "DG-E01"


def test_not_published_yet_is_not_a_failure(settings: Settings) -> None:
    p, store = pipeline(settings, [], behaviour="missing")
    report = p.run("fake", "demand")
    assert report.status == "not_available" and report.succeeded and not store.due(10)


def test_backfill_estimates_available_at(settings: Settings) -> None:
    p, store = pipeline(settings, [30000])
    window = Window(NOW - timedelta(days=1), NOW, Mode.BACKFILL)
    report = p.run("fake", "demand", window)
    msg = messages(store)[0][1]
    assert report.sent == 1 and msg["mode"] == "backfill" and msg["lineage"]["available_at"] == "estimated"
    assert msg["rows"][0][4] == "2026-11-11T11:55:00Z"            # event 11:45 + publication lag 10 min


@pytest.mark.parametrize(("pub", "expected"), [({"at": "13:00", "day_offset": -1}, "2026-11-10T12:00:00Z"),
                                               ({}, "2026-11-11T12:00:00Z")])
def test_publication_rules(settings: Settings, pub: dict[str, Any], expected: str) -> None:
    p, _ = pipeline(settings, [])
    ds = p.connectors["fake"].datasets["demand"]
    ds = Dataset(**{**vars(ds), "publication": pub})
    obs = Fake(settings, []).obs("x", "ES", datetime(2026, 11, 11, 11, 45, tzinfo=UTC), 1, "MW")
    assert p.estimate_available(ds, obs).strftime("%Y-%m-%dT%H:%M:%SZ") == expected
