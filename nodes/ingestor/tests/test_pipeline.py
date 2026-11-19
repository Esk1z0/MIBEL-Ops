"""End-to-end through the core with a fake connector, an in-memory store and no network."""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import pytest

from mibel_ingestor.connectors import Connector, Dataset
from mibel_ingestor.gates import Gates
from mibel_ingestor.http import HttpClient
from mibel_ingestor.model import UTC, CaptureMode, ContractError, NotAvailable, ParseResult, Request, Window
from mibel_ingestor.pipeline import JobStatus, Pipeline
from mibel_ingestor.settings import Settings, SourceSettings
from mibel_ingestor.store import Store

from conftest import MADRID

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
FAKE_DATASETS = {
    "demand": {
        "nature": "observation",
        "resolution": "15m",
        "window": {"back": "2h", "ahead": "0s"},
        "publication": {"lag": "10m"},
    },
}


class Behaviour(str, Enum):
    """How the fake source answers."""

    OK = "ok"
    MISSING = "missing"
    BROKEN = "broken"


class FakeConnector(Connector):
    """A source that returns the given demand values, one per quarter-hour before ``NOW``."""

    name = "fake"

    def __init__(self, settings: Settings, values: list[float], behaviour: Behaviour = Behaviour.OK) -> None:
        source_settings = SourceSettings("fake", True, {"datasets": FAKE_DATASETS}, FAKE_DATASETS)
        super().__init__(source_settings, HttpClient(settings.http), MADRID)
        self.values = values
        self.behaviour = behaviour

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """One fake request."""
        return [Request("https://example.invalid", meta={"n": 1})]

    def download(self, request: Request) -> bytes:
        """Return a dummy payload, or raise NotAvailable when the file is 'missing'."""
        if self.behaviour is Behaviour.MISSING:
            raise NotAvailable("not yet")
        return b"payload"

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Return the configured values, or break the contract."""
        if self.behaviour is Behaviour.BROKEN:
            raise ContractError("bad structure")
        result = ParseResult()
        for i, value in enumerate(self.values):
            event_time = NOW - timedelta(minutes=15 * (i + 1))
            result.add(self.make_observation("demand_real", "ES", event_time, value, "MW"))
        return result


@dataclass(frozen=True)
class DeliveredMessage:
    """A message read back from the outbox."""

    topic: str
    document: dict[str, Any]


def build_pipeline(
    settings: Settings,
    values: list[float],
    behaviour: Behaviour = Behaviour.OK,
) -> tuple[Pipeline, Store]:
    """A pipeline over the fake source with an in-memory store and a fixed clock."""
    store = Store(":memory:")
    pipeline = Pipeline(
        settings,
        store,
        Gates(settings.quality, "production"),
        {"fake": FakeConnector(settings, values, behaviour)},
        clock=lambda: NOW,
    )
    return pipeline, store


def read_outbox(store: Store) -> list[DeliveredMessage]:
    """Return every message waiting in the outbox, decoded."""
    return [
        DeliveredMessage(pending.message.topic, json.loads(pending.message.payload))
        for pending in store.fetch_due(100)
    ]


def test_batch_contract_and_lineage(settings: Settings) -> None:
    pipeline, store = build_pipeline(settings, [30000, 31000])
    report = pipeline.run("fake", "demand")
    assert report.status is JobStatus.OK
    assert report.sent == 2
    (message,) = read_outbox(store)
    document = message.document
    assert message.topic == "mibel.raw.fake"
    assert document["contract"] == "mibel.observations/1"
    assert document["mode"] == "live"
    assert document["nature"] == "observation"
    assert document["fields"] == ["id", "variable", "node", "event_time", "available_at", "value", "unit"]
    first_row = dict(zip(document["fields"], document["rows"][0], strict=True))
    assert first_row["event_time"] == "2026-11-11T11:45:00Z"
    assert first_row["available_at"] == "2026-11-11T12:00:00Z"
    assert document["lineage"]["gates"]["profile"] == "production"
    assert document["lineage"]["available_at"] == "first_seen"


def test_unchanged_values_are_not_sent_again(settings: Settings) -> None:
    pipeline, store = build_pipeline(settings, [30000, 31000])
    pipeline.run("fake", "demand")
    store.mark_delivered([pending.id for pending in store.fetch_due(100)])
    report = pipeline.run("fake", "demand")
    assert report.unchanged == 2
    assert report.sent == 0
    assert not store.fetch_due(100)
    pipeline.connectors["fake"].values = [30000, 31500]  # type: ignore[attr-defined]
    assert pipeline.run("fake", "demand").sent == 1  # only the revision travels


def test_ids_are_deterministic(settings: Settings) -> None:
    first_pipeline, first_store = build_pipeline(settings, [30000])
    second_pipeline, second_store = build_pipeline(settings, [30000])
    first_pipeline.run("fake", "demand")
    second_pipeline.run("fake", "demand")
    first_id = read_outbox(first_store)[0].document["rows"][0][0]
    second_id = read_outbox(second_store)[0].document["rows"][0][0]
    assert first_id == second_id


def test_rejected_values_go_to_quarantine_and_dlq(settings: Settings) -> None:
    pipeline, store = build_pipeline(settings, [30000, 5])  # 5 MW is outside DG-E10
    report = pipeline.run("fake", "demand")
    assert report.sent == 1
    assert report.rejected == 1
    dlq = [message.document for message in read_outbox(store) if message.topic == "mibel.dlq"]
    assert dlq[0]["rule"] == "DG-E10"
    assert dlq[0]["count"] == 1


def test_contract_error_reports_dg_e01(settings: Settings) -> None:
    pipeline, store = build_pipeline(settings, [], Behaviour.BROKEN)
    report = pipeline.run("fake", "demand")
    assert report.status is JobStatus.CONTRACT_ERROR
    assert not report.succeeded
    assert read_outbox(store)[0].document["rule"] == "DG-E01"


def test_not_published_yet_is_not_a_failure(settings: Settings) -> None:
    pipeline, store = build_pipeline(settings, [], Behaviour.MISSING)
    report = pipeline.run("fake", "demand")
    assert report.status is JobStatus.NOT_AVAILABLE
    assert report.succeeded
    assert not store.fetch_due(10)


def test_backfill_estimates_available_at(settings: Settings) -> None:
    pipeline, store = build_pipeline(settings, [30000])
    window = Window(NOW - timedelta(days=1), NOW, CaptureMode.BACKFILL)
    report = pipeline.run("fake", "demand", window)
    document = read_outbox(store)[0].document
    assert report.sent == 1
    assert document["mode"] == "backfill"
    assert document["lineage"]["available_at"] == "estimated"
    assert document["rows"][0][4] == "2026-11-11T11:55:00Z"  # event 11:45 + publication lag 10 min


def test_report_as_dict_is_plain_json(settings: Settings) -> None:
    pipeline, _ = build_pipeline(settings, [30000])
    report = pipeline.run("fake", "demand").as_dict()
    assert report["status"] == "ok"
    assert set(report["stages"]) >= {"download", "parse", "detect", "gates", "serialize"}
    json.dumps(report)


@pytest.mark.parametrize(
    ("publication", "expected"),
    [({"at": "13:00", "day_offset": -1}, "2026-11-10T12:00:00Z"), ({}, "2026-11-11T12:00:00Z")],
)
def test_publication_rules(settings: Settings, publication: dict[str, Any], expected: str) -> None:
    pipeline, _ = build_pipeline(settings, [])
    dataset = dataclasses.replace(pipeline.connectors["fake"].datasets["demand"], publication=publication)
    observation = FakeConnector.make_observation("x", "ES", datetime(2026, 11, 11, 11, 45, tzinfo=UTC), 1, "MW")
    estimate = pipeline.estimate_available_at(dataset, observation)
    assert estimate.strftime("%Y-%m-%dT%H:%M:%SZ") == expected
