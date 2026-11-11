"""Core data model: plain dataclasses with ``__slots__`` (no pandas, no pydantic) to stay light on a Pi.

The wire contract is ``mibel.observations/1`` (see docs/03_diagrams.md §12 and ADR-05):
one message per (source, dataset, fetch) batch, with lineage and compact rows.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

UTC = timezone.utc
CONTRACT = "mibel.observations/1"
DLQ_CONTRACT = "mibel.dlq/1"
ROW_FIELDS = ("id", "variable", "node", "event_time", "available_at", "value", "unit")


class Mode(str, Enum):
    """How a batch was captured. The pipeline is identical; only the window and this tag change."""

    LIVE = "live"          # real-time capture: available_at = first time the value was seen
    BACKFILL = "backfill"  # historical download: available_at is an estimate (see lineage)


class Nature(str, Enum):
    """What a dataset represents. Some edge gates apply only to measured observations."""

    OBSERVATION = "observation"  # measured value (demand, real PV, weather station)
    FORECAST = "forecast"        # value about the future (weather, H+3, D+1)
    MARKET = "market"            # market result for a delivery period (prices)


class ContractError(ValueError):
    """The payload does not have the structure its source contract expects (DG-E01)."""


class NotAvailable(Exception):
    """The requested resource is not published yet (e.g. OMIE file for tomorrow). Not a failure."""


@dataclass(frozen=True, slots=True)
class Window:
    """Half-open time interval [start, end) in UTC."""

    start: datetime
    end: datetime
    mode: Mode = Mode.LIVE

    def days(self, tz: Any) -> list[Any]:
        """Local calendar days touched by the window (for day-based files such as OMIE)."""
        first = self.start.astimezone(tz).date()
        last = (self.end - timedelta(microseconds=1)).astimezone(tz).date()
        return [first + timedelta(days=i) for i in range((last - first).days + 1)]


@dataclass(frozen=True, slots=True)
class Request:
    """One download. ``secret`` hides URL and parameters everywhere (they may carry a token)."""

    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    secret: bool = False
    meta: dict[str, Any] = field(default_factory=dict)   # non-secret context for the parser and lineage


@dataclass(slots=True)
class Observation:
    variable: str
    node: str
    event_time: datetime          # start of the period the value refers to (aware, UTC)
    value: float
    unit: str
    available_at: datetime | None = None   # set by the connector only when the source states it


@dataclass(slots=True)
class ParseResult:
    observations: list[Observation] = field(default_factory=list)
    steps: dict[str, int] = field(default_factory=dict)   # lineage: transformation -> rows affected

    def step(self, name: str, count: int = 1) -> None:
        if count:
            self.steps[name] = self.steps.get(name, 0) + count

    def extend(self, other: ParseResult) -> None:
        self.observations.extend(other.observations)
        for name, count in other.steps.items():
            self.step(name, count)


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(ts: datetime) -> str:
    """Compact ISO-8601 in UTC with a trailing Z (seconds precision)."""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime:
    ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        raise ContractError(f"timestamp without timezone: {text!r}")
    return ts.astimezone(UTC)


def message_id(source: str, obs: Observation) -> str:
    """Deterministic idempotency key (FR-05). ``available_at`` is part of it so that forecast
    vintages and revisions of the same period are distinct messages (DG-G01 key)."""
    assert obs.available_at is not None
    key = f"{source}|{obs.variable}|{obs.node}|{iso(obs.event_time)}|{iso(obs.available_at)}"
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def dumps(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def build_batch(*, source: str, dataset: str, nature: Nature, mode: Mode, ingestor: str,
                observations: list[Observation], lineage: dict[str, Any], created_at: datetime) -> bytes:
    rows = [[message_id(source, o), o.variable, o.node, iso(o.event_time),
             iso(o.available_at) if o.available_at else None, o.value, o.unit] for o in observations]
    batch_key = f"{source}|{dataset}|{ingestor}|{iso(created_at)}|{rows[0][0] if rows else ''}"
    return dumps({
        "contract": CONTRACT,
        "batch_id": hashlib.sha1(batch_key.encode()).hexdigest()[:16],
        "ingestor": ingestor,
        "source": source,
        "dataset": dataset,
        "nature": nature.value,
        "mode": mode.value,
        "created_at": iso(created_at),
        "lineage": lineage,
        "fields": list(ROW_FIELDS),
        "rows": rows,
    })


def build_dlq(*, source: str, dataset: str, ingestor: str, rule: str, reason: str,
              samples: list[Observation], count: int, created_at: datetime) -> bytes:
    return dumps({
        "contract": DLQ_CONTRACT,
        "ingestor": ingestor,
        "source": source,
        "dataset": dataset,
        "rule": rule,
        "reason": reason,
        "count": count,
        "samples": [[o.variable, o.node, iso(o.event_time), o.value, o.unit] for o in samples[:3]],
        "created_at": iso(created_at),
    })
