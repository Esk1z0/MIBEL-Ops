"""Core data model of the ingestor: observations, windows, requests and the wire contract.

Plain dataclasses with ``__slots__`` (no pandas, no pydantic) keep memory and start-up time low on
a Raspberry Pi (NFR-02, ADR-05).

Wire contract ``mibel.observations/1`` (docs/03_diagrams.md §12, ADR-05 §5.3): one message per
(source, dataset, fetch) batch, carrying its lineage and compact rows::

    {"contract", "batch_id", "ingestor", "source", "dataset", "nature", "mode", "created_at",
     "lineage": {...}, "fields": [...ROW_FIELDS], "rows": [[id, variable, node, ...], ...]}

Rejected observations travel as ``mibel.dlq/1`` messages (rule, reason, count, up to 3 samples).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


UTC = timezone.utc
CONTRACT = "mibel.observations/1"
DLQ_CONTRACT = "mibel.dlq/1"
ROW_FIELDS = ("id", "variable", "node", "event_time", "available_at", "value", "unit")
DLQ_MAX_SAMPLES = 3
ID_LENGTH = 16


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class CaptureMode(str, Enum):
    """How a batch was captured. The pipeline is identical; only the window and this tag change."""

    LIVE = "live"
    BACKFILL = "backfill"


class Nature(str, Enum):
    """What a dataset represents. Some edge gates apply only to measured observations."""

    OBSERVATION = "observation"
    FORECAST = "forecast"
    MARKET = "market"


class ContractError(ValueError):
    """The payload does not have the structure its source contract expects (DG-E01)."""


class NotAvailable(Exception):
    """The requested resource is not published yet (e.g. OMIE file for tomorrow). Not a failure."""


@dataclass(frozen=True, slots=True)
class Window:
    """Half-open time interval ``[start, end)`` in UTC, tagged with how it is captured."""

    start: datetime
    end: datetime
    mode: CaptureMode = CaptureMode.LIVE

    @property
    def is_live(self) -> bool:
        """Whether the window belongs to real-time capture (not to a backfill)."""
        return self.mode is CaptureMode.LIVE

    def local_days(self, local_timezone: ZoneInfo) -> list[date]:
        """List the local calendar days touched by the window (for day-based files such as OMIE)."""
        first_day = self.start.astimezone(local_timezone).date()
        last_moment = self.end - timedelta(microseconds=1)
        last_day = last_moment.astimezone(local_timezone).date()
        day_count = (last_day - first_day).days + 1
        return [first_day + timedelta(days=i) for i in range(day_count)]


@dataclass(frozen=True, slots=True)
class Request:
    """One download.

    ``secret`` hides the URL and parameters everywhere, because they may carry a token (AGENTS.md §3).
    ``meta`` is non-secret context for the parser and for the lineage.
    """

    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    secret: bool = False
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Observation:
    """One value of one variable at one node for one period.

    ``event_time`` is the start of the period the value refers to (aware, UTC). ``available_at`` is
    set by the connector only when the source states it; otherwise the pipeline fills it.
    """

    variable: str
    node: str
    event_time: datetime
    value: float
    unit: str
    available_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Rejection:
    """An observation blocked by the edge gates, with the first rule that blocked it."""

    observation: Observation
    rule: str


@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    """A serialized message waiting for delivery to a sink (Kafka topic, key and payload)."""

    topic: str
    key: str
    payload: bytes


@dataclass(slots=True)
class ParseResult:
    """Observations produced by a parser plus the lineage steps applied to them.

    ``steps`` maps a transformation name (e.g. ``A03_forward_fill``) to how many rows it affected.
    """

    observations: list[Observation] = field(default_factory=list)
    steps: dict[str, int] = field(default_factory=dict)

    def add(self, observation: Observation) -> None:
        """Append one observation."""
        self.observations.append(observation)

    def count_step(self, name: str, count: int = 1) -> None:
        """Add ``count`` rows to the lineage step ``name`` (zero counts are not recorded)."""
        if count:
            self.steps[name] = self.steps.get(name, 0) + count

    def extend(self, other: ParseResult) -> None:
        """Merge the observations and steps of another result into this one."""
        self.observations.extend(other.observations)
        for name, count in other.steps.items():
            self.count_step(name, count)


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def utc_now() -> datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.now(UTC)


def to_iso(moment: datetime) -> str:
    """Format a datetime as compact ISO-8601 in UTC with a trailing Z (seconds precision)."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime:
    """Parse an ISO-8601 timestamp that must carry a timezone, returning it in UTC.

    Raises:
        ContractError: the text is not a timestamp, or it has no timezone, so its meaning would be
            ambiguous. Timestamps come from source payloads, so both break the source contract (DG-E01).
    """
    try:
        moment = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        raise ContractError(f"invalid timestamp: {text!r}") from None
    if moment.tzinfo is None:
        raise ContractError(f"timestamp without timezone: {text!r}")
    return moment.astimezone(UTC)


def message_id(source: str, observation: Observation) -> str:
    """Return the deterministic idempotency key of an observation (FR-05).

    ``available_at`` is part of the key so that forecast vintages and revisions of the same period
    are distinct messages (DG-G01 key).
    """
    if observation.available_at is None:
        raise ValueError("message_id needs available_at to be set")
    key = "|".join((
        source,
        observation.variable,
        observation.node,
        to_iso(observation.event_time),
        to_iso(observation.available_at),
    ))
    return _short_hash(key)


def to_json_bytes(payload: dict[str, Any]) -> bytes:
    """Serialize a payload as compact UTF-8 JSON; NaN and infinity are refused."""
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def build_batch(
    *,
    source: str,
    dataset: str,
    nature: Nature,
    mode: CaptureMode,
    ingestor: str,
    observations: list[Observation],
    lineage: dict[str, Any],
    created_at: datetime,
) -> bytes:
    """Serialize a batch of observations as a ``mibel.observations/1`` message."""
    rows = [_to_row(source, observation) for observation in observations]
    first_id = rows[0][0] if rows else ""
    batch_key = f"{source}|{dataset}|{ingestor}|{to_iso(created_at)}|{first_id}"
    return to_json_bytes({
        "contract": CONTRACT,
        "batch_id": _short_hash(batch_key),
        "ingestor": ingestor,
        "source": source,
        "dataset": dataset,
        "nature": nature.value,
        "mode": mode.value,
        "created_at": to_iso(created_at),
        "lineage": lineage,
        "fields": list(ROW_FIELDS),
        "rows": rows,
    })


def build_dlq(
    *,
    source: str,
    dataset: str,
    ingestor: str,
    rule: str,
    reason: str,
    samples: list[Observation],
    count: int,
    created_at: datetime,
) -> bytes:
    """Serialize a ``mibel.dlq/1`` message: why ``count`` observations were rejected, with samples."""
    return to_json_bytes({
        "contract": DLQ_CONTRACT,
        "ingestor": ingestor,
        "source": source,
        "dataset": dataset,
        "rule": rule,
        "reason": reason,
        "count": count,
        "samples": [_to_sample(observation) for observation in samples[:DLQ_MAX_SAMPLES]],
        "created_at": to_iso(created_at),
    })


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _short_hash(text: str) -> str:
    """Return the first ``ID_LENGTH`` hex characters of the SHA-1 of ``text``."""
    return hashlib.sha1(text.encode()).hexdigest()[:ID_LENGTH]


def _to_row(source: str, observation: Observation) -> list[Any]:
    """Return an observation as a compact row in ``ROW_FIELDS`` order."""
    available_at = to_iso(observation.available_at) if observation.available_at else None
    return [
        message_id(source, observation),
        observation.variable,
        observation.node,
        to_iso(observation.event_time),
        available_at,
        observation.value,
        observation.unit,
    ]


def _to_sample(observation: Observation) -> list[Any]:
    """Return the short form of a rejected observation used in DLQ samples."""
    return [
        observation.variable,
        observation.node,
        to_iso(observation.event_time),
        observation.value,
        observation.unit,
    ]
