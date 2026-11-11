"""Connector port and registry.

Adding a source (e.g. ENTSO-E) = one module in this package with a ``@register`` class, plus its
block in ``config/ingestor.yaml``. Only enabled connectors are imported (lazy), so a disabled
source costs no memory or start-up time on the Pi.

Every connector follows the same template, for live capture and for backfill alike:

    requests(dataset, window) -> [Request]      # what to download (pure, no I/O)
    download(request)         -> bytes          # I/O (default: HTTP GET)
    parse(dataset, request, payload) -> ParseResult   # bytes -> observations (pure, no I/O)

Keeping ``requests`` and ``parse`` pure is what allows recorded-payload tests and the offline
benchmark (parse cost measured without the network).
"""

from __future__ import annotations

import importlib
import math
import pkgutil
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, TypeVar
from zoneinfo import ZoneInfo

from ..http import HttpClient
from ..model import ContractError, Nature, Observation, ParseResult, Request, Window
from ..settings import ConfigError, SourceSettings, duration, secret


@dataclass(frozen=True)
class Dataset:
    """Generic, validated part of a dataset's settings. Connector-specific keys stay in ``raw``."""

    name: str
    nature: Nature
    resolution: float                 # seconds; used by the grid gate (DG-E03)
    schedule_every: float
    schedule_offset: float
    window_back: float
    window_ahead: float
    publication: dict[str, Any]       # backfill estimate of available_at: {lag} or {at, day_offset}
    raw: dict[str, Any]

    @classmethod
    def from_settings(cls, name: str, raw: dict[str, Any]) -> Dataset:
        try:
            schedule = raw.get("schedule") or {}
            window = raw.get("window") or {}
            return cls(name=name, nature=Nature(raw.get("nature", "observation")),
                       resolution=duration(raw.get("resolution", "15m")),
                       schedule_every=duration(schedule.get("every", "15m")),
                       schedule_offset=duration(schedule.get("offset", "0s")),
                       window_back=duration(window.get("back", "3h")),
                       window_ahead=duration(window.get("ahead", "0s")),
                       publication=dict(raw.get("publication") or {}), raw=raw)
        except (ValueError, KeyError) as exc:
            raise ConfigError(f"dataset {name!r}: {exc}") from None


class Connector(ABC):
    name: ClassVar[str]
    version: ClassVar[str] = "0.1.0"
    secrets: ClassVar[tuple[str, ...]] = ()      # environment variables the source needs

    def __init__(self, cfg: SourceSettings, http: HttpClient, tz: ZoneInfo) -> None:
        self.cfg = cfg
        self.http = http
        self.tz = tz
        self.datasets = {n: Dataset.from_settings(n, raw) for n, raw in cfg.datasets.items()}
        for ds in self.datasets.values():
            self.validate(ds)

    # -- template ---------------------------------------------------------------------------
    def validate(self, ds: Dataset) -> None:  # noqa: B027 - optional hook
        """Check connector-specific dataset keys at start-up (fail fast, before any request)."""

    @abstractmethod
    def requests(self, ds: Dataset, window: Window) -> list[Request]: ...

    def download(self, req: Request) -> bytes:
        return self.http.get(req)

    @abstractmethod
    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult: ...

    # -- helpers ----------------------------------------------------------------------------
    def secret(self, name: str) -> str:
        value = secret(name)
        if value is None:
            raise ConfigError(f"{self.name}: environment variable {name} is not set")
        return value

    @staticmethod
    def require(ds: Dataset, *keys: str) -> None:
        missing = [k for k in keys if k not in ds.raw]
        if missing:
            raise ConfigError(f"dataset {ds.name!r}: missing key(s) {', '.join(missing)}")

    @staticmethod
    def number(value: Any, what: str) -> float | None:
        """Float or None for empty/missing values; anything else non-numeric breaks the contract."""
        if value is None or value == "":
            return None
        try:
            out = float(value)
        except (TypeError, ValueError):
            raise ContractError(f"{what}: not a number: {value!r}") from None
        return out if math.isfinite(out) else None

    @staticmethod
    def obs(variable: str, node: str, event_time: datetime, value: float, unit: str,
            available_at: datetime | None = None) -> Observation:
        return Observation(variable, node, event_time, value, unit, available_at)


C = TypeVar("C", bound=type[Connector])
_REGISTRY: dict[str, type[Connector]] = {}


def available() -> list[str]:
    """Source names = module names in this package (convention: module ``<source>.py``)."""
    package = importlib.import_module(__package__ or "mibel_ingestor.connectors")
    return sorted(m.name for m in pkgutil.iter_modules(package.__path__) if m.name != "base")


def register(cls: C) -> C:
    _REGISTRY[cls.name] = cls
    return cls


def get(name: str) -> type[Connector]:
    if name not in _REGISTRY:
        if name not in available():
            raise ConfigError(f"unknown source {name!r}; available: {', '.join(available())}")
        importlib.import_module(f"{__package__}.{name}")      # lazy: only enabled sources are imported
    return _REGISTRY[name]


def build(cfg: SourceSettings, http_factory: Callable[[float], HttpClient], tz: ZoneInfo) -> Connector:
    cls = get(cfg.name)
    return cls(cfg, http_factory(duration(cfg.raw.get("min_interval", 0))), tz)
