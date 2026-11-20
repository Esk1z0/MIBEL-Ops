"""Connector port, shared helpers and registry.

Adding a source = one module ``connectors/<source>.py`` with a ``@register`` class, plus its block in
``config/ingestor.yaml``. Only enabled connectors are imported (lazily), so a disabled source costs no
memory or start-up time on the Pi (ADR-05 §5.2).

Every connector follows the same template, for live capture and backfill alike::

    requests(dataset, window)          -> [Request]     what to download   (pure, no I/O)
    download(request)                  -> bytes         the I/O            (default: HTTP GET)
    parse(dataset, request, payload)   -> ParseResult   bytes → values     (pure, no I/O)

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
from ..settings import ConfigError, SourceSettings, parse_duration, read_secret


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


BASE_MODULE = "base"


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


@dataclass(frozen=True)
class Dataset:
    """Generic, validated part of a dataset's settings.

    ``resolution`` (seconds) feeds the grid gate DG-E03. ``publication`` describes when values are
    published, to estimate ``available_at`` in backfill: ``{lag}`` or ``{at, day_offset}``.
    Connector-specific keys stay in ``options``.
    """

    name: str
    nature: Nature
    resolution: float
    schedule_every: float
    schedule_offset: float
    window_back: float
    window_ahead: float
    publication: dict[str, Any]
    options: dict[str, Any]

    @classmethod
    def from_settings(cls, name: str, options: dict[str, Any]) -> Dataset:
        """Build a dataset from its merged YAML options, applying the generic defaults.

        Raises:
            ConfigError: an invalid nature or duration.
        """
        schedule = options.get("schedule") or {}
        window = options.get("window") or {}
        try:
            return cls(
                name=name,
                nature=Nature(options.get("nature", Nature.OBSERVATION.value)),
                resolution=parse_duration(options.get("resolution", "15m")),
                schedule_every=parse_duration(schedule.get("every", "15m")),
                schedule_offset=parse_duration(schedule.get("offset", "0s")),
                window_back=parse_duration(window.get("back", "3h")),
                window_ahead=parse_duration(window.get("ahead", "0s")),
                publication=dict(options.get("publication") or {}),
                options=options,
            )
        except (ValueError, KeyError) as exc:
            raise ConfigError(f"dataset {name!r}: {exc}") from None


class Connector(ABC):
    """Port implemented by every source.

    Subclasses set ``name`` (equal to their module name) and implement ``requests`` and ``parse``;
    ``download`` is overridden only when a source is not a plain GET (e.g. AEMET's two-step API).
    """

    name: ClassVar[str]
    version: ClassVar[str] = "0.1.0"
    secrets: ClassVar[tuple[str, ...]] = ()

    def __init__(self, source_settings: SourceSettings, http: HttpClient, local_timezone: ZoneInfo) -> None:
        self.source_settings = source_settings
        self.http = http
        self.timezone = local_timezone
        self.datasets = {
            name: Dataset.from_settings(name, options) for name, options in source_settings.datasets.items()
        }
        for dataset in self.datasets.values():
            self.validate(dataset)

    # --- template ---
    def validate(self, dataset: Dataset) -> None:  # noqa: B027 - optional hook, empty on purpose
        """Check connector-specific dataset options at start-up, so errors appear before any request."""

    @abstractmethod
    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """List the downloads needed to cover ``window`` (pure, no I/O)."""

    def download(self, request: Request) -> bytes:
        """Fetch one request (default: HTTP GET through the source's client)."""
        return self.http.get(request)

    @abstractmethod
    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Convert a payload into observations (pure, no I/O).

        Raises:
            ContractError: the payload does not have the expected structure (DG-E01).
        """

    # --- helpers for subclasses ---
    def read_secret(self, name: str) -> str:
        """Return a required secret from the environment.

        Raises:
            ConfigError: the variable is missing or still holds the placeholder.
        """
        value = read_secret(name)
        if value is None:
            raise ConfigError(f"{self.name}: environment variable {name} is not set")
        return value

    @staticmethod
    def require_options(dataset: Dataset, *keys: str) -> None:
        """Fail at start-up if the dataset lacks any of the given option keys."""
        missing = [key for key in keys if key not in dataset.options]
        if missing:
            raise ConfigError(f"dataset {dataset.name!r}: missing key(s) {', '.join(missing)}")

    @staticmethod
    def parse_number(value: Any, what: str) -> float | None:
        """Return a finite float, or None for empty, missing, NaN or infinite values.

        Raises:
            ContractError: the value is present but not numeric, which breaks the source contract.
        """
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise ContractError(f"{what}: not a number: {value!r}") from None
        return number if math.isfinite(number) else None

    @staticmethod
    def make_observation(
        variable: str,
        node: str,
        event_time: datetime,
        value: float,
        unit: str,
        available_at: datetime | None = None,
    ) -> Observation:
        """Build an observation (``available_at`` only when the source states it)."""
        return Observation(variable, node, event_time, value, unit, available_at)


ConnectorClass = TypeVar("ConnectorClass", bound=type[Connector])
HttpFactory = Callable[[float], HttpClient]


# ##################################################################################################
# ##################################     PUBLIC API: REGISTRY     ##################################
# ##################################################################################################


_REGISTRY: dict[str, type[Connector]] = {}


def available_sources() -> list[str]:
    """List the source names that have a connector module (convention: module ``<source>.py``)."""
    package = importlib.import_module(__package__ or "mibel_ingestor.connectors")
    modules = pkgutil.iter_modules(package.__path__)
    return sorted(module.name for module in modules if module.name != BASE_MODULE)


def register(connector_class: ConnectorClass) -> ConnectorClass:
    """Class decorator that adds a connector to the registry under its ``name``."""
    _REGISTRY[connector_class.name] = connector_class
    return connector_class


def get_connector_class(name: str) -> type[Connector]:
    """Return the connector class of a source, importing its module on first use.

    Raises:
        ConfigError: no connector module exists for that source.
    """
    if name not in _REGISTRY:
        known = available_sources()
        if name not in known:
            raise ConfigError(f"unknown source {name!r}; available: {', '.join(known)}")
        # lazy import: only enabled sources are loaded (memory and start-up time on the Pi)
        importlib.import_module(f"{__package__}.{name}")
    return _REGISTRY[name]


def build_connector(
    source_settings: SourceSettings,
    http_factory: HttpFactory,
    local_timezone: ZoneInfo,
) -> Connector:
    """Instantiate the connector of a source with its own rate-limited HTTP client."""
    connector_class = get_connector_class(source_settings.name)
    min_interval = parse_duration(source_settings.options.get("min_interval", 0))
    return connector_class(source_settings, http_factory(min_interval), local_timezone)
