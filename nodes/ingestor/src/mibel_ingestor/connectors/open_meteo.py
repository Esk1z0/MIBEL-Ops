"""Open-Meteo, no key (non-commercial use, CC BY 4.0 attribution — NFR-12).

``api: forecast`` uses the Forecast API live and the Historical Forecast API (forecasts as issued) in
backfill. ``api: satellite`` uses the Satellite Radiation API (observed). Wind speeds are always
requested in m/s. ``wind_speed_100m`` is derived from 80 m and 120 m with the power law
(docs/05_data_sources.md §7.1). One request per weather node.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from ..model import UTC, CaptureMode, ContractError, ParseResult, Request, Window
from ..settings import ConfigError
from .base import Connector, Dataset, register

# --------------------------------------------------------------------------- constants
# Derived variable -> the two hourly inputs it is computed from
DERIVED_INPUTS = {"wind_speed_100m": ("wind_speed_80m", "wind_speed_120m")}
# Below this speed (m/s) the power law is unstable, so the midpoint is used instead
CALM_SPEED = 0.1
LOWER_HEIGHT = 80
UPPER_HEIGHT = 120
TARGET_HEIGHT = 100
HOUR_FORMAT = "%Y-%m-%dT%H:00"


# --------------------------------------------------------------------------- types
class Api(str, Enum):
    """Which Open-Meteo product a dataset reads."""

    FORECAST = "forecast"
    SATELLITE = "satellite"


URLS = {
    (Api.FORECAST, CaptureMode.LIVE): "https://api.open-meteo.com/v1/forecast",
    (Api.FORECAST, CaptureMode.BACKFILL): "https://historical-forecast-api.open-meteo.com/v1/forecast",
    (Api.SATELLITE, CaptureMode.LIVE): "https://satellite-api.open-meteo.com/v1/archive",
    (Api.SATELLITE, CaptureMode.BACKFILL): "https://satellite-api.open-meteo.com/v1/archive",
}


@dataclass(frozen=True)
class VariableSpec:
    """One configured variable: the Open-Meteo parameter it comes from and its unit."""

    name: str
    param: str
    unit: str


@dataclass(frozen=True)
class HourlyBlock:
    """The ``hourly`` block of a response: parameter columns and their UTC time axis."""

    columns: dict[str, Any]
    times: list[datetime]


# --------------------------------------------------------------------------- public API
def wind_at_100m(speed_80m: float | None, speed_120m: float | None) -> float | None:
    """Interpolate the wind speed at 100 m from 80 m and 120 m with the power law.

    Falls back to the midpoint when either speed is calm, where the power law is unstable.
    """
    if speed_80m is None or speed_120m is None:
        return None
    if speed_80m > CALM_SPEED and speed_120m > CALM_SPEED:
        alpha = math.log(speed_120m / speed_80m) / math.log(UPPER_HEIGHT / LOWER_HEIGHT)
        return float(speed_80m * (TARGET_HEIGHT / LOWER_HEIGHT) ** alpha)
    return speed_80m + (speed_120m - speed_80m) * 0.5


@register
class OpenMeteo(Connector):
    """Connector for Open-Meteo forecasts and satellite radiation at the configured weather nodes."""

    name = "open_meteo"

    def validate(self, dataset: Dataset) -> None:
        """Require ``variables``, a known ``api`` and at least one weather node."""
        self.require_options(dataset, "variables")
        _api_of(dataset)
        if not self.source_settings.options.get("nodes"):
            raise ConfigError("open_meteo: no weather nodes configured")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request the window for every applicable weather node (satellite: solar nodes only)."""
        api = _api_of(dataset)
        params = _time_params(api, window)
        params["hourly"] = ",".join(_hourly_params(_variable_specs(dataset)))
        params["timezone"] = "UTC"
        params["wind_speed_unit"] = "ms"
        url = URLS[(api, window.mode)]
        requests = []
        for node, location in self.source_settings.options["nodes"].items():
            if api is Api.SATELLITE and location.get("kind", "solar") != "solar":
                continue
            requests.append(Request(
                url,
                params={**params, "latitude": location["lat"], "longitude": location["lon"]},
                meta={"node": node, "api": api.value},
            ))
        return requests

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse the hourly block of one node into observations, deriving variables where configured."""
        hourly = _load_hourly(payload)
        node = str(request.meta["node"])
        result = ParseResult()
        for spec in _variable_specs(dataset):
            column = self._column_for(spec, hourly.columns, result)
            if len(column) != len(hourly.times):
                raise ContractError(f"Open-Meteo: {spec.param} length differs from time axis")
            for moment, raw_value in zip(hourly.times, column, strict=True):
                value = self.parse_number(raw_value, f"Open-Meteo {spec.param}")
                if value is None:
                    result.count_step("null_dropped")
                    continue
                result.add(self.make_observation(spec.name, node, moment, value, spec.unit))
        return result

    # --- parsing ---
    @staticmethod
    def _column_for(spec: VariableSpec, hourly: dict[str, Any], result: ParseResult) -> list[Any]:
        """Return the hourly values of a variable, computing derived ones from their inputs.

        Raises:
            ContractError: the parameter or the inputs of a derived variable are missing or misaligned.
        """
        if spec.param not in DERIVED_INPUTS:
            if spec.param not in hourly:
                raise ContractError(f"Open-Meteo: variable {spec.param} missing")
            return list(hourly[spec.param])
        lower_name, upper_name = DERIVED_INPUTS[spec.param]
        lower = hourly.get(lower_name)
        upper = hourly.get(upper_name)
        if lower is None or upper is None:
            raise ContractError(f"Open-Meteo: inputs for {spec.param} missing")
        if len(lower) != len(upper):
            raise ContractError(f"Open-Meteo: inputs for {spec.param} differ in length")
        column = [wind_at_100m(speed_80m, speed_120m) for speed_80m, speed_120m in zip(lower, upper, strict=True)]
        result.count_step(f"derived:{spec.name}(power_law_80_120m)", len(column))
        return column


# --------------------------------------------------------------------------- private helpers
def _api_of(dataset: Dataset) -> Api:
    """Return the dataset's API (default ``forecast``)."""
    name = dataset.options.get("api", Api.FORECAST.value)
    try:
        return Api(name)
    except ValueError:
        raise ConfigError(f"open_meteo dataset {dataset.name!r}: api must be forecast or satellite") from None


def _variable_specs(dataset: Dataset) -> list[VariableSpec]:
    """Return the configured variables; a plain string value is shorthand for ``{unit: ...}``."""
    specs = []
    for name, options in dataset.options["variables"].items():
        if isinstance(options, str):
            options = {"unit": options}
        specs.append(VariableSpec(name=name, param=str(options.get("param", name)), unit=str(options["unit"])))
    return specs


def _hourly_params(specs: list[VariableSpec]) -> list[str]:
    """List the hourly parameters to request, replacing derived ones by their inputs (no duplicates)."""
    params: list[str] = []
    for spec in specs:
        for param in DERIVED_INPUTS.get(spec.param, (spec.param,)):
            if param not in params:
                params.append(param)
    return params


def _time_params(api: Api, window: Window) -> dict[str, Any]:
    """Return the time range parameters: hours for live forecasts, whole days otherwise."""
    if api is Api.FORECAST and window.is_live:
        return {
            "start_hour": window.start.astimezone(UTC).strftime(HOUR_FORMAT),
            "end_hour": window.end.astimezone(UTC).strftime(HOUR_FORMAT),
        }
    last_moment = window.end - timedelta(seconds=1)
    return {
        "start_date": window.start.astimezone(UTC).date().isoformat(),
        "end_date": last_moment.astimezone(UTC).date().isoformat(),
    }


def _load_hourly(payload: bytes) -> HourlyBlock:
    """Return the ``hourly`` block with its time axis in UTC.

    Raises:
        ContractError: the response has no ``hourly.time``.
    """
    try:
        hourly = json.loads(payload)["hourly"]
        times = [datetime.fromisoformat(text).replace(tzinfo=UTC) for text in hourly["time"]]
    except (ValueError, KeyError, TypeError):
        raise ContractError("Open-Meteo: missing hourly.time") from None
    return HourlyBlock(columns=hourly, times=times)
