"""Open-Meteo, no key (non-commercial use, CC BY 4.0 attribution — NFR-12).

``api: forecast`` uses the Forecast API live and the Historical Forecast API (forecasts as issued)
in backfill. ``api: satellite`` uses the Satellite Radiation API (observed). Always ``wind_speed_unit=ms``.
``wind_speed_100m`` is derived from 80 m and 120 m with the power law (docs/05_data_sources.md §7.1).
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta
from typing import Any

from ..model import UTC, ContractError, Mode, ParseResult, Request, Window
from ..settings import ConfigError
from .base import Connector, Dataset, register

URLS = {
    ("forecast", Mode.LIVE): "https://api.open-meteo.com/v1/forecast",
    ("forecast", Mode.BACKFILL): "https://historical-forecast-api.open-meteo.com/v1/forecast",
    ("satellite", Mode.LIVE): "https://satellite-api.open-meteo.com/v1/archive",
    ("satellite", Mode.BACKFILL): "https://satellite-api.open-meteo.com/v1/archive",
}
DERIVED = {"wind_speed_100m": ("wind_speed_80m", "wind_speed_120m")}


def wind_at_100m(v80: float | None, v120: float | None) -> float | None:
    """Power-law interpolation between 80 m and 120 m; linear when a speed is ~0."""
    if v80 is None or v120 is None:
        return None
    if v80 > 0.1 and v120 > 0.1:
        alpha = math.log(v120 / v80) / math.log(120 / 80)
        return float(v80 * (100 / 80) ** alpha)
    return v80 + (v120 - v80) * 0.5


@register
class OpenMeteo(Connector):
    name = "open_meteo"

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "variables")
        if ds.raw.get("api", "forecast") not in ("forecast", "satellite"):
            raise ConfigError(f"open_meteo dataset {ds.name!r}: api must be forecast or satellite")
        if not self.cfg.raw.get("nodes"):
            raise ConfigError("open_meteo: no weather nodes configured")

    def _variables(self, ds: Dataset) -> dict[str, dict[str, str]]:
        out = {}
        for name, spec in ds.raw["variables"].items():
            spec = {"unit": spec} if isinstance(spec, str) else dict(spec)
            spec.setdefault("param", name)
            out[name] = spec
        return out

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        api = ds.raw.get("api", "forecast")
        params_needed: list[str] = []
        for spec in self._variables(ds).values():
            for p in DERIVED.get(spec["param"], (spec["param"],)):
                if p not in params_needed:
                    params_needed.append(p)
        base: dict[str, Any] = {"hourly": ",".join(params_needed), "timezone": "UTC", "wind_speed_unit": "ms"}
        if api == "forecast" and window.mode is Mode.LIVE:
            base["start_hour"] = window.start.astimezone(UTC).strftime("%Y-%m-%dT%H:00")
            base["end_hour"] = window.end.astimezone(UTC).strftime("%Y-%m-%dT%H:00")
        else:
            base["start_date"] = window.start.astimezone(UTC).date().isoformat()
            base["end_date"] = (window.end - timedelta(seconds=1)).astimezone(UTC).date().isoformat()
        url = URLS[(api, window.mode)]
        return [Request(url, params={**base, "latitude": loc["lat"], "longitude": loc["lon"]},
                        meta={"node": node, "api": api})
                for node, loc in self.cfg.raw["nodes"].items()
                if api != "satellite" or loc.get("kind", "solar") == "solar"]

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        try:
            hourly = json.loads(payload)["hourly"]
            times = [datetime.fromisoformat(t).replace(tzinfo=UTC) for t in hourly["time"]]
        except (ValueError, KeyError, TypeError):
            raise ContractError("Open-Meteo: missing hourly.time") from None
        out = ParseResult()
        node = str(req.meta["node"])
        for name, spec in self._variables(ds).items():
            param = spec["param"]
            if param in DERIVED:
                a, b = (hourly.get(p) for p in DERIVED[param])
                if a is None or b is None:
                    raise ContractError(f"Open-Meteo: inputs for {param} missing")
                if len(a) != len(b):
                    raise ContractError(f"Open-Meteo: inputs for {param} differ in length")
                column = [wind_at_100m(x, y) for x, y in zip(a, b, strict=True)]
                out.step(f"derived:{name}(power_law_80_120m)", len(column))
            elif param in hourly:
                column = hourly[param]
            else:
                raise ContractError(f"Open-Meteo: variable {param} missing")
            if len(column) != len(times):
                raise ContractError(f"Open-Meteo: {param} length differs from time axis")
            for when, raw in zip(times, column, strict=True):
                value = self.number(raw, f"Open-Meteo {param}")
                if value is None:
                    out.step("null_dropped")
                    continue
                out.observations.append(self.obs(name, node, when, value, spec["unit"]))
        return out
