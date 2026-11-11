"""OMIE public files (no key): day-ahead, intraday auctions (IDA) and continuous intraday prices.

Formats confirmed in wip/ingestion/01 (docs/05_data_sources.md §7):
- ``marginalpdbc_YYYYMMDD.1`` / ``marginalpibc_YYYYMMDDSS.1``: ``year;month;day;period;PT;ES;`` (dot decimal).
- ``precios_pibcic_YYYYMMDD.1``: Latin-1, decimal comma, emission time in the first line,
  max/min/VWAP per zone. ``max = min = VWAP = 0`` means "no trades" → dropped (DG-E16).
Periods are local quarter-hours since 2025-10-01 (92/96/100 per day), hours before.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta

from ..model import UTC, ContractError, ParseResult, Request, Window
from ..settings import ConfigError
from .base import Connector, Dataset, register

URL = "https://www.omie.es/es/file-download"
FILES = ("marginalpdbc", "marginalpibc", "precios_pibcic")
_EMISSION = re.compile(r"(\d{2})/(\d{2})/(\d{4})\s*-\s*(\d{2}):(\d{2})")


@register
class Omie(Connector):
    name = "omie"

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "file")
        if ds.raw["file"] not in FILES:
            raise ConfigError(f"omie dataset {ds.name!r}: file must be one of {FILES}")

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        kind = ds.raw["file"]
        sessions = [f"{int(s):02d}" for s in ds.raw.get("sessions", [1, 2, 3])] if kind == "marginalpibc" else [""]
        out = []
        for day in window.days(self.tz):
            for session in sessions:
                filename = f"{kind}_{day:%Y%m%d}{session}.1"
                out.append(Request(URL, params={"parents": kind, "filename": filename},
                                   meta={"file": filename, "day": day.isoformat(), "session": session}))
        return out

    # -- parsing ----------------------------------------------------------------------------
    def _starts(self, day: date, periods: int) -> tuple[datetime, timedelta]:
        """First period start (UTC) and step. Adding absolute minutes to local midnight is DST-safe."""
        step = timedelta(minutes=15 if periods > 25 else 60)
        return datetime(day.year, day.month, day.day, tzinfo=self.tz).astimezone(UTC), step

    def _rows(self, payload: bytes, min_fields: int) -> list[list[str]]:
        rows = []
        for line in payload.decode("latin-1").splitlines():
            parts = line.strip().split(";")
            if len(parts) >= min_fields and parts[0].isdigit():
                rows.append(parts)
        if not rows:
            raise ContractError("no data lines in OMIE file")
        return rows

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        if payload.lstrip()[:1] == b"<":
            raise ContractError("OMIE returned HTML instead of a data file")
        zones = ds.raw.get("zones", ["ES", "PT"])
        if ds.raw["file"] == "precios_pibcic":
            return self._continuous(payload, zones)
        variable = ds.raw.get("variable", "price_day_ahead")
        if ds.raw["file"] == "marginalpibc":
            variable = f"price_ida{int(req.meta['session'])}"
        return self._marginal(payload, variable, zones)

    def _marginal(self, payload: bytes, variable: str, zones: list[str]) -> ParseResult:
        out = ParseResult()
        rows = self._rows(payload, 6)
        n = max(int(r[3]) for r in rows)
        for r in rows:
            day = date(int(r[0]), int(r[1]), int(r[2]))
            t0, step = self._starts(day, n)
            start = t0 + step * (int(r[3]) - 1)
            for zone, raw in (("PT", r[4]), ("ES", r[5])):
                if zone in zones:
                    value = self.number(raw, "OMIE price")
                    if value is not None:
                        out.observations.append(self.obs(variable, zone, start, value, "EUR/MWh"))
        return out

    @staticmethod
    def _comma(text: str) -> float | None:
        text = text.strip()
        if not text:
            return None
        try:
            return float(text.replace(".", "").replace(",", "."))
        except ValueError:
            raise ContractError(f"OMIE continuous: not a number: {text!r}") from None

    def _continuous(self, payload: bytes, zones: list[str]) -> ParseResult:
        out = ParseResult()
        first = payload[:200].decode("latin-1")
        m = _EMISSION.search(first)
        emitted = None
        if m:
            d, mo, y, hh, mm = (int(x) for x in m.groups())
            emitted = datetime(y, mo, d, hh, mm, tzinfo=self.tz).astimezone(UTC)
        else:
            out.step("emission_time_missing")
        rows = self._rows(payload, 10)
        n = max(int(r[3]) for r in rows)
        for r in rows:
            day = date(int(r[0]), int(r[1]), int(r[2]))
            t0, step = self._starts(day, n)
            start = t0 + step * (int(r[3]) - 1)
            # columns: max ES, max PT, min ES, min PT, VWAP ES, VWAP PT
            for zone, (i_max, i_min, i_avg) in (("ES", (4, 6, 8)), ("PT", (5, 7, 9))):
                if zone not in zones:
                    continue
                vmax, vmin, vavg = self._comma(r[i_max]), self._comma(r[i_min]), self._comma(r[i_avg])
                if vmax == vmin == vavg == 0.0:
                    out.step("DG-E16:no_trade_to_missing")       # no trades, not a 0 €/MWh price
                    continue
                for variable, value in (("price_continuous_vwap", vavg), ("price_continuous_max", vmax),
                                        ("price_continuous_min", vmin)):
                    if value is not None:
                        # The file states its own publication time: it is the true available_at
                        out.observations.append(self.obs(variable, zone, start, value, "EUR/MWh", emitted))
        return out
