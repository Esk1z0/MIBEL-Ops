"""OMIE public files (no key): day-ahead, intraday auctions (IDA) and continuous intraday prices.

Formats confirmed in the exploration notebook (docs/05_data_sources.md §7):

- ``marginalpdbc_YYYYMMDD.1`` (day-ahead) and ``marginalpibc_YYYYMMDDSS.1`` (IDA session SS):
  ``year;month;day;period;PT;ES;`` with dot decimals.
- ``precios_pibcic_YYYYMMDD.1`` (continuous): Latin-1, decimal comma, emission time in the first line,
  ``year;month;day;period;max ES;max PT;min ES;min PT;VWAP ES;VWAP PT``.
  ``max = min = VWAP = 0`` means "no trades", so the period is dropped as missing (DG-E16).

Periods are local quarter-hours since 2025-10-01 (92/96/100 per day) and hours before.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import NamedTuple

from ..model import UTC, ContractError, ParseResult, Request, Window
from ..settings import ConfigError
from .base import Connector, Dataset, register

# --------------------------------------------------------------------------- constants
URL = "https://www.omie.es/es/file-download"
DAY_AHEAD_FILE = "marginalpdbc"
INTRADAY_AUCTION_FILE = "marginalpibc"
CONTINUOUS_FILE = "precios_pibcic"
FILES = (DAY_AHEAD_FILE, INTRADAY_AUCTION_FILE, CONTINUOUS_FILE)
UNIT = "EUR/MWh"
DEFAULT_ZONES = ("ES", "PT")
DEFAULT_SESSIONS = (1, 2, 3)
# A day has at most 25 hourly periods (DST change); more periods means quarter-hours
MAX_HOURLY_PERIODS = 25

# Column positions shared by every OMIE price file
COLUMN_YEAR = 0
COLUMN_MONTH = 1
COLUMN_DAY = 2
COLUMN_PERIOD = 3
MARGINAL_MIN_FIELDS = 6
CONTINUOUS_MIN_FIELDS = 10
MARGINAL_PRICE_COLUMN = {"PT": 4, "ES": 5}

_EMISSION_TIME = re.compile(r"(\d{2})/(\d{2})/(\d{4})\s*-\s*(\d{2}):(\d{2})")
_HEADER_BYTES = 200


# --------------------------------------------------------------------------- types
class ContinuousColumns(NamedTuple):
    """Column positions of one zone in the continuous file."""

    zone: str
    max_price: int
    min_price: int
    vwap: int


CONTINUOUS_COLUMNS = (
    ContinuousColumns(zone="ES", max_price=4, min_price=6, vwap=8),
    ContinuousColumns(zone="PT", max_price=5, min_price=7, vwap=9),
)


class PeriodGrid(NamedTuple):
    """How to turn a period number into a UTC start for one delivery day."""

    first_start: datetime
    step: timedelta

    def start_of(self, period: int) -> datetime:
        """Return the UTC start of a 1-based period."""
        return self.first_start + self.step * (period - 1)


# --------------------------------------------------------------------------- public API
@register
class Omie(Connector):
    """Connector for OMIE's daily price files."""

    name = "omie"

    def validate(self, dataset: Dataset) -> None:
        """Require a known ``file`` kind."""
        self.require_options(dataset, "file")
        if dataset.options["file"] not in FILES:
            raise ConfigError(f"omie dataset {dataset.name!r}: file must be one of {FILES}")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request one file per local day (and per IDA session for intraday auctions)."""
        kind = dataset.options["file"]
        sessions = self._session_suffixes(dataset)
        requests = []
        for day in window.local_days(self.timezone):
            for session in sessions:
                filename = f"{kind}_{day:%Y%m%d}{session}.1"
                requests.append(Request(
                    URL,
                    params={"parents": kind, "filename": filename},
                    meta={"file": filename, "day": day.isoformat(), "session": session},
                ))
        return requests

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse a price file into observations per zone.

        Raises:
            ContractError: OMIE returned an HTML page or a file without data lines.
        """
        if payload.lstrip()[:1] == b"<":
            raise ContractError("OMIE returned HTML instead of a data file")
        zones = list(dataset.options.get("zones", DEFAULT_ZONES))
        kind = dataset.options["file"]
        if kind == CONTINUOUS_FILE:
            return self._parse_continuous(payload, zones)
        if kind == INTRADAY_AUCTION_FILE:
            variable = f"price_ida{int(request.meta['session'])}"
        else:
            variable = str(dataset.options.get("variable", "price_day_ahead"))
        return self._parse_marginal(payload, variable, zones)

    # --- requests ---
    @staticmethod
    def _session_suffixes(dataset: Dataset) -> list[str]:
        """Return the two-digit IDA session suffixes, or a single empty suffix for daily files."""
        if dataset.options["file"] != INTRADAY_AUCTION_FILE:
            return [""]
        sessions = dataset.options.get("sessions", DEFAULT_SESSIONS)
        return [f"{int(session):02d}" for session in sessions]

    # --- parsing ---
    def _parse_marginal(self, payload: bytes, variable: str, zones: list[str]) -> ParseResult:
        """Parse a day-ahead or IDA file: one price per zone and period."""
        result = ParseResult()
        rows = _data_rows(payload, MARGINAL_MIN_FIELDS)
        grids = self._grids_by_day(rows)
        for row in rows:
            start = grids[_row_day(row)].start_of(int(row[COLUMN_PERIOD]))
            for zone in zones:
                if zone not in MARGINAL_PRICE_COLUMN:
                    continue
                value = self.parse_number(row[MARGINAL_PRICE_COLUMN[zone]], "OMIE price")
                if value is not None:
                    result.add(self.make_observation(variable, zone, start, value, UNIT))
        return result

    def _parse_continuous(self, payload: bytes, zones: list[str]) -> ParseResult:
        """Parse the continuous file: max, min and VWAP per zone and period.

        The file states its own emission time, which is the true ``available_at`` of its values.
        """
        result = ParseResult()
        emitted_at = self._emission_time(payload)
        if emitted_at is None:
            result.count_step("emission_time_missing")
        rows = _data_rows(payload, CONTINUOUS_MIN_FIELDS)
        grids = self._grids_by_day(rows)
        for row in rows:
            start = grids[_row_day(row)].start_of(int(row[COLUMN_PERIOD]))
            for columns in CONTINUOUS_COLUMNS:
                if columns.zone not in zones:
                    continue
                self._add_continuous_zone(result, row, columns, start, emitted_at)
        return result

    def _add_continuous_zone(
        self,
        result: ParseResult,
        row: list[str],
        columns: ContinuousColumns,
        start: datetime,
        emitted_at: datetime | None,
    ) -> None:
        """Add the three continuous prices of one zone and period, or record a no-trade period."""
        max_price = _parse_comma_number(row[columns.max_price])
        min_price = _parse_comma_number(row[columns.min_price])
        vwap = _parse_comma_number(row[columns.vwap])
        if max_price == min_price == vwap == 0.0:
            # DG-E16: no trades in this period, not a real price of 0 €/MWh
            result.count_step("DG-E16:no_trade_to_missing")
            return
        prices = {
            "price_continuous_vwap": vwap,
            "price_continuous_max": max_price,
            "price_continuous_min": min_price,
        }
        for variable, value in prices.items():
            if value is None:
                continue
            result.add(self.make_observation(variable, columns.zone, start, value, UNIT, emitted_at))

    def _emission_time(self, payload: bytes) -> datetime | None:
        """Return the emission time stated in the file header, in UTC, if present."""
        header = payload[:_HEADER_BYTES].decode("latin-1")
        match = _EMISSION_TIME.search(header)
        if match is None:
            return None
        day, month, year, hour, minute = (int(part) for part in match.groups())
        return datetime(year, month, day, hour, minute, tzinfo=self.timezone).astimezone(UTC)

    def _grids_by_day(self, rows: list[list[str]]) -> dict[date, PeriodGrid]:
        """Return the period grid of every delivery day in the file (computed once per day, not per row)."""
        period_count = _period_count(rows)
        return {day: self._period_grid(day, period_count) for day in {_row_day(row) for row in rows}}

    def _period_grid(self, day: date, period_count: int) -> PeriodGrid:
        """Return the first UTC period start and step of a delivery day.

        Adding absolute minutes to local midnight converted to UTC is DST-safe: 23- and 25-hour days
        simply have fewer or more periods.
        """
        step = timedelta(minutes=15 if period_count > MAX_HOURLY_PERIODS else 60)
        local_midnight = datetime(day.year, day.month, day.day, tzinfo=self.timezone)
        return PeriodGrid(first_start=local_midnight.astimezone(UTC), step=step)


# --------------------------------------------------------------------------- private helpers
def _data_rows(payload: bytes, min_fields: int) -> list[list[str]]:
    """Return the data lines split by ``;`` (header and footer lines are skipped).

    Raises:
        ContractError: the file has no data line at all.
    """
    rows = []
    for line in payload.decode("latin-1").splitlines():
        fields = line.strip().split(";")
        if len(fields) >= min_fields and fields[COLUMN_YEAR].isdigit():
            rows.append(fields)
    if not rows:
        raise ContractError("no data lines in OMIE file")
    return rows


def _period_count(rows: list[list[str]]) -> int:
    """Return the highest period number of the file, which tells hourly from quarter-hourly files."""
    return max(int(row[COLUMN_PERIOD]) for row in rows)


def _row_day(row: list[str]) -> date:
    """Return the delivery day of a data row."""
    return date(int(row[COLUMN_YEAR]), int(row[COLUMN_MONTH]), int(row[COLUMN_DAY]))


def _parse_comma_number(text: str) -> float | None:
    """Parse a Spanish-format number (``1.234,56``); empty means missing.

    Raises:
        ContractError: the text is not a number.
    """
    text = text.strip()
    if not text:
        return None
    try:
        return float(text.replace(".", "").replace(",", "."))
    except ValueError:
        raise ContractError(f"OMIE continuous: not a number: {text!r}") from None
