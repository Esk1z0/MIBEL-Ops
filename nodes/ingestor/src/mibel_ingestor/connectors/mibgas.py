"""MIBGAS yearly Excel file (no key). Daily gas indices in sheet ``MIBGAS Indexes``.

Needs the optional ``excel`` extra (openpyxl), imported only when this source is enabled. The workbook
is read in read-only, values-only mode to keep memory low on the Pi. Parsing the 3.3 MB yearly file is
the costliest operation of the node (~3 s of CPU on a Pi 3B+), which is why it runs every 12 h.
"""

from __future__ import annotations

import io
import re
from datetime import date, datetime
from typing import Any

from ..model import UTC, ContractError, ParseResult, Request, Window
from .base import Connector, Dataset, register


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


URL = "https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx"
NODE = "ES"
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")
_UNIT_SUFFIX = re.compile(r"\s*\[.*?\]\s*")


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def clean_header(header: Any) -> str:
    """Strip the unit and line breaks from a header: ``'MIBGAS-ES Index\\n[EUR/MWh]'`` → ``'MIBGAS-ES Index'``."""
    return _UNIT_SUFFIX.sub("", str(header or "")).replace("\n", " ").strip()


@register
class Mibgas(Connector):
    """Connector for the MIBGAS yearly workbook."""

    name = "mibgas"

    def validate(self, dataset: Dataset) -> None:
        """Require the sheet, the date column, the wanted columns and the unit."""
        self.require_options(dataset, "sheet", "date_column", "columns", "unit")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request the yearly workbook of every year touched by the window."""
        years = sorted({day.year for day in window.local_days(self.timezone)})
        return [
            Request(URL.format(year=year), params={"path": f"AGNO_{year}/XLS"}, meta={"year": year})
            for year in years
        ]

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse the configured index columns into one daily observation per column.

        Raises:
            ContractError: the payload is not a workbook, or the sheet or columns are missing.
        """
        workbook = _open_workbook(payload)
        try:
            sheet_name = dataset.options["sheet"]
            if sheet_name not in workbook.sheetnames:
                raise ContractError(f"MIBGAS: sheet {sheet_name!r} not found")
            rows = workbook[sheet_name].iter_rows(values_only=True)
            header = [clean_header(cell) for cell in next(rows, ())]
            date_index = _column_index(header, dataset.options["date_column"])
            variable_by_index = {
                _column_index(header, column): variable for column, variable in dataset.options["columns"].items()
            }
            return self._parse_rows(dataset, rows, date_index, variable_by_index)
        finally:
            workbook.close()

    # --- parsing ---
    def _parse_rows(
        self,
        dataset: Dataset,
        rows: Any,
        date_index: int,
        variable_by_index: dict[int, str],
    ) -> ParseResult:
        """Turn the data rows into observations; rows without a date are counted and skipped."""
        result = ParseResult()
        unit = str(dataset.options["unit"])
        for row in rows:
            day = _parse_day(_cell(row, date_index))
            if day is None:
                result.count_step("row_without_date")
                continue
            start = datetime(day.year, day.month, day.day, tzinfo=self.timezone).astimezone(UTC)
            for index, variable in variable_by_index.items():
                value = self.parse_number(_cell(row, index), f"MIBGAS {variable}")
                if value is not None:
                    result.add(self.make_observation(variable, NODE, start, value, unit))
        return result


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _open_workbook(payload: bytes) -> Any:
    """Open the payload as a read-only workbook.

    Raises:
        RuntimeError: the ``excel`` extra (openpyxl) is not installed.
        ContractError: the payload is not a valid workbook.
    """
    try:
        from openpyxl import load_workbook
    except ImportError:
        raise RuntimeError("mibgas needs the 'excel' extra: pip install 'mibel-ingestor[excel]'") from None
    try:
        return load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
    except Exception as exc:  # zip/xml errors of many kinds: the payload is simply not a workbook
        raise ContractError(f"MIBGAS: not a valid workbook ({type(exc).__name__})") from None


def _column_index(header: list[str], column: str) -> int:
    """Return the position of a column in the cleaned header.

    Raises:
        ContractError: the column is not in the header.
    """
    try:
        return header.index(column)
    except ValueError:
        raise ContractError(f"MIBGAS: expected column {column!r} not found in header") from None


def _cell(row: tuple[Any, ...], index: int) -> Any:
    """Return a cell of a row, or None when the row is shorter (trailing empty cells)."""
    return row[index] if index < len(row) else None


def _parse_day(value: Any) -> date | None:
    """Return the date in a cell (datetime, date or text), or None if it holds no date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(value.strip()[:10], date_format).date()
        except ValueError:
            continue
    return None
