"""MIBGAS yearly Excel file (no key). Daily indices in sheet ``MIBGAS Indexes``.

Needs the optional ``excel`` extra (openpyxl), imported only when this source is enabled.
The workbook is read in read-only, values-only mode to keep memory low on the Pi.
"""

from __future__ import annotations

import io
import re
from datetime import date, datetime
from typing import Any

from ..model import UTC, ContractError, ParseResult, Request, Window
from .base import Connector, Dataset, register

URL = "https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx"


def clean(header: Any) -> str:
    """'MIBGAS-ES Index\\n[EUR/MWh]' -> 'MIBGAS-ES Index'."""
    return re.sub(r"\s*\[.*?\]\s*", "", str(header or "")).replace("\n", " ").strip()


@register
class Mibgas(Connector):
    name = "mibgas"

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "sheet", "date_column", "columns", "unit")

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        years = sorted({d.year for d in window.days(self.tz)})
        return [Request(URL.format(year=y), params={"path": f"AGNO_{y}/XLS"}, meta={"year": y}) for y in years]

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        try:
            from openpyxl import load_workbook
        except ImportError:
            raise RuntimeError("mibgas needs the 'excel' extra: pip install 'mibel-ingestor[excel]'") from None
        try:
            wb = load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
        except Exception as exc:                       # zip/xml errors: the payload is not a workbook
            raise ContractError(f"MIBGAS: not a valid workbook ({type(exc).__name__})") from None
        try:
            if ds.raw["sheet"] not in wb.sheetnames:
                raise ContractError(f"MIBGAS: sheet {ds.raw['sheet']!r} not found")
            rows = wb[ds.raw["sheet"]].iter_rows(values_only=True)
            header = [clean(h) for h in next(rows, ())]
            columns: dict[str, str] = ds.raw["columns"]
            try:
                i_date = header.index(ds.raw["date_column"])
                wanted = {header.index(col): var for col, var in columns.items()}
            except ValueError:
                raise ContractError("MIBGAS: expected columns not found in header") from None
            out = ParseResult()
            for row in rows:
                day = self._day(row[i_date] if i_date < len(row) else None)
                if day is None:
                    out.step("row_without_date")
                    continue
                start = datetime(day.year, day.month, day.day, tzinfo=self.tz).astimezone(UTC)
                for i, variable in wanted.items():
                    value = self.number(row[i] if i < len(row) else None, f"MIBGAS {variable}")
                    if value is not None:
                        out.observations.append(self.obs(variable, "ES", start, value, ds.raw["unit"]))
            return out
        finally:
            wb.close()

    @staticmethod
    def _day(value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(value.strip()[:10], fmt).date()
                except ValueError:
                    pass
        return None
