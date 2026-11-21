"""Synthetic payloads in the exact wire format of each source (invented values, no real market data).

Used by the unit tests and by the offline benchmark (benchmarks/bench.py), so the same parsing work can
be measured on any machine without network access.
"""

from __future__ import annotations

import io
import json
import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


UTC = timezone.utc
MADRID = ZoneInfo("Europe/Madrid")
ESIOS_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ENTSOE_TIME_FORMAT = "%Y-%m-%dT%H:%MZ"


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class EsiosPoint(NamedTuple):
    """One ESIOS value: when, which geography and the value (None = missing)."""

    moment: datetime
    geo_name: str
    value: float | None


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def omie_marginal(day: date, *, periods: int | None = None, header: str = "MARGINALPDBC;") -> bytes:
    """A day-ahead/IDA file: ``year;month;day;period;PT;ES;`` with one line per period."""
    period_count = periods or count_periods(day)
    lines = [header]
    for period in range(1, period_count + 1):
        spain_price = round(80 + 40 * math.sin(period / period_count * 2 * math.pi), 2)
        lines.append(f"{day.year};{day.month:02d};{day.day:02d};{period};{spain_price + 1};{spain_price};")
    lines.append("*")
    return ("\r\n".join(lines) + "\r\n").encode("latin-1")


def omie_continuous(
    day: date,
    *,
    emitted: datetime | None = None,
    no_trade: frozenset[int] | set[int] = frozenset({3}),
    periods: int | None = None,
) -> bytes:
    """A continuous-market file with emission time, decimal commas and some no-trade periods."""
    period_count = periods or count_periods(day)
    emitted = emitted or datetime(day.year, day.month, day.day, 2, 47) + timedelta(days=1)
    lines = [
        f"Precios del mercado intradiario continuo - {emitted:%d/%m/%Y - %H:%M}",
        "Año;Mes;Día;Periodo;Máximo ES;Máximo PT;Mínimo ES;Mínimo PT;Medio ES;Medio PT;",
    ]
    for period in range(1, period_count + 1):
        prices = ["0,00"] * 6 if period in no_trade else _continuous_prices(period, period_count)
        lines.append(f"{day.year};{day.month:02d};{day.day:02d};{period};" + ";".join(prices) + ";")
    return ("\r\n".join(lines) + "\r\n").encode("latin-1")


def esios(points: list[EsiosPoint]) -> bytes:
    """An ESIOS indicator response with the given values."""
    values = [_esios_value(point) for point in points]
    return json.dumps({"indicator": {"id": 1293, "name": "synthetic", "values": values}}).encode()


def redata(series: dict[str, list[tuple[datetime, float]]]) -> bytes:
    """A REData widget response with one series per title."""
    included = [_redata_series(title, points) for title, points in series.items()]
    return json.dumps({"data": {"type": "synthetic"}, "included": included}).encode()


def open_meteo(start: datetime, hours: int, params: list[str]) -> bytes:
    """An Open-Meteo hourly response; the last parameter has one missing value, as the API sends them."""
    times = [(start + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M") for hour in range(hours)]
    hourly: dict[str, list[Any]] = {"time": times}
    for index, param in enumerate(params):
        hourly[param] = [_open_meteo_value(param, index, hour) for hour in range(hours)]
    if hours > 2:
        hourly[params[-1]][1] = None
    return json.dumps({"latitude": 41.8, "longitude": -1.5, "hourly": hourly}).encode()


def mibgas(days: list[date], sheet: str = "MIBGAS Indexes") -> bytes:
    """A small MIBGAS workbook with the index sheet (the real yearly file is ~3.3 MB)."""
    from openpyxl import Workbook

    workbook = Workbook()
    workbook.active.title = "Trading Data"
    index_sheet = workbook.create_sheet(sheet)
    index_sheet.append([
        "Delivery day",
        "MIBGAS-ES Index\n[EUR/MWh]",
        "MIBGAS PVB Average Price Index Day-Ahead\n[EUR/MWh]",
    ])
    for i, day in enumerate(days):
        index_sheet.append([datetime(day.year, day.month, day.day), 30 + i * 0.1, 30.5 + i * 0.1])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def aemet(start: datetime, hours: int, station: str = "9311X") -> bytes:
    """AEMET station records; the first one omits the wind, as stations sometimes do."""
    records = [_aemet_record(start + timedelta(hours=hour), hour, station) for hour in range(hours)]
    records[0].pop("vv")
    return json.dumps(records).encode("latin-1")


def entsoe(start: datetime, points: list[float | None], *, curve: str = "A01", minutes: int = 60) -> bytes:
    """An ENTSO-E publication document; None points are omitted (as in A03 curves)."""
    end = start + timedelta(minutes=minutes * len(points))
    point_xml = "".join(
        f"<Point><position>{position}</position><quantity>{value}</quantity></Point>"
        for position, value in enumerate(points, 1)
        if value is not None
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">'
        f"<TimeSeries><curveType>{curve}</curveType><Period><timeInterval>"
        f"<start>{start:{ENTSOE_TIME_FORMAT}}</start><end>{end:{ENTSOE_TIME_FORMAT}}</end></timeInterval>"
        f"<resolution>PT{minutes}M</resolution>{point_xml}</Period></TimeSeries>"
        "</Publication_MarketDocument>"
    ).encode()


def entsoe_acknowledgement(reason: str = "No matching data found") -> bytes:
    """The document ENTSO-E returns when there is no data for the query."""
    return (
        '<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
        f"<Reason><code>999</code><text>{reason}</text></Reason></Acknowledgement_MarketDocument>"
    ).encode()


def count_periods(day: date, minutes: int = 15) -> int:
    """Number of periods of a Madrid delivery day (92/96/100 quarter-hours with DST)."""
    start = datetime.combine(day, time(0), MADRID)
    end = datetime.combine(day + timedelta(days=1), time(0), MADRID)
    # subtract in UTC: aware datetimes sharing a tzinfo are subtracted as wall-clock times
    duration = end.astimezone(UTC) - start.astimezone(UTC)
    return int(duration.total_seconds() // (minutes * 60))


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _spanish_number(value: float) -> str:
    """Format a number the OMIE way: ``1.234,56``."""
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _continuous_prices(period: int, period_count: int) -> list[str]:
    """The six price columns of one continuous period: max ES/PT, min ES/PT, VWAP ES/PT."""
    vwap = 95 + 30 * math.cos(period / period_count * 2 * math.pi)
    return [
        _spanish_number(vwap + 20),
        _spanish_number(vwap + 21),
        _spanish_number(vwap - 15),
        _spanish_number(vwap - 14),
        _spanish_number(vwap),
        _spanish_number(vwap + 0.5),
    ]


def _esios_value(point: EsiosPoint) -> dict[str, Any]:
    """One item of ``indicator.values``."""
    return {
        "value": point.value,
        "datetime_utc": point.moment.astimezone(UTC).strftime(ESIOS_TIME_FORMAT),
        "datetime": point.moment.isoformat(),
        "geo_name": point.geo_name,
        "geo_id": 8741,
    }


def _redata_series(title: str, points: list[tuple[datetime, float]]) -> dict[str, Any]:
    """One item of ``included`` with its values."""
    values = [
        {"value": value, "percentage": 1, "datetime": moment.isoformat(timespec="milliseconds")}
        for moment, value in points
    ]
    return {"type": title, "attributes": {"title": title, "values": values}}


def _open_meteo_value(param: str, index: int, hour: int) -> float:
    """A plausible hourly value for a parameter (wind, radiation, cloud cover or temperature)."""
    if "wind" in param:
        return round(6 + index + 2 * math.sin(hour / 6), 2)
    if "radiation" in param:
        return max(0.0, round(700 * math.sin((hour % 24 - 6) / 12 * math.pi), 1))
    if "cloud" in param:
        return float(hour % 100)
    return round(12 + 5 * math.sin(hour / 24 * 2 * math.pi), 1)


def _aemet_record(moment: datetime, hour: int, station: str) -> dict[str, Any]:
    """One AEMET observation record."""
    return {
        "idema": station,
        "fint": moment.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "vv": round(4 + hour % 5 * 0.7, 1),
        "ta": 10 + hour % 7,
        "hr": 70,
    }
