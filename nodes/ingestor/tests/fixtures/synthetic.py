"""Synthetic payloads in the exact wire format of each source (invented values, no real market data).

Used by the unit tests and by the offline benchmark (benchmarks/bench.py), so the same parsing work
can be measured on any machine without network access.
"""

from __future__ import annotations

import io
import json
import math
from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc


def _periods(day: date, tz: object, minutes: int = 15) -> int:
    from zoneinfo import ZoneInfo
    z = tz if isinstance(tz, ZoneInfo) else ZoneInfo("Europe/Madrid")
    start = datetime(day.year, day.month, day.day, tzinfo=z)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), z)
    # subtract in UTC: aware datetimes sharing a tzinfo are subtracted as wall-clock times
    return int((end.astimezone(UTC) - start.astimezone(UTC)).total_seconds() // (minutes * 60))


def omie_marginal(day: date, *, periods: int | None = None, header: str = "MARGINALPDBC;") -> bytes:
    n = periods or _periods(day, None)
    lines = [header]
    for p in range(1, n + 1):
        es = round(80 + 40 * math.sin(p / n * 2 * math.pi), 2)
        lines.append(f"{day.year};{day.month:02d};{day.day:02d};{p};{es + 1};{es};")
    lines.append("*")
    return ("\r\n".join(lines) + "\r\n").encode("latin-1")


def omie_continuous(day: date, *, emitted: datetime | None = None, no_trade: set[int] = frozenset({3}),
                    periods: int | None = None) -> bytes:
    n = periods or _periods(day, None)
    emitted = emitted or datetime(day.year, day.month, day.day, 2, 47) + timedelta(days=1)
    lines = [f"Precios del mercado intradiario continuo - {emitted:%d/%m/%Y - %H:%M}",
             "Año;Mes;Día;Periodo;Máximo ES;Máximo PT;Mínimo ES;Mínimo PT;Medio ES;Medio PT;"]

    def c(x: float) -> str:
        return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    for p in range(1, n + 1):
        if p in no_trade:
            vals = ["0,00"] * 6
        else:
            avg = 95 + 30 * math.cos(p / n * 2 * math.pi)
            vals = [c(avg + 20), c(avg + 21), c(avg - 15), c(avg - 14), c(avg), c(avg + 0.5)]
        lines.append(f"{day.year};{day.month:02d};{day.day:02d};{p};" + ";".join(vals) + ";")
    return ("\r\n".join(lines) + "\r\n").encode("latin-1")


def esios(points: list[tuple[datetime, str, float | None]]) -> bytes:
    values = [{"value": v, "datetime_utc": t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "datetime": t.isoformat(), "geo_name": g, "geo_id": 8741} for t, g, v in points]
    return json.dumps({"indicator": {"id": 1293, "name": "synthetic", "values": values}}).encode()


def redata(series: dict[str, list[tuple[datetime, float]]]) -> bytes:
    included = [{"type": title, "attributes": {"title": title, "values": [
        {"value": v, "percentage": 1, "datetime": t.isoformat(timespec="milliseconds")} for t, v in pts]}}
        for title, pts in series.items()]
    return json.dumps({"data": {"type": "synthetic"}, "included": included}).encode()


def open_meteo(start: datetime, hours: int, params: list[str]) -> bytes:
    times = [(start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours)]
    hourly: dict[str, list[object]] = {"time": times}
    for i, p in enumerate(params):
        if "wind" in p:
            hourly[p] = [round(6 + i + 2 * math.sin(h / 6), 2) for h in range(hours)]
        elif "radiation" in p:
            hourly[p] = [max(0.0, round(700 * math.sin((h % 24 - 6) / 12 * math.pi), 1)) for h in range(hours)]
        elif "cloud" in p:
            hourly[p] = [h % 100 for h in range(hours)]
        else:
            hourly[p] = [round(12 + 5 * math.sin(h / 24 * 2 * math.pi), 1) for h in range(hours)]
    if hours > 2:
        hourly[params[-1]][1] = None                       # a missing value, as Open-Meteo sends them
    return json.dumps({"latitude": 41.8, "longitude": -1.5, "hourly": hourly}).encode()


def mibgas(days: list[date], sheet: str = "MIBGAS Indexes") -> bytes:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Trading Data"
    idx = wb.create_sheet(sheet)
    idx.append(["Delivery day", "MIBGAS-ES Index\n[EUR/MWh]", "MIBGAS PVB Average Price Index Day-Ahead\n[EUR/MWh]"])
    for i, d in enumerate(days):
        idx.append([datetime(d.year, d.month, d.day), 30 + i * 0.1, 30.5 + i * 0.1])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def aemet(start: datetime, hours: int, station: str = "9311X") -> bytes:
    recs = [{"idema": station, "fint": (start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
             "vv": round(4 + h % 5 * 0.7, 1), "ta": 10 + h % 7, "hr": 70} for h in range(hours)]
    recs[0].pop("vv")                                      # stations sometimes omit a variable
    return json.dumps(recs).encode("latin-1")


def entsoe(start: datetime, points: list[float | None], *, curve: str = "A01", minutes: int = 60) -> bytes:
    end = start + timedelta(minutes=minutes * len(points))
    pts = "".join(f"<Point><position>{i}</position><quantity>{v}</quantity></Point>"
                  for i, v in enumerate(points, 1) if v is not None)
    fmt = "%Y-%m-%dT%H:%MZ"
    return (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">'
            f"<TimeSeries><curveType>{curve}</curveType><Period><timeInterval><start>{start:{fmt}}</start>"
            f"<end>{end:{fmt}}</end></timeInterval><resolution>PT{minutes}M</resolution>{pts}</Period>"
            f"</TimeSeries></Publication_MarketDocument>").encode()


def entsoe_ack(reason: str = "No matching data found") -> bytes:
    return (f'<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
            f"<Reason><code>999</code><text>{reason}</text></Reason></Acknowledgement_MarketDocument>").encode()
