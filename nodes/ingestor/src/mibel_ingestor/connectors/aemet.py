"""AEMET OpenData station observations — optional, needs ``AEMET_API_KEY``.

Two-step API: the first call returns a temporary ``datos`` URL with the actual data.
Only the configured stations are requested (one small call each), not the national dump.
Live only: AEMET observations cover the last 24 h (no backfill through this endpoint).
"""

from __future__ import annotations

import json
from datetime import datetime

from ..model import UTC, ContractError, Mode, ParseResult, Request, Window
from .base import Connector, Dataset, register

URL = "https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/{idema}"


def parse_fint(text: str) -> datetime:
    """AEMET times are UTC, e.g. '2026-11-11T10:00:00+0000' or without offset."""
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            ts = datetime.strptime(text, fmt)
            return ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC)
        except ValueError:
            pass
    raise ContractError(f"AEMET: unexpected time {text!r}")


@register
class Aemet(Connector):
    name = "aemet"
    secrets = ("AEMET_API_KEY",)

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "stations", "variables")

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        if window.mode is Mode.BACKFILL:
            raise NotImplementedError("aemet: backfill is not supported by the observation endpoint")
        headers = {"api_key": self.secret("AEMET_API_KEY")}
        return [Request(URL.format(idema=idema), headers=headers, secret=True,
                        meta={"node": node, "station": idema})
                for node, idema in ds.raw["stations"].items()]

    def download(self, req: Request) -> bytes:
        try:
            meta = json.loads(self.http.get(req))
            datos = meta["datos"]
        except (ValueError, KeyError, TypeError):
            raise ContractError("AEMET: first call did not return a 'datos' URL") from None
        return self.http.get(Request(str(datos), secret=True))

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        try:
            records = json.loads(payload.decode("latin-1"))
        except ValueError:
            raise ContractError("AEMET: data is not JSON") from None
        if not isinstance(records, list):
            raise ContractError("AEMET: data is not a list of records")
        out = ParseResult()
        node = str(req.meta["node"])
        for rec in records:
            when = parse_fint(str(rec.get("fint", "")))
            for field, spec in ds.raw["variables"].items():
                value = self.number(rec.get(field), f"AEMET {field}")
                if value is None:
                    out.step(f"null_dropped:{field}")
                    continue
                out.observations.append(self.obs(spec["variable"], node, when, value, spec["unit"]))
        return out
