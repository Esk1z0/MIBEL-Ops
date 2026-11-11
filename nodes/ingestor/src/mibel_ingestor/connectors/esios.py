"""ESIOS (REE) indicators — needs ``ESIOS_TOKEN``.

One dataset per indicator, declared in YAML (``indicator``, ``unit`` and either ``geo`` + ``node`` or a
``geos: {geo_name: node}`` map that splits one response into several nodes, e.g. indicator 600 for
Spain and France in a single request). Adding an indicator is a configuration change.
ESIOS keeps only the latest version of each forecast, so forecast vintages exist only if they are
captured live (docs/05_data_sources.md §7.1).
"""

from __future__ import annotations

import json

from ..model import ContractError, ParseResult, Request, Window, iso, parse_iso
from .base import Connector, Dataset, register

URL = "https://api.esios.ree.es/indicators/{id}"


@register
class Esios(Connector):
    name = "esios"
    secrets = ("ESIOS_TOKEN",)

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "indicator", "unit")

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        headers = {"Accept": "application/json; application/vnd.esios-api-v1+json",
                   "x-api-key": self.secret("ESIOS_TOKEN")}
        return [Request(URL.format(id=int(ds.raw["indicator"])),
                        params={"start_date": iso(window.start), "end_date": iso(window.end)},
                        headers=headers, secret=True,
                        meta={"indicator": int(ds.raw["indicator"]),
                              "geos": sorted(ds.raw.get("geos") or [g for g in [ds.raw.get("geo")] if g])})]

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        try:
            values = json.loads(payload)["indicator"]["values"]
        except (ValueError, KeyError, TypeError):
            raise ContractError("ESIOS: missing indicator.values") from None
        out = ParseResult()
        unit, variable = str(ds.raw["unit"]), str(ds.raw.get("variable", ds.name))
        geos: dict[str, str] | None = ds.raw.get("geos")
        if geos is None and ds.raw.get("geo"):
            geos = {str(ds.raw["geo"]): str(ds.raw.get("node", "ES"))}
        for item in values:
            if geos is None:
                node = str(ds.raw.get("node", "ES"))
            elif item.get("geo_name") in geos:
                node = geos[item["geo_name"]]
            else:
                out.step("filter:geo_not_configured")
                continue
            try:
                when = parse_iso(item["datetime_utc"])
            except KeyError:
                raise ContractError("ESIOS: value without datetime_utc") from None
            value = self.number(item.get("value"), "ESIOS value")
            if value is None:
                out.step("null_dropped")
                continue
            out.observations.append(self.obs(variable, node, when, value, unit))
        return out
