"""ENTSO-E Transparency Platform — needs ``ENTSOE_TOKEN`` (enabled automatically when it is set).

Generic TimeSeries/Period/Point parser: each dataset declares its document query in YAML
(``document_type``, ``in_domain``, ``out_domain``, optional ``process_type``) plus ``variable``,
``node`` and ``unit``. The token travels as a query parameter, so requests are always ``secret``.
XML is parsed with the standard library (expat ≥ 2.4.1 rejects entity expansion attacks and never
fetches external entities); the payload is treated as data only.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import timedelta

from ..model import UTC, ContractError, NotAvailable, ParseResult, Request, Window, parse_iso
from .base import Connector, Dataset, register

URL = "https://web-api.tp.entsoe.eu/api"
_RES = re.compile(r"PT(\d+)M")


@register
class Entsoe(Connector):
    name = "entsoe"
    secrets = ("ENTSOE_TOKEN",)

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "document_type", "in_domain", "variable", "unit")

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        fmt = "%Y%m%d%H%M"
        params = {"documentType": ds.raw["document_type"], "in_Domain": ds.raw["in_domain"],
                  "periodStart": window.start.astimezone(UTC).strftime(fmt),
                  "periodEnd": window.end.astimezone(UTC).strftime(fmt),
                  "securityToken": self.secret("ENTSOE_TOKEN")}
        if "out_domain" in ds.raw:
            params["out_Domain"] = ds.raw["out_domain"]
        if "process_type" in ds.raw:
            params["processType"] = ds.raw["process_type"]
        return [Request(URL, params=params, secret=True, meta={"document_type": ds.raw["document_type"]})]

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        try:
            root = ET.fromstring(payload)
        except ET.ParseError:
            raise ContractError("ENTSO-E: response is not XML") from None
        ns = {"ns": root.tag.split("}")[0].strip("{")} if root.tag.startswith("{") else {"ns": ""}
        if root.tag.endswith("Acknowledgement_MarketDocument"):
            reason = root.findtext(".//ns:Reason/ns:text", default="no data", namespaces=ns)
            raise NotAvailable(f"ENTSO-E acknowledgement: {reason[:120]}")
        out = ParseResult()
        variable, node, unit = str(ds.raw["variable"]), str(ds.raw.get("node", "ES")), str(ds.raw["unit"])
        for ts in root.findall("ns:TimeSeries", ns):
            curve = ts.findtext("ns:curveType", default="A01", namespaces=ns)
            for period in ts.findall("ns:Period", ns):
                start_text = period.findtext("ns:timeInterval/ns:start", namespaces=ns)
                res_text = period.findtext("ns:resolution", default="", namespaces=ns)
                m = _RES.fullmatch(res_text)
                if start_text is None or m is None:
                    raise ContractError(f"ENTSO-E: unsupported period ({res_text!r})")
                t0, step = parse_iso(start_text), timedelta(minutes=int(m.group(1)))
                points = {}
                for pt in period.findall("ns:Point", ns):
                    pos = int(pt.findtext("ns:position", default="0", namespaces=ns))
                    points[pos] = self.number(pt.findtext("ns:quantity", namespaces=ns), "ENTSO-E quantity")
                last = None
                for pos in range(1, max(points, default=0) + 1):
                    value = points.get(pos)
                    if value is None and curve == "A03":       # A03: a point holds until the next one
                        value = last
                        out.step("A03_forward_fill")
                    if value is None:
                        continue
                    last = value
                    out.observations.append(self.obs(variable, node, t0 + step * (pos - 1), value, unit))
        return out
