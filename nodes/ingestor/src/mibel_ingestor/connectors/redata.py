"""REData / APIdatos (REE), no key. Used as fallback and cross-check of ESIOS (FR-03).

Dates are sent in Spanish local time. ``series`` maps REData series titles to variables;
``variable_prefix`` captures every series as ``<prefix><slug(title)>`` instead.
"""

from __future__ import annotations

import json
import re
from typing import Any

from ..model import ContractError, ParseResult, Request, Window, parse_iso
from .base import Connector, Dataset, register

URL = "https://apidatos.ree.es/en/datos/{category}/{widget}"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@register
class Redata(Connector):
    name = "redata"

    def validate(self, ds: Dataset) -> None:
        self.require(ds, "category", "widget", "unit")

    def requests(self, ds: Dataset, window: Window) -> list[Request]:
        fmt = "%Y-%m-%dT%H:%M"
        params = {"start_date": window.start.astimezone(self.tz).strftime(fmt),
                  "end_date": window.end.astimezone(self.tz).strftime(fmt),
                  "time_trunc": ds.raw.get("time_trunc", "hour")}
        params.update(ds.raw.get("params", {}))
        return [Request(URL.format(category=ds.raw["category"], widget=ds.raw["widget"]), params=params,
                        meta={"widget": f"{ds.raw['category']}/{ds.raw['widget']}"})]

    def parse(self, ds: Dataset, req: Request, payload: bytes) -> ParseResult:
        try:
            js = json.loads(payload)
        except ValueError:
            raise ContractError("REData: response is not JSON") from None
        if not isinstance(js, dict) or "errors" in js or "included" not in js:
            raise ContractError(f"REData: unexpected document ({str(js.get('errors', ''))[:120]})"
                                if isinstance(js, dict) else "REData: unexpected document")
        out = ParseResult()
        series: dict[str, str] = ds.raw.get("series", {})
        prefix = ds.raw.get("variable_prefix")
        node, unit = str(ds.raw.get("node", "ES")), str(ds.raw["unit"])

        def collect(item: dict[str, Any]) -> None:
            attrs = item.get("attributes") or {}
            title = str(attrs.get("title", ""))
            variable = series.get(title) or (f"{prefix}{slug(title)}" if prefix else None)
            values = attrs.get("values") or []
            if variable is None:
                if values:
                    out.step(f"series_ignored:{slug(title)}", len(values))
            else:
                for v in values:
                    value = self.number(v.get("value"), "REData value")
                    if value is None:
                        out.step("null_dropped")
                        continue
                    out.observations.append(self.obs(variable, node, parse_iso(v["datetime"]), value, unit))
            for sub in attrs.get("content") or []:          # nested widgets (e.g. balance)
                collect(sub)

        for item in js["included"]:
            collect(item)
        return out
