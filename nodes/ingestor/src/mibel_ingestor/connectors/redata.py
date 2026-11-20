"""REData / APIdatos (REE), no key. Used as fallback and cross-check of ESIOS (FR-03).

Dates are sent in Spanish local time. ``series`` maps REData series titles to variables;
``variable_prefix`` captures every series as ``<prefix><slug(title)>`` instead (e.g. installed
capacity per technology for DG-E11). Widgets may nest series inside ``attributes.content``.
"""

from __future__ import annotations

import json
import re
from typing import Any

from ..model import ContractError, ParseResult, Request, Window, parse_iso
from .base import Connector, Dataset, register


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


URL = "https://apidatos.ree.es/en/datos/{category}/{widget}"
LOCAL_TIME_FORMAT = "%Y-%m-%dT%H:%M"
DEFAULT_NODE = "ES"
ERROR_PREVIEW_CHARS = 120


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def slugify(text: str) -> str:
    """Turn a series title into a lowercase identifier: ``'Solar photovoltaic'`` → ``'solar_photovoltaic'``."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@register
class Redata(Connector):
    """Connector for REData widgets."""

    name = "redata"

    def validate(self, dataset: Dataset) -> None:
        """Require ``category``, ``widget`` and ``unit``."""
        self.require_options(dataset, "category", "widget", "unit")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request one widget for the window, with dates in local time as REData expects."""
        category = dataset.options["category"]
        widget = dataset.options["widget"]
        params = {
            "start_date": window.start.astimezone(self.timezone).strftime(LOCAL_TIME_FORMAT),
            "end_date": window.end.astimezone(self.timezone).strftime(LOCAL_TIME_FORMAT),
            "time_trunc": dataset.options.get("time_trunc", "hour"),
        }
        params.update(dataset.options.get("params", {}))
        return [Request(
            URL.format(category=category, widget=widget),
            params=params,
            meta={"widget": f"{category}/{widget}"},
        )]

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse every series of the widget (nested ones included) into observations."""
        document = _load_document(payload)
        result = ParseResult()
        for item in document["included"]:
            self._collect_series(dataset, item, result)
        return result

    # --- parsing ---
    def _collect_series(self, dataset: Dataset, item: dict[str, Any], result: ParseResult) -> None:
        """Add the values of one series and recurse into its nested series."""
        attributes = item.get("attributes") or {}
        title = str(attributes.get("title", ""))
        values = attributes.get("values") or []
        variable = _variable_for(dataset, title)
        if variable is None:
            if values:
                result.count_step(f"series_ignored:{slugify(title)}", len(values))
        else:
            self._add_values(dataset, variable, values, result)
        for nested in attributes.get("content") or []:
            self._collect_series(dataset, nested, result)

    def _add_values(self, dataset: Dataset, variable: str, values: list[dict[str, Any]], result: ParseResult) -> None:
        """Add the points of one series as observations of ``variable``."""
        node = str(dataset.options.get("node", DEFAULT_NODE))
        unit = str(dataset.options["unit"])
        for point in values:
            value = self.parse_number(point.get("value"), "REData value")
            if value is None:
                result.count_step("null_dropped")
                continue
            moment = point.get("datetime")
            if moment is None:
                raise ContractError("REData: value without datetime")
            result.add(self.make_observation(variable, node, parse_iso(moment), value, unit))


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _load_document(payload: bytes) -> dict[str, Any]:
    """Return a REData JSON document that has an ``included`` list.

    Raises:
        ContractError: not JSON, an error document, or no ``included`` key.
    """
    try:
        document = json.loads(payload)
    except ValueError:
        raise ContractError("REData: response is not JSON") from None
    if not isinstance(document, dict):
        raise ContractError("REData: unexpected document")
    if "errors" in document or "included" not in document:
        errors = str(document.get("errors", ""))[:ERROR_PREVIEW_CHARS]
        raise ContractError(f"REData: unexpected document ({errors})")
    return document


def _variable_for(dataset: Dataset, title: str) -> str | None:
    """Return the variable of a series title, or None when the series is not configured."""
    series: dict[str, str] = dataset.options.get("series", {})
    if title in series:
        return series[title]
    prefix = dataset.options.get("variable_prefix")
    if prefix:
        return f"{prefix}{slugify(title)}"
    return None
