"""ESIOS (REE) indicators — needs ``ESIOS_TOKEN``.

One dataset per indicator, declared in YAML: ``indicator``, ``unit`` and either ``geo`` + ``node`` or a
``geos: {geo_name: node}`` map that splits one response into several nodes (e.g. indicator 600 for
Spain and France in a single request). Adding an indicator is a configuration change.

ESIOS keeps only the latest version of each forecast, so forecast vintages exist only if they are
captured live (docs/05_data_sources.md §7.1).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from ..model import ContractError, ParseResult, Request, Window, parse_iso, to_iso
from .base import Connector, Dataset, register


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


URL = "https://api.esios.ree.es/indicators/{indicator}"
ACCEPT = "application/json; application/vnd.esios-api-v1+json"
DEFAULT_NODE = "ES"


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


@register
class Esios(Connector):
    """Connector for ESIOS indicators."""

    name = "esios"
    secrets = ("ESIOS_TOKEN",)

    def validate(self, dataset: Dataset) -> None:
        """Require ``indicator`` and ``unit``."""
        self.require_options(dataset, "indicator", "unit")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request the indicator values for the window (the token travels in a header)."""
        indicator = int(dataset.options["indicator"])
        headers = {"Accept": ACCEPT, "x-api-key": self.read_secret("ESIOS_TOKEN")}
        return [Request(
            URL.format(indicator=indicator),
            params={"start_date": to_iso(window.start), "end_date": to_iso(window.end)},
            headers=headers,
            secret=True,
            meta={"indicator": indicator, "geos": sorted(_node_by_geo(dataset) or {})},
        )]

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse ``indicator.values`` into observations, keeping only the configured geographies."""
        values = _indicator_values(payload)
        node_by_geo = _node_by_geo(dataset)
        default_node = str(dataset.options.get("node", DEFAULT_NODE))
        unit = str(dataset.options["unit"])
        variable = str(dataset.options.get("variable", dataset.name))
        result = ParseResult()
        for item in values:
            node = _node_of(item, node_by_geo, default_node)
            if node is None:
                result.count_step("filter:geo_not_configured")
                continue
            value = self.parse_number(item.get("value"), "ESIOS value")
            if value is None:
                result.count_step("null_dropped")
                continue
            result.add(self.make_observation(variable, node, _value_time(item), value, unit))
        return result


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _indicator_values(payload: bytes) -> list[dict[str, Any]]:
    """Return ``indicator.values`` from an ESIOS response.

    Raises:
        ContractError: the document does not contain that list.
    """
    try:
        values = json.loads(payload)["indicator"]["values"]
    except (ValueError, KeyError, TypeError):
        raise ContractError("ESIOS: missing indicator.values") from None
    if not isinstance(values, list):
        raise ContractError("ESIOS: indicator.values is not a list")
    return values


def _node_by_geo(dataset: Dataset) -> dict[str, str] | None:
    """Return the ``geo_name → node`` map, built from ``geos`` or from ``geo`` + ``node``.

    None means "no geography filter": every value belongs to the dataset's node.
    """
    geos = dataset.options.get("geos")
    if geos:
        return {str(geo): str(node) for geo, node in geos.items()}
    geo = dataset.options.get("geo")
    if geo:
        return {str(geo): str(dataset.options.get("node", DEFAULT_NODE))}
    return None


def _node_of(item: dict[str, Any], node_by_geo: dict[str, str] | None, default_node: str) -> str | None:
    """Return the node of a value, or None when its geography is not configured."""
    if node_by_geo is None:
        return default_node
    return node_by_geo.get(str(item.get("geo_name")))


def _value_time(item: dict[str, Any]) -> datetime:
    """Return the UTC time of an ESIOS value.

    Raises:
        ContractError: the value has no ``datetime_utc``.
    """
    try:
        return parse_iso(item["datetime_utc"])
    except KeyError:
        raise ContractError("ESIOS: value without datetime_utc") from None
