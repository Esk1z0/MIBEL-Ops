"""ENTSO-E Transparency Platform — needs ``ENTSOE_TOKEN`` (enabled automatically when it is set).

Generic TimeSeries/Period/Point parser: each dataset declares its document query in YAML
(``document_type``, ``in_domain``, optional ``out_domain`` and ``process_type``) plus ``variable``,
``node`` and ``unit``. The token travels as a query parameter, so requests are always ``secret``.

XML is parsed with the standard library (expat ≥ 2.4.1 rejects entity expansion attacks and never
fetches external entities); the payload is treated as data only.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta

from ..model import UTC, ContractError, NotAvailable, ParseResult, Request, Window, parse_iso
from .base import Connector, Dataset, register


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


URL = "https://web-api.tp.entsoe.eu/api"
PERIOD_FORMAT = "%Y%m%d%H%M"
DEFAULT_NODE = "ES"
# Curve type A03 omits repeated points: a point holds until the next one (forward fill)
CURVE_HOLDS_UNTIL_NEXT = "A03"
DEFAULT_CURVE = "A01"
ACKNOWLEDGEMENT_TAG = "Acknowledgement_MarketDocument"
REASON_PREVIEW_CHARS = 120
_RESOLUTION = re.compile(r"PT(\d+)M")


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


@register
class Entsoe(Connector):
    """Connector for ENTSO-E documents (e.g. A11 cross-border physical flows)."""

    name = "entsoe"
    secrets = ("ENTSOE_TOKEN",)

    def validate(self, dataset: Dataset) -> None:
        """Require the document query and the variable description."""
        self.require_options(dataset, "document_type", "in_domain", "variable", "unit")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request one document covering the window."""
        options = dataset.options
        params = {
            "documentType": options["document_type"],
            "in_Domain": options["in_domain"],
            "periodStart": window.start.astimezone(UTC).strftime(PERIOD_FORMAT),
            "periodEnd": window.end.astimezone(UTC).strftime(PERIOD_FORMAT),
            "securityToken": self.read_secret("ENTSOE_TOKEN"),
        }
        if "out_domain" in options:
            params["out_Domain"] = options["out_domain"]
        if "process_type" in options:
            params["processType"] = options["process_type"]
        return [Request(URL, params=params, secret=True, meta={"document_type": options["document_type"]})]

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse every TimeSeries/Period of the document into observations.

        Raises:
            ContractError: not XML, or a period with an unsupported resolution.
            NotAvailable: ENTSO-E answered with an acknowledgement ("no matching data").
        """
        root = _parse_xml(payload)
        namespaces = {"ns": _namespace_of(root)}
        if root.tag.endswith(ACKNOWLEDGEMENT_TAG):
            reason = root.findtext(".//ns:Reason/ns:text", default="no data", namespaces=namespaces)
            raise NotAvailable(f"ENTSO-E acknowledgement: {reason[:REASON_PREVIEW_CHARS]}")
        result = ParseResult()
        for time_series in root.findall("ns:TimeSeries", namespaces):
            curve = time_series.findtext("ns:curveType", default=DEFAULT_CURVE, namespaces=namespaces)
            for period in time_series.findall("ns:Period", namespaces):
                self._parse_period(dataset, period, curve, namespaces, result)
        return result

    # --- parsing ---
    def _parse_period(
        self,
        dataset: Dataset,
        period: ElementTree.Element,
        curve: str,
        namespaces: dict[str, str],
        result: ParseResult,
    ) -> None:
        """Add the points of one period, forward-filling the gaps of A03 curves up to the period end.

        A03 omits every point that repeats the previous value, the trailing ones included: a flow that
        stays at 0 until the end of the day has no points after its last change. So the number of
        positions comes from ``timeInterval`` (start to end), not from the last point received.
        """
        start_text = period.findtext("ns:timeInterval/ns:start", namespaces=namespaces)
        end_text = period.findtext("ns:timeInterval/ns:end", namespaces=namespaces)
        resolution_text = period.findtext("ns:resolution", default="", namespaces=namespaces)
        resolution = _RESOLUTION.fullmatch(resolution_text)
        if start_text is None or resolution is None:
            raise ContractError(f"ENTSO-E: unsupported period ({resolution_text!r})")
        period_start = parse_iso(start_text)
        step = timedelta(minutes=int(resolution.group(1)))
        values = self._point_values(period, namespaces)
        position_count = _position_count(period_start, end_text, step, values)
        variable = str(dataset.options["variable"])
        node = str(dataset.options.get("node", DEFAULT_NODE))
        unit = str(dataset.options["unit"])
        last_value = None
        for position in range(1, position_count + 1):
            value = values.get(position)
            if value is None and curve == CURVE_HOLDS_UNTIL_NEXT and last_value is not None:
                value = last_value
                result.count_step("A03_forward_fill")
            if value is None:
                continue
            last_value = value
            event_time = period_start + step * (position - 1)
            result.add(self.make_observation(variable, node, event_time, value, unit))

    def _point_values(self, period: ElementTree.Element, namespaces: dict[str, str]) -> dict[int, float | None]:
        """Return the quantity of every point of a period, keyed by its 1-based position."""
        values = {}
        for point in period.findall("ns:Point", namespaces):
            position = _parse_position(point.findtext("ns:position", default="", namespaces=namespaces))
            quantity = point.findtext("ns:quantity", namespaces=namespaces)
            values[position] = self.parse_number(quantity, "ENTSO-E quantity")
        return values


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _parse_xml(payload: bytes) -> ElementTree.Element:
    """Parse the payload as XML.

    Raises:
        ContractError: the payload is not XML.
    """
    try:
        return ElementTree.fromstring(payload)
    except ElementTree.ParseError:
        raise ContractError("ENTSO-E: response is not XML") from None


def _position_count(
    period_start: datetime,
    end_text: str | None,
    step: timedelta,
    values: dict[int, float | None],
) -> int:
    """Return how many positions a period has: from its time interval, or its last point if no end is given.

    Raises:
        ContractError: the interval end is before its start.
    """
    last_point = max(values, default=0)
    if end_text is None:
        return last_point
    duration = parse_iso(end_text) - period_start
    if duration < timedelta(0):
        raise ContractError("ENTSO-E: period ends before it starts")
    return max(last_point, int(duration / step))


def _parse_position(text: str) -> int:
    """Return a 1-based point position.

    Raises:
        ContractError: the position is missing, not an integer or below 1.
    """
    try:
        position = int(text)
    except ValueError:
        raise ContractError(f"ENTSO-E: invalid point position {text!r}") from None
    if position < 1:
        raise ContractError(f"ENTSO-E: invalid point position {text!r}")
    return position


def _namespace_of(root: ElementTree.Element) -> str:
    """Return the XML namespace of the root element (empty if it has none)."""
    if not root.tag.startswith("{"):
        return ""
    return root.tag.split("}")[0].strip("{")
