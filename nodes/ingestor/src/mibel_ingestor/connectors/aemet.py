"""AEMET OpenData station observations — optional, needs ``AEMET_API_KEY``.

Two-step API: the first call returns a temporary ``datos`` URL with the actual records. Only the
configured stations are requested (one small call each), not the national dump. Live only: the
observation endpoint covers the last 24 h, so there is no backfill through it.
"""

from __future__ import annotations

import json
from datetime import datetime

from ..model import UTC, ContractError, ParseResult, Request, Window
from .base import Connector, Dataset, register


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


URL = "https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/{station}"
TIME_FORMATS = ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S")


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def parse_observation_time(text: str) -> datetime:
    """Parse an AEMET ``fint`` time, which is UTC with or without offset (``2026-11-11T10:00:00+0000``).

    Raises:
        ContractError: the text matches no known format.
    """
    for time_format in TIME_FORMATS:
        try:
            moment = datetime.strptime(text, time_format)
        except ValueError:
            continue
        return moment.astimezone(UTC) if moment.tzinfo else moment.replace(tzinfo=UTC)
    raise ContractError(f"AEMET: unexpected time {text!r}")


@register
class Aemet(Connector):
    """Connector for AEMET conventional station observations."""

    name = "aemet"
    secrets = ("AEMET_API_KEY",)

    def validate(self, dataset: Dataset) -> None:
        """Require ``stations`` (node → station id) and ``variables``."""
        self.require_options(dataset, "stations", "variables")

    def requests(self, dataset: Dataset, window: Window) -> list[Request]:
        """Request each configured station (the key travels in a header).

        Raises:
            NotImplementedError: backfill, which this endpoint cannot serve.
        """
        if not window.is_live:
            raise NotImplementedError("aemet: backfill is not supported by the observation endpoint")
        headers = {"api_key": self.read_secret("AEMET_API_KEY")}
        return [
            Request(URL.format(station=station), headers=headers, secret=True, meta={"node": node, "station": station})
            for node, station in dataset.options["stations"].items()
        ]

    def download(self, request: Request) -> bytes:
        """Follow AEMET's two-step API: ask for the ``datos`` URL, then download the records.

        Raises:
            ContractError: the first call did not return a ``datos`` URL.
        """
        try:
            first_answer = json.loads(self.http.get(request))
            data_url = first_answer["datos"]
        except (ValueError, KeyError, TypeError):
            raise ContractError("AEMET: first call did not return a 'datos' URL") from None
        return self.http.get(Request(str(data_url), secret=True))

    def parse(self, dataset: Dataset, request: Request, payload: bytes) -> ParseResult:
        """Parse the station records into one observation per configured field and hour."""
        records = _load_records(payload)
        node = str(request.meta["node"])
        result = ParseResult()
        for record in records:
            moment = parse_observation_time(str(record.get("fint", "")))
            for field, spec in dataset.options["variables"].items():
                value = self.parse_number(record.get(field), f"AEMET {field}")
                if value is None:
                    result.count_step(f"null_dropped:{field}")
                    continue
                result.add(self.make_observation(spec["variable"], node, moment, value, spec["unit"]))
        return result


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _load_records(payload: bytes) -> list[dict[str, object]]:
    """Return the list of station records (AEMET serves them in Latin-1).

    Raises:
        ContractError: the payload is not a JSON list.
    """
    try:
        records = json.loads(payload.decode("latin-1"))
    except ValueError:
        raise ContractError("AEMET: data is not JSON") from None
    if not isinstance(records, list):
        raise ContractError("AEMET: data is not a list of records")
    return records
