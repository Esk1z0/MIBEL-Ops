"""Contract tests per connector: synthetic payload in the source's wire format → observations."""

from __future__ import annotations

import itertools
from datetime import date, datetime, time, timedelta

import pytest

from mibel_ingestor.connectors import Dataset, available_sources, get_connector_class
from mibel_ingestor.model import UTC, CaptureMode, ContractError, NotAvailable, Request, Window
from mibel_ingestor.settings import ConfigError

import synthetic
from conftest import MADRID, make_connector


QUARTER_HOUR = timedelta(minutes=15)


def fake_request(**meta: object) -> Request:
    """A request whose only relevant part for parsing is its ``meta``."""
    return Request("https://example.invalid", meta=dict(meta))


def utc_window(start: datetime, end: datetime, mode: CaptureMode = CaptureMode.LIVE) -> Window:
    """A window between two UTC datetimes."""
    return Window(start, end, mode)


# ##################################################################################################
# ########################################     REGISTRY     ########################################
# ##################################################################################################


def test_registry_lists_every_source_module() -> None:
    assert available_sources() == ["aemet", "entsoe", "esios", "mibgas", "omie", "open_meteo", "redata"]


def test_unknown_source_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="unknown source"):
        get_connector_class("nope")


# ##################################################################################################
# ##########################################     OMIE     ##########################################
# ##################################################################################################


def test_omie_day_ahead_quarter_hours_in_utc() -> None:
    connector = make_connector("omie")
    result = connector.parse(
        connector.datasets["day_ahead"],
        fake_request(file="x"),
        synthetic.omie_marginal(date(2026, 11, 10)),
    )
    spain = [observation for observation in result.observations if observation.node == "ES"]
    assert len(spain) == 96
    assert len(result.observations) == 192
    assert spain[0].event_time == datetime(2026, 11, 9, 23, 0, tzinfo=UTC)  # 00:00 local (CET)
    assert spain[1].event_time - spain[0].event_time == QUARTER_HOUR
    assert {observation.variable for observation in spain} == {"price_day_ahead"}


@pytest.mark.parametrize(("day", "period_count"), [(date(2026, 10, 25), 100), (date(2026, 3, 29), 92)])
def test_omie_dst_days_are_continuous(day: date, period_count: int) -> None:
    connector = make_connector("omie")
    result = connector.parse(connector.datasets["day_ahead"], fake_request(), synthetic.omie_marginal(day))
    starts = [observation.event_time for observation in result.observations if observation.node == "ES"]
    next_local_midnight = datetime.combine(day + timedelta(days=1), time(0), MADRID).astimezone(UTC)
    assert len(starts) == period_count
    assert all(later - earlier == QUARTER_HOUR for earlier, later in itertools.pairwise(starts))
    assert starts[-1] + QUARTER_HOUR == next_local_midnight


def test_omie_hourly_files_before_october_2025() -> None:
    connector = make_connector("omie")
    payload = synthetic.omie_marginal(date(2025, 6, 1), periods=24)
    result = connector.parse(connector.datasets["day_ahead"], fake_request(), payload)
    starts = [observation.event_time for observation in result.observations if observation.node == "ES"]
    assert starts[1] - starts[0] == timedelta(hours=1)


def test_omie_ida_session_variable() -> None:
    connector = make_connector("omie")
    result = connector.parse(
        connector.datasets["intraday_auctions"],
        fake_request(session="02"),
        synthetic.omie_marginal(date(2026, 11, 10)),
    )
    assert {observation.variable for observation in result.observations} == {"price_ida2"}


def test_omie_continuous_no_trade_and_emission_time() -> None:
    connector = make_connector("omie")
    emitted_local = datetime(2026, 11, 11, 2, 47)
    payload = synthetic.omie_continuous(date(2026, 11, 10), emitted=emitted_local, no_trade={3, 7})
    result = connector.parse(connector.datasets["continuous"], fake_request(), payload)
    assert result.steps["DG-E16:no_trade_to_missing"] == 4  # 2 periods x 2 zones
    spain_vwap = [
        observation for observation in result.observations
        if observation.variable == "price_continuous_vwap" and observation.node == "ES"
    ]
    emitted_utc = emitted_local.replace(tzinfo=MADRID).astimezone(UTC)
    assert len(spain_vwap) == 94
    assert all(observation.available_at == emitted_utc for observation in spain_vwap)
    assert {observation.variable for observation in result.observations} == {
        "price_continuous_vwap",
        "price_continuous_max",
        "price_continuous_min",
    }


def test_omie_html_page_breaks_the_contract() -> None:
    connector = make_connector("omie")
    with pytest.raises(ContractError):
        connector.parse(connector.datasets["day_ahead"], fake_request(), b"<!DOCTYPE html><html></html>")


def test_omie_requests_one_file_per_local_day_and_session() -> None:
    connector = make_connector("omie")
    window = utc_window(datetime(2026, 11, 10, 23, 0, tzinfo=UTC), datetime(2026, 11, 12, 23, 0, tzinfo=UTC))
    requests = connector.requests(connector.datasets["intraday_auctions"], window)
    filenames = [request.params["filename"] for request in requests]
    assert filenames[:3] == ["marginalpibc_2026111101.1", "marginalpibc_2026111102.1", "marginalpibc_2026111103.1"]
    assert len(filenames) == 6


# ##################################################################################################
# #########################################     ESIOS     ##########################################
# ##################################################################################################


def test_esios_geos_split_one_response_into_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ESIOS_TOKEN", "dummy")
    connector = make_connector("esios")
    moment = datetime(2026, 11, 11, 10, tzinfo=UTC)
    payload = synthetic.esios([
        synthetic.EsiosPoint(moment, "España", 90.0),
        synthetic.EsiosPoint(moment, "Francia", 70.0),
        synthetic.EsiosPoint(moment, "Portugal", 90.0),
        synthetic.EsiosPoint(moment, "España", None),
    ])
    result = connector.parse(connector.datasets["price_day_ahead"], fake_request(), payload)
    values_by_node = sorted((observation.node, observation.value) for observation in result.observations)
    assert values_by_node == [("ES", 90.0), ("FR", 70.0)]
    assert result.steps == {"filter:geo_not_configured": 1, "null_dropped": 1}


def test_esios_request_is_secret_and_token_not_in_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ESIOS_TOKEN", "super-secret")
    connector = make_connector("esios")
    window = utc_window(datetime(2026, 11, 11, 8, tzinfo=UTC), datetime(2026, 11, 11, 10, tzinfo=UTC))
    (request,) = connector.requests(connector.datasets["demand_real"], window)
    assert request.secret
    assert "super-secret" not in str(request.meta)
    assert "super-secret" not in request.url


def test_esios_without_token_fails_cleanly() -> None:
    connector = make_connector("esios")
    window = utc_window(datetime(2026, 11, 11, 8, tzinfo=UTC), datetime(2026, 11, 11, 10, tzinfo=UTC))
    with pytest.raises(ConfigError, match="ESIOS_TOKEN"):
        connector.requests(connector.datasets["demand_real"], window)


# ##################################################################################################
# #########################################     REDATA     #########################################
# ##################################################################################################


def test_redata_series_mapping() -> None:
    connector = make_connector("redata")
    moment = datetime(2026, 11, 11, 10, 5, tzinfo=MADRID)
    payload = synthetic.redata({"Real": [(moment, 30000.0)], "Programada": [(moment, 29000.0)]})
    result = connector.parse(connector.datasets["demand_real_time"], fake_request(), payload)
    (observation,) = result.observations
    assert observation.variable == "demand_real"
    assert observation.event_time == moment.astimezone(UTC)
    assert result.steps == {"series_ignored:programada": 1}


def test_redata_error_document() -> None:
    connector = make_connector("redata")
    with pytest.raises(ContractError):
        connector.parse(connector.datasets["demand_real_time"], fake_request(), b'{"errors": [{"code": 500}]}')


# ##################################################################################################
# #######################################     OPEN-METEO     #######################################
# ##################################################################################################


def test_open_meteo_derives_wind_at_100m_and_drops_nulls() -> None:
    connector = make_connector("open_meteo")
    params = ["wind_speed_80m", "wind_speed_120m", "direct_radiation", "diffuse_radiation", "cloud_cover",
              "temperature_2m"]
    payload = synthetic.open_meteo(datetime(2026, 11, 11, tzinfo=UTC), 24, params)
    result = connector.parse(connector.datasets["forecast"], fake_request(node="aragon"), payload)
    wind = [observation for observation in result.observations if observation.variable == "wind_speed_100m"]
    assert len(wind) == 24
    assert all(4 < observation.value < 9 for observation in wind)  # between the 80 m and 120 m inputs
    assert result.steps["null_dropped"] == 1
    assert {observation.node for observation in result.observations} == {"aragon"}


def test_open_meteo_backfill_uses_historical_forecast_api() -> None:
    connector = make_connector("open_meteo")
    window = utc_window(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC), CaptureMode.BACKFILL)
    requests = connector.requests(connector.datasets["forecast"], window)
    assert len(requests) == 4
    assert all("historical-forecast" in request.url for request in requests)
    assert requests[0].params["start_date"] == "2026-01-01"
    assert requests[0].params["end_date"] == "2026-01-31"
    assert requests[0].params["wind_speed_unit"] == "ms"


def test_open_meteo_satellite_only_for_solar_nodes() -> None:
    connector = make_connector("open_meteo")
    window = utc_window(datetime(2026, 11, 10, tzinfo=UTC), datetime(2026, 11, 11, tzinfo=UTC))
    requests = connector.requests(connector.datasets["satellite"], window)
    assert {request.meta["node"] for request in requests} == {"extremadura", "andalucia"}


# ##################################################################################################
# #########################################     MIBGAS     #########################################
# ##################################################################################################


def test_mibgas_workbook() -> None:
    pytest.importorskip("openpyxl")
    connector = make_connector("mibgas")
    days = [date(2026, 11, 1) + timedelta(days=i) for i in range(5)]
    result = connector.parse(connector.datasets["daily_index"], fake_request(year=2026), synthetic.mibgas(days))
    spain = [observation for observation in result.observations if observation.variable == "gas_mibgas_es_index"]
    assert len(spain) == 5
    assert spain[0].event_time == datetime(2026, 10, 31, 23, tzinfo=UTC)


def test_mibgas_missing_sheet() -> None:
    pytest.importorskip("openpyxl")
    connector = make_connector("mibgas")
    payload = synthetic.mibgas([date(2026, 11, 1)], sheet="Other")
    with pytest.raises(ContractError, match="sheet"):
        connector.parse(connector.datasets["daily_index"], fake_request(), payload)


# ##################################################################################################
# #########################################     AEMET     ##########################################
# ##################################################################################################


def test_aemet_records() -> None:
    connector = make_connector("aemet")
    payload = synthetic.aemet(datetime(2026, 11, 11, tzinfo=UTC), 6)
    result = connector.parse(connector.datasets["stations"], fake_request(node="aragon"), payload)
    wind = [observation for observation in result.observations if observation.variable == "wind_speed_station_10m"]
    assert len(wind) == 5
    assert result.steps == {"null_dropped:vv": 1}


def test_aemet_backfill_not_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AEMET_API_KEY", "dummy")
    connector = make_connector("aemet")
    window = utc_window(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC), CaptureMode.BACKFILL)
    with pytest.raises(NotImplementedError):
        connector.requests(connector.datasets["stations"], window)


# ##################################################################################################
# ########################################     ENTSO-E     #########################################
# ##################################################################################################


def test_entsoe_a03_forward_fill() -> None:
    connector = make_connector("entsoe")
    first_hour = datetime(2026, 11, 10, 23, tzinfo=UTC)
    payload = synthetic.entsoe(first_hour, [100, None, None, 250], curve="A03")
    result = connector.parse(connector.datasets["flow_fr_es"], fake_request(), payload)
    assert [observation.value for observation in result.observations] == [100, 100, 100, 250]
    assert result.observations[3].event_time == first_hour + timedelta(hours=3)


def test_entsoe_a03_fills_omitted_trailing_points_up_to_the_period_end() -> None:
    # A03 omits trailing repeats too: a flow that stays at 250 has no points after position 3
    connector = make_connector("entsoe")
    first_hour = datetime(2026, 11, 10, 23, tzinfo=UTC)
    payload = synthetic.entsoe(first_hour, [100, None, 250, None, None], curve="A03")
    result = connector.parse(connector.datasets["flow_fr_es"], fake_request(), payload)
    assert [observation.value for observation in result.observations] == [100, 100, 250, 250, 250]
    assert result.observations[-1].event_time == first_hour + timedelta(hours=4)
    assert result.steps == {"A03_forward_fill": 3}


def test_entsoe_a01_missing_points_stay_missing() -> None:
    connector = make_connector("entsoe")
    payload = synthetic.entsoe(datetime(2026, 11, 10, 23, tzinfo=UTC), [100, None, None])
    result = connector.parse(connector.datasets["flow_fr_es"], fake_request(), payload)
    assert [observation.value for observation in result.observations] == [100]


def test_entsoe_invalid_position_breaks_the_contract() -> None:
    connector = make_connector("entsoe")
    payload = synthetic.entsoe(datetime(2026, 11, 10, 23, tzinfo=UTC), [100])
    broken = payload.replace(b"<position>1</position>", b"<position>first</position>")
    with pytest.raises(ContractError, match="position"):
        connector.parse(connector.datasets["flow_fr_es"], fake_request(), broken)


def test_entsoe_acknowledgement_means_not_available() -> None:
    connector = make_connector("entsoe")
    with pytest.raises(NotAvailable):
        connector.parse(connector.datasets["flow_fr_es"], fake_request(), synthetic.entsoe_acknowledgement())


def test_entsoe_token_only_in_secret_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENTSOE_TOKEN", "tok")
    connector = make_connector("entsoe")
    window = utc_window(datetime(2026, 11, 10, tzinfo=UTC), datetime(2026, 11, 11, tzinfo=UTC))
    (request,) = connector.requests(connector.datasets["flow_fr_es"], window)
    assert request.secret
    assert request.params["periodStart"] == "202611100000"
    assert "tok" not in str(request.meta)


# ##################################################################################################
# ####################################     DATASET SETTINGS     ####################################
# ##################################################################################################


@pytest.mark.parametrize(
    "schedule",
    [{"every": "0s"}, {"every": "1h", "offset": "24h"}, {"every": "1h", "offset": "-5m"}],
)
def test_impossible_schedule_is_a_config_error(schedule: dict[str, str]) -> None:
    with pytest.raises(ConfigError, match="schedule"):
        Dataset.from_settings("broken", {"schedule": schedule})


def test_zero_resolution_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="resolution"):
        Dataset.from_settings("broken", {"resolution": "0s"})
