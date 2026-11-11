"""Contract tests per connector: synthetic payload in the source's wire format -> observations."""

from __future__ import annotations

import itertools
from datetime import date, datetime, timedelta

import pytest

from mibel_ingestor.connectors import available, get
from mibel_ingestor.model import UTC, ContractError, Mode, NotAvailable, Request, Window
from mibel_ingestor.settings import ConfigError

import synthetic
from conftest import TZ, make_connector


def req(**meta: object) -> Request:
    return Request("https://example.invalid", meta=dict(meta))


def test_registry_lists_every_source_module() -> None:
    assert available() == ["aemet", "entsoe", "esios", "mibgas", "omie", "open_meteo", "redata"]


def test_unknown_source_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="unknown source"):
        get("nope")


# ------------------------------------------------------------------------------------- OMIE
def test_omie_day_ahead_quarter_hours_in_utc() -> None:
    conn = make_connector("omie")
    ds = conn.datasets["day_ahead"]
    res = conn.parse(ds, req(file="x"), synthetic.omie_marginal(date(2026, 11, 10)))
    es = [o for o in res.observations if o.node == "ES"]
    assert len(es) == 96 and len(res.observations) == 192
    assert es[0].event_time == datetime(2026, 11, 9, 23, 0, tzinfo=UTC)      # 00:00 local (CET)
    assert es[1].event_time - es[0].event_time == timedelta(minutes=15)
    assert {o.variable for o in es} == {"price_day_ahead"}


@pytest.mark.parametrize(("day", "periods"), [(date(2026, 10, 25), 100), (date(2026, 3, 29), 92)])
def test_omie_dst_days_are_continuous(day: date, periods: int) -> None:
    conn = make_connector("omie")
    res = conn.parse(conn.datasets["day_ahead"], req(), synthetic.omie_marginal(day))
    es = [o.event_time for o in res.observations if o.node == "ES"]
    assert len(es) == periods
    assert all(b - a == timedelta(minutes=15) for a, b in itertools.pairwise(es))
    assert es[-1] + timedelta(minutes=15) == datetime.combine(day + timedelta(days=1), datetime.min.time(),
                                                             TZ).astimezone(UTC)


def test_omie_hourly_files_before_october_2025() -> None:
    conn = make_connector("omie")
    res = conn.parse(conn.datasets["day_ahead"], req(), synthetic.omie_marginal(date(2025, 6, 1), periods=24))
    es = [o.event_time for o in res.observations if o.node == "ES"]
    assert es[1] - es[0] == timedelta(hours=1)


def test_omie_ida_session_variable() -> None:
    conn = make_connector("omie")
    res = conn.parse(conn.datasets["intraday_auctions"], req(session="02"), synthetic.omie_marginal(date(2026, 11, 10)))
    assert {o.variable for o in res.observations} == {"price_ida2"}


def test_omie_continuous_no_trade_and_emission_time() -> None:
    conn = make_connector("omie")
    emitted = datetime(2026, 11, 11, 2, 47)
    res = conn.parse(conn.datasets["continuous"], req(),
                     synthetic.omie_continuous(date(2026, 11, 10), emitted=emitted, no_trade={3, 7}))
    assert res.steps["DG-E16:no_trade_to_missing"] == 4                    # 2 periods x 2 zones
    vwap = [o for o in res.observations if o.variable == "price_continuous_vwap" and o.node == "ES"]
    assert len(vwap) == 94
    assert all(o.available_at == emitted.replace(tzinfo=TZ).astimezone(UTC) for o in vwap)
    assert {o.variable for o in res.observations} == {"price_continuous_vwap", "price_continuous_max",
                                                      "price_continuous_min"}


def test_omie_html_page_breaks_the_contract() -> None:
    conn = make_connector("omie")
    with pytest.raises(ContractError):
        conn.parse(conn.datasets["day_ahead"], req(), b"<!DOCTYPE html><html></html>")


def test_omie_requests_one_file_per_local_day_and_session() -> None:
    conn = make_connector("omie")
    w = Window(datetime(2026, 11, 10, 23, 0, tzinfo=UTC), datetime(2026, 11, 12, 23, 0, tzinfo=UTC))
    files = [r.params["filename"] for r in conn.requests(conn.datasets["intraday_auctions"], w)]
    assert files[:3] == ["marginalpibc_2026111101.1", "marginalpibc_2026111102.1", "marginalpibc_2026111103.1"]
    assert len(files) == 6


# ------------------------------------------------------------------------------------ ESIOS
def test_esios_geos_split_one_response_into_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ESIOS_TOKEN", "dummy")
    conn = make_connector("esios")
    ds = conn.datasets["price_day_ahead"]
    t = datetime(2026, 11, 11, 10, tzinfo=UTC)
    payload = synthetic.esios([(t, "España", 90.0), (t, "Francia", 70.0), (t, "Portugal", 90.0), (t, "España", None)])
    res = conn.parse(ds, req(), payload)
    assert sorted((o.node, o.value) for o in res.observations) == [("ES", 90.0), ("FR", 70.0)]
    assert res.steps == {"filter:geo_not_configured": 1, "null_dropped": 1}


def test_esios_request_is_secret_and_token_not_in_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ESIOS_TOKEN", "super-secret")
    conn = make_connector("esios")
    w = Window(datetime(2026, 11, 11, 8, tzinfo=UTC), datetime(2026, 11, 11, 10, tzinfo=UTC))
    (r,) = conn.requests(conn.datasets["demand_real"], w)
    assert r.secret and "super-secret" not in str(r.meta) and "super-secret" not in r.url


def test_esios_without_token_fails_cleanly() -> None:
    conn = make_connector("esios")
    w = Window(datetime(2026, 11, 11, 8, tzinfo=UTC), datetime(2026, 11, 11, 10, tzinfo=UTC))
    with pytest.raises(ConfigError, match="ESIOS_TOKEN"):
        conn.requests(conn.datasets["demand_real"], w)


# ----------------------------------------------------------------------------------- REData
def test_redata_series_mapping() -> None:
    conn = make_connector("redata")
    t = datetime(2026, 11, 11, 10, 5, tzinfo=TZ)
    res = conn.parse(conn.datasets["demand_real_time"], req(),
                     synthetic.redata({"Real": [(t, 30000.0)], "Programada": [(t, 29000.0)]}))
    assert [(o.variable, o.event_time) for o in res.observations] == [("demand_real", t.astimezone(UTC))]
    assert res.steps == {"series_ignored:programada": 1}


def test_redata_error_document() -> None:
    conn = make_connector("redata")
    with pytest.raises(ContractError):
        conn.parse(conn.datasets["demand_real_time"], req(), b'{"errors": [{"code": 500}]}')


# ------------------------------------------------------------------------------- Open-Meteo
def test_open_meteo_derives_wind_at_100m_and_drops_nulls() -> None:
    conn = make_connector("open_meteo")
    ds = conn.datasets["forecast"]
    params = ["wind_speed_80m", "wind_speed_120m", "direct_radiation", "diffuse_radiation", "cloud_cover",
              "temperature_2m"]
    res = conn.parse(ds, req(node="aragon"), synthetic.open_meteo(datetime(2026, 11, 11, tzinfo=UTC), 24, params))
    wind = [o for o in res.observations if o.variable == "wind_speed_100m"]
    assert len(wind) == 24 and all(4 < o.value < 9 for o in wind)       # between the 80 m and 120 m inputs
    assert res.steps["null_dropped"] == 1
    assert {o.node for o in res.observations} == {"aragon"}


def test_open_meteo_backfill_uses_historical_forecast_api() -> None:
    conn = make_connector("open_meteo")
    w = Window(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC), Mode.BACKFILL)
    reqs = conn.requests(conn.datasets["forecast"], w)
    assert len(reqs) == 4 and all("historical-forecast" in r.url for r in reqs)
    assert reqs[0].params["start_date"] == "2026-01-01" and reqs[0].params["end_date"] == "2026-01-31"
    assert reqs[0].params["wind_speed_unit"] == "ms"


def test_open_meteo_satellite_only_for_solar_nodes() -> None:
    conn = make_connector("open_meteo")
    w = Window(datetime(2026, 11, 10, tzinfo=UTC), datetime(2026, 11, 11, tzinfo=UTC))
    assert {r.meta["node"] for r in conn.requests(conn.datasets["satellite"], w)} == {"extremadura", "andalucia"}


# ----------------------------------------------------------------------------------- MIBGAS
def test_mibgas_workbook() -> None:
    pytest.importorskip("openpyxl")
    conn = make_connector("mibgas")
    days = [date(2026, 11, 1) + timedelta(days=i) for i in range(5)]
    res = conn.parse(conn.datasets["daily_index"], req(year=2026), synthetic.mibgas(days))
    es = [o for o in res.observations if o.variable == "gas_mibgas_es_index"]
    assert len(es) == 5 and es[0].event_time == datetime(2026, 10, 31, 23, tzinfo=UTC)


def test_mibgas_missing_sheet() -> None:
    pytest.importorskip("openpyxl")
    conn = make_connector("mibgas")
    with pytest.raises(ContractError, match="sheet"):
        conn.parse(conn.datasets["daily_index"], req(), synthetic.mibgas([date(2026, 11, 1)], sheet="Other"))


# ------------------------------------------------------------------------------------ AEMET
def test_aemet_records() -> None:
    conn = make_connector("aemet")
    res = conn.parse(conn.datasets["stations"], req(node="aragon"),
                     synthetic.aemet(datetime(2026, 11, 11, tzinfo=UTC), 6))
    assert len([o for o in res.observations if o.variable == "wind_speed_station_10m"]) == 5
    assert res.steps == {"null_dropped:vv": 1}


def test_aemet_backfill_not_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AEMET_API_KEY", "dummy")
    conn = make_connector("aemet")
    w = Window(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC), Mode.BACKFILL)
    with pytest.raises(NotImplementedError):
        conn.requests(conn.datasets["stations"], w)


# ---------------------------------------------------------------------------------- ENTSO-E
def test_entsoe_a03_forward_fill() -> None:
    conn = make_connector("entsoe")
    t0 = datetime(2026, 11, 10, 23, tzinfo=UTC)
    res = conn.parse(conn.datasets["flow_fr_es"], req(), synthetic.entsoe(t0, [100, None, None, 250], curve="A03"))
    assert [o.value for o in res.observations] == [100, 100, 100, 250]
    assert res.observations[3].event_time == t0 + timedelta(hours=3)


def test_entsoe_acknowledgement_means_not_available() -> None:
    conn = make_connector("entsoe")
    with pytest.raises(NotAvailable):
        conn.parse(conn.datasets["flow_fr_es"], req(), synthetic.entsoe_ack())


def test_entsoe_token_only_in_secret_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENTSOE_TOKEN", "tok")
    conn = make_connector("entsoe")
    w = Window(datetime(2026, 11, 10, tzinfo=UTC), datetime(2026, 11, 11, tzinfo=UTC))
    (r,) = conn.requests(conn.datasets["flow_fr_es"], w)
    assert r.secret and r.params["periodStart"] == "202611100000" and "tok" not in str(r.meta)
