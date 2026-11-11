"""Edge gates: acceptance criteria of US-03 and the accepted DG-E11 change."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from mibel_ingestor import settings
from mibel_ingestor.gates import Context, Gates
from mibel_ingestor.model import UTC, Nature, Observation

from conftest import CONFIG_DIR, TZ

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


def gates(profile: str = "production") -> Gates:
    return Gates(settings.load(CONFIG_DIR, "dev").quality, profile)


def ctx(nature: Nature = Nature.OBSERVATION, resolution: float = 900, capacities: dict[str, float] | None = None
        ) -> Context:
    return Context(NOW, TZ, nature, resolution, capacities or {})


def o(variable: str, value: float, unit: str, when: datetime = NOW - timedelta(hours=1)) -> Observation:
    return Observation(variable, "ES", when, value, unit, NOW)


def test_negative_radiation_is_rejected() -> None:            # US-03 acceptance
    r = gates().evaluate([o("direct_radiation", -12, "W/m2")], ctx(Nature.FORECAST, 3600))
    assert r.rejected[0][1] == "DG-E06" and not r.accepted


def test_negative_price_is_valid() -> None:                   # US-03 acceptance
    r = gates().evaluate([o("price_day_ahead", -5, "EUR/MWh")], ctx(Nature.MARKET))
    assert len(r.accepted) == 1 and not r.rejected


def test_grid_alignment() -> None:
    bad = o("demand_real", 30000, "MW", NOW - timedelta(minutes=7))
    good = o("demand_real", 30000, "MW", NOW - timedelta(minutes=10))
    r = gates().evaluate([bad, good], ctx(resolution=300))
    assert r.blocked == {"DG-E03": 1} and r.accepted == [good]


def test_daily_products_start_at_local_midnight() -> None:
    midnight = datetime(2026, 11, 10, tzinfo=TZ).astimezone(UTC)        # 23:00 UTC the day before
    r = gates().evaluate([o("gas_mibgas_es_index", 30, "EUR/MWh", midnight),
                          o("gas_mibgas_es_index", 30, "EUR/MWh", midnight + timedelta(hours=1))],
                         ctx(Nature.MARKET, 86400))
    assert len(r.accepted) == 1 and r.blocked == {"DG-E03": 1}


def test_future_measurement_rejected_but_forecast_allowed() -> None:
    future = NOW + timedelta(hours=2)
    assert "DG-E05" in gates().evaluate([o("demand_real", 30000, "MW", future)], ctx()).blocked
    assert gates().evaluate([o("demand_forecast_h3", 30000, "MW", future)], ctx(Nature.FORECAST)).accepted


def test_unknown_unit_rejected() -> None:
    assert gates().evaluate([o("demand_real", 30000, "GW")], ctx()).blocked == {"DG-E15": 1}


def test_dg_e11_tolerance_uses_installed_capacity() -> None:
    caps = {"solar_photovoltaic": 30000.0}
    r = gates().evaluate([o("pv_real", -1, "MW"), o("pv_real", 31000, "MW"), o("pv_real", 32000, "MW"),
                          o("generation_combined_cycle", -50, "MW")], ctx(capacities=caps))
    # PV may not be negative; 31,000 ≤ 30,000 × 1.05; thermal may consume up to 1 % of its capacity
    assert [x.value for x in r.accepted] == [31000, -50]
    assert r.blocked == {"DG-E11": 2}


def test_development_profile_warns_instead_of_blocking() -> None:
    r = gates("development").evaluate([o("pv_real", -1, "MW")], ctx())
    assert r.accepted and r.warned == {"DG-E11": 1}


def test_unknown_check_is_a_config_error() -> None:
    with pytest.raises(settings.ConfigError):
        Gates({"criteria": {"X": {"check": "magic"}}}, "production")
