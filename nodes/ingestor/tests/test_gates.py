"""Edge gates: acceptance criteria of US-03 and the accepted DG-E11 change."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from mibel_ingestor import settings
from mibel_ingestor.gates import Context, Gates
from mibel_ingestor.model import UTC, Nature, Observation

from conftest import CONFIG_DIR, MADRID

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
ONE_HOUR_AGO = NOW - timedelta(hours=1)


def build_gates(profile: str = "production") -> Gates:
    """Gates compiled from the real quality.yaml."""
    return Gates(settings.load(CONFIG_DIR, "dev").quality, profile)


def make_context(
    nature: Nature = Nature.OBSERVATION,
    resolution: float = 900,
    capacities: dict[str, float] | None = None,
) -> Context:
    """A batch context at ``NOW`` in Madrid."""
    return Context(NOW, MADRID, nature, resolution, capacities or {})


def make_observation(variable: str, value: float, unit: str, event_time: datetime = ONE_HOUR_AGO) -> Observation:
    """An observation at node ES, available at ``NOW``."""
    return Observation(variable, "ES", event_time, value, unit, NOW)


def test_negative_radiation_is_rejected() -> None:  # US-03 acceptance
    result = build_gates().evaluate(
        [make_observation("direct_radiation", -12, "W/m2")],
        make_context(Nature.FORECAST, 3600),
    )
    assert result.rejected[0].rule == "DG-E06"
    assert not result.accepted


def test_negative_price_is_valid() -> None:  # US-03 acceptance
    result = build_gates().evaluate([make_observation("price_day_ahead", -5, "EUR/MWh")], make_context(Nature.MARKET))
    assert len(result.accepted) == 1
    assert not result.rejected


def test_grid_alignment() -> None:
    misaligned = make_observation("demand_real", 30000, "MW", NOW - timedelta(minutes=7))
    aligned = make_observation("demand_real", 30000, "MW", NOW - timedelta(minutes=10))
    result = build_gates().evaluate([misaligned, aligned], make_context(resolution=300))
    assert result.blocked == {"DG-E03": 1}
    assert result.accepted == [aligned]


def test_daily_products_start_at_local_midnight() -> None:
    local_midnight = datetime(2026, 11, 10, tzinfo=MADRID).astimezone(UTC)  # 23:00 UTC the day before
    observations = [
        make_observation("gas_mibgas_es_index", 30, "EUR/MWh", local_midnight),
        make_observation("gas_mibgas_es_index", 30, "EUR/MWh", local_midnight + timedelta(hours=1)),
    ]
    result = build_gates().evaluate(observations, make_context(Nature.MARKET, 86400))
    assert len(result.accepted) == 1
    assert result.blocked == {"DG-E03": 1}


def test_future_measurement_rejected_but_forecast_allowed() -> None:
    in_two_hours = NOW + timedelta(hours=2)
    measurement = make_observation("demand_real", 30000, "MW", in_two_hours)
    forecast = make_observation("demand_forecast_h3", 30000, "MW", in_two_hours)
    assert "DG-E05" in build_gates().evaluate([measurement], make_context()).blocked
    assert build_gates().evaluate([forecast], make_context(Nature.FORECAST)).accepted


def test_unknown_unit_rejected() -> None:
    result = build_gates().evaluate([make_observation("demand_real", 30000, "GW")], make_context())
    assert result.blocked == {"DG-E15": 1}


def test_dg_e11_tolerance_uses_installed_capacity() -> None:
    capacities = {"solar_photovoltaic": 30000.0}
    observations = [
        make_observation("pv_real", -1, "MW"),
        make_observation("pv_real", 31000, "MW"),
        make_observation("pv_real", 32000, "MW"),
        make_observation("generation_combined_cycle", -50, "MW"),
    ]
    result = build_gates().evaluate(observations, make_context(capacities=capacities))
    # PV may not be negative; 31,000 ≤ 30,000 × 1.05; thermal may consume up to 1 % of its capacity
    assert [observation.value for observation in result.accepted] == [31000, -50]
    assert result.blocked == {"DG-E11": 2}


def test_development_profile_warns_instead_of_blocking() -> None:
    result = build_gates("development").evaluate([make_observation("pv_real", -1, "MW")], make_context())
    assert result.accepted
    assert result.warned == {"DG-E11": 1}


def test_unknown_check_is_a_config_error() -> None:
    with pytest.raises(settings.ConfigError):
        Gates({"criteria": {"X": {"check": "magic"}}}, "production")


def test_invalid_mode_is_a_config_error() -> None:
    criteria = {"X": {"check": "units", "production": {"mode": "maybe"}}}
    with pytest.raises(settings.ConfigError, match="invalid mode"):
        Gates({"criteria": criteria}, "production")
