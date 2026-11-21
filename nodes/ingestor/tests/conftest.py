"""Shared fixtures: a clean environment per test, test-mode settings and connectors built from the real YAML."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest


sys.path.insert(0, str(Path(__file__).parent / "fixtures"))

from mibel_ingestor import settings as settings_module
from mibel_ingestor.connectors import Connector, get_connector_class
from mibel_ingestor.http import HttpClient
from mibel_ingestor.settings import SourceSettings
from mibel_ingestor.store import Store


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
MADRID = ZoneInfo("Europe/Madrid")
ENVIRONMENT_VARIABLES = (
    "MIBEL_NODE_ID",
    "MIBEL_KAFKA_BOOTSTRAP",
    "MIBEL_INGESTOR_MODE",
    "MIBEL_METRICS_HOST",
    "ESIOS_TOKEN",
    "ENTSOE_TOKEN",
    "AEMET_API_KEY",
    "STATE_DIRECTORY",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Remove deployment variables and secrets, and point the state directory to a temporary folder."""
    for name in ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MIBEL_STATE_DIR", str(tmp_path / "state"))


@pytest.fixture(autouse=True)
def close_stores(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Close every store a test opened, whoever created it (tests, services, the CLI).

    Python 3.13+ warns (ResourceWarning) about SQLite connections left open, which ``-W error`` turns
    into test errors. Closing twice is harmless, so services that close their own store are fine.
    """
    opened: list[Store] = []
    original_init = Store.__init__

    def tracking_init(store: Store, path: Path | str) -> None:
        original_init(store, path)
        opened.append(store)

    monkeypatch.setattr(Store, "__init__", tracking_init)
    yield
    for store in opened:
        store.close()


@pytest.fixture
def settings() -> settings_module.Settings:
    """Test-mode settings (production rules) with the null sink."""
    return settings_module.load(CONFIG_DIR, "test", {"sink.type": "null"})


def make_connector(source: str, extra_options: dict[str, Any] | None = None) -> Connector:
    """Build a connector from the real YAML block of ``source``, optionally patching its options."""
    dev_settings = settings_module.load(CONFIG_DIR, "dev", {"sink.type": "null"})
    source_settings = dev_settings.sources[source]
    if extra_options:
        source_settings = SourceSettings(
            name=source_settings.name,
            enabled=True,
            options={**source_settings.options, **extra_options},
            datasets=source_settings.datasets,
        )
    connector_class = get_connector_class(source)
    return connector_class(source_settings, HttpClient(dev_settings.http), MADRID)
