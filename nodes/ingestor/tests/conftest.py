from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).parent / "fixtures"))

from mibel_ingestor import settings as settings_mod
from mibel_ingestor.connectors import Connector, Dataset, get
from mibel_ingestor.http import HttpClient
from mibel_ingestor.settings import SourceSettings

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
TZ = ZoneInfo("Europe/Madrid")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in ("MIBEL_NODE_ID", "MIBEL_KAFKA_BOOTSTRAP", "MIBEL_INGESTOR_MODE", "MIBEL_METRICS_HOST",
                 "ESIOS_TOKEN", "ENTSOE_TOKEN", "AEMET_API_KEY", "STATE_DIRECTORY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MIBEL_STATE_DIR", str(tmp_path / "state"))


@pytest.fixture
def settings() -> settings_mod.Settings:
    return settings_mod.load(CONFIG_DIR, "test", {"sink.type": "null"})


def make_connector(source: str, raw_source: dict[str, Any] | None = None) -> Connector:
    """Connector built from the real YAML block of ``source`` (optionally patched)."""
    cfg = settings_mod.load(CONFIG_DIR, "dev", {"sink.type": "null"}).sources[source]
    if raw_source:
        cfg = SourceSettings(cfg.name, True, {**cfg.raw, **raw_source}, cfg.datasets)
    cls = get(source)
    return cls(cfg, HttpClient(settings_mod.load(CONFIG_DIR, "dev").http), TZ)


def dataset(conn: Connector, name: str) -> Dataset:
    return conn.datasets[name]
