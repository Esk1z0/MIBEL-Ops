"""Configuration: durations, run modes, overrides, environment and source defaults."""

from __future__ import annotations

from pathlib import Path

import pytest

from mibel_ingestor import settings
from mibel_ingestor.settings import ConfigError, QualityProfile, SinkType, parse_duration

from conftest import CONFIG_DIR


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("5m", 300), ("1h30m", 5400), ("2d", 172800), (30, 30), ("0s", 0), ("-1d", -86400), ("250ms", 0.25)],
)
def test_parse_duration(text: object, seconds: float) -> None:
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("bad_text", ["5", "five minutes", "1x", ""])
def test_parse_duration_rejects_garbage(bad_text: str) -> None:
    with pytest.raises(ConfigError):
        parse_duration(bad_text)


def test_pro_needs_node_id_and_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="MIBEL_NODE_ID"):
        settings.load(CONFIG_DIR, "pro")
    monkeypatch.setenv("MIBEL_NODE_ID", "ingestor-x")
    with pytest.raises(ConfigError, match="MIBEL_KAFKA_BOOTSTRAP"):
        settings.load(CONFIG_DIR, "pro")
    monkeypatch.setenv("MIBEL_KAFKA_BOOTSTRAP", "broker:9092")
    pro_settings = settings.load(CONFIG_DIR, "pro")
    assert pro_settings.quality_profile is QualityProfile.PRODUCTION
    assert pro_settings.sink.type is SinkType.KAFKA
    assert pro_settings.sink.bootstrap == "broker:9092"


def test_pro_allows_no_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MIBEL_NODE_ID", "ingestor-x")
    with pytest.raises(ConfigError, match="does not allow"):
        settings.load(CONFIG_DIR, "pro", {"sink.type": "jsonl"})


def test_test_mode_overrides_output_only() -> None:
    test_settings = settings.load(CONFIG_DIR, "test", {"sink.type": "jsonl", "sources.omie.enabled": False})
    assert test_settings.sink.type is SinkType.JSONL
    assert test_settings.quality_profile is QualityProfile.PRODUCTION
    assert not test_settings.sources["omie"].enabled
    with pytest.raises(ConfigError, match=r"http\.retries"):
        settings.load(CONFIG_DIR, "test", {"sink.type": "jsonl", "http.retries": 9})
    with pytest.raises(ConfigError):
        settings.load(CONFIG_DIR, "test", {"sink.type": "jsonl", "buffer.pending_ttl": "30d"})


def test_dev_defaults_and_free_overrides() -> None:
    dev_settings = settings.load(CONFIG_DIR, "dev")
    assert dev_settings.sink.type is SinkType.STDOUT
    assert dev_settings.quality_profile is QualityProfile.DEVELOPMENT
    dev_settings = settings.load(CONFIG_DIR, "dev", {"http.retries": 0, "buffer.pending_ttl": "1h"})
    assert dev_settings.http.retries == 0
    assert dev_settings.buffer.pending_ttl == 3600


def test_unknown_mode_is_refused() -> None:
    with pytest.raises(ConfigError, match="unknown mode"):
        settings.load(CONFIG_DIR, "staging")


def test_auto_enabled_follows_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    assert not settings.load(CONFIG_DIR, "dev").sources["entsoe"].enabled
    monkeypatch.setenv("ENTSOE_TOKEN", "<your-token-here>")  # the placeholder counts as missing
    assert not settings.load(CONFIG_DIR, "dev").sources["entsoe"].enabled
    monkeypatch.setenv("ENTSOE_TOKEN", "real")
    assert settings.load(CONFIG_DIR, "dev").sources["entsoe"].enabled


def test_disabled_dataset_is_dropped() -> None:
    assert "installed_capacity" not in settings.load(CONFIG_DIR, "dev").sources["redata"].datasets


def test_dataset_inherits_source_defaults() -> None:
    datasets = settings.load(CONFIG_DIR, "dev").sources["omie"].datasets
    assert datasets["day_ahead"]["schedule"] == {"every": "1h", "offset": "7m"}
    assert datasets["continuous"]["window"] == {"back": "2d", "ahead": "0s"}


def test_local_override_file_follows_mode_rules(tmp_path: Path) -> None:
    for name in ("ingestor.yaml", "quality.yaml"):
        (tmp_path / name).write_text((CONFIG_DIR / name).read_text())
    (tmp_path / "ingestor.local.yaml").write_text("sink: {type: jsonl}\nhttp: {retries: 1}\n")
    with pytest.raises(ConfigError, match=r"http\.retries"):
        settings.load(tmp_path, "test")
    assert settings.load(tmp_path, "dev").http.retries == 1


def test_state_dir_from_systemd(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("MIBEL_STATE_DIR")
    monkeypatch.setenv("STATE_DIRECTORY", str(tmp_path))
    assert settings.load(CONFIG_DIR, "dev").buffer.path == tmp_path / "buffer.sqlite"


def test_env_file_loader(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nESIOS_TOKEN='abc'\nexport AEMET_API_KEY=xyz\n")
    settings.load_env_file(env_file)
    assert settings.read_secret("ESIOS_TOKEN") == "abc"
    assert settings.read_secret("AEMET_API_KEY") == "xyz"


def test_null_sink_from_set_argument() -> None:
    # `--set sink.type=null` is parsed as YAML, where `null` means None
    test_settings = settings.load(CONFIG_DIR, "test", settings.parse_set_args(["sink.type=null"]))
    assert test_settings.sink.type is SinkType.NULL


def test_unknown_sink_type_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="unknown sink type"):
        settings.load(CONFIG_DIR, "dev", {"sink.type": "carrier-pigeon"})
