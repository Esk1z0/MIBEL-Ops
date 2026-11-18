"""Configuration: ``config/ingestor.yaml`` + run mode + controlled overrides + environment.

Run modes (ADR-05):
- ``pro``:  fixed behaviour, production quality profile, Kafka sink, short buffer life. No overrides.
- ``test``: identical to ``pro`` but the output (sink), metrics, buffer path, log level and the
  enabled sources may be overridden, e.g. to write JSON lines while Kafka is not running yet.
- ``dev``:  everything may be overridden; development quality profile.

Secrets and deployment values (node id, Kafka address, metrics bind address) come only from the
environment, never from versioned files (AGENTS.md §3).
"""

from __future__ import annotations

import copy
import fnmatch
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

MODES = ("pro", "test", "dev")
DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
_DURATION = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|d|h|m|s)")
_UNITS = {"d": 86400.0, "h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001}


class ConfigError(ValueError):
    pass


def duration(value: Any) -> float:
    """'5m' -> 300.0 · '1h30m' -> 5400.0 · 30 -> 30.0 (seconds)."""
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "")
    negative = text.startswith("-")
    text = text.lstrip("-")
    parts = _DURATION.findall(text)
    if not parts or "".join(n + u for n, u in parts) != text:
        raise ConfigError(f"invalid duration: {value!r}")
    seconds = sum(float(n) * _UNITS[u] for n, u in parts)
    return -seconds if negative else seconds


def secret(name: str) -> str | None:
    """Secret from the environment; the ``.env.example`` placeholder ``<...>`` counts as missing."""
    value = os.environ.get(name, "").strip()
    return value if value and not value.startswith("<") else None


def load_env_file(path: Path) -> None:
    """Minimal ``KEY=VALUE`` loader for development (no python-dotenv dependency).
    Existing environment variables win; values are never printed."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.removeprefix("export ").strip()
        os.environ.setdefault(key, value.strip().strip("'\""))


# --------------------------------------------------------------------------- typed sections
@dataclass(frozen=True)
class HttpSettings:
    timeout: float
    retries: int
    backoff: float
    max_response_bytes: int
    user_agent: str


@dataclass(frozen=True)
class SinkSettings:
    type: str
    topic_prefix: str
    dlq_topic: str
    path: Path
    max_bytes: int
    kafka: dict[str, Any]
    bootstrap: str | None


@dataclass(frozen=True)
class BufferSettings:
    path: Path
    pending_ttl: float
    max_bytes: int
    flush_every: float
    batch: int
    seen_ttl: float
    quarantine_ttl: float


@dataclass(frozen=True)
class MetricsSettings:
    enabled: bool
    host: str
    port: int


@dataclass(frozen=True)
class SourceSettings:
    name: str
    enabled: bool
    raw: dict[str, Any]                       # connector-specific keys (validated by the connector)
    datasets: dict[str, dict[str, Any]]       # dataset name -> merged settings (source defaults applied)


@dataclass(frozen=True)
class Settings:
    mode: str
    quality_profile: str
    node_id: str
    timezone: ZoneInfo
    state_dir: Path
    config_dir: Path
    log_level: str
    workers: int
    retry_first: float
    retry_max: float
    http: HttpSettings
    sink: SinkSettings
    buffer: BufferSettings
    metrics: MetricsSettings
    sources: dict[str, SourceSettings] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- overrides
def _flatten(data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in data.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict) and value:
            out.update(_flatten(value, path + "."))
        else:
            out[path] = value
    return out


def _allowed(key: str, patterns: list[str]) -> bool:
    return any(key == p or key.startswith(p + ".") or fnmatch.fnmatchcase(key, p) for p in patterns)


def _set_path(data: dict[str, Any], key: str, value: Any) -> None:
    node = data
    *parents, last = key.split(".")
    for part in parents:
        node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise ConfigError(f"cannot override {key!r}: {part!r} is not a section")
    node[last] = value


def apply_overrides(data: dict[str, Any], overrides: dict[str, Any], mode: str) -> dict[str, Any]:
    patterns = list(data["modes"][mode].get("overridable", []))
    refused = [k for k in overrides if not _allowed(k, patterns)]
    if refused:
        raise ConfigError(f"mode {mode!r} does not allow overriding: {', '.join(sorted(refused))}")
    data = copy.deepcopy(data)
    for key, value in overrides.items():
        _set_path(data, key, value)
    return data


def parse_set_args(items: list[str]) -> dict[str, Any]:
    """``--set sink.type=jsonl`` -> {'sink.type': 'jsonl'} (values parsed as YAML scalars)."""
    out: dict[str, Any] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key.strip():
            raise ConfigError(f"--set expects key=value, got {item!r}")
        out[key.strip()] = yaml.safe_load(value) if value else ""
    return out


# --------------------------------------------------------------------------- loading
def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"configuration file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name}: top level must be a mapping")
    return data


def _merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in extra.items():
        out[key] = _merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) \
            else copy.deepcopy(value)
    return out


def _enabled(value: Any, secrets: list[str]) -> bool:
    if value == "auto":                       # on only when every required secret is present
        return all(secret(s) for s in secrets)
    return bool(value)


def _sources(data: dict[str, Any]) -> dict[str, SourceSettings]:
    out = {}
    for name, raw in (data.get("sources") or {}).items():
        raw = raw or {}
        defaults = {k: raw[k] for k in ("schedule", "window", "nature", "resolution", "publication") if k in raw}
        datasets = {}
        for ds_name, ds in (raw.get("datasets") or {}).items():
            merged = _merge(defaults, ds or {})
            if merged.get("enabled", True) is False:
                continue
            datasets[ds_name] = merged
        out[name] = SourceSettings(name=name, enabled=_enabled(raw.get("enabled", True), raw.get("secrets", [])),
                                   raw=raw, datasets=datasets)
    return out


def load(config_dir: Path | None = None, mode: str | None = None,
         overrides: dict[str, Any] | None = None) -> Settings:
    config_dir = Path(config_dir or os.environ.get("MIBEL_INGESTOR_CONFIG") or DEFAULT_CONFIG_DIR)
    mode = mode or os.environ.get("MIBEL_INGESTOR_MODE", "dev")
    if mode not in MODES:
        raise ConfigError(f"unknown mode {mode!r} (expected one of {', '.join(MODES)})")

    data = _read_yaml(config_dir / "ingestor.yaml")
    for key, value in (data["modes"][mode].get("defaults") or {}).items():   # versioned per-mode defaults
        _set_path(data, key, value)
    local = config_dir / "ingestor.local.yaml"            # optional, ignored by Git, same override rules
    merged_overrides = _flatten(_read_yaml(local)) if local.is_file() else {}
    merged_overrides.update(overrides or {})
    data = apply_overrides(data, merged_overrides, mode)

    mode_cfg = data["modes"][mode]
    state_dir = Path(os.environ.get("MIBEL_STATE_DIR") or os.environ.get("STATE_DIRECTORY") or ".state")
    node_id = os.environ.get("MIBEL_NODE_ID", "").strip()
    if not node_id:
        if mode == "pro":
            raise ConfigError("MIBEL_NODE_ID must be set in pro mode")
        node_id = f"{mode}-ingestor"

    svc, http, sink, buf, met = (data.get(k, {}) for k in ("service", "http", "sink", "buffer", "metrics"))
    raw_type = sink.get("type", "kafka")
    sink_type = "null" if raw_type is None else str(raw_type)   # YAML reads `null` as None
    bootstrap = os.environ.get("MIBEL_KAFKA_BOOTSTRAP") or None
    if sink_type == "kafka" and not bootstrap:
        raise ConfigError("sink.type is kafka but MIBEL_KAFKA_BOOTSTRAP is not set "
                          "(in test/dev mode use --set sink.type=jsonl)")
    retry = svc.get("retry_on_failure", {})

    def in_state(value: str) -> Path:
        p = Path(value)
        return p if p.is_absolute() else state_dir / p

    quality = _read_yaml(config_dir / "quality.yaml")
    return Settings(
        mode=mode,
        quality_profile=str(mode_cfg["quality_profile"]),
        node_id=node_id,
        timezone=ZoneInfo(str(data.get("timezone", "Europe/Madrid"))),
        state_dir=state_dir,
        config_dir=config_dir,
        log_level=str(svc.get("log_level", "INFO")),
        workers=int(svc.get("workers", 3)),
        retry_first=duration(retry.get("first", "1m")),
        retry_max=duration(retry.get("max", "15m")),
        http=HttpSettings(timeout=duration(http.get("timeout", "30s")), retries=int(http.get("retries", 3)),
                          backoff=duration(http.get("backoff", "2s")),
                          max_response_bytes=int(float(http.get("max_response_mb", 25)) * 1e6),
                          user_agent=str(http.get("user_agent", "MIBEL-Ops-ingestor"))),
        sink=SinkSettings(type=sink_type, topic_prefix=str(sink.get("topic_prefix", "mibel.raw")),
                          dlq_topic=str(sink.get("dlq_topic", "mibel.dlq")),
                          path=in_state(str(sink.get("path", "ingestor-out.jsonl"))),
                          max_bytes=int(float(sink.get("max_mb", 50)) * 1e6),
                          kafka=dict(sink.get("kafka", {})), bootstrap=bootstrap),
        buffer=BufferSettings(path=in_state(str(buf.get("path", "buffer.sqlite"))),
                              pending_ttl=duration(buf.get("pending_ttl", "30d")),
                              max_bytes=int(float(buf.get("max_mb", 1000)) * 1e6),
                              flush_every=duration(buf.get("flush_every", "5s")),
                              batch=int(buf.get("batch", 200)),
                              seen_ttl=duration(buf.get("seen_ttl", "8d")),
                              quarantine_ttl=duration(buf.get("quarantine_ttl", "24h"))),
        metrics=MetricsSettings(enabled=bool(met.get("enabled", True)),
                                host=os.environ.get("MIBEL_METRICS_HOST") or str(met.get("host", "127.0.0.1")),
                                port=int(met.get("port", 9108))),
        sources=_sources(data),
        quality=quality,
    )
