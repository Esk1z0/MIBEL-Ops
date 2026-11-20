"""Configuration: ``config/ingestor.yaml`` + run mode + controlled overrides + environment.

Loading order (later wins)::

    ingestor.yaml → modes.<mode>.defaults → ingestor.local.yaml → --set key=value
                                            (both checked against modes.<mode>.overridable)

Run modes (ADR-05):

- ``pro``: fixed behaviour, production quality profile, Kafka sink. No overrides.
- ``test``: same as ``pro``, but the sink, metrics, buffer path, log level and enabled sources may be
  overridden, e.g. to write JSON lines while Kafka is not running yet.
- ``dev``: everything may be overridden; development quality profile.

Secrets and deployment values (node id, Kafka address, metrics bind address, state directory) come
only from the environment, never from versioned files (AGENTS.md §3).
"""

from __future__ import annotations

import copy
import fnmatch
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
DEFAULT_STATE_DIR = ".state"
DATASET_DEFAULT_KEYS = ("schedule", "window", "nature", "resolution", "publication")
MEGABYTE = 1_000_000

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|d|h|m|s)")
_SECONDS_PER_UNIT = {"d": 86400.0, "h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001}


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class ConfigError(ValueError):
    """The configuration is invalid or incomplete; the ingestor refuses to start."""


class RunMode(str, Enum):
    """Run mode of the node (ADR-05 §5.2)."""

    PRO = "pro"
    TEST = "test"
    DEV = "dev"


class SinkType(str, Enum):
    """Where delivered messages go (see :mod:`mibel_ingestor.sinks`)."""

    KAFKA = "kafka"
    JSONL = "jsonl"
    STDOUT = "stdout"
    NULL = "null"


class QualityProfile(str, Enum):
    """Quality profile whose thresholds and modes the edge gates apply (docs/quality/)."""

    DEVELOPMENT = "development"
    PRODUCTION = "production"


@dataclass(frozen=True)
class HttpSettings:
    """HTTP client limits shared by every source."""

    timeout: float
    retries: int
    backoff: float
    max_response_bytes: int
    user_agent: str


@dataclass(frozen=True)
class SinkSettings:
    """Output of delivered messages. ``bootstrap`` comes from ``MIBEL_KAFKA_BOOTSTRAP``."""

    type: SinkType
    topic_prefix: str
    dlq_topic: str
    path: Path
    max_bytes: int
    kafka: dict[str, Any]
    bootstrap: str | None


@dataclass(frozen=True)
class BufferSettings:
    """Life limits of the local SQLite state (NFR-03: 30 days / 1 GB of undelivered messages)."""

    path: Path
    pending_ttl: float
    max_bytes: int
    flush_every: float
    batch: int
    seen_ttl: float
    quarantine_ttl: float


@dataclass(frozen=True)
class MetricsSettings:
    """Prometheus endpoint. ``host`` may come from ``MIBEL_METRICS_HOST`` (the VPN address)."""

    enabled: bool
    host: str
    port: int


@dataclass(frozen=True)
class SourceSettings:
    """One source block of the YAML.

    ``options`` holds the connector-specific keys (validated by the connector). ``datasets`` maps each
    enabled dataset name to its options with the source defaults already merged in.
    """

    name: str
    enabled: bool
    options: dict[str, Any]
    datasets: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class Settings:
    """Fully resolved configuration of one ingestor process."""

    mode: RunMode
    quality_profile: QualityProfile
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


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def parse_duration(value: Any) -> float:
    """Convert a duration such as ``'5m'``, ``'1h30m'``, ``'250ms'`` or ``30`` to seconds.

    Raises:
        ConfigError: the text is not a sequence of ``<number><unit>`` parts.
    """
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "")
    is_negative = text.startswith("-")
    text = text.lstrip("-")
    parts = _DURATION_PART.findall(text)
    rebuilt = "".join(number + unit for number, unit in parts)
    if not parts or rebuilt != text:
        raise ConfigError(f"invalid duration: {value!r}")
    seconds = sum(float(number) * _SECONDS_PER_UNIT[unit] for number, unit in parts)
    return -seconds if is_negative else seconds


def read_secret(name: str) -> str | None:
    """Read a secret from the environment; the ``.env.example`` placeholder ``<...>`` counts as missing."""
    value = os.environ.get(name, "").strip()
    if not value or value.startswith("<"):
        return None
    return value


def load_env_file(path: Path) -> None:
    """Load ``KEY=VALUE`` lines into the environment (development only, no python-dotenv dependency).

    Variables already present in the environment win. Values are never printed.
    """
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.removeprefix("export ").strip()
        os.environ.setdefault(key, value.strip().strip("'\""))


def parse_set_args(items: list[str]) -> dict[str, Any]:
    """Turn ``--set sink.type=jsonl`` arguments into ``{'sink.type': 'jsonl'}``.

    Values are parsed as YAML scalars, so numbers and booleans keep their type.
    """
    overrides: dict[str, Any] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key.strip():
            raise ConfigError(f"--set expects key=value, got {item!r}")
        overrides[key.strip()] = yaml.safe_load(value) if value else ""
    return overrides


def apply_overrides(data: dict[str, Any], overrides: dict[str, Any], mode: RunMode) -> dict[str, Any]:
    """Return a copy of ``data`` with ``overrides`` applied, if the mode allows every key.

    Raises:
        ConfigError: some key is not listed in ``modes.<mode>.overridable``.
    """
    patterns = list(data["modes"][mode.value].get("overridable", []))
    refused = [key for key in overrides if not _is_overridable(key, patterns)]
    if refused:
        raise ConfigError(f"mode {mode.value!r} does not allow overriding: {', '.join(sorted(refused))}")
    result = copy.deepcopy(data)
    for key, value in overrides.items():
        _set_path(result, key, value)
    return result


def load(
    config_dir: Path | None = None,
    mode: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> Settings:
    """Load and validate the configuration for one run mode.

    Args:
        config_dir: folder with ``ingestor.yaml`` and ``quality.yaml``; defaults to
            ``$MIBEL_INGESTOR_CONFIG`` or the node's ``config/`` folder.
        mode: ``pro``, ``test`` or ``dev``; defaults to ``$MIBEL_INGESTOR_MODE`` or ``dev``.
        overrides: ``--set`` values, checked against the mode's ``overridable`` list.

    Raises:
        ConfigError: unknown mode, forbidden override, missing deployment value or invalid value.
    """
    config_dir = Path(config_dir or os.environ.get("MIBEL_INGESTOR_CONFIG") or DEFAULT_CONFIG_DIR)
    run_mode = _resolve_mode(mode)
    data = _read_yaml(config_dir / "ingestor.yaml")
    _apply_mode_defaults(data, run_mode)
    data = apply_overrides(data, _collect_overrides(config_dir, overrides), run_mode)

    state_dir = Path(os.environ.get("MIBEL_STATE_DIR") or os.environ.get("STATE_DIRECTORY") or DEFAULT_STATE_DIR)
    service = data.get("service", {})
    retry = service.get("retry_on_failure", {})
    mode_config = data["modes"][run_mode.value]
    return Settings(
        mode=run_mode,
        quality_profile=QualityProfile(str(mode_config["quality_profile"])),
        node_id=_resolve_node_id(run_mode),
        timezone=ZoneInfo(str(data.get("timezone", "Europe/Madrid"))),
        state_dir=state_dir,
        config_dir=config_dir,
        log_level=str(service.get("log_level", "INFO")),
        workers=int(service.get("workers", 3)),
        retry_first=parse_duration(retry.get("first", "1m")),
        retry_max=parse_duration(retry.get("max", "15m")),
        http=_build_http(data.get("http", {})),
        sink=_build_sink(data.get("sink", {}), state_dir),
        buffer=_build_buffer(data.get("buffer", {}), state_dir),
        metrics=_build_metrics(data.get("metrics", {})),
        sources=_build_sources(data),
        quality=_read_yaml(config_dir / "quality.yaml"),
    )


# ##################################################################################################
# ################################     PRIVATE HELPERS: LOADING     ################################
# ##################################################################################################


def _resolve_mode(mode: str | None) -> RunMode:
    """Return the run mode from the argument or ``$MIBEL_INGESTOR_MODE`` (default ``dev``)."""
    name = mode or os.environ.get("MIBEL_INGESTOR_MODE", RunMode.DEV.value)
    try:
        return RunMode(name)
    except ValueError:
        expected = ", ".join(member.value for member in RunMode)
        raise ConfigError(f"unknown mode {name!r} (expected one of {expected})") from None


def _apply_mode_defaults(data: dict[str, Any], mode: RunMode) -> None:
    """Apply the versioned ``modes.<mode>.defaults`` (e.g. ``dev`` prints to stdout instead of Kafka)."""
    defaults = data["modes"][mode.value].get("defaults") or {}
    for key, value in defaults.items():
        _set_path(data, key, value)


def _collect_overrides(config_dir: Path, overrides: dict[str, Any] | None) -> dict[str, Any]:
    """Merge the optional, unversioned ``ingestor.local.yaml`` with the ``--set`` values (which win)."""
    local_file = config_dir / "ingestor.local.yaml"
    collected = _flatten(_read_yaml(local_file)) if local_file.is_file() else {}
    collected.update(overrides or {})
    return collected


def _resolve_node_id(mode: RunMode) -> str:
    """Return ``$MIBEL_NODE_ID``; outside ``pro`` a placeholder such as ``test-ingestor`` is used."""
    node_id = os.environ.get("MIBEL_NODE_ID", "").strip()
    if node_id:
        return node_id
    if mode is RunMode.PRO:
        raise ConfigError("MIBEL_NODE_ID must be set in pro mode")
    return f"{mode.value}-ingestor"


def _build_http(section: dict[str, Any]) -> HttpSettings:
    """Build the HTTP limits from the ``http`` section."""
    return HttpSettings(
        timeout=parse_duration(section.get("timeout", "30s")),
        retries=int(section.get("retries", 3)),
        backoff=parse_duration(section.get("backoff", "2s")),
        max_response_bytes=int(float(section.get("max_response_mb", 25)) * MEGABYTE),
        user_agent=str(section.get("user_agent", "MIBEL-Ops-ingestor")),
    )


def _build_sink(section: dict[str, Any], state_dir: Path) -> SinkSettings:
    """Build the sink settings; a Kafka sink requires ``MIBEL_KAFKA_BOOTSTRAP``."""
    sink_type = _parse_sink_type(section.get("type", SinkType.KAFKA.value))
    bootstrap = os.environ.get("MIBEL_KAFKA_BOOTSTRAP") or None
    if sink_type is SinkType.KAFKA and not bootstrap:
        raise ConfigError(
            "sink.type is kafka but MIBEL_KAFKA_BOOTSTRAP is not set (in test/dev mode use --set sink.type=jsonl)"
        )
    return SinkSettings(
        type=sink_type,
        topic_prefix=str(section.get("topic_prefix", "mibel.raw")),
        dlq_topic=str(section.get("dlq_topic", "mibel.dlq")),
        path=_inside_state_dir(str(section.get("path", "ingestor-out.jsonl")), state_dir),
        max_bytes=int(float(section.get("max_mb", 50)) * MEGABYTE),
        kafka=dict(section.get("kafka", {})),
        bootstrap=bootstrap,
    )


def _parse_sink_type(value: Any) -> SinkType:
    """Return the sink type; YAML reads a bare ``null`` as None, which means the null sink."""
    name = SinkType.NULL.value if value is None else str(value)
    try:
        return SinkType(name)
    except ValueError:
        expected = " | ".join(member.value for member in SinkType)
        raise ConfigError(f"unknown sink type {name!r} ({expected})") from None


def _build_buffer(section: dict[str, Any], state_dir: Path) -> BufferSettings:
    """Build the life limits of the local state from the ``buffer`` section."""
    return BufferSettings(
        path=_inside_state_dir(str(section.get("path", "buffer.sqlite")), state_dir),
        pending_ttl=parse_duration(section.get("pending_ttl", "30d")),
        max_bytes=int(float(section.get("max_mb", 1000)) * MEGABYTE),
        flush_every=parse_duration(section.get("flush_every", "5s")),
        batch=int(section.get("batch", 200)),
        seen_ttl=parse_duration(section.get("seen_ttl", "8d")),
        quarantine_ttl=parse_duration(section.get("quarantine_ttl", "24h")),
    )


def _build_metrics(section: dict[str, Any]) -> MetricsSettings:
    """Build the Prometheus endpoint settings; ``MIBEL_METRICS_HOST`` overrides the bind address."""
    return MetricsSettings(
        enabled=bool(section.get("enabled", True)),
        host=os.environ.get("MIBEL_METRICS_HOST") or str(section.get("host", "127.0.0.1")),
        port=int(section.get("port", 9108)),
    )


def _build_sources(data: dict[str, Any]) -> dict[str, SourceSettings]:
    """Build every source block, merging source defaults into each enabled dataset."""
    sources = {}
    for name, options in (data.get("sources") or {}).items():
        options = options or {}
        sources[name] = SourceSettings(
            name=name,
            enabled=_is_enabled(options.get("enabled", True), options.get("secrets", [])),
            options=options,
            datasets=_build_datasets(options),
        )
    return sources


def _build_datasets(source_options: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return the enabled datasets of a source with the source-level defaults merged in."""
    defaults = {key: source_options[key] for key in DATASET_DEFAULT_KEYS if key in source_options}
    datasets = {}
    for dataset_name, dataset_options in (source_options.get("datasets") or {}).items():
        merged = _merge(defaults, dataset_options or {})
        if merged.get("enabled", True) is False:
            continue
        datasets[dataset_name] = merged
    return datasets


def _is_enabled(value: Any, secrets: list[str]) -> bool:
    """Resolve ``enabled``; ``auto`` means on only when every required secret is present."""
    if value == "auto":
        return all(read_secret(name) for name in secrets)
    return bool(value)


def _inside_state_dir(value: str, state_dir: Path) -> Path:
    """Resolve a relative path against the state directory; absolute paths are kept."""
    path = Path(value)
    return path if path.is_absolute() else state_dir / path


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML file whose top level must be a mapping."""
    if not path.is_file():
        raise ConfigError(f"configuration file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name}: top level must be a mapping")
    return data


# ##################################################################################################
# #############################     PRIVATE HELPERS: DICTIONARIES     ##############################
# ##################################################################################################


def _flatten(data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Turn nested mappings into dotted keys: ``{'sink': {'type': 'x'}}`` → ``{'sink.type': 'x'}``."""
    flat: dict[str, Any] = {}
    for key, value in data.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict) and value:
            flat.update(_flatten(value, path + "."))
        else:
            flat[path] = value
    return flat


def _is_overridable(key: str, patterns: list[str]) -> bool:
    """Whether a dotted key matches a pattern exactly, as a child section, or as a glob."""
    for pattern in patterns:
        if key == pattern or key.startswith(pattern + ".") or fnmatch.fnmatchcase(key, pattern):
            return True
    return False


def _set_path(data: dict[str, Any], key: str, value: Any) -> None:
    """Set a dotted key inside nested mappings, creating the intermediate sections."""
    node = data
    *parents, last = key.split(".")
    for part in parents:
        node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise ConfigError(f"cannot override {key!r}: {part!r} is not a section")
    node[last] = value


def _merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge two mappings into a new one; values of ``extra`` win."""
    merged = copy.deepcopy(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged
