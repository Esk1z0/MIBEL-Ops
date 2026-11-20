"""Edge data gates (DG-E) loaded from ``config/quality.yaml`` (docs/quality/01_data_gates.md §2).

Each criterion has a ``check`` type and, per quality profile, a ``value`` and a ``mode``:

- ``block``: the observation is rejected (quarantine + DLQ).
- ``warn``: the observation is kept and the violation is counted in the lineage.
- ``off``: the rule is skipped.

Rules are compiled once at start-up. Whether a rule applies to a variable is cached, so the cost per
observation stays at a few comparisons (lightness criterion, ADR-05).

DG-E01 (structure) is enforced by the connectors raising ``ContractError``, and DG-E16 (OMIE no-trade
periods) by the OMIE parser. Both are declared in the YAML for traceability only.
"""

from __future__ import annotations

import fnmatch
import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

from .model import UTC, Nature, Observation, Rejection
from .settings import ConfigError, QualityProfile, parse_duration


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SECONDS_PER_DAY = 86400
DEFAULT_FUTURE_MARGIN = "5m"
DEFAULT_NEGATIVE_SHARE = 0.01
DEFAULT_MAX_FACTOR = 1.05
# Check types enforced elsewhere (connectors), declared in the YAML for traceability only
DECLARATIVE_CHECKS = frozenset({"contract", "connector"})


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class GateMode(str, Enum):
    """What happens when an observation fails a rule."""

    BLOCK = "block"
    WARN = "warn"
    OFF = "off"


@dataclass(frozen=True)
class Context:
    """Facts shared by every observation of one batch.

    ``capacities`` maps a technology slug to its installed MW (DG-E11).
    """

    now: datetime
    timezone: ZoneInfo
    nature: Nature
    resolution: float
    capacities: dict[str, float]


Check = Callable[[Observation, Context], bool]
CheckBuilder = Callable[[Any, dict[str, Any]], Check]


@dataclass
class Rule:
    """One compiled criterion: its check plus the variables and natures it applies to.

    ``variables`` holds glob patterns; None means every variable. ``natures`` None means every nature.
    """

    id: str
    mode: GateMode
    check: Check
    variables: list[str] | None = None
    natures: set[Nature] | None = None
    _applies_to_variable: dict[str, bool] = field(default_factory=dict, repr=False)

    def applies(self, observation: Observation, nature: Nature) -> bool:
        """Whether the rule must be evaluated for this observation (result cached per variable)."""
        if self.natures is not None and nature not in self.natures:
            return False
        if self.variables is None:
            return True
        cached = self._applies_to_variable.get(observation.variable)
        if cached is None:
            cached = _matches_any(observation.variable, self.variables)
            self._applies_to_variable[observation.variable] = cached
        return cached


@dataclass
class GateResult:
    """Outcome of evaluating a batch: accepted observations, rejections and violation counts per rule."""

    accepted: list[Observation]
    rejected: list[Rejection]
    warned: Counter[str]
    blocked: Counter[str]

    def summary(self, profile: str, version: str) -> dict[str, Any]:
        """Return the gate part of the batch lineage."""
        return {
            "profile": profile,
            "rules_version": version,
            "warn": dict(self.warned),
            "rejected": dict(self.blocked),
        }


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


class Gates:
    """The edge gates of one quality profile, compiled from ``quality.yaml``."""

    def __init__(self, quality: dict[str, Any], profile: QualityProfile | str) -> None:
        # `.value`, not str(): str() of a (str, Enum) member is "QualityProfile.PRODUCTION"
        self.profile = QualityProfile(profile).value
        self.version = str(quality.get("version", "?"))
        self.rules: list[Rule] = []
        for rule_id, spec in (quality.get("criteria") or {}).items():
            rule = _compile_rule(rule_id, spec, self.profile)
            if rule is not None:
                self.rules.append(rule)

    def evaluate(self, observations: list[Observation], context: Context) -> GateResult:
        """Apply every rule to every observation.

        An observation is rejected if any ``block`` rule fails; the first failing ``block`` rule is the
        one recorded. ``warn`` violations are only counted.
        """
        result = GateResult(accepted=[], rejected=[], warned=Counter(), blocked=Counter())
        for observation in observations:
            first_blocking_rule = self._evaluate_one(observation, context, result)
            if first_blocking_rule is None:
                result.accepted.append(observation)
            else:
                result.rejected.append(Rejection(observation, first_blocking_rule))
        return result

    # --- private ---
    def _evaluate_one(self, observation: Observation, context: Context, result: GateResult) -> str | None:
        """Count the violations of one observation and return its first blocking rule, if any."""
        first_blocking_rule = None
        for rule in self.rules:
            if not rule.applies(observation, context.nature) or rule.check(observation, context):
                continue
            if rule.mode is GateMode.WARN:
                result.warned[rule.id] += 1
                continue
            result.blocked[rule.id] += 1
            if first_blocking_rule is None:
                first_blocking_rule = rule.id
        return first_blocking_rule


# ##################################################################################################
# ##############################     PRIVATE HELPERS: COMPILATION     ##############################
# ##################################################################################################


def _compile_rule(rule_id: str, spec: dict[str, Any], profile: str) -> Rule | None:
    """Compile one criterion for a profile; declarative criteria and ``off`` rules return None.

    Raises:
        ConfigError: unknown check type or invalid mode.
    """
    check_type = spec.get("check")
    if check_type in DECLARATIVE_CHECKS:
        return None
    if check_type not in CHECK_BUILDERS:
        raise ConfigError(f"{rule_id}: unknown check {check_type!r}")
    profile_spec = spec.get(profile) or {}
    mode = _parse_mode(rule_id, profile_spec.get("mode", GateMode.BLOCK.value))
    if mode is GateMode.OFF:
        return None
    natures = {Nature(name) for name in spec["natures"]} if spec.get("natures") else None
    check = CHECK_BUILDERS[check_type](profile_spec.get("value"), spec)
    return Rule(rule_id, mode, check, spec.get("variables"), natures)


def _parse_mode(rule_id: str, value: Any) -> GateMode:
    """Return the gate mode of a rule."""
    try:
        return GateMode(value)
    except ValueError:
        raise ConfigError(f"{rule_id}: invalid mode {value!r}") from None


def _matches_any(variable: str, patterns: list[str]) -> bool:
    """Whether a variable name matches any of the glob patterns."""
    return any(fnmatch.fnmatchcase(variable, pattern) for pattern in patterns)


# ##################################################################################################
# ############################     PRIVATE HELPERS: CHECK BUILDERS     #############################
# ##################################################################################################


# Each builder receives the profile ``value`` and the whole criterion ``spec`` and returns the check.
def _build_not_null(_value: Any, _spec: dict[str, Any]) -> Check:
    """DG-E02: identifiers present and a finite value."""
    def check(observation: Observation, context: Context) -> bool:
        has_identity = bool(observation.variable and observation.node and observation.unit)
        return has_identity and observation.event_time is not None and math.isfinite(observation.value)
    return check


def _build_grid(_value: Any, _spec: dict[str, Any]) -> Check:
    """DG-E03: ``event_time`` in UTC and aligned to the dataset resolution.

    Daily products are aligned to local midnight instead, which is not a multiple of 24 h in UTC.
    """
    def check(observation: Observation, context: Context) -> bool:
        if observation.event_time.utcoffset() != timedelta(0):
            return False
        if context.resolution >= SECONDS_PER_DAY:
            local = observation.event_time.astimezone(context.timezone)
            return local.hour == 0 and local.minute == 0 and local.second == 0
        seconds_since_epoch = (observation.event_time - EPOCH).total_seconds()
        return seconds_since_epoch % context.resolution == 0
    return check


def _build_available_after_event(_value: Any, _spec: dict[str, Any]) -> Check:
    """DG-E04: a value cannot be published before the period it describes starts."""
    def check(observation: Observation, context: Context) -> bool:
        return observation.available_at is None or observation.available_at >= observation.event_time
    return check


def _build_not_future(value: Any, _spec: dict[str, Any]) -> Check:
    """DG-E05: measured values cannot lie in the future (beyond a small clock margin)."""
    margin = timedelta(seconds=parse_duration(value if value is not None else DEFAULT_FUTURE_MARGIN))

    def check(observation: Observation, context: Context) -> bool:
        return observation.event_time <= context.now + margin
    return check


def _build_range(value: Any, _spec: dict[str, Any]) -> Check:
    """Physical or market range ``[min, max]`` of a variable."""
    try:
        minimum, maximum = (float(bound) for bound in value)
    except (TypeError, ValueError):
        raise ConfigError(f"range value must be [min, max], got {value!r}") from None

    def check(observation: Observation, context: Context) -> bool:
        return minimum <= observation.value <= maximum
    return check


def _build_units(value: Any, _spec: dict[str, Any]) -> Check:
    """The unit must be one of the allowed ones."""
    allowed_units = frozenset(value or ())

    def check(observation: Observation, context: Context) -> bool:
        return observation.unit in allowed_units
    return check


def _build_generation(value: Any, spec: dict[str, Any]) -> Check:
    """DG-E11: generation limits relative to installed capacity.

    Technologies that may consume (thermal, storage) accept ``-negative_share × capacity ≤ value``; the
    rest must be ``≥ 0``. Every technology must stay ``≤ max_factor × capacity``. Without a known
    capacity only the sign is checked.
    """
    options = value or {}
    negative_share = float(options.get("negative_share", DEFAULT_NEGATIVE_SHARE))
    max_factor = float(options.get("max_factor", DEFAULT_MAX_FACTOR))
    may_consume_patterns = list(spec.get("negative_allowed", []))
    technology_by_pattern: dict[str, str] = dict(spec.get("capacity_of", {}))

    def capacity_of(observation: Observation, context: Context) -> float | None:
        for pattern, technology in technology_by_pattern.items():
            if fnmatch.fnmatchcase(observation.variable, pattern):
                return context.capacities.get(technology)
        return None

    def check(observation: Observation, context: Context) -> bool:
        capacity = capacity_of(observation, context)
        if capacity is not None and observation.value > capacity * max_factor:
            return False
        if observation.value >= 0:
            return True
        if not _matches_any(observation.variable, may_consume_patterns):
            return False
        return capacity is None or observation.value >= -negative_share * capacity
    return check


CHECK_BUILDERS: dict[str, CheckBuilder] = {
    "not_null": _build_not_null,
    "grid": _build_grid,
    "available_after_event": _build_available_after_event,
    "not_future": _build_not_future,
    "range": _build_range,
    "units": _build_units,
    "generation": _build_generation,
}
