"""Edge data gates (DG-E) loaded from ``config/quality.yaml`` (docs/quality/01_data_gates.md §2).

Each criterion has a ``check`` type and, per quality profile, a ``value`` and a ``mode``:
``block`` rejects the observation (quarantine + DLQ), ``warn`` keeps it and counts it in the
lineage, ``off`` skips it. Rules are compiled once; rule applicability is cached per variable so
the per-observation cost stays at a few comparisons.

DG-E01 (structure) is enforced by the connectors raising ``ContractError``; DG-E16 (OMIE no-trade
periods) by the OMIE parser. Both are declared in the YAML for traceability.
"""

from __future__ import annotations

import fnmatch
import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .model import UTC, Nature, Observation
from .settings import ConfigError, duration

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class Context:
    now: datetime
    tz: ZoneInfo
    nature: Nature
    resolution: float
    capacities: dict[str, float]        # technology slug -> installed MW (DG-E11)


Check = Callable[[Observation, Context], bool]


@dataclass
class Rule:
    id: str
    mode: str                                     # block | warn
    check: Check
    variables: list[str] | None = None            # glob patterns; None = every variable
    natures: set[Nature] | None = None
    _cache: dict[str, bool] = field(default_factory=dict, repr=False)

    def applies(self, obs: Observation, nature: Nature) -> bool:
        if self.natures is not None and nature not in self.natures:
            return False
        if self.variables is None:
            return True
        hit = self._cache.get(obs.variable)
        if hit is None:
            hit = any(fnmatch.fnmatchcase(obs.variable, p) for p in self.variables)
            self._cache[obs.variable] = hit
        return hit


@dataclass
class GateResult:
    accepted: list[Observation]
    rejected: list[tuple[Observation, str]]      # (observation, first blocking rule)
    warned: Counter[str]
    blocked: Counter[str]

    def summary(self, profile: str, version: str) -> dict[str, Any]:
        return {"profile": profile, "rules_version": version,
                "warn": dict(self.warned), "rejected": dict(self.blocked)}


# --------------------------------------------------------------------------- check builders
def _not_null(_: Any, __: dict[str, Any]) -> Check:
    def check(o: Observation, c: Context) -> bool:
        return bool(o.variable and o.node and o.unit) and o.event_time is not None and math.isfinite(o.value)
    return check


def _grid(_: Any, __: dict[str, Any]) -> Check:
    def check(o: Observation, c: Context) -> bool:
        if o.event_time.utcoffset() != timedelta(0):
            return False
        if c.resolution >= 86400:                      # daily products start at local midnight
            local = o.event_time.astimezone(c.tz)
            return local.hour == 0 and local.minute == 0 and local.second == 0
        return (o.event_time - EPOCH).total_seconds() % c.resolution == 0
    return check


def _available_after_event(_: Any, __: dict[str, Any]) -> Check:
    def check(o: Observation, c: Context) -> bool:
        return o.available_at is None or o.available_at >= o.event_time
    return check


def _not_future(value: Any, _: dict[str, Any]) -> Check:
    margin = timedelta(seconds=duration(value if value is not None else "5m"))

    def check(o: Observation, c: Context) -> bool:
        return o.event_time <= c.now + margin
    return check


def _range(value: Any, _: dict[str, Any]) -> Check:
    try:
        lo, hi = (float(v) for v in value)
    except (TypeError, ValueError):
        raise ConfigError(f"range value must be [min, max], got {value!r}") from None

    def check(o: Observation, c: Context) -> bool:
        return lo <= o.value <= hi
    return check


def _units(value: Any, _: dict[str, Any]) -> Check:
    allowed = frozenset(value or ())

    def check(o: Observation, c: Context) -> bool:
        return o.unit in allowed
    return check


def _generation(value: Any, spec: dict[str, Any]) -> Check:
    """DG-E11: ``-negative_share × capacity ≤ value ≤ max_factor × capacity`` for technologies that may
    consume (thermal, storage); ``0 ≤ value`` for the rest. Without known capacity only the sign is checked."""
    share = float((value or {}).get("negative_share", 0.01))
    factor = float((value or {}).get("max_factor", 1.05))
    negative_ok = list(spec.get("negative_allowed", []))
    capacity_of: dict[str, str] = dict(spec.get("capacity_of", {}))

    def capacity(o: Observation, c: Context) -> float | None:
        for pattern, tech in capacity_of.items():
            if fnmatch.fnmatchcase(o.variable, pattern):
                return c.capacities.get(tech)
        return None

    def check(o: Observation, c: Context) -> bool:
        cap = capacity(o, c)
        may_consume = any(fnmatch.fnmatchcase(o.variable, p) for p in negative_ok)
        if cap is not None and o.value > cap * factor:
            return False
        if o.value >= 0:
            return True
        return may_consume and (cap is None or o.value >= -share * cap)
    return check


CHECKS: dict[str, Callable[[Any, dict[str, Any]], Check]] = {
    "not_null": _not_null, "grid": _grid, "available_after_event": _available_after_event,
    "not_future": _not_future, "range": _range, "units": _units, "generation": _generation,
}
DECLARATIVE = {"contract", "connector"}           # enforced elsewhere, declared for traceability


class Gates:
    def __init__(self, quality: dict[str, Any], profile: str) -> None:
        self.profile = profile
        self.version = str(quality.get("version", "?"))
        self.rules: list[Rule] = []
        for rule_id, spec in (quality.get("criteria") or {}).items():
            kind = spec.get("check")
            if kind in DECLARATIVE:
                continue
            if kind not in CHECKS:
                raise ConfigError(f"{rule_id}: unknown check {kind!r}")
            prof = spec.get(profile) or {}
            mode = prof.get("mode", "block")
            if mode not in ("block", "warn", "off"):
                raise ConfigError(f"{rule_id}: invalid mode {mode!r}")
            if mode == "off":
                continue
            natures = {Nature(n) for n in spec["natures"]} if spec.get("natures") else None
            self.rules.append(Rule(rule_id, mode, CHECKS[kind](prof.get("value"), spec),
                                   spec.get("variables"), natures))

    def evaluate(self, observations: list[Observation], ctx: Context) -> GateResult:
        accepted, rejected = [], []
        warned: Counter[str] = Counter()
        blocked: Counter[str] = Counter()
        for obs in observations:
            first_block = None
            for rule in self.rules:
                if not rule.applies(obs, ctx.nature) or rule.check(obs, ctx):
                    continue
                if rule.mode == "warn":
                    warned[rule.id] += 1
                else:
                    blocked[rule.id] += 1
                    first_block = first_block or rule.id
            if first_block:
                rejected.append((obs, first_block))
            else:
                accepted.append(obs)
        return GateResult(accepted, rejected, warned, blocked)
