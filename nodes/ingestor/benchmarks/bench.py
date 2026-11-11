"""Cross-platform benchmark of the ingestor (development PC vs Raspberry Pi).

    python benchmarks/bench.py offline [--repeat 10] [--out FILE]   # CPU/memory only, synthetic payloads
    python benchmarks/bench.py online  [--env-file ../../.env]       # one real pass over every source
    python benchmarks/bench.py idle    [--seconds 60]                # service loop at rest
    python benchmarks/bench.py compare A.json B.json                 # side-by-side table (B / A)

``offline`` runs the real pipeline (parse → change detection → gates → serialisation → SQLite) on
synthetic payloads with the same size and shape as each source, so two machines do exactly the same
work. Each job is measured twice per repeat: ``first`` (every value is new) and ``repoll`` (nothing
changed — the common live case). Nothing is sent anywhere (null sink, in-memory store).
Results are JSON files under ``benchmarks/results/`` (ignored by Git: they describe a machine).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import resource
import statistics
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "tests" / "fixtures"))

T0 = time.perf_counter()
from mibel_ingestor import settings as settings_mod  # noqa: E402
from mibel_ingestor.connectors import Connector, Dataset  # noqa: E402
from mibel_ingestor.model import UTC, Request, Window  # noqa: E402
from mibel_ingestor.pipeline import Pipeline  # noqa: E402
from mibel_ingestor.service import Service  # noqa: E402
from mibel_ingestor.sinks import NullSink  # noqa: E402
from mibel_ingestor.store import Store  # noqa: E402

IMPORT_S = time.perf_counter() - T0
REF_NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)          # fixed clock: identical work everywhere


# --------------------------------------------------------------------------- environment
def machine() -> dict[str, Any]:
    info: dict[str, Any] = {"hostname_hash": hash(platform.node()) & 0xFFFF,   # no hostname in results
                            "system": platform.system(), "release": platform.release(),
                            "machine": platform.machine(), "python": platform.python_version(),
                            "cpus": os.cpu_count()}
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text()
        for key in ("model name", "Model", "Hardware"):
            for line in cpuinfo.splitlines():
                if line.startswith(key):
                    info.setdefault("cpu", line.split(":", 1)[1].strip())
        mem = Path("/proc/meminfo").read_text().splitlines()[0].split()[1]
        info["mem_total_mb"] = round(int(mem) / 1024)
    except OSError:
        pass
    return info


def peak_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024, 1)


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))]


# --------------------------------------------------------------------------- synthetic payloads
def payload_for(conn: Connector, ds: Dataset, req: Request, window: Window) -> bytes:
    import synthetic as syn

    step = timedelta(seconds=max(ds.resolution, 300))
    times = []
    t = window.start
    while t < window.end:
        times.append(t)
        t += step
    if conn.name == "omie":
        day = date.fromisoformat(str(req.meta["day"]))
        return syn.omie_continuous(day) if ds.raw["file"] == "precios_pibcic" else syn.omie_marginal(day)
    if conn.name == "esios":
        geos = list((ds.raw.get("geos") or {ds.raw.get("geo") or "Península": "ES"}).keys())
        base = 30.0 if ds.raw["unit"] == "EUR/MWh" else 25000.0
        return syn.esios([(t, g, base + i % 50) for i, t in enumerate(times) for g in geos])
    if conn.name == "redata":
        return syn.redata({title: [(t, 28000.0 + i) for i, t in enumerate(times)] for title in ds.raw["series"]})
    if conn.name == "open_meteo":
        hours = max(1, int((window.end - window.start).total_seconds() // 3600))
        return syn.open_meteo(window.start.replace(minute=0, second=0), hours, req.params["hourly"].split(","))
    if conn.name == "mibgas":
        first = date(int(req.meta["year"]), 1, 1)
        return syn.mibgas([first + timedelta(days=i) for i in range(365)])     # the real file is a full year
    if conn.name == "aemet":
        return syn.aemet(window.start, 24)
    if conn.name == "entsoe":
        return syn.entsoe(window.start, [500.0 + i for i in range(len(times))], minutes=int(step.seconds // 60))
    raise KeyError(conn.name)


def build_offline() -> tuple[settings_mod.Settings, dict[str, Connector]]:
    for name in ("ESIOS_TOKEN", "ENTSOE_TOKEN", "AEMET_API_KEY"):     # never real secrets here
        os.environ[name] = "offline-benchmark"
    cfg = settings_mod.load(None, "dev", {"sink.type": "null", "buffer.path": ":memory:"})
    svc = Service(cfg, sink=NullSink(), store=Store(":memory:"), only=set(cfg.sources))
    return cfg, svc.connectors


def offline(repeat: int) -> dict[str, Any]:
    cfg, connectors = build_offline()
    results: dict[str, Any] = {}
    cache: dict[tuple[str, str, int], bytes] = {}
    for conn in connectors.values():
        for ds in conn.datasets.values():
            try:
                window = Pipeline(cfg, Store(":memory:"), None, connectors, lambda: REF_NOW).live_window(ds)  # type: ignore[arg-type]
                if conn.name == "mibgas":
                    window = Window(REF_NOW - timedelta(days=365), REF_NOW + timedelta(days=1))
                reqs = conn.requests(ds, window)
            except NotImplementedError:
                continue
            for i, req in enumerate(reqs):
                cache[(conn.name, ds.name, i)] = payload_for(conn, ds, req, window)
            def key(r: Request) -> str:
                return r.url + repr(sorted(r.params.items()))
            by_key = {key(r): cache[(conn.name, ds.name, i)] for i, r in enumerate(reqs)}
            conn.download = (lambda by: lambda r: by[key(r)])(by_key)  # type: ignore[method-assign]
            samples: dict[str, list[dict[str, float]]] = {"first": [], "repoll": []}
            observations = 0
            for _ in range(repeat):
                store = Store(":memory:")
                from mibel_ingestor.gates import Gates
                pipe = Pipeline(cfg, store, Gates(cfg.quality, cfg.quality_profile), connectors, lambda: REF_NOW)
                for phase in ("first", "repoll"):
                    w0, c0 = time.perf_counter(), time.process_time()
                    rep = pipe.run(conn.name, ds.name, window)
                    sample = {"wall_ms": (time.perf_counter() - w0) * 1e3, "cpu_ms": (time.process_time() - c0) * 1e3}
                    sample.update({f"{k}_cpu_ms": v[1] * 1e3 for k, v in rep.stages.items()})
                    samples[phase].append(sample)
                    if phase == "first":
                        observations = rep.parsed
                store.close()
            summary: dict[str, Any] = {"observations": observations,
                                       "payload_kb": round(sum(len(cache[(conn.name, ds.name, i)])
                                                               for i in range(len(reqs))) / 1e3, 1)}
            for phase, rows in samples.items():
                keys = sorted({k for r in rows for k in r})
                summary[phase] = {k: {"p50": round(statistics.median([r.get(k, 0) for r in rows]), 3),
                                      "p95": round(pct([r.get(k, 0) for r in rows], 0.95), 3)} for k in keys}
            results[f"{conn.name}/{ds.name}"] = summary
    total_cpu = sum(v["first"]["cpu_ms"]["p50"] for v in results.values())
    total_obs = sum(v["observations"] for v in results.values())
    return {"jobs": results, "totals": {"first_cpu_ms_p50": round(total_cpu, 1), "observations": total_obs,
                                        "observations_per_cpu_s": round(total_obs / (total_cpu / 1e3), 0)
                                        if total_cpu else None}}


# --------------------------------------------------------------------------- online / idle
def online(env_file: Path | None) -> dict[str, Any]:
    if env_file:
        settings_mod.load_env_file(env_file)
    cfg = settings_mod.load(None, "test", {"sink.type": "null", "buffer.path": ":memory:",
                                           "service.log_level": "ERROR"})
    svc = Service(cfg, sink=NullSink(), store=Store(":memory:"))
    w0, c0 = time.perf_counter(), time.process_time()
    reports = svc.run_once()
    return {"wall_s": round(time.perf_counter() - w0, 2), "cpu_s": round(time.process_time() - c0, 3),
            "jobs": {f"{r.source}/{r.dataset}": job_summary(r) for r in reports}}


def job_summary(r: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"status": r.status, "parsed": r.parsed, "kb": round(r.bytes / 1e3, 1)}
    for k, (wall, cpu) in r.stages.items():
        out[f"{k}_wall_ms"], out[f"{k}_cpu_ms"] = round(wall * 1e3, 1), round(cpu * 1e3, 1)
    return out


def idle(seconds: float) -> dict[str, Any]:
    _cfg, connectors = build_offline()
    cfg = settings_mod.load(None, "dev", {"sink.type": "null", "buffer.path": ":memory:"})
    svc = Service(cfg, sink=NullSink(), store=Store(":memory:"), only=set(connectors))
    far = datetime(2100, 1, 1, tzinfo=UTC)
    for job in svc.scheduler.jobs:
        job.next_run = far                                    # nothing due: measures the loop itself
    thread = threading.Thread(target=svc.scheduler.run_forever, daemon=True)
    c0, w0 = time.process_time(), time.perf_counter()
    thread.start()
    time.sleep(seconds)
    svc.scheduler.stop.set()
    thread.join(timeout=30)
    cpu = time.process_time() - c0
    return {"seconds": seconds, "cpu_s": round(cpu, 4), "cpu_percent": round(100 * cpu / (time.perf_counter() - w0), 3),
            "jobs_loaded": len(svc.scheduler.jobs)}


# --------------------------------------------------------------------------- compare
def compare(a_path: Path, b_path: Path) -> None:
    a, b = (json.loads(p.read_text()) for p in (a_path, b_path))
    for label, doc in (("A", a), ("B", b)):
        print(f"{label} = {doc['machine'].get('cpu', doc['machine']['machine'])}")
    print()
    print(f"{'job':38} {'obs':>6} {'A cpu ms':>9} {'B cpu ms':>9} {'B/A':>6}   {'A repoll':>8} {'B repoll':>8}")
    for key, ja in a.get("offline", {}).get("jobs", {}).items():
        jb = b.get("offline", {}).get("jobs", {}).get(key)
        if not jb:
            continue
        ca, cb = ja["first"]["cpu_ms"]["p50"], jb["first"]["cpu_ms"]["p50"]
        print(f"{key:38} {ja['observations']:>6} {ca:>9.2f} {cb:>9.2f} {cb / ca if ca else 0:>6.1f}   "
              f"{ja['repoll']['cpu_ms']['p50']:>8.2f} {jb['repoll']['cpu_ms']['p50']:>8.2f}")
    for label, path in (("import_s", ("startup", "import_s")), ("peak_rss_mb", ("peak_rss_mb",)),
                        ("idle cpu %", ("idle", "cpu_percent"))):
        def get(d: dict[str, Any], p: tuple[str, ...] = path) -> Any:
            for k in p:
                d = d.get(k, {}) if isinstance(d, dict) else {}
            return d if not isinstance(d, dict) else "-"
        print(f"{label:38} {'':>6} {get(a)!s:>9} {get(b)!s:>9}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    off = sub.add_parser("offline")
    off.add_argument("--repeat", type=int, default=10)
    on = sub.add_parser("online")
    on.add_argument("--env-file", type=Path)
    idl = sub.add_parser("idle")
    idl.add_argument("--seconds", type=float, default=60)
    for sp in (off, on, idl):
        sp.add_argument("--out", type=Path, help="JSON output (default: benchmarks/results/<cmd>-<machine>.json)")
    cmp_ = sub.add_parser("compare")
    cmp_.add_argument("a", type=Path)
    cmp_.add_argument("b", type=Path)
    args = p.parse_args()
    if args.cmd == "compare":
        compare(args.a, args.b)
        return 0
    result: dict[str, Any] = {"benchmark": args.cmd, "created": datetime.now(UTC).isoformat(timespec="seconds"),
                              "machine": machine(), "startup": {"import_s": round(IMPORT_S, 3)}}
    if args.cmd == "offline":
        result["offline"] = offline(args.repeat)
    elif args.cmd == "online":
        result["online"] = online(args.env_file)
    else:
        result["idle"] = idle(args.seconds)
    result["peak_rss_mb"] = peak_rss_mb()
    out = args.out or HERE / "results" / f"{args.cmd}-{platform.machine()}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=1))
    print(json.dumps({k: v for k, v in result.items() if k not in ("offline", "online")}, indent=1))
    if "offline" in result:
        print(json.dumps(result["offline"]["totals"], indent=1))
    print(f"written {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
