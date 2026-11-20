"""Cross-platform benchmark of the ingestor (development PC vs Raspberry Pi).

    python benchmarks/bench.py offline [--repeat 10] [--out FILE]   # CPU/memory only, synthetic payloads
    python benchmarks/bench.py online  [--env-file ../../.env]       # one real pass over every source
    python benchmarks/bench.py idle    [--seconds 60]                # service loop at rest
    python benchmarks/bench.py compare A.json B.json                 # side-by-side table (B / A)

``offline`` runs the real pipeline (parse → change detection → gates → serialization → SQLite) on
synthetic payloads with the same size and shape as each source, so two machines do exactly the same
work. Each job is measured twice per repeat: ``first`` (every value is new) and ``repoll`` (nothing
changed, the common live case). Nothing is sent anywhere (null sink, in-memory store).

Results are JSON files under ``benchmarks/results/`` (ignored by Git: they describe a machine; the
hostname is never stored, only a short hash).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import platform
import resource
import statistics
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "tests" / "fixtures"))

# The import time of the package is itself a measurement (start-up cost on the Pi)
IMPORT_START = time.perf_counter()
from mibel_ingestor import settings as settings_module  # noqa: E402
from mibel_ingestor.connectors import Connector, Dataset  # noqa: E402
from mibel_ingestor.gates import Gates  # noqa: E402
from mibel_ingestor.logs import configure_logging  # noqa: E402
from mibel_ingestor.model import UTC, Request, Window  # noqa: E402
from mibel_ingestor.pipeline import JobReport, Pipeline  # noqa: E402
from mibel_ingestor.service import Service  # noqa: E402
from mibel_ingestor.sinks import NullSink  # noqa: E402
from mibel_ingestor.store import Store  # noqa: E402


IMPORT_SECONDS = time.perf_counter() - IMPORT_START


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


# Fixed clock: identical windows, and therefore identical work, on every machine
REFERENCE_NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
FAR_FUTURE = datetime(2100, 1, 1, tzinfo=UTC)
SECRETS = ("ESIOS_TOKEN", "ENTSOE_TOKEN", "AEMET_API_KEY")
PHASES = ("first", "repoll")
MIN_SYNTHETIC_STEP_S = 300
DAYS_IN_MIBGAS_FILE = 365
RESULTS_DIR = HERE / "results"


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


@dataclass(frozen=True)
class OfflineSetup:
    """Dev settings and every connector, built without network or real secrets."""

    settings: settings_module.Settings
    connectors: dict[str, Connector]


@dataclass(frozen=True)
class TimedRun:
    """One measured run: the job report plus wall, CPU and per-stage CPU in milliseconds."""

    report: JobReport
    sample: dict[str, float]


@dataclass(frozen=True)
class PreparedJob:
    """One dataset ready for the offline benchmark: its window and the synthetic payload per request."""

    connector: Connector
    dataset: Dataset
    window: Window
    payload_by_request: dict[str, bytes]

    @property
    def payload_kb(self) -> float:
        """Total size of the synthetic payloads, in kB."""
        return round(sum(len(payload) for payload in self.payload_by_request.values()) / 1e3, 1)


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


def main() -> int:
    """Run the chosen benchmark and write its JSON result (or print a comparison)."""
    args = _build_parser().parse_args()
    if args.command == "compare":
        compare(args.a, args.b)
        return 0
    result: dict[str, Any] = {
        "benchmark": args.command,
        "created": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": describe_machine(),
        "startup": {"import_s": round(IMPORT_SECONDS, 3)},
    }
    if args.command == "offline":
        result["offline"] = run_offline(args.repeat)
    elif args.command == "online":
        result["online"] = run_online(args.env_file)
    else:
        result["idle"] = run_idle(args.seconds)
    result["peak_rss_mb"] = peak_rss_mb()
    output = args.out or RESULTS_DIR / f"{args.command}-{platform.machine()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=1))
    _print_result_summary(result)
    print(f"written {output}")
    return 0


def describe_machine() -> dict[str, Any]:
    """Describe the machine without identifying it (the hostname is only kept as a short hash)."""
    info: dict[str, Any] = {
        "hostname_hash": hash(platform.node()) & 0xFFFF,
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
    }
    # outside Linux there is no /proc: the platform fields above are enough
    with contextlib.suppress(OSError):
        info.update(_read_linux_hardware())
    return info


def peak_rss_mb() -> float:
    """Return the peak resident memory of this process, in MB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports kilobytes
    megabytes = peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024
    return round(megabytes, 1)


def run_offline(repeat: int) -> dict[str, Any]:
    """Measure every dataset on synthetic payloads, ``repeat`` times, first pass and repoll."""
    setup = _build_offline_setup()
    jobs_result: dict[str, Any] = {}
    for connector in setup.connectors.values():
        for dataset in connector.datasets.values():
            job = _prepare_job(setup, connector, dataset)
            if job is None:
                continue
            jobs_result[f"{connector.name}/{dataset.name}"] = _measure_job(setup, job, repeat)
    return {"jobs": jobs_result, "totals": _offline_totals(jobs_result)}


def run_online(env_file: Path | None) -> dict[str, Any]:
    """Run one real pass over every enabled source (test mode, null sink) and measure it."""
    if env_file:
        settings_module.load_env_file(env_file)
    node_settings = settings_module.load(None, "test", {
        "sink.type": "null",
        "buffer.path": ":memory:",
        "service.log_level": "ERROR",
    })
    configure_logging(node_settings.log_level)  # failed jobs log a WARNING line; keep the output clean
    service = Service(node_settings, sink=NullSink(), store=Store(":memory:"))
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    reports = service.run_once()
    return {
        "wall_s": round(time.perf_counter() - wall_start, 2),
        "cpu_s": round(time.process_time() - cpu_start, 3),
        "jobs": {f"{report.source}/{report.dataset}": _summarize_online_job(report) for report in reports},
    }


def run_idle(seconds: float) -> dict[str, Any]:
    """Measure the CPU of the service loop with every job loaded but none due."""
    _set_dummy_secrets()
    node_settings = settings_module.load(None, "dev", {"sink.type": "null", "buffer.path": ":memory:"})
    service = Service(node_settings, sink=NullSink(), store=Store(":memory:"), only=set(node_settings.sources))
    for job in service.scheduler.jobs:
        job.next_run = FAR_FUTURE  # nothing due: this measures the loop itself
    loop = threading.Thread(target=service.scheduler.run_forever, daemon=True)
    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    loop.start()
    time.sleep(seconds)
    service.scheduler.request_stop()
    loop.join(timeout=30)
    cpu_seconds = time.process_time() - cpu_start
    wall_seconds = time.perf_counter() - wall_start
    return {
        "seconds": seconds,
        "cpu_s": round(cpu_seconds, 4),
        "cpu_percent": round(100 * cpu_seconds / wall_seconds, 3),
        "jobs_loaded": len(service.scheduler.jobs),
    }


def compare(first_path: Path, second_path: Path) -> None:
    """Print two result files side by side (offline per job, online per job, start-up, memory, idle)."""
    first = json.loads(first_path.read_text())
    second = json.loads(second_path.read_text())
    print(f"A = {_cpu_name(first)}")
    print(f"B = {_cpu_name(second)}")
    print()
    _print_offline_comparison(first, second)
    _print_online_comparison(first, second)
    _print_general_comparison(first, second)


# ##################################################################################################
# ################################     PRIVATE HELPERS: OFFLINE     ################################
# ##################################################################################################


def _set_dummy_secrets() -> None:
    """Enable every source with dummy tokens: offline runs never contact the APIs."""
    for name in SECRETS:
        os.environ[name] = "offline-benchmark"


def _build_offline_setup() -> OfflineSetup:
    """Build dev settings and every connector, without network or real secrets."""
    _set_dummy_secrets()
    node_settings = settings_module.load(None, "dev", {"sink.type": "null", "buffer.path": ":memory:"})
    service = Service(node_settings, sink=NullSink(), store=Store(":memory:"), only=set(node_settings.sources))
    return OfflineSetup(node_settings, service.connectors)


def _prepare_job(setup: OfflineSetup, connector: Connector, dataset: Dataset) -> PreparedJob | None:
    """Build the window and synthetic payloads of a dataset and patch its download; None if not runnable."""
    window = _offline_window(setup, connector, dataset)
    try:
        requests = connector.requests(dataset, window)
    except NotImplementedError:
        return None
    payload_by_request = {
        _request_key(request): _synthetic_payload(connector, dataset, request, window) for request in requests
    }
    connector.download = _serve_from(payload_by_request)  # type: ignore[method-assign,assignment]
    return PreparedJob(connector, dataset, window, payload_by_request)


def _offline_window(setup: OfflineSetup, connector: Connector, dataset: Dataset) -> Window:
    """Return the live window at the reference time (MIBGAS: a whole year, like the real file)."""
    if connector.name == "mibgas":
        return Window(REFERENCE_NOW - timedelta(days=DAYS_IN_MIBGAS_FILE), REFERENCE_NOW + timedelta(days=1))
    pipeline = Pipeline(
        setup.settings,
        Store(":memory:"),
        Gates(setup.settings.quality, setup.settings.quality_profile),
        setup.connectors,
        lambda: REFERENCE_NOW,
    )
    return pipeline.live_window(dataset)


def _measure_job(setup: OfflineSetup, job: PreparedJob, repeat: int) -> dict[str, Any]:
    """Run a prepared job ``repeat`` times (first pass + repoll each) and summarize p50/p95."""
    samples: dict[str, list[dict[str, float]]] = {phase: [] for phase in PHASES}
    observations = 0
    for _ in range(repeat):
        store = Store(":memory:")
        gates = Gates(setup.settings.quality, setup.settings.quality_profile)
        pipeline = Pipeline(setup.settings, store, gates, setup.connectors, lambda: REFERENCE_NOW)
        for phase in PHASES:
            timed_run = _timed_run(pipeline, job)
            samples[phase].append(timed_run.sample)
            if phase == "first":
                observations = timed_run.report.parsed
        store.close()
    summary: dict[str, Any] = {"observations": observations, "payload_kb": job.payload_kb}
    for phase, phase_samples in samples.items():
        summary[phase] = _percentiles(phase_samples)
    return summary


def _timed_run(pipeline: Pipeline, job: PreparedJob) -> TimedRun:
    """Run a job once and measure it."""
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    report = pipeline.run(job.connector.name, job.dataset.name, job.window)
    sample = {
        "wall_ms": (time.perf_counter() - wall_start) * 1e3,
        "cpu_ms": (time.process_time() - cpu_start) * 1e3,
    }
    for stage, timing in report.stages.items():
        sample[f"{stage}_cpu_ms"] = timing.cpu_s * 1e3
    return TimedRun(report, sample)


def _percentiles(samples: list[dict[str, float]]) -> dict[str, dict[str, float]]:
    """Return p50 and p95 of every measured key."""
    keys = sorted({key for sample in samples for key in sample})
    summary = {}
    for key in keys:
        values = [sample.get(key, 0.0) for sample in samples]
        summary[key] = {"p50": round(statistics.median(values), 3), "p95": round(_percentile(values, 0.95), 3)}
    return summary


def _percentile(values: list[float], quantile: float) -> float:
    """Return the nearest-rank percentile of a list (0 for an empty list)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = math.ceil(quantile * len(ordered)) - 1
    return ordered[min(len(ordered) - 1, max(0, rank))]


def _offline_totals(jobs_result: dict[str, Any]) -> dict[str, Any]:
    """Sum the first-pass CPU and observations of every job."""
    total_cpu_ms = sum(job["first"]["cpu_ms"]["p50"] for job in jobs_result.values())
    total_observations = sum(job["observations"] for job in jobs_result.values())
    per_cpu_second = round(total_observations / (total_cpu_ms / 1e3), 0) if total_cpu_ms else None
    return {
        "first_cpu_ms_p50": round(total_cpu_ms, 1),
        "observations": total_observations,
        "observations_per_cpu_s": per_cpu_second,
    }


def _request_key(request: Request) -> str:
    """Identify a request by its URL and parameters (to serve the matching synthetic payload)."""
    return request.url + repr(sorted(request.params.items()))


def _serve_from(payload_by_request: dict[str, bytes]) -> Callable[[Request], bytes]:
    """Return a download function that answers from prepared payloads instead of the network."""
    def download(request: Request) -> bytes:
        return payload_by_request[_request_key(request)]
    return download


def _synthetic_payload(connector: Connector, dataset: Dataset, request: Request, window: Window) -> bytes:
    """Build a payload with the shape and size the real source would return for this request."""
    import synthetic

    options = dataset.options
    moments = _synthetic_moments(dataset, window)
    if connector.name == "omie":
        day = date.fromisoformat(str(request.meta["day"]))
        if options["file"] == "precios_pibcic":
            return synthetic.omie_continuous(day)
        return synthetic.omie_marginal(day)
    if connector.name == "esios":
        geos = list(options.get("geos") or {options.get("geo") or "Península": "ES"})
        base_value = 30.0 if options["unit"] == "EUR/MWh" else 25000.0
        points = [
            synthetic.EsiosPoint(moment, geo, base_value + i % 50) for i, moment in enumerate(moments) for geo in geos
        ]
        return synthetic.esios(points)
    if connector.name == "redata":
        series = {title: [(moment, 28000.0 + i) for i, moment in enumerate(moments)] for title in options["series"]}
        return synthetic.redata(series)
    if connector.name == "open_meteo":
        hours = max(1, int((window.end - window.start).total_seconds() // 3600))
        first_hour = window.start.replace(minute=0, second=0)
        return synthetic.open_meteo(first_hour, hours, request.params["hourly"].split(","))
    if connector.name == "mibgas":
        first_day = date(int(request.meta["year"]), 1, 1)
        return synthetic.mibgas([first_day + timedelta(days=i) for i in range(DAYS_IN_MIBGAS_FILE)])
    if connector.name == "aemet":
        return synthetic.aemet(window.start, 24)
    if connector.name == "entsoe":
        step_minutes = int(_synthetic_step(dataset).total_seconds() // 60)
        return synthetic.entsoe(window.start, [500.0 + i for i in range(len(moments))], minutes=step_minutes)
    raise KeyError(connector.name)


def _synthetic_step(dataset: Dataset) -> timedelta:
    """Spacing of synthetic values: the dataset resolution, at least 5 minutes."""
    return timedelta(seconds=max(dataset.resolution, MIN_SYNTHETIC_STEP_S))


def _synthetic_moments(dataset: Dataset, window: Window) -> list[datetime]:
    """List the moments of the window at the synthetic step."""
    step = _synthetic_step(dataset)
    moments = []
    moment = window.start
    while moment < window.end:
        moments.append(moment)
        moment += step
    return moments


# ##################################################################################################
# ##########################     PRIVATE HELPERS: ONLINE AND MACHINE     ###########################
# ##################################################################################################


def _summarize_online_job(report: JobReport) -> dict[str, Any]:
    """Return status, size and per-stage wall/CPU milliseconds of one real job."""
    summary: dict[str, Any] = {
        "status": report.status.value,
        "error": report.error,
        "parsed": report.parsed,
        "kb": round(report.bytes / 1e3, 1),
    }
    for stage, timing in report.stages.items():
        summary[f"{stage}_wall_ms"] = round(timing.wall_s * 1e3, 1)
        summary[f"{stage}_cpu_ms"] = round(timing.cpu_s * 1e3, 1)
    return summary


def _read_linux_hardware() -> dict[str, Any]:
    """Read the CPU model and total memory from ``/proc`` (Linux only)."""
    hardware: dict[str, Any] = {}
    cpuinfo = Path("/proc/cpuinfo").read_text().splitlines()
    # x86 uses "model name"; Raspberry Pi kernels expose "Model" or "Hardware"
    for key in ("model name", "Model", "Hardware"):
        for line in cpuinfo:
            if line.startswith(key):
                hardware.setdefault("cpu", line.split(":", 1)[1].strip())
    first_meminfo_line = Path("/proc/meminfo").read_text().splitlines()[0]
    total_kb = int(first_meminfo_line.split()[1])
    hardware["mem_total_mb"] = round(total_kb / 1024)
    return hardware


# ##################################################################################################
# ################################     PRIVATE HELPERS: OUTPUT     #################################
# ##################################################################################################


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with its subcommands."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    offline = commands.add_parser("offline")
    offline.add_argument("--repeat", type=int, default=10)
    online = commands.add_parser("online")
    online.add_argument("--env-file", type=Path)
    idle = commands.add_parser("idle")
    idle.add_argument("--seconds", type=float, default=60)
    for command in (offline, online, idle):
        command.add_argument(
            "--out",
            type=Path,
            help="JSON output (default: benchmarks/results/<command>-<machine>.json)",
        )
    compare_command = commands.add_parser("compare")
    compare_command.add_argument("a", type=Path)
    compare_command.add_argument("b", type=Path)
    return parser


def _print_result_summary(result: dict[str, Any]) -> None:
    """Print the result without the per-job details (those stay in the JSON file)."""
    summary = {key: value for key, value in result.items() if key not in ("offline", "online")}
    print(json.dumps(summary, indent=1))
    if "offline" in result:
        print(json.dumps(result["offline"]["totals"], indent=1))
    if "online" in result:
        print(json.dumps({"wall_s": result["online"]["wall_s"], "cpu_s": result["online"]["cpu_s"]}, indent=1))


def _cpu_name(document: dict[str, Any]) -> str:
    """Return the CPU model of a result file, or its architecture."""
    machine = document["machine"]
    return str(machine.get("cpu", machine["machine"]))


def _print_offline_comparison(first: dict[str, Any], second: dict[str, Any]) -> None:
    """Print the offline CPU per job and the B/A ratio."""
    first_jobs = first.get("offline", {}).get("jobs", {})
    second_jobs = second.get("offline", {}).get("jobs", {})
    if not first_jobs or not second_jobs:
        return
    print(f"{'offline job':38} {'obs':>6} {'A cpu ms':>9} {'B cpu ms':>9} {'B/A':>6}   {'A repoll':>8} {'B repoll':>8}")
    for name, first_job in first_jobs.items():
        second_job = second_jobs.get(name)
        if not second_job:
            continue
        first_cpu = first_job["first"]["cpu_ms"]["p50"]
        second_cpu = second_job["first"]["cpu_ms"]["p50"]
        ratio = second_cpu / first_cpu if first_cpu else 0.0
        print(f"{name:38} {first_job['observations']:>6} {first_cpu:>9.2f} {second_cpu:>9.2f} {ratio:>6.1f}   "
              f"{first_job['repoll']['cpu_ms']['p50']:>8.2f} {second_job['repoll']['cpu_ms']['p50']:>8.2f}")
    print()


def _print_online_comparison(first: dict[str, Any], second: dict[str, Any]) -> None:
    """Print the real pass per job: status, total CPU and download wall time."""
    first_online = first.get("online", {})
    second_online = second.get("online", {})
    if not first_online or not second_online:
        return
    print(f"{'online job':38} {'A st':>5} {'B st':>5} {'A cpu ms':>9} {'B cpu ms':>9} {'B dl s':>7}")
    for name, second_job in second_online.get("jobs", {}).items():
        first_job = first_online.get("jobs", {}).get(name, {})
        print(f"{name:38} {str(first_job.get('status', '-'))[:4]:>5} {str(second_job['status'])[:4]:>5} "
              f"{_total_cpu_ms(first_job):>9.0f} {_total_cpu_ms(second_job):>9.0f} "
              f"{second_job.get('download_wall_ms', 0) / 1e3:>7.1f}")
    print(f"{'total':38} {'':>5} {'':>5} {first_online['cpu_s'] * 1e3:>9.0f} {second_online['cpu_s'] * 1e3:>9.0f}")
    print()


def _total_cpu_ms(job: dict[str, Any]) -> float:
    """Sum the per-stage CPU milliseconds of an online job."""
    return float(sum(value for key, value in job.items() if key.endswith("_cpu_ms")))


def _print_general_comparison(first: dict[str, Any], second: dict[str, Any]) -> None:
    """Print start-up time, peak memory and idle CPU."""
    measures = {
        "import_s": ("startup", "import_s"),
        "peak_rss_mb": ("peak_rss_mb",),
        "idle cpu %": ("idle", "cpu_percent"),
    }
    for label, path in measures.items():
        print(f"{label:38} {_lookup(first, path)!s:>9} {_lookup(second, path)!s:>9}")


def _lookup(document: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Follow a key path in a result file; ``-`` when it is missing."""
    value: Any = document
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return "-"
        value = value[key]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
