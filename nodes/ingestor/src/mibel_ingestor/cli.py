"""Command line: ``mibel-ingestor <command> [--mode pro|test|dev] [--set key=value ...]``.

Commands:

- ``run``: service loop (systemd entry point).
- ``once``: run every enabled job (or one source/dataset) once and print a report.
- ``backfill``: historical download by date range, resumable.
- ``check``: validate the configuration and list the jobs that would run.
- ``sources``: list the available connectors and why each one is enabled or not.

Exit codes: 0 success, 1 some job failed (``once``), 2 configuration error.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from . import __version__, settings
from .logs import configure_logging
from .metrics import start_metrics_server
from .service import Service, describe_sources

# --------------------------------------------------------------------------- constants
EXIT_OK = 0
EXIT_JOB_FAILED = 1
EXIT_CONFIG_ERROR = 2


# --------------------------------------------------------------------------- public API
def main(argv: list[str] | None = None) -> int:
    """Parse the arguments, load the configuration and run the chosen command."""
    args = _build_parser().parse_args(argv)
    if args.env_file:
        settings.load_env_file(args.env_file)
    try:
        node_settings = settings.load(args.config, args.mode, settings.parse_set_args(args.set))
    except settings.ConfigError as exc:
        return _config_error(exc)
    configure_logging(node_settings.log_level)

    if args.command == "sources":
        _print_json([asdict(status) for status in describe_sources(node_settings)])
        return EXIT_OK

    only = {args.source} if getattr(args, "source", None) else None
    try:
        service = Service(node_settings, only=only)
    except (settings.ConfigError, RuntimeError, ValueError) as exc:
        return _config_error(exc)
    try:
        return _run_command(args, node_settings, service)
    finally:
        service.close()


# --------------------------------------------------------------------------- private helpers
def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with its subcommands."""
    parser = argparse.ArgumentParser(prog="mibel-ingestor", description="MIBEL-Ops ingestor node")
    parser.add_argument("--version", action="version", version=__version__)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--mode",
        choices=[mode.value for mode in settings.RunMode],
        help="run mode (default: $MIBEL_INGESTOR_MODE or dev)",
    )
    common.add_argument("--config", type=Path, help="configuration directory (default: nodes/ingestor/config)")
    common.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override a setting allowed by the mode (e.g. sink.type=jsonl)",
    )
    common.add_argument("--env-file", type=Path, help="load KEY=VALUE secrets from this file (development)")

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", parents=[common], help="run the service")
    once = commands.add_parser("once", parents=[common], help="run jobs once")
    once.add_argument("--source")
    once.add_argument("--dataset")
    backfill = commands.add_parser("backfill", parents=[common], help="historical download")
    backfill.add_argument("--source", required=True)
    backfill.add_argument("--dataset", required=True)
    backfill.add_argument("--start", type=date.fromisoformat, required=True)
    backfill.add_argument("--end", type=date.fromisoformat, required=True)
    commands.add_parser("check", parents=[common], help="validate configuration")
    commands.add_parser("sources", parents=[common], help="list connectors")
    return parser


def _run_command(args: argparse.Namespace, node_settings: settings.Settings, service: Service) -> int:
    """Dispatch to the chosen command and return its exit code."""
    if args.command == "check":
        _print_json(_describe_configuration(node_settings, service))
        return EXIT_OK
    if args.command == "once":
        reports = service.run_once(args.source, args.dataset)
        _print_json([report.as_dict() for report in reports])
        return EXIT_OK if all(report.succeeded for report in reports) else EXIT_JOB_FAILED
    if args.command == "backfill":
        reports = service.backfill(args.source, args.dataset, args.start, args.end)
        _print_json({
            "chunks": len(reports),
            "failed": [report.as_dict() for report in reports if not report.succeeded],
            "sent": sum(report.sent for report in reports),
            "rejected": sum(report.rejected for report in reports),
        })
        return EXIT_OK
    # command == "run"
    if node_settings.metrics.enabled:
        start_metrics_server(node_settings.metrics.host, node_settings.metrics.port)
    signal.signal(signal.SIGTERM, lambda *_: service.scheduler.request_stop())
    signal.signal(signal.SIGINT, lambda *_: service.scheduler.request_stop())
    service.run_forever()
    return EXIT_OK


def _describe_configuration(node_settings: settings.Settings, service: Service) -> dict[str, Any]:
    """Summarize the resolved configuration and the jobs that would run (``check``)."""
    jobs = [
        {"source": job.source, "dataset": job.dataset, "every_s": job.every, "offset_s": job.offset}
        for job in service.scheduler.jobs
    ]
    return {
        "mode": node_settings.mode.value,
        "quality_profile": node_settings.quality_profile.value,
        "node": node_settings.node_id,
        "sink": node_settings.sink.type.value,
        "state_dir": str(node_settings.state_dir),
        "gates": len(service.gates.rules),
        "jobs": jobs,
    }


def _config_error(error: Exception) -> int:
    """Print a configuration error to stderr and return its exit code."""
    print(f"configuration error: {error}", file=sys.stderr)
    return EXIT_CONFIG_ERROR


def _print_json(data: Any) -> None:
    """Print data as indented JSON on stdout."""
    print(json.dumps(data, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    raise SystemExit(main())
