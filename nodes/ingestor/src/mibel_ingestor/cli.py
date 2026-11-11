"""Command line: ``mibel-ingestor <command> [--mode pro|test|dev] [--set key=value ...]``.

Commands:
  run        service loop (systemd entry point)
  once       run every enabled job (or one source/dataset) once and print a report
  backfill   historical download by date range, resumable
  check      validate the configuration and list the jobs that would run
  sources    list available connectors and why each one is enabled or not
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
from datetime import date
from pathlib import Path
from typing import Any

from . import __version__, logs, metrics, settings
from .service import Service, source_status


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mibel-ingestor", description="MIBEL-Ops ingestor node")
    p.add_argument("--version", action="version", version=__version__)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--mode", choices=settings.MODES, help="run mode (default: $MIBEL_INGESTOR_MODE or dev)")
    common.add_argument("--config", type=Path, help="configuration directory (default: nodes/ingestor/config)")
    common.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                        help="override a setting allowed by the mode (e.g. sink.type=jsonl)")
    common.add_argument("--env-file", type=Path, help="load KEY=VALUE secrets from this file (development)")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("run", parents=[common], help="run the service")
    once = sub.add_parser("once", parents=[common], help="run jobs once")
    once.add_argument("--source")
    once.add_argument("--dataset")
    bf = sub.add_parser("backfill", parents=[common], help="historical download")
    bf.add_argument("--source", required=True)
    bf.add_argument("--dataset", required=True)
    bf.add_argument("--start", type=date.fromisoformat, required=True)
    bf.add_argument("--end", type=date.fromisoformat, required=True)
    sub.add_parser("check", parents=[common], help="validate configuration")
    sub.add_parser("sources", parents=[common], help="list connectors")
    return p


def _print(data: Any) -> None:
    print(json.dumps(data, indent=1, ensure_ascii=False, default=str))


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.env_file:
        settings.load_env_file(args.env_file)
    try:
        cfg = settings.load(args.config, args.mode, settings.parse_set_args(args.set))
    except settings.ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    logs.setup(cfg.log_level)

    if args.command == "sources":
        _print([vars(s) for s in source_status(cfg)])
        return 0

    only = {args.source} if getattr(args, "source", None) else None
    try:
        service = Service(cfg, only=only)
    except (settings.ConfigError, RuntimeError, ValueError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        if args.command == "check":
            _print({"mode": cfg.mode, "quality_profile": cfg.quality_profile, "node": cfg.node_id,
                    "sink": cfg.sink.type, "state_dir": str(cfg.state_dir), "gates": len(service.gates.rules),
                    "jobs": [{"source": j.source, "dataset": j.dataset, "every_s": j.every, "offset_s": j.offset}
                             for j in service.scheduler.jobs]})
        elif args.command == "once":
            reports = service.run_once(args.source, args.dataset)
            _print([r.as_dict() for r in reports])
            return 0 if all(r.succeeded for r in reports) else 1
        elif args.command == "backfill":
            reports = service.backfill(args.source, args.dataset, args.start, args.end)
            _print({"chunks": len(reports), "failed": [r.as_dict() for r in reports if not r.succeeded],
                    "sent": sum(r.sent for r in reports), "rejected": sum(r.rejected for r in reports)})
        elif args.command == "run":
            if cfg.metrics.enabled:
                metrics.serve(cfg.metrics.host, cfg.metrics.port)
            signal.signal(signal.SIGTERM, lambda *_: service.scheduler.stop.set())
            signal.signal(signal.SIGINT, lambda *_: service.scheduler.stop.set())
            service.run_forever()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
