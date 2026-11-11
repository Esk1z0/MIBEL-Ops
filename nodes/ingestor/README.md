# Ingestor node

**Role:** capture its configured sources, clean and validate them at the edge (DG-E), and deliver curated observations with their lineage to the manager. Nothing else: no raw payloads, no long-term storage. Runs as a lightweight `systemd` service (no Docker) on a Raspberry Pi or any Linux machine.

Design: [ADR-05](../../docs/06_technical_decisions.md) · Requirements: FR-03 … FR-06, NFR-02, NFR-03 in [docs/01_requirements.md](../../docs/01_requirements.md) · Stories: US-03, US-04 in [docs/02_user_stories.md](../../docs/02_user_stories.md) · Gates: DG-E in [docs/quality/01_data_gates.md](../../docs/quality/01_data_gates.md) · Sources: [docs/05_data_sources.md](../../docs/05_data_sources.md)

The node is **self-contained**: this folder can be copied to a machine and installed on its own (it does not depend on `packages/mibel_common`).

## Layout
| Path | Content |
|---|---|
| `config/ingestor.yaml` | Sources, datasets, cadences, windows, run modes (no secrets) |
| `config/quality.yaml` | Edge data gates DG-E per quality profile |
| `src/mibel_ingestor/connectors/` | `base.py` (port, template and registry) + one module per source: `omie`, `esios`, `redata`, `open_meteo`, `mibgas`, `aemet`, `entsoe` |
| `src/mibel_ingestor/pipeline.py` | The single processing path: requests → parse → change detection → gates → outbox |
| `src/mibel_ingestor/scheduler.py` | In-process scheduler (per-dataset cadence, thread pool, retries) and deliverer |
| `src/mibel_ingestor/` (rest) | `model` (contract), `gates`, `store` (SQLite), `sinks`, `http`, `settings`, `metrics`, `logs`, `service`, `cli` |
| `tests/` | Contract tests per connector with synthetic payloads (`tests/fixtures/synthetic.py`) |
| `benchmarks/` | Development PC vs Raspberry Pi benchmark ([README](benchmarks/README.md)) |
| `deploy/` | `install.sh`, `mibel-ingestor.service`, `env.example` |
| `requirements.lock.txt` | Tested runtime pins |

## Run modes
| Mode | Quality profile | What can be changed | Typical use |
|---|---|---|---|
| `pro` | production | Nothing (only secrets and deployment values from the environment) | `systemd` service on a node |
| `test` | production | `sink`, `metrics`, `buffer.path`, `service.log_level`, `sources.*.enabled` | Same as production while Kafka is not running: `--set sink.type=jsonl` |
| `dev` | development | Everything (default sink: `stdout`) | Partial tests of one source or dataset |

Overrides: `--set key=value` or an unversioned `config/ingestor.local.yaml`; both follow the same rules.
Environment: `MIBEL_NODE_ID` (required in `pro`), `MIBEL_KAFKA_BOOTSTRAP` (Kafka sink), `MIBEL_METRICS_HOST` (VPN address to expose metrics), `MIBEL_STATE_DIR` or systemd's `STATE_DIRECTORY`, and the tokens `ESIOS_TOKEN`, `AEMET_API_KEY`, `ENTSOE_TOKEN` (see [.env.example](../../.env.example)).

## Quick start (development machine)
```bash
python -m venv .venv && source .venv/bin/activate        # from the repository root
pip install -e "nodes/ingestor[excel,dev]"
cd nodes/ingestor
mibel-ingestor sources --env-file ../../.env              # which sources are on, and why
mibel-ingestor once --source omie                         # dev mode: one pass, printed to stdout
mibel-ingestor once --mode test --set sink.type=jsonl --env-file ../../.env    # production rules, file output
mibel-ingestor run  --mode test --set sink.type=jsonl --env-file ../../.env    # the service loop
mibel-ingestor backfill --source omie --dataset day_ahead --start 2025-10-01 --end 2025-10-31
pytest && ruff check . && mypy
```

## Adding a source
1. Create `src/mibel_ingestor/connectors/<source>.py` with a class decorated with `@register`, `name = "<source>"`, and the methods `requests()` and `parse()` (plus `download()` only if the source is not a plain GET).
2. Add its block under `sources:` in `config/ingestor.yaml` (datasets, cadence, window; `enabled: auto` + `secrets:` if it needs a token).
3. Add a contract test with a synthetic payload in `tests/` and, if useful, a generator in `tests/fixtures/synthetic.py` (the benchmark reuses it).
4. If it brings new variables, check that a DG-E range rule covers them in `config/quality.yaml`.

## Installing on a Raspberry Pi
`sudo deploy/install.sh [--with-kafka]`: it creates the `mibel` user, a venv in `/opt/mibel-ingestor` from `requirements.lock.txt` and the `systemd` unit. It changes the operating system, so it needs the maintainer's approval ([AGENTS.md](../../AGENTS.md) §4). Then fill `/etc/mibel-ingestor/env` and `systemctl enable --now mibel-ingestor`.

Status: **v0.1** · tested on the development desktop PC (x86_64, Python 3.12) on 2026-11-11 · Raspberry Pi and Kafka pending.
