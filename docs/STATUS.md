# Project status

> Where the project stands, for the maintainer and for any agent starting a session. Read it first;
> update it in the same change whenever the state changes ([AGENTS.md](../AGENTS.md) §7). Work follows
> the incremental flow of [AGENTS.md](../AGENTS.md) §9: plan first, then one class at a time.

**Last updated:** 2026-11-21 · **Active branch:** `fix/ingestor-review` → PR to `main`

## Now
- Merging the ingestor node into `main` in one PR (`fix/ingestor-review`, which contains `feat/ingestor-core`).
- Next step right after the merge: start the **manager node** (see [Next](#next)).

## Machines
| Role | Machine | Use now |
|---|---|---|
| Development | Desktop PC | Write and test everything first |
| Manager | Laptop | Second test of the manager, once it works on the desktop |
| Ingestor | Raspberry Pi 3B+ (aarch64, Python 3.14, 894 MB RAM) | Runs the ingestor; benchmarks done |

No hostnames, VPN addresses or personal data here ([AGENTS.md](../AGENTS.md) §3).

## Done
### Ingestor node (US-03, US-04 · [ADR-05](06_technical_decisions.md))
- Lightweight hexagonal node in [nodes/ingestor/](../nodes/ingestor/README.md): connector registry with 7 sources
  (OMIE, ESIOS, REData, Open-Meteo, MIBGAS, AEMET, ENTSO-E), one pipeline for live and backfill,
  edge data gates ([DG-E](quality/01_data_gates.md)), SQLite outbox (30 days / 1 GB), sinks kafka / jsonl / stdout / null,
  run modes pro / test / dev, Prometheus metrics and JSON logs.
- Code style guide applied ([AGENTS.md](../AGENTS.md) §8, A-27).
- Review fixes (branch `fix/ingestor-review`): token redaction in HTTP library logs, scheduler across DST
  and job crashes, ENTSO-E A03 tail filling, config validation, bounded HTTP waits, SQLite connections closed.
- Tests: 114 passing with `python -X dev -W error -m pytest` on Python 3.12 and 3.13; ruff and `mypy --strict` clean.
- Real runs: 21/21 jobs OK on desktop and Pi; ~120 s of CPU per day on the Pi (0.14 % of one core), 45–71 MB RSS.

## Next
In this order; each item starts with a plan to be confirmed ([AGENTS.md](../AGENTS.md) §9).

1. **Ingestor under systemd on the Pi**, so the manager has live data to test with.
2. **Manager node skeleton + deployed services** (US-05, US-08): Kafka, Prometheus, Grafana and the other
   services the manager needs, to see the data in real time and its lineage. Tested first on the desktop,
   then on the laptop.
3. **Model functionality** (US-09 to US-11).
4. **Compute (processor) node.**

## Pending decisions
| Topic | Question | Where |
|---|---|---|
| DG-E12 intraday auction prices | IDA prices may use SIDC limits (±9999 €/MWh) instead of [−500, 4000]; a production threshold, needs the maintainer | [config/quality.yaml](../nodes/ingestor/config/quality.yaml), [data gates](quality/01_data_gates.md) |
| Manager services | Exact list of services and versions for the skeleton | Plan of the next step |

## Not verified yet
- Kafka sink (`confluent-kafka`) against a real broker: comes with the manager.
- systemd unit of the ingestor on the Pi.
- Test run on the Pi (Python 3.14) after the SQLite connection fix (`9cfec56`).
- ENTSO-E A03 filling with document types other than A11 flows.

## Test evidence
- Benchmarks and how to run them: [nodes/ingestor/benchmarks/README.md](../nodes/ingestor/benchmarks/README.md)
  (`run_suite.sh` packs a full manual test of a machine).
- Pi vs desktop summary: [ADR-05 §5.4](06_technical_decisions.md).
- Raw result files stay local (`benchmarks/results/` is ignored by git: they may hold market data).
