# Ingestor benchmark (development PC vs Raspberry Pi)

`bench.py` measures the ingestor on any machine with the same workload, so platforms can be compared. Results are written as JSON to `benchmarks/results/` (ignored by Git: they describe a machine; the hostname is never stored).

| Command | What it measures | Network |
|---|---|---|
| `python benchmarks/bench.py offline --repeat 10` | CPU and wall time per dataset and per stage (`parse`, `detect`, `gates`, `serialize`) on synthetic payloads with the shape and size of each source; `first` pass (all values new) and `repoll` (nothing changed) | No |
| `python benchmarks/bench.py online --env-file ../../.env` | One real pass over every enabled source (download time, payload size, CPU) | Yes, ~30 requests |
| `python benchmarks/bench.py idle --seconds 60` | CPU of the service loop at rest with every job loaded | No |
| `python benchmarks/bench.py compare A.json B.json` | Side-by-side table with the B / A ratio | — |

All commands also report import time and peak RSS. Run them from `nodes/ingestor/` with the node's venv active.

**Caveat:** the synthetic MIBGAS workbook is much smaller than the real yearly file (3.3 MB), so `online` is the reference for MIBGAS.

## Reference results (development desktop PC, 2026-11-11)
AMD Ryzen 5 9600X · Python 3.12.3 · x86_64

| Measure | Value |
|---|---|
| `offline`: 21 datasets, 6,468 observations | 93 ms CPU (~70,000 observations per CPU second) |
| `online`: 18 datasets | 36 s wall · 1.4 s CPU (1.0 s parsing the MIBGAS Excel) |
| `idle` | 0.008 % CPU |
| Peak RSS | 58 MB offline · 68 MB online |
| Import time | 0.06 s |

Raspberry Pi: pending. Expected order of magnitude: 10–20× more CPU per operation; the budget is < 200 MB RAM (NFR-02) and a low average CPU.
