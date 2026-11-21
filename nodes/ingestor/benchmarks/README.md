# Ingestor benchmark (development PC vs Raspberry Pi)

`bench.py` measures the ingestor on any machine with the same workload, so platforms can be compared. Results are written as JSON to `benchmarks/results/` (ignored by Git: they describe a machine; the hostname is never stored).

| Command | What it measures | Network |
|---|---|---|
| `python benchmarks/bench.py offline --repeat 10` | CPU and wall time per dataset and per stage (`parse`, `detect`, `gates`, `serialize`) on synthetic payloads with the shape and size of each source; `first` pass (all values new) and `repoll` (nothing changed) | No |
| `python benchmarks/bench.py online --env-file ../../.env` | One real pass over every enabled source (download time, payload size, CPU) | Yes, ~30 requests |
| `python benchmarks/bench.py idle --seconds 60` | CPU of the service loop at rest with every job loaded | No |
| `python benchmarks/bench.py compare A.json B.json` | Side-by-side table with the B / A ratio | — |

All commands also report import time and peak RSS. Run them from `nodes/ingestor/` with the node's venv active.

## Full manual test on a machine: `run_suite.sh`
One script runs everything in order and packs the results in a single `.tar.gz` (no tokens, no hostname, no market data):

```bash
cd nodes/ingestor && source .venv/bin/activate
benchmarks/run_suite.sh ~/mibel.env 15        # env file with the tokens, minutes of service loop (0 skips it)
```

| Step | Output file |
|---|---|
| Machine description (architecture, Python, board model, memory, CPUs) | `system.txt` |
| `check` and `sources` (configuration and enabled sources) | `check.json`, `sources.json` |
| `once` in test mode against the real APIs, with peak memory (`/usr/bin/time -v`) | `once.json`, `once.log` |
| `bench.py offline`, `online` and `idle` (`IDLE_SECONDS`, default 120) | `offline.json`, `online.json`, `idle.json` |
| Service loop for N minutes, with a Prometheus snapshot 15 s before the stop (SIGTERM) | `run.log`, `metrics.txt` |

It takes about 6 minutes plus the service loop. On a Raspberry Pi over SSH, run it inside `tmux` so a dropped connection does not stop it. Compare two machines with `bench.py compare A/offline.json B/offline.json` (and the same for `online.json`).

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

## Raspberry Pi vs development PC (2026-11-19)
A = AMD Ryzen 5 9600X · Python 3.12.3 · x86_64 · B = Raspberry Pi 3 Model B+ (4 cores, 894 MB RAM, no swap) · Ubuntu Server 64-bit · Python 3.14.4 · aarch64. Same code, both measured on the same days.

| Measure | Development PC | Raspberry Pi 3B+ | Pi / PC |
|---|---|---|---|
| `offline`: CPU per dataset (first pass) | 0.2–21 ms | 4–440 ms | **18–24×** |
| `offline`: repeated poll with no changes | 0.07–14 ms | 1.7–234 ms | ~4× cheaper than the first pass on both |
| `online`: 21 datasets | 56 s wall · 1.45 s CPU | 74 s wall · **6.2 s CPU** | 4.3× (network dominates) |
| `online`: costliest job | MIBGAS 1.0 s (Excel) | MIBGAS 3.3 s (Excel 3.0 s) · AEMET 0.6 s · OMIE IDA 0.7 s (TLS handshakes) | |
| `once` (test mode, real APIs) | 21/21 ok · 1.6 s CPU | 21/21 ok · 0 rejected · 3,385 observations · 8.6 s CPU · 48 s wall | |
| `run` for 15 min | — | 32/32 jobs ok, no warnings, clean stop | |
| `idle` | 0.003 % CPU | **0.015 % CPU** | |
| Peak RSS | 58–68 MB | **45–59 MB** | Limit 200 MB (NFR-02) |
| Import time | 0.05–0.10 s | 0.95–1.4 s | Only at start-up |

**Daily budget on the Pi** (online cost per job × its cadence, worst case where every pass costs like the first): **~120 s of CPU per day ≈ 0.14 % of one core** (0.035 % of the four). The heaviest contributors are ESIOS real-time demand (every 5 min, ~24 s/day), OMIE intraday auctions and AEMET (TLS handshakes to several hosts). Conclusion: the Pi 3B+ runs the ingestor with a wide margin; no optimisation is needed now.
