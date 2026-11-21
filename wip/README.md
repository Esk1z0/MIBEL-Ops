# wip — work in progress (local only)

Space for work that **has not been tested yet and is not part of a package**: exploration notebooks, prototypes and spikes. Nothing in `wip/` is imported by `packages/` or `nodes/`, and nothing here is deployed.

**This folder is ignored by Git** (only this README is versioned, see [.gitignore](../.gitignore)). Half-finished work never reaches the repository; anything worth keeping is promoted to its target module.

## Rules
1. **Status first**: every item starts with a short header stating its status (`exploration`, `prototype`, `ready to promote`) and its target module (e.g. `nodes/ingestor/connectors/omie.py`).
2. **No secrets**: tokens are read from `.env` (see [.env.example](../.env.example)), never written in cells or files.
3. **Local data only**: downloads go to a local `_cache/` folder.
4. **Naming**: `NN_topic.ipynb` or `NN_topic.py`, grouped by node (`ingestion/`, `manager/`, `compute/`); graphic material goes in `visuals/`.
5. **Promotion**: when a prototype works, it moves to its target module in a `feat/<scope>-<topic>` branch, with tests and the matching documentation update ([AGENTS.md](../AGENTS.md) §7). Anything a node needs at runtime (e.g. dependency pins, benchmark scripts for the Raspberry Pi) must be promoted, because `wip/` does not travel through Git.

## Typical local contents
| Path | Status | Target |
|---|---|---|
| `ingestion/01_data_sources_exploration.ipynb` | promoted | Parsers now in [nodes/ingestor/src/mibel_ingestor/connectors/](../nodes/ingestor/README.md) · SP-01 |
| `ingestion/requirements*.txt` | promoted | Runtime pins now in [nodes/ingestor/requirements.lock.txt](../nodes/ingestor/requirements.lock.txt) |
| `visuals/` | prototype | Communication material |
