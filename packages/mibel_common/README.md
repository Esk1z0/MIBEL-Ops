# mibel_common

Shared package installed on the **manager and compute nodes** (the ingestor is self-contained, ADR-05; it shares only the versioned wire contract `mibel.observations/1`). See [docs/03_diagrams.md](../../docs/03_diagrams.md) §12 and §17, and user story US-01 in [docs/02_user_stories.md](../../docs/02_user_stories.md).

| Module | Content | Installed on |
|---|---|---|
| `contracts/` | Versioned schemas: one per source, `FeatureVector`, `PredictionBatch`, `SkippedWindow`, `NodeRegistration` | Manager, compute |
| `rules/` | Validation rules for the manager and compute nodes (DG-P). The ingestor's DG-E rules live in the self-contained ingestor (ADR-05) | Manager, compute |
| `features/` | Point-in-time feature library (thermal gap, renewable deviation, lags, calendar) | Compute (`[compute]` extra) |
| `config/` | Loading and validation of `config/*.yaml` | All |

Status: **empty**. Prototypes are developed first in [wip/](../../wip/README.md).
