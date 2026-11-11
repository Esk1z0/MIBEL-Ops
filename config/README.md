# Configuration

Central, versioned configuration shared by the manager and compute nodes (no secrets: those go in `.env`, see [.env.example](../.env.example)).

The **ingestor is self-contained**: its sources, cadences and edge data gates (DG-E) live in [nodes/ingestor/config/](../nodes/ingestor/config/) so it can be deployed alone (ADR-05). Each node keeps its own quality file.

| File | Content | Reference |
|---|---|---|
| `settings.yaml` | Weather nodes, schedules, horizon and segments | [ADR-04](../docs/06_technical_decisions.md) |
| `retention.yaml` | Retention policy | [docs/04_retention_policy.md](../docs/04_retention_policy.md) |
| `quality/data_gates.yaml` | Data gates DG-G, DG-D, DG-P (DG-E: in the ingestor) | [docs/quality/01_data_gates.md](../docs/quality/01_data_gates.md) |
| `quality/service_baseline.yaml` | Service baseline (SB) | [docs/quality/02_service_baseline.md](../docs/quality/02_service_baseline.md) |
| `quality/promotion.yaml` | Promotion criteria (PC) | [docs/quality/03_promotion_criteria.md](../docs/quality/03_promotion_criteria.md) |

Changing the `production` profile in `quality/` requires the maintainer's confirmation ([AGENTS.md](../AGENTS.md) §4).

Status: **empty**.
