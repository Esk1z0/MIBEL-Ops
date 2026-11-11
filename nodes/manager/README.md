# Manager node

**Role:** strict coordinator. It receives, stores, versions, registers models, orchestrates, evaluates with stored data, applies retention and serves dashboards. **It never trains, serves or runs a model** (decision A-02).

References: FR-07 … FR-14 in [docs/01_requirements.md](../../docs/01_requirements.md) · US-05 … US-08 in [docs/02_user_stories.md](../../docs/02_user_stories.md) · ADR-01 and ADR-02 in [docs/06_technical_decisions.md](../../docs/06_technical_decisions.md) · [docs/04_retention_policy.md](../../docs/04_retention_policy.md)

| Path | Content |
|---|---|
| `consumer/` | Kafka → dedup → batch Great Expectations → TimescaleDB; emits `mibel.window.ready` |
| `expectations/` | Great Expectations suites (data gates DG-G) |
| `storage/` | DB migrations, compression, continuous aggregates, wide views |
| `dataset/` | Parquet export + DVC |
| `registry/` | MLflow aliases, quality gate, promotion, rollback |
| `orchestration/` | Prefect flows and automations |
| `evaluation/` | Model-free metrics, internal baselines, Deepchecks, health states |
| `node_registry/` | Ingestor inventory and operating mode |
| `retention/` | `retention_flow` (dry-run by default) |
| `dashboards/` | `grafana/` and `superset/` definitions (versioned, no secrets) |
| `deploy/` | `docker-compose.yml`, K3s server configuration, Traefik |

Status: **empty**.
