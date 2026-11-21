# MIBEL-Ops Documentation (v0.3 · intraday focus)

Distributed MLOps platform that **forecasts, every 15 minutes, the price of the upcoming quarter-hours of the Spanish intraday electricity market**. It is organised by node role: ingestors (1..N), one coordinating manager (1) and compute nodes (0..N, home or cloud).

**Current progress, next step and pending decisions: [STATUS.md](STATUS.md).**

| Document | Content |
|---|---|
| [01_requirements.md](01_requirements.md) | Vision, measurable goals, target variable, roles and operating modes, 19 functional and 12 non-functional requirements, risks and future work |
| [02_user_stories.md](02_user_stories.md) | 1 exploration task + 13 coarse-grained user stories and release plan R0–R5 |
| [03_diagrams.md](03_diagrams.md) | 16 Mermaid diagrams and repository structure by node (`venv` + `pip`) |
| [04_retention_policy.md](04_retention_policy.md) | Retention of data, models and artifacts; 200 GB budget; overflow to Google Drive |
| [05_data_sources.md](05_data_sources.md) | Study of free data sources (OMIE, ESIOS, REData, ENTSO-E, Open-Meteo, MIBGAS, AEMET) |
| [06_technical_decisions.md](06_technical_decisions.md) | Decision log and accepted ADR-01 to ADR-05: Compose + K3s, Job queue with native K3s node liveness, ONNX only, 8 quarters (nowcast/forecast, extensible), ingestor architecture |
| [quality/](quality/README.md) | Quality framework in 3 editable documents with development and production profiles: [data gates](quality/01_data_gates.md), [service baseline](quality/02_service_baseline.md) and [promotion criteria](quality/03_promotion_criteria.md) |

The original vision document is [whitepaper.md](../whitepaper.md).

To view the diagrams in VS Code: install the **Markdown Preview Mermaid Support** extension and open the preview with `Ctrl+Shift+V`.
