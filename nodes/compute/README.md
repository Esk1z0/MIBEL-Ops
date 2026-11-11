# Compute node

**Role:** every heavy task: training, testing, packaging and deploying models, **building point-in-time features and generating predictions**. Runs as a K3s agent (`role=compute`, `gpu=true|false`) on home PCs or cloud VMs from any provider.

References: FR-15 … FR-19 in [docs/01_requirements.md](../../docs/01_requirements.md) · US-09 … US-13 in [docs/02_user_stories.md](../../docs/02_user_stories.md) · ADR-02, ADR-03, ADR-04 in [docs/06_technical_decisions.md](../../docs/06_technical_decisions.md) · [docs/quality/](../../docs/quality/README.md)

| Path | Content |
|---|---|
| `arena/` | Common `ArenaModel` interface (`train / evaluate / export_onnx`); `trees/`, `neural/`, `community/` (teammates' models), `evaluation/` (walk-forward, model card) |
| `training/` | Entry point of the K3s training Jobs |
| `serving/` | FastAPI service: point-in-time features + ONNX Runtime inference (CPU/CUDA) |
| `deploy/` | `Dockerfile.cuda`, `Dockerfile.cpu`, `k8s/` (Deployment, Job, mirroring) |

Status: **empty**.
