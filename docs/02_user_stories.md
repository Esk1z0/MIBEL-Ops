# MIBEL-Ops — User Stories (v0.3 · intraday focus)

> Requirements: [01_requirements.md](01_requirements.md) · Quality: [quality/](quality/README.md) · Data sources: [05](05_data_sources.md) · Decisions: [06](06_technical_decisions.md)
>
> Size: **S** ≈ 2–3 days · **M** ≈ 1 week · **L** ≈ 2 weeks (one part-time person).

## Summary

| Epic | Story | Node | Size | Release |
|---|---|---|---|---|
| A · Foundations | SP-01 API and token exploration | — | S | R0 |
| | US-01 Common package (contracts, rules, point-in-time features) | All | M | R0 |
| | US-02 Network and dynamic node onboarding | All | M | R0 |
| B · Ingestion | US-03 Lightweight ingestor with pluggable connectors | Ingestor | L | R1 |
| | US-04 Historical backfill | Any | M | R1 |
| C · Manager | US-05 *Streaming* data pipeline → DB → DVC | Manager | L | R1 |
| | US-06 Orchestration and operating modes | Manager | M | R2 |
| | US-07 Retention and disk budget | Manager | M | R4 |
| | US-08 Dashboards | Manager | M | R2–R4 |
| D · Compute | US-09 Model arena | Compute | L | R2 |
| | US-10 Quality gate, aliases and rollback | Compute + Manager | M | R3 |
| | US-11 Intraday forecast every 15 min | Compute | L | R3 |
| | US-12 Evaluation, drift and distributed retraining | Manager + Compute | L | R4 |
| E · Scaling | US-13 External and cloud nodes | Compute / Ingestor | M | R5 |

---

## A · Foundations

### SP-01 · API and token exploration
**As** the team **we want** to confirm latencies, indicator IDs and source formats **so that** connectors are designed on real data rather than assumptions.
- [ ] ESIOS and ENTSO-E tokens requested and received.
- [ ] Real publication latency of the continuous price (OMIE and ESIOS) measured and documented.
- [ ] ESIOS indicator ID list finalised.
- [ ] RAM test of the connectors on the Pi.
- [ ] [VERIFY] items in the [data source study](05_data_sources.md) resolved.

### US-01 · Common package
**As** the team **we want** a shared package with per-source contracts, validation rules and the feature library **so that** all nodes speak the same language.
- Contracts with `event_time` and `available_at`.
- Physical rules (lightweight core, valid on the Pi).
- Features: day-ahead/IDA price of the quarter, continuous-price lags, renewable deviation, thermal gap (with hydro), wind interpolated to 100 m, quarter-hourly calendar and gas.
- Installable with `pip` in a `venv`, with role extras (`[ingestor]`, `[manager]`, `[compute]`).

Acceptance:
- [ ] **Point-in-time test**: building the features for *t* never uses data with `available_at > t`.
- [ ] Unit tests for contracts and features (e.g. gap = demand − wind − solar − nuclear − hydro).

### US-02 · Network and dynamic node onboarding
**As** a platform engineer **I want** any node to join by declaring its role **so that** the system grows without code changes.
- VPN, `join-node.sh --role` script, registration and inventory in Grafana.
- Compose on the manager + K3s *server* with a *taint* on the manager + *agents* on compute nodes ([ADR-01](06_technical_decisions.md)).
- Node liveness with **native K3s mechanisms** ([ADR-02](06_technical_decisions.md)): `node-monitor-grace-period=30s`, `tolerationSeconds=60`, *probes*, *Graceful Node Shutdown*.
- `kube-state-metrics` → Prometheus → node dashboard and alerts.

Acceptance:
- [ ] A new node appears in the inventory in under 5 min.
- [ ] When a node is disconnected, it becomes `NotReady` in ≈ 30 s and its pods are recreated on another node (or stay Pending) in ≈ 1.5 min.
- [ ] Grafana alert if a node has been `NotReady` for more than 5 min.
- [ ] Shutting down the desktop gracefully drains its workloads without losing progress.
- [ ] No service listens outside the VPN.

---

## B · Ingestion

### US-03 · Lightweight ingestor with pluggable connectors
**As** the manager **I want** to receive only valid data from each ingestor, each source on its own topic, **so that** adding sources breaks nothing.
- v1 connectors: ESIOS, OMIE, Open-Meteo, MIBGAS and REData (fallback).
- Edge validation, quarantine, 30-day / 1 GB SQLite buffer, `systemd`.

Acceptance:
```gherkin
Given a radiation reading of -12 W/m²
Then it is not sent, it is quarantined and its reason is published to mibel.dlq

Given a price of -5 €/MWh
Then it is valid

Given the laptop is off for 10 days
When it comes back
Then the ingestor resends everything in order and without duplicates

Given a new connector enabled through configuration
Then it publishes to mibel.raw.<source> without changes to the other connectors
```
- [ ] RAM < 200 MB on the Pi.

### US-04 · Historical backfill
**As** a data scientist **I want** at least 1 year of consistent history **so that** I can train from day one.
- [ ] Resumable CLI by date range.
- [ ] Harmonises hourly data before Oct-2025 with the 15-min data.
- [ ] Uses Open-Meteo's Historical Forecast API, to train on forecasts as they were issued.
- [ ] Goes through the same validation rules.

---

## C · Manager

### US-05 · *Streaming* data pipeline
**As** the system **I want** to store each window as soon as it arrives and announce that it is ready **so that** I can forecast in near real time.
- Kafka → deduplication → batch GE → TimescaleDB in point-in-time long format.
- `mibel.window.ready` event with configurable timeout.
- Parquet + DVC export.

Acceptance:
- [ ] A data point is available in the DB in under 2 min.
- [ ] `window.ready` is emitted with the minimum data or on timeout, listing the missing sources.
- [ ] Adding a new source requires no schema migration.

### US-06 · Orchestration and operating modes
**As** the system **I want** to react to events and to the available nodes **so that** I never stop.
- Prefect automations: `window.ready` → prediction; daily evaluation; weekly consolidation and retention.
- Mode computation: operational, collection only, degraded.

Acceptance:
```gherkin
Given there are no compute nodes
When window.ready arrives
Then no prediction is generated and the gap is recorded in the coverage

Given a compute node with the Champion deployed connects
Then the next window already has a prediction
```

### US-07 · Retention and disk budget
**As** the laptop owner **I want** history to stay within the budget **so that** the manager can run indefinitely.
- [ ] Weekly `retention_flow` with *dry-run* mode.
- [ ] Lineage protection (protected items are never deleted).
- [ ] Disk thresholds at 70 %, 85 % and 95 % of the budget.

### US-08 · Dashboards
**As** a team member **I want** to see the system and the tournament **so that** I understand what is happening without logging into the nodes.
- Grafana: node inventory and operating mode; ingestion and DLQ; actual vs. predicted per horizon; **coverage** (% of windows with a forecast); latencies; disk.
- Superset: error per model crossed with conditions (wind, renewable deviation, gas, thermal gap).

Acceptance:
- [ ] Alerts: ingestor without data > 30 min, node down, collection-only mode > 24 h, disk > 85 %.
- [ ] Dashboards versioned in the repository and free of secrets (ready to be published in the future).

---

## D · Compute

### US-09 · Model arena
**As** a data scientist **I want** a common interface to train and compare model families **so that** everyone can contribute their own model.
- `train / evaluate / export_onnx`, *walk-forward*, MLflow (metrics per horizon and per nowcast/forecast segment, DVC hash, commit).
- **ONNX only** ([ADR-03](06_technical_decisions.md)): multi-horizon output `[n, H]` in a single graph, with H and the segments declared in the metadata (nothing assumes 8: see the extension parameters in ADR-04).
- Idempotent registration: nothing is registered if a version with the same `family + DVC hash` already exists.
- `arena/community/` template.

Acceptance:
- [ ] CI validates the interface and the feature contract.
- [ ] Works on a node without a GPU.
- [ ] Native ↔ ONNX parity < 0.01 €/MWh for every model; a model that cannot be exported cannot be registered.

### US-10 · Quality gate, aliases and rollback
**As** the service owner **I want** only models that meet the baseline to be deployed, and only models that truly beat the Champion to be promoted, **so that** the forecast never gets worse.
- Generic evaluator that reads the three files in `config/quality/` ([data gates](quality/01_data_gates.md), [service baseline](quality/02_service_baseline.md), [promotion](quality/03_promotion_criteria.md)) and applies the active profile (`development` / `production`) and each criterion's mode (`block` / `warn` / `off`).
- The result of each criterion (value, threshold, profile, pass or fail) is stored in the model card in MLflow and feeds the "Calibration" dashboard.

Acceptance:
```gherkin
Given the production profile and a candidate with aggregate rMAE = 0.95 against the day-ahead price
Then it is rejected (SB-P01 ≤ 0.90)

Given the development profile and the same candidate
Then it is deployed as a Challenger and SB-P01 is recorded as a warning

Given a Challenger that after 7 days in shadow improves the aggregate MAE by ≥ 3 %, worsens no segment by > 1 % and wins ≥ 60 % of the days
Then it becomes Champion and the previous one stays in shadow for 72 h

Given that during those 72 h the new Champion performs > 3 % worse than the previous one
Then an automatic rollback happens (PC-R2)

When "mibel rollback" is run
Then the previous Champion returns without losing any window
```
- [ ] Changing a threshold, a mode or the active profile requires no code change and no model redeployment.

### US-11 · Intraday forecast every 15 minutes
**As** the system **I want** the Champion and the Challengers to forecast the next 8 quarters (2 h) on every ready window **so that** there is a continuous tournament.
- FastAPI service on the compute nodes.
- Point-in-time features built on the compute node itself by reading the DB.
- Mirrored copy to the Challengers.
- Predictions stored with alias, node and latency.

Acceptance:
- [ ] Forecast stored ≤ 5 min after the window closes (in operational mode).
- [ ] If critical features are missing, no forecast is made and the reason is recorded.
- [ ] Champion change without losing windows.
- [ ] If the Champion's node fails and another exists, the service is rescheduled.

### US-12 · Evaluation, drift and distributed retraining
**As** the system **I want** to detect degradation without loading models on the manager and retrain wherever compute is available **so that** I heal myself.
- Daily evaluation on the manager: MAE and rMAE per horizon, bias and coverage.
- Model-free Deepchecks: PSI per feature and rolling MAE.
- Retraining order with cooldown; one Job per family (ADR-02).

Acceptance:
```gherkin
Given the 7-day rolling MAE is greater than 1.3 × the validation MAE for 3 days
Then retrain_requested is emitted

Given 2 compute nodes (one with a GPU)
Then neural networks train on the GPU node and tree models on the other, in parallel

Given there are 0 compute nodes
Then the order waits in the queue and is not duplicated

Given the GPU node shuts down in the middle of training
Then K3s recreates the Job on another suitable node (or it stays Pending) in ≈ 1.5 min
And the previous attempt's run is marked FAILED
And there are never two registered versions with the same family and DVC hash

Given a Job Pending for 2 hours
Then the Prefect flow keeps waiting (pod_watch_timeout_seconds raised)
```
- [ ] The dashboard shows the state per segment (nowcast and forecast) and the overall state as the worse of the two.

---

## E · Scaling

### US-13 · External and cloud nodes
**As** a collaborator **I want** to add my PC, a VM from any provider, or an ingestor with new sources **so that** I can contribute capacity or information.
- [ ] Step-by-step guide, `compute` images (CUDA and CPU) and ingestor package.
- [ ] An ephemeral VM from any provider trains, registers in MLflow and shuts down.
- [ ] An ingestor with a new source publishes to its topic without changing anything else.

---

## Release plan
| Release | Demonstrable result | Stories |
|---|---|---|
| R0 · Foundations | Verified APIs, nodes on the VPN, common package | SP-01, US-01, US-02 |
| R1 · Data | Capture every 15 min + 1 year of history in DB and DVC | US-03, US-04, US-05 |
| R2 · First models | Models trained in MLflow, orchestration and basic dashboards | US-06, US-09, US-08 (Grafana) |
| R3 · Production | Champion + Challengers forecasting every 15 min | US-10, US-11 |
| R4 · Self-healing | Drift, distributed retraining and retention | US-12, US-07, US-08 (Superset) |
| R5 · Scaling | External or cloud nodes | US-13 |

## Definition of Done
- PR reviewed and CI green (lint + tests + point-in-time test).
- Contracts updated if the data changes.
- Deployable in its role with the corresponding script or manifest.
- Metrics and logs in Grafana.
- Module README updated.
