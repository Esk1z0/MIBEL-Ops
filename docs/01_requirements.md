# MIBEL-Ops — Requirements (v0.3 · intraday focus)

> Source: [whitepaper.md](../whitepaper.md) · Date: 2026-11-03
> Related: [user stories](02_user_stories.md) · [diagrams](03_diagrams.md) · [retention](04_retention_policy.md) · [quality](quality/README.md) · [data sources](05_data_sources.md) · [decisions](06_technical_decisions.md)
>
> Priority: **M** must · **S** should · **C** could.

---

## 1. Vision and goals

**MIBEL-Ops** is a distributed MLOps platform running on home hardware and extensible to the cloud. **Every 15 minutes it forecasts the price of the upcoming quarter-hours of the Spanish intraday market.** It runs a continuous tournament of models that are monitored, retrained and replaced automatically.

| # | Goal | Success indicator |
|---|---|---|
| G-1 | Continuous and reliable capture | ≥ 99 % of 15-min windows captured per month; zero data loss for manager outages of up to 30 days |
| G-2 | Useful intraday forecast | Champion with **rMAE ≤ 0.90 against the day-ahead price** (see the [service baseline](quality/02_service_baseline.md)) |
| G-3 | Autonomous MLOps loop | Drift → retraining → quality gate → promotion without human intervention |
| G-4 | Elasticity | Add or remove ingestors and compute nodes (home or cloud) without code changes |
| G-5 | Transparency | Dashboards with error, coverage and tournament history; public in the future |

**Scope v1:** quarter-hourly intraday, one manager, 1..N ingestors, 0..N compute nodes, and dashboards for the team.
**Out of scope v1:** D+1 day-ahead forecasting, public API or notifications, distributed manager, real trading (see §9).

## 2. Target variable
| Aspect | Definition |
|---|---|
| Main target | **Volume-weighted average price of the continuous intraday market** (OMIE) for each quarter-hour, in €/MWh |
| Secondary targets | Intraday auction (IDA) prices and PVPC (C) |
| Frequency | One forecast per 15-min window |
| Horizon | The next **8 quarter-hours (2 h)**: h1–h4 *nowcast* (quarters already closed for trading) and h5–h8 *forecast* (still open). Each segment has its own safeguards, and the design allows extending up to 32 quarters through configuration only ([ADR-04](06_technical_decisions.md)) |
| Baseline to beat | **Day-ahead market** price for the same quarter-hour (known since D−1) |
| No fallback | If there is no compute node or not enough data, **nothing is published** for that window and the gap remains visible |

## 3. Topology and modes

| Role | Count | Initial hardware | Responsibility |
|---|---|---|---|
| **Ingestor** | 1..N | Raspberry Pi 3B+ | Capture its sources, **validate structure and content**, and send to the manager. If the manager is unavailable, buffer and resend later. |
| **Manager** | 1 | Laptop (16 GB, 350 GB disk) | **Strict coordinator**: receive, store, version, register models, orchestrate, evaluate with stored data, retain and display. Never runs models. |
| **Compute** | 0..N | Desktop (32 GB, RTX 5060 Ti); friends' PCs; VMs from any provider | Train, test, deploy, **build features and generate predictions**. |

| Mode | Condition | Behaviour |
|---|---|---|
| **Operational** | Manager on and ≥ 1 compute node with a Champion | Full loop every 15 min |
| **Collection only** | Manager on and 0 compute nodes | Data is captured and stored. No predictions. Retraining waits in the queue. |
| **Manager off** | Laptop off | Ingestors buffer locally (up to 30 days) and resend when it comes back. No predictions. |
| **Degraded** | A source or an ingestor fails | Continue with what is available; affected features are flagged and, if critical, no prediction is made |

## 4. Functional requirements

### 4.1 Common
| ID | Requirement | Prio |
|---|---|---|
| FR-01 | **Versioned contracts per source** (one schema per connector), plus the feature vector, the prediction and the node registration. Every data point carries `event_time` (the instant it refers to) and `available_at` (the instant it became known). | M |
| FR-02 | **Dynamic node onboarding**: an ingestor or compute node of any origin joins with VPN + role and capability registration, without code changes. Compute-node liveness is handled **natively by K3s** (failure detected in ~30 s, workloads rescheduled in ~1.5 min); ingestor liveness is inferred from the arrival of their data ([ADR-02](06_technical_decisions.md)). | M |

### 4.2 Ingestor
| ID | Requirement | Prio |
|---|---|---|
| FR-03 | **Pluggable connectors**: one connector per source, enabled by configuration. v1: ESIOS, OMIE, Open-Meteo, MIBGAS and REData as fallback (see the [data source study](05_data_sources.md)). Each source publishes to its own topic `mibel.raw.<source>`. | M |
| FR-04 | **Edge validation** with lightweight rules: contract structure and physical ranges (negative prices allowed). Invalid data goes to local quarantine and the reason is sent to the manager. | M |
| FR-05 | **Reliable delivery**: persistent local buffer of up to **30 days or 1 GB**, retries with *backoff*, and deterministic ID `hash(source, event_time, variable)`. | M |
| FR-06 | **Backfill** of at least 1 year, runnable on any node. Harmonises OMIE's change from hourly to 15-min resolution (Oct-2025) and uses historical weather forecasts as they were issued. | M |

### 4.3 Manager
| ID | Requirement | Prio |
|---|---|---|
| FR-07 | **Point-in-time storage**: consume from Kafka, deduplicate, apply a second batch validation (Great Expectations) and store in TimescaleDB in long format (`source, variable, node, event_time, available_at, value`). A new source therefore needs no schema change. | M |
| FR-08 | **Window-ready event**: when a 15-min window has the minimum data (or its timeout expires), `mibel.window.ready` is emitted, triggering the prediction. | M |
| FR-09 | **Versioned dataset**: periodic export to Parquet + DVC (remote on the laptop; Google Drive if space runs out). | M |
| FR-10 | **Model registry** in MLflow with aliases `champion` and `challenger-*`, full lineage and rollback. | M |
| FR-11 | **Orchestration** (Prefect): prediction triggering, daily evaluation, consolidation, retention and retraining orders, with operating-mode detection. | M |
| FR-12 | **Model-free evaluation and drift**: metrics and drift computed only from stored data and predictions (Deepchecks in model-free mode). | M |
| FR-13 | **Retention** according to the [policy](04_retention_policy.md). | M |
| FR-14 | **Observability**: Grafana (nodes, ingestion, actual vs. predicted per horizon, coverage, latencies, disk) and Superset (historical analysis). | M / S |

### 4.4 Compute
| ID | Requirement | Prio |
|---|---|---|
| FR-15 | **Model arena**: tree models (RF, LightGBM, XGBoost) and neural networks (MLP, LSTM) with a common interface, *walk-forward* validation and MLflow registration. **Every model is exported and served in ONNX** ([ADR-03](06_technical_decisions.md)). | M |
| FR-16 | Automatic **quality gate** before assigning any alias. | M |
| FR-17 | **Intraday prediction service**: on each `window.ready`, the Champion builds point-in-time features by reading the manager's DB, predicts the next H quarters and stores the result. Challengers do the same in shadow mode. | M |
| FR-18 | **Distributed, failure-tolerant retraining**: model families are spread across available nodes, with a queue when there are none. If a node fails, K3s recreates the workload on another node or leaves it queued. MLflow registration is idempotent (no duplicates) ([ADR-02](06_technical_decisions.md)). | M |
| FR-19 | **Cloud-agnostic**: same container and same onboarding on any provider or VM, without provider-specific services. | C |

## 5. Reference cycle

| When | What | Where |
|---|---|---|
| Every 15 min (t) | Window capture and validation | Ingestor |
| t + ~2 min | Storage and `window.ready` event | Manager |
| t + ~3 min | Point-in-time features and forecast of t+1…t+H (Champion + Challengers) | Compute |
| Daily (once D−1 continuous prices are published) | Evaluation, drift and, if needed, retraining order | Manager |
| Weekly | Parquet + DVC consolidation and retention | Manager |

## 6. Features (summary; details in the [data source study §4](05_data_sources.md))
- **Day-ahead** and **IDA** prices for the target quarter (the strongest baseline).
- **Last known continuous price** and its lags (depending on the real publication latency, [VERIFY]).
- **Renewable deviation**: actual generation − forecast.
- **Thermal gap** = demand − (wind + solar + nuclear + **hydro**).
- Interconnection with France, weather at the 4 nodes (wind interpolated to 100 m from 80/120 m), MIBGAS gas and quarter-hourly calendar.
- Only data with `available_at ≤ t`. The same library is used for training and prediction.

## 7. Non-functional requirements
| ID | Requirement | Default criterion |
|---|---|---|
| NFR-01 | Forecast freshness | In operational mode, forecast stored ≤ 5 min after the window closes |
| NFR-02 | Lightweight ingestor | < 200 MB RAM on the Pi 3B+; `systemd` service without Docker |
| NFR-03 | Capture continuity | ≥ 99 % of windows per month; 30 days of autonomy without the manager |
| NFR-04 | Manager footprint | ≤ 10 GB RAM; disk ≤ budget (default 200 GB of the 350 GB) |
| NFR-05 | Elasticity | Hot addition and removal of nodes |
| NFR-06 | Zero-downtime deployment | Champion change without losing any window |
| NFR-07 | Security | VPN-only traffic; secrets and tokens (ESIOS, ENTSO-E) kept out of the public repository |
| NFR-08 | Reproducibility | Same commit + same DVC version + same seed ⇒ same metrics (± tolerance) |
| NFR-09 | Cost | €0: free APIs; optional cloud |
| NFR-10 | Portability | `venv` + `pip`; Python version not pinned (CI tests a version matrix) |
| NFR-11 | Model quality | [Data gates](quality/01_data_gates.md), [service baseline](quality/02_service_baseline.md) and [promotion criteria](quality/03_promotion_criteria.md) |
| NFR-12 | Publishability | Secret-free metrics and dashboards, ready to be published in the future; CC BY attribution for Open-Meteo |

## 8. Main risks
| Risk | Mitigation |
|---|---|
| The continuous price may not be published in near real time | Exploration task SP-01; features based on the real latency; daily evaluation |
| The manager is a single point of failure | 30-day buffer on ingestors; backups; distributed manager as future work |
| No compute nodes means no predictions | Accepted (A-05); coverage is measured and displayed |
| *Leakage* in intraday features | `available_at` model + automatic point-in-time test |
| ESIOS and ENTSO-E tokens pending approval | Request them in R0; REData as a token-free fallback |
| Artifact growth | Retention policy with lineage protection |

## 9. Future work
- **Publishing results**: public page (e.g. GitHub Pages updated by GitHub Actions) with error, coverage and model ranking; Grafana public dashboards.
- Public prediction API and notifications (Telegram, Discord, email).
- Distributed manager (K3s HA, multi-broker Kafka, replicated Postgres, object storage).
- D+1 day-ahead model (24/36/48 h horizon) as a second tournament.
- CO₂ price once a reliable free source exists.

## 10. Traceability
| Requirements | User stories |
|---|---|
| FR-01 | US-01 |
| FR-02, FR-19 | US-02, US-13 |
| FR-03 … FR-06 | US-03, US-04 |
| FR-07 … FR-09 | US-05 |
| FR-11 | US-06 |
| FR-13 | US-07 |
| FR-14 | US-08 |
| FR-15 | US-09 |
| FR-10, FR-16 | US-10 |
| FR-17 | US-11 |
| FR-12, FR-18 | US-12 |
