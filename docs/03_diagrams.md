# MIBEL-Ops — Architecture Diagrams (v0.3 · intraday)

> Requirements: [01](01_requirements.md) · User stories: [02](02_user_stories.md) · Retention: [04](04_retention_policy.md) · Quality: [quality/](quality/README.md) · Data sources: [05](05_data_sources.md) · Decisions: [06](06_technical_decisions.md)
>
> Deployment according to **ADR-01 (accepted)**: Compose on the manager + K3s for compute. Models in **ONNX** (ADR-03). Horizon of **8 quarters** split into nowcast (h1–h4) and forecast (h5–h8) (ADR-04, accepted). Node liveness and failure recovery use native K3s mechanisms (ADR-02). All ADRs are in [06_technical_decisions.md](06_technical_decisions.md).

Contents
1. [Context](#1-context)
2. [MIBEL markets over time](#2-mibel-markets-over-time)
3. [Topology by role](#3-topology-by-role)
4. [Deployment per node](#4-deployment-per-node)
5. [Streaming data flow](#5-streaming-data-flow)
6. [Sequence · One 15-minute window](#6-sequence--one-15-minute-window)
7. [Sequence · Ingestor with the manager off](#7-sequence--ingestor-with-the-manager-off)
8. [Sequence · Drift and distributed retraining](#8-sequence--drift-and-distributed-retraining)
9. [Sequence · Node onboarding](#9-sequence--node-onboarding)
10. [States · Operating modes](#10-states--operating-modes)
11. [States · Model lifecycle](#11-states--model-lifecycle)
12. [Classes · Contracts](#12-classes--contracts)
13. [Point-in-time data model](#13-point-in-time-data-model)
14. [Point-in-time features](#14-point-in-time-features)
15. [Market logic](#15-market-logic)
16. [Future evolution](#16-future-evolution)
17. [Repository structure](#17-repository-structure)

---

## 1. Context

```mermaid
flowchart LR
    ESIOS["ESIOS (REE)<br/>real-time system"]
    OMIE["OMIE<br/>day-ahead / IDA / continuous prices"]
    OM["Open-Meteo<br/>weather forecast"]
    MG["MIBGAS<br/>gas"]
    RD["REData<br/>fallback"]

    SYS(("MIBEL-Ops"))

    DS["Data scientists"]
    PE["Platform engineer"]
    EQ["Team<br/>(dashboards)"]
    COL["Collaborators / cloud<br/>(contribute nodes)"]

    ESIOS & OMIE & OM & MG & RD --> SYS
    DS -->|"models"| SYS
    PE -->|"operates"| SYS
    COL -->|"compute / ingestion"| SYS
    SYS -->|"forecast every 15 min,<br/>metrics, alerts"| EQ
```

---

## 2. MIBEL markets over time
What is known and when, for delivery day D (all markets use 15-min periods since Oct-2025).

```mermaid
flowchart LR
    A["D−1 12:00<br/>DAY-AHEAD auction<br/>→ 96 prices for D"] --> B["D−1 15:00<br/>IDA1"]
    B --> C["D−1 22:00<br/>IDA2"]
    C --> E["D 10:00<br/>IDA3<br/>(second half of D)"]
    A -.-> CONT["CONTINUOUS INTRADAY<br/>from D−1 until ~1 h<br/>before each quarter"]
    CONT --> T(["🎯 Target:<br/>weighted average<br/>of each quarter"])
    A --> R(["📏 Baseline to beat:<br/>day-ahead price"])
```

---

## 3. Topology by role

```mermaid
flowchart LR
    subgraph ING["Ingestors · 1..N (different sources)"]
        I1["Pi 3B+<br/>ESIOS · OMIE · Weather · Gas"]
        I2["Future: ENTSO-E / satellite"]
        I3["Future: AEMET"]
    end
    subgraph MAN["Manager · 1 · strict coordinator"]
        M["Laptop<br/>data · registry · orchestration · evaluation"]
    end
    subgraph CMP["Compute · 0..N"]
        C1["Desktop<br/>RTX 5060 Ti"]
        C2["Friend's PC"]
        C3["VM from any<br/>provider"]
    end
    I1 & I2 & I3 -->|"mibel.raw.&lt;source&gt;"| M
    M -->|"window.ready · Jobs"| C1 & C2 & C3
    C1 & C2 & C3 -->|"predictions · models"| M
```

---

## 4. Deployment per node

```mermaid
flowchart TB
    subgraph VPN["Tailscale / WireGuard VPN"]
        subgraph NI["Ingestor · systemd, no Docker"]
            CON["Connector registry<br/>(one per source)"] --> EV["Edge gates DG-E"] --> BUF[("SQLite outbox<br/>undelivered only, ≤ 30 d")] --> PRD["Sink: Kafka"]
        end

        subgraph NG["Manager · Docker Compose + K3s server (taint)"]
            KAF[["Kafka KRaft"]]
            CNS["Consumer<br/>dedup + GE"]
            DB[("TimescaleDB<br/>point-in-time")]
            WR["window.ready<br/>detector"]
            PF["Prefect<br/>+ K8s worker"]
            MLF["MLflow"]
            DVC[("DVC remote")]
            EVA["Evaluation + Deepchecks<br/>(model-free)"]
            RET["Retention"]
            OBS["Grafana · Superset"]
            K3S{{"K3s server<br/>+ Traefik"}}
        end

        subgraph NC["Compute · K3s agent (role=compute)"]
            CH["Champion<br/>FastAPI: features + inference"]
            CL["Challengers<br/>FastAPI"]
            JOB["Training Jobs"]
        end
    end

    PRD --> KAF --> CNS --> DB
    CNS --> WR --> PF
    PF -->|"POST /predict t"| K3S
    K3S --> CH
    K3S -. "mirroring" .-> CL
    CH & CL -->|"read PIT features"| DB
    CH & CL -->|"write predictions"| DB
    CH & CL -->|"model by alias"| MLF
    DB --> EVA -->|"retrain_requested"| PF
    PF -->|"creates Jobs"| K3S --> JOB
    JOB <--> DVC
    JOB --> MLF
    OBS --> DB
    RET --> DB & MLF & DVC
```

---

## 5. Streaming data flow

```mermaid
flowchart LR
    A["APIs"] --> B["Ingestor<br/>validates"]
    B -- "invalid" --> Q[["quarantine<br/>+ mibel.dlq"]]
    B -- "valid" --> C[["mibel.raw.&lt;source&gt;"]]
    C --> D["Manager<br/>dedup + GE"] --> E[("TimescaleDB<br/>event_time + available_at")]
    D --> W{{"window t<br/>complete?"}}
    W -- "yes / timeout" --> R[["window.ready(t)"]]
    R --> P["Compute: Champion + Challengers<br/>PIT features → t+1…t+H"]
    E --> P
    P --> F[("predictions")]
    E --> G["Parquet + DVC"] --> T["Compute: training"] --> M[("MLflow")]
    M --> P
    F --> V["Manager: daily evaluation<br/>+ drift (model-free)"]
    E --> V
    V -- "retrain_requested" --> T
    F --> O["Dashboards"]
```

---

## 6. Sequence · One 15-minute window

```mermaid
sequenceDiagram
    autonumber
    participant I as Ingestor
    participant K as Kafka
    participant C as Consumer (manager)
    participant DB as TimescaleDB
    participant PF as Prefect
    participant T as Traefik
    participant CH as Champion (compute)
    participant CL as Challenger (compute)

    Note over I: t = 10:15
    I->>I: capture ESIOS · OMIE · Weather (validated)
    I->>K: mibel.raw.esios / omie / openmeteo
    K->>C: consume
    C->>DB: upsert (available_at = reception)
    C->>C: minimum data for t?
    C->>PF: window.ready(t, missing sources)
    alt operational mode
        PF->>T: POST /predict {t}
        par
            T->>CH: request
            CH->>DB: features with available_at ≤ t
            CH->>CH: check data gates DG-P01…DG-P06 per segment
            alt enough data
                CH->>DB: 8 predictions (alias champion)
            else not enough data
                CH->>DB: record "no prediction" + reason
            end
        and
            T-)CL: mirrored copy
            CL->>DB: 8 predictions (alias challenger-x)
        end
    else collection only
        PF->>DB: record window without prediction
    end
```

---

## 7. Sequence · Ingestor with the manager off

```mermaid
sequenceDiagram
    autonumber
    participant I as Ingestor
    participant B as SQLite buffer
    participant K as Kafka (manager)

    loop every 15 min while the laptop is off
        I->>I: capture + validate
        I->>B: store (deterministic id)
        I-xK: send fails
    end
    Note over B: undelivered only: up to 30 days / 1 GB (ADR-05)
    K-->>I: the manager comes back
    I->>K: resend in order (in batches)
    K-->>I: ack
    I->>B: delete delivered messages
    Note over K: Gaps longer than the buffer life are<br/>recovered with backfill from the sources.<br/>No predictions for that period.
```

---

## 8. Sequence · Drift and distributed retraining
Queue mechanism details in ADR-02 ([06_technical_decisions.md](06_technical_decisions.md)).

```mermaid
sequenceDiagram
    autonumber
    participant EV as Evaluation (manager)
    participant PF as Prefect
    participant K8 as K3s
    participant GPU as GPU compute
    participant CPU as CPU compute
    participant ML as MLflow

    EV->>EV: daily: MAE / rMAE_DA per horizon + PSI<br/>(stored data and predictions only)
    alt degraded or critical and outside cooldown
        EV->>PF: retrain_requested
        PF->>PF: pending order? → deduplicate
        PF->>K8: neural Job (gpu=true) + trees Job
        alt nodes available
            par
                K8->>GPU: neural
                GPU->>ML: register + model card
            and
                K8->>CPU: trees
                CPU->>ML: register + model card
            end
            PF->>ML: quality gate → challenger-*
            opt Champion critical and valid candidate
                PF->>ML: emergency promotion
                PF->>K8: rolling update
            end
        else 0 nodes
            Note over K8: Jobs Pending = queue
        end
    end
```

---

## 9. Sequence · Node onboarding

```mermaid
sequenceDiagram
    autonumber
    actor U as Collaborator
    participant N as New node
    participant TS as Tailscale
    participant G as Manager
    participant K as K3s server

    U->>N: join-node.sh --role compute | ingestor
    N->>TS: join the VPN
    N->>N: detect CPU / RAM / GPU
    alt compute
        N->>K: k3s agent (role=compute, gpu=…)
        loop every 10 s (native K3s)
            N->>K: renew Lease
        end
        K-->>G: node state via kube-state-metrics → Prometheus
    else ingestor
        N->>N: venv + pip install mibel-common[ingestor]<br/>+ systemd service with its connectors
        N->>G: NodeRegistration (sources it provides)
        loop every 15 min
            N->>G: data (data arrival is the liveness signal)
        end
    end
    G->>G: recompute operating mode
```

---

## 10. States · Operating modes

```mermaid
stateDiagram-v2
    [*] --> CollectionOnly
    CollectionOnly --> Operational: ≥1 compute node with Champion
    Operational --> CollectionOnly: 0 compute nodes
    Operational --> Degraded: a source or ingestor fails
    Degraded --> Operational: recovered
    CollectionOnly --> ManagerOff: laptop off
    Operational --> ManagerOff: laptop off
    ManagerOff --> CollectionOnly: laptop on (buffers resent)
    note right of ManagerOff
        only ingestors work:
        capture + local buffer
    end note
```

---

## 11. States · Model lifecycle

```mermaid
stateDiagram-v2
    [*] --> Trained
    Trained --> Rejected: fails the gate
    Trained --> Challenger: passes → challenger-x
    Challenger --> Champion: 7 days in shadow, ≥3 % better (weighted) and no segment worse than 1 %
    Challenger --> Champion: emergency
    Champion --> ExChampion: replaced
    ExChampion --> Champion: rollback
    ExChampion --> Retired: outside the last 5
    Rejected --> Tombstone: 30 days
    Retired --> Tombstone: 365 days
    Tombstone --> [*]
```

---

## 12. Classes · Contracts

```mermaid
classDiagram
    direction LR
    class Observation {
        +str message_id
        +str contract_version
        +str source
        +str ingestor_id
        +str variable
        +str node
        +datetime event_time
        +datetime available_at
        +float value
        +str unit
    }
    class ForecastObservation {
        +datetime issued_at
        +int lead_minutes
    }
    class FeatureVector {
        +str features_version
        +datetime as_of
        +datetime target_quarter
        +int horizon
        +float price_day_ahead
        +float price_ida_last
        +float price_continuous_d1
        +float renewable_deviation_mw
        +float thermal_gap_mw
        +float exchange_france_mw
        +float gas_price
        +float quarter_sin
        +float quarter_cos
        +float dow_sin
        +float dow_cos
        +bool holiday
        +dict weather_by_node
    }
    class PredictionBatch {
        +datetime as_of
        +str model_name
        +int model_version
        +str alias
        +str compute_node
        +list~float~ prices_eur_mwh
        +float latency_ms
        +bool low_confidence
    }
    class SkippedWindow {
        +datetime as_of
        +str model_name
        +str segment
        +str reason
    }
    class NodeRegistration {
        +str node_id
        +str role
        +dict capabilities
        +list~str~ sources
        +str provider
    }
    class ArenaModel {
        <<interface>>
        +train(dataset)
        +evaluate(dataset) Metrics
        +export_onnx(path)
    }
    Observation <|-- ForecastObservation
    PredictionBatch ..> FeatureVector : uses
    ArenaModel <|.. RandomForestModel
    ArenaModel <|.. LightGBMModel
    ArenaModel <|.. XGBoostModel
    ArenaModel <|.. MLPModel
    ArenaModel <|.. LSTMModel
```

---

## 13. Point-in-time data model

```mermaid
erDiagram
    OBSERVATIONS {
        text source PK
        text variable PK
        text node PK
        timestamptz event_time PK
        timestamptz available_at PK
        float value
        text ingestor_id
    }
    FORECASTS {
        text source PK
        text variable PK
        text node PK
        timestamptz issued_at PK
        timestamptz valid_at PK
        float value
    }
    PREDICTIONS {
        timestamptz as_of PK
        timestamptz target_quarter PK
        text model_name PK
        int model_version
        int horizon
        text alias
        text compute_node
        float predicted_eur_mwh
        float latency_ms
    }
    SKIPPED_WINDOWS {
        timestamptz as_of PK
        text model_name PK
        text segment PK
        text reason
    }
    MODEL_EVAL_DAILY {
        date day PK
        text model_name PK
        int horizon PK
        float mae
        float rmae_da
        float bias
        float coverage
        text health_state
    }
    NODES {
        text node_id PK
        text role
        jsonb capabilities
        timestamptz last_seen
    }
    SOURCES {
        text source PK
        text contract_version
        jsonb variables
    }

    SOURCES ||--o{ OBSERVATIONS : "defines"
    SOURCES ||--o{ FORECASTS : "defines"
    NODES ||--o{ PREDICTIONS : "generates"
    PREDICTIONS }o--|| MODEL_EVAL_DAILY : "summarised in"
    OBSERVATIONS ||--o{ MODEL_EVAL_DAILY : "ground truth (continuous price)"
```

> The **long format** (`source, variable, node`) means a new ingestor with new sources needs no migrations; registering the source in `SOURCES` is enough. Wide views are created for Superset.

---

## 14. Point-in-time features

```mermaid
flowchart LR
    T(["instant t"]) --> F{"filter<br/>available_at ≤ t"}
    subgraph Sources
        P1["Day-ahead price of quarter q"]
        P2["Latest IDA price of q"]
        P3["Latest published continuous price"]
        S1["Actual demand and generation"]
        S2["Renewable forecast"]
        W["Weather at 4 nodes<br/>wind 80/120 m → 100 m"]
        G["MIBGAS gas"]
        X["Interconnection with France"]
    end
    P1 & P2 & P3 & S1 & S2 & W & G & X --> F
    F --> D1["Renewable deviation<br/>actual − forecast"]
    F --> D2["Thermal gap<br/>demand − (wind + solar + nuclear + hydro)"]
    F --> D3["Quarter-hourly calendar"]
    D1 & D2 & D3 & F --> FV(["FeatureVector × H"])
```

---

## 15. Market logic

```mermaid
flowchart TD
    A["The day-ahead market sets a price for quarter q"] --> B{"What happens in real time?"}
    B -- "More wind or sun than forecast" --> C["Surplus of cheap energy<br/>→ intraday BELOW day-ahead"]
    B -- "Less renewable or more demand" --> D["Thermal gap grows<br/>→ intraday ABOVE day-ahead"]
    D --> E["If combined cycles come online:<br/>the price tends towards the gas ceiling"]
    B -- "As forecast" --> F["Intraday ≈ day-ahead"]
```

---

## 16. Future evolution

```mermaid
flowchart LR
    subgraph v1["v1"]
        G1["Single manager"]
        PUB0["Internal dashboards"]
    end
    subgraph fut["Future"]
        HA["Distributed manager<br/>K3s HA · Kafka 3 brokers<br/>Postgres HA · MinIO/S3"]
        PUB["Public metrics page<br/>(GitHub Pages + Actions)"]
        API["Public API and notifications"]
        DA["D+1 day-ahead tournament"]
    end
    G1 ==> HA
    PUB0 ==> PUB
    PUB ==> API
    G1 ==> DA
```

---

## 17. Repository structure

```text
mibel-ops/
├── packages/
│   └── mibel_common/                 # installed on ALL nodes
│       ├── pyproject.toml            #   extras: [ingestor] [manager] [compute]
│       └── mibel_common/
│           ├── contracts/            #   one schema per source + features + prediction + node
│           ├── rules/                #   physical rules (lightweight core, no pandas)
│           ├── features/             #   point-in-time, thermal gap, renewable deviation  [compute]
│           └── config/
│
├── nodes/
│   ├── ingestor/                     # ── INGESTOR · lightweight, systemd, self-contained (ADR-05)
│   │   ├── pyproject.toml            #   package mibel-ingestor · extras [excel] [kafka] [dev]
│   │   ├── requirements.lock.txt     #   runtime pins for the nodes
│   │   ├── config/                   #   ingestor.yaml (sources, cadences, modes) · quality.yaml (DG-E)
│   │   ├── src/mibel_ingestor/
│   │   │   ├── connectors/           #   base.py (port + registry) · one module per source
│   │   │   ├── pipeline.py           #   requests → parse → change detection → gates → outbox
│   │   │   ├── gates.py · store.py · sinks.py · scheduler.py · metrics.py · cli.py
│   │   ├── tests/                    #   contract tests per connector, synthetic payloads
│   │   ├── benchmarks/               #   bench.py: dev PC vs Raspberry Pi
│   │   └── deploy/                   #   install.sh · mibel-ingestor.service · env.example
│   │
│   ├── manager/                      # ── MANAGER · strict coordinator
│   │   ├── consumer/                 #   Kafka → dedup → GE → TimescaleDB · window.ready
│   │   ├── expectations/
│   │   ├── storage/                  #   migrations, compression, wide views
│   │   ├── dataset/                  #   Parquet + DVC
│   │   ├── registry/                 #   aliases, quality gate, promotion, rollback
│   │   ├── orchestration/            #   Prefect flows and automations
│   │   ├── evaluation/               #   metrics, internal baselines, model-free Deepchecks, health states
│   │   ├── node_registry/
│   │   ├── retention/
│   │   ├── dashboards/               #   grafana/ · superset/
│   │   ├── requirements.txt
│   │   └── deploy/                   #   docker-compose.yml · k3s-server/ · traefik/
│   │
│   └── compute/                      # ── COMPUTE · train, serve, predict
│       ├── arena/
│       │   ├── base.py               #   ArenaModel interface
│       │   ├── trees/  neural/  community/
│       │   └── evaluation/           #   walk-forward, model card
│       ├── training/                 #   Job entry point
│       ├── serving/                  #   FastAPI: PIT features + ONNX Runtime inference (CPU/CUDA)
│       ├── requirements.txt
│       └── deploy/                   #   Dockerfile.cuda · Dockerfile.cpu · k8s/ (Deployment, Job, mirroring)
│
├── infra/
│   ├── join-node.sh
│   ├── vpn/
│   └── cloud/                        # generic VM template (cloud-init), no provider-specific services
├── config/                           # settings.yaml · retention.yaml · sources.yaml · quality/{data_gates,service_baseline,promotion}.yaml
├── data/                             # *.dvc
├── tests/                            # cross-node: unit · point-in-time · e2e (node tests live in each node)
├── wip/                              # untested work in progress (notebooks, prototypes); local only, ignored by Git
├── docs/
├── .env.example                      # ESIOS / ENTSO-E tokens (never in the repo)
└── .github/workflows/                # CI (Python version matrix) + images per role
```

Installation per role (no `uv`):
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e packages/mibel_common[manager]    # or [compute]
pip install -r nodes/ingestor/requirements.lock.txt && pip install --no-deps -e nodes/ingestor   # ingestor: self-contained
```
