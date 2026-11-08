# Project Definition and Architecture Document: MIBEL-Ops

> **Note:** this is the original vision document. Several decisions have since evolved (intraday 15-minute focus, node roles, ONNX-only serving, etc.). The current design lives in [docs/](docs/README.md) and the decision log in [docs/06_technical_decisions.md](docs/06_technical_decisions.md).

**MIBEL-Ops** is a distributed MLOps platform designed to forecast the hourly electricity price (in euros per megawatt-hour) in the Iberian Electricity Market (MIBEL), covering both the next-day auction (*Day-Ahead*) and real-time variations every fifteen minutes. The purpose of the project is to build a complete Machine Learning lifecycle in production using distributed home hardware connected over a network, simulating the real data-engineering and platform environment of a technology company.

---

## 1. The End Result: How does the system work once finished?

Once deployed and running, the project operates fully autonomously, like a living organism divided into four visible experiences:

1. **Continuous Capture and Monitoring (24/7):** An always-on Raspberry Pi queries, every fifteen minutes, the real state of the Spanish power grid, the weather forecast in the areas with the highest concentration of wind and solar farms, and the price of natural gas. That data travels in real time to the main laptop, where it passes through an automatic "quality customs" that discards broken sensor readings before storing them.
2. **Real-Time Model Competition (The Tournament):** The main laptop runs a lightweight Kubernetes cluster that keeps several artificial-intelligence models running at the same time (for example, a Random Forest running on the CPU and a Neural Network running on the GTX 1650 graphics card, plus the models created by other team members). A load balancer distributes prediction requests among all of them to forecast how much electricity will cost in the coming hours.
3. **Live Dashboards:** Any team member can open two visual dashboards in their browser. The first, in **Grafana**, shows in real time the official electricity price curve against each model's predictions, the response latency in milliseconds and the laptop's memory usage. The second, in **Apache Superset**, allows historical analysis of which model is most accurate during wind storms, heat waves or gas price spikes.
4. **Self-Healing and Automatic Retraining:** Once a day, the system compares the models' predictions with the final real price published by the market operator. If it detects that the weather has changed abruptly or that the models are starting to fail (*Drift*), it automatically orders the most powerful desktop computer (with the RTX 5060 Ti and 32 GB of RAM) to download the most recent data, train new versions of the models, choose the new winner and update the laptop without interrupting the service for a single second.

---

## 2. Problem Logic and Data Sources

The electricity price in Spain does not rise or fall smoothly or linearly; it works through a marginal-pricing auction. Renewable energy (solar and wind) and nuclear offer at prices close to zero euros (or even negative prices at midday when there is excess sun). However, when the country's demand exceeds what sun, wind and nuclear can produce, what the sector calls the **Thermal Gap** opens up. At that exact moment the combined-cycle thermal plants (which burn natural gas) must switch on, and the price of all electricity suddenly jumps above one hundred euros per megawatt-hour.

To anticipate that jump, the project is fed by three public and free data sources that do not require paid keys:

* **Red Eléctrica de España (REE) APIdatos:** The official source of the system operator. Every fifteen minutes it provides actual and forecast peninsular electricity demand, generation broken down by technology (how many megawatts wind, solar PV, nuclear, hydro and gas combined cycles are producing) and the target variable we want to predict: the wholesale market price (OMIE Spot) and the regulated price (PVPC).
* **Open-Meteo API (Weather at Strategic Nodes):** Instead of querying the weather of a random city, the system queries the hour-by-hour weather forecast at four geographical points where Spain's renewable production is concentrated: **Aragón and Galicia** to measure wind speed at one hundred metres (the hub height of modern wind turbines), and **Extremadura and Andalucía** to measure direct solar radiation and cloud cover.
* **MIBGAS (Iberian Gas Market):** Publishes open daily files with the price of a megawatt-hour of natural gas in Spain. This data is essential for the models because it acts as the "market ceiling": it tells at what price electricity will spike if there is no wind at night and gas plants come online.

From these three raw sources, the system builds its own feature engineering by computing the **Thermal Gap** (total demand minus renewable and nuclear generation), the theoretical gas-cost ceiling, cyclical encoding of the hour of day and day of week, and the market's recent memory (the price one hour ago, twenty-four hours ago and exactly one week ago).

---

## 3. Physical Hardware Distribution (Multi-Node Architecture)

To let the old laptop with 16 GB of RAM and a GTX 1650 graphics card run smoothly, the project splits the work into three physical tiers connected through a virtual private network (**Tailscale** or **WireGuard**), which also allows friends' computers to join from their own homes as if they were plugged into the same network cable:

* **Tier 1 — Continuous Capture Node (Raspberry Pi 3B+):** Acts as an *Edge* device. Since it consumes very little power, it stays on twenty-four hours a day running very lightweight Python scripts that query the Red Eléctrica, Open-Meteo and MIBGAS APIs, unify the responses and send them over the network to the laptop. It performs no heavy computation and trains no models.
* **Tier 2 — Core and Production Cluster (Laptop with 16 GB RAM and GTX 1650):** The operational heart of the system. It receives data from the Raspberry Pi, validates it, stores it in the database and keeps the lightweight Kubernetes cluster (**K3s**) running, where the prediction APIs and the load balancer live. Its GTX 1650 graphics card (4 GB of VRAM) is reserved exclusively for fast real-time inference with already-trained neural networks, avoiding loading its memory with heavy training.
* **Tier 3 — Training Farm and Batch Processing (Desktop PC with 32 GB RAM, Ryzen and RTX 5060 Ti + Friends' PCs):** The computational muscle. It does not need to be on all day; when large volumes of historical data must be processed, model degradation evaluated or deep neural networks and massive tree ensembles retrained, this computer takes over, completes the job in a few minutes thanks to the RTX 5060 Ti and sends the resulting packaged model back to the laptop.

---

## 4. The Technology Stack and the Role of Each Tool

Each technology from your course fulfils a specific mission within a chain without overlaps:

* **Apache Kafka (in KRaft mode):** The central real-time messaging pipeline. It decouples the Raspberry Pi from the rest of the system: the Raspberry drops raw data into an input channel and forgets about it. Configured in KRaft mode (no ZooKeeper needed), it uses less than one gigabyte of memory on the laptop.
* **Great Expectations:** Data quality control at the entrance. It listens to the Kafka channel and checks that each message makes physical sense before storing it: it verifies that solar radiation is not negative, that wind is within real limits and that no Red Eléctrica sensor has returned empty values, while allowing legitimate negative electricity prices when there is excess solar. If a record arrives corrupt, it is moved to an error queue so it does not contaminate the models.
* **PostgreSQL with TimescaleDB:** The central database optimised for time series. It stores both the clean history of energy and weather and the log of every prediction each model issues minute by minute.
* **DVC (Data Version Control):** Works as a "Git for heavy data". Each time a historical training batch is consolidated into compressed Parquet files, DVC stores an exact fingerprint of the dataset so the whole team trains on exactly the same information and can travel back in time if something fails.
* **MLflow:** The official laboratory and model registry. It stores the metrics of each training run (how many euros of error each model makes), records which DVC data version it was trained with, stores the final model file and manages the system's official titles: who is the incumbent model (**Champion**) and who are the contenders (**Challengers**).
* **FastAPI and ONNX Runtime:** FastAPI wraps the mathematical model inside an ultra-fast web service that can be asked for predictions over the network. By exporting all models to the **ONNX** standard, the same FastAPI service can run both a Random Forest on the CPU and a Neural Network on the GTX 1650 while using very little memory.
* **Docker and Kubernetes (K3s):** Docker packages each service with its exact dependencies, and K3s (an official, lightweight version of Kubernetes ideal for laptops) keeps the containers alive, assigns access to the graphics card and applies a **Load Balancer** (Traefik) that distributes requests between the incumbent model and the contender models.
* **Deepchecks:** The health auditor of production models. Every day it checks whether weather or gas variables are changing anomalously compared with the data the model was trained on (*Data Drift*) and whether the error in euros is rising more than allowed (*Concept Drift*).
* **Prefect:** The conductor of batch processes. It automates scheduled tasks and reacts to Deepchecks alerts by waking up the retraining processes on the desktop PC with the RTX 5060 Ti, updating DVC and promoting the new winning model in MLflow and Kubernetes.
* **Grafana and Apache Superset:** The visual layer. Grafana monitors the system in real time (technical metrics, alerts, API outages and live predictions) while Superset allows exploring and cross-referencing historical electricity-market data through analytical charts.

---

## 5. Logical Project Structure (How to organise the software blocks)

To develop the project in an orderly way in Visual Studio Code and let several people work at the same time without blocking each other, the repository is structured into six independent functional blocks that communicate through fixed data contracts:

1. **Common Contracts and Configuration Block:** Defines the rules of the game for the whole project. It specifies which exact fields an energy and weather message carries, which variables go into the model and what format a prediction returns. As long as this contract does not change, one teammate can modify the input APIs without breaking the work of the teammate training neural networks.
2. **Ingestion Block (Producers):** Contains the connectors that talk to Red Eléctrica, Open-Meteo and MIBGAS, plus a historical download module (*Backfill*) able to fetch the entire last year of data at once, so models can be trained from the first weekend without waiting months of live capture.
3. **Validation and Storage Block (Consumers):** Contains the Great Expectations validation rules and the service that reads from Kafka, filters dirty data and stores clean data in the database and in the DVC-controlled historical files.
4. **Feature Engineering and Training Block (Model Arena):** Contains the mathematical transformation of the data (Thermal Gap computation and time lags) and an independent folder for each model type. Here, algorithm families with opposite behaviour in the electricity market are compared: tree models (Random Forest, LightGBM or XGBoost), which learn sharp price steps very well but cannot extrapolate when gas reaches unprecedented records, versus Neural Networks (MLP or LSTM), which capture the temporal inertia of thermal plant start-ups and can extrapolate new trends.
5. **Inference and Deployment Block (Serving & K8s):** Contains the FastAPI application code that automatically downloads from MLflow the model it must run, together with the Docker templates and the Kubernetes and load-balancer configuration files.
6. **Orchestration and Observability Block:** Contains the automated Prefect flows, the Deepchecks audit reports and the Grafana and Superset dashboard templates.

---

## 6. The Complete End-to-End Workflow

To understand how all the pieces fit together in the system's daily life, this is the journey of a data point from its birth until it closes the loop:

* **Step 1 (Capture):** It is 19:00 and the wind drops in Aragón. The Raspberry Pi queries Red Eléctrica and Open-Meteo, detects the wind drop and the rise in night-time demand, and sends the data package to Kafka on the laptop.
* **Step 2 (Customs):** The Great Expectations validator picks up the message from Kafka, checks that all figures are valid, computes that the Thermal Gap has just jumped and stores the record in the TimescaleDB database.
* **Step 3 (Balanced Prediction):** The system requests the price forecast for the next hours from the laptop's Kubernetes cluster. The load balancer sends the request to the incumbent model (for example, a Random Forest on the CPU) and simultaneously sends a parallel copy to the contender model (the Neural Network on the GTX 1650). Both respond in under fifty milliseconds and their predictions are stored in the database and instantly visible in the Grafana dashboard.
* **Step 4 (Degradation Detection):** The next day, when the official market closing prices are published, Deepchecks analyses the results. It discovers that gas has risen to a level not seen in recent months and that the incumbent Random Forest has under-predicted at night, exceeding the tolerable error limit.
* **Step 5 (Automatic Retraining and Handover):** Deepchecks raises an alert to Prefect. Prefect automatically wakes up a task on the desktop PC with the RTX 5060 Ti, updates the DVC historical file with the data from the last few days, trains new models with that fresh data and registers them in MLflow. On verifying that the new Neural Network has a lower error than the previous model, it assigns it the title of new **Champion** in MLflow and orders Kubernetes on the laptop to update the containers transparently.
