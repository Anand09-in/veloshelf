# VeloShelf — System Overview

A production-grade streaming ML platform for quick-commerce dark stores. Order and inventory events flow through Kafka → PyFlink → Postgres in real time; windowed features drive anomaly detection (Isolation Forest) and demand forecasting (XGBoost); Evidently monitors for drift and triggers automated closed-loop retraining via Dagster; the whole stack is provisioned on AWS with Terraform and deployed through GitHub Actions.

---

## Architecture

```
Generator (Poisson + Zipf + anomaly injection)
    │
    ▼  raw-orders / raw-inventory
Kafka 3.8 (KRaft, 3 partitions)
    │
    ▼
PyFlink 1.18 (event-time tumbling 1-min windows)
    │  validation → dead-letter topic
    │  windowed features (order_rate, depletion_vel, demand_momentum, on_hand_est)
    ▼
Postgres 16 — windowed_features + alerts tables
    │
    ├── Online scorer (Flink FeatureSinkFn) ─── MLflow registry (hot-swap, 5-min poll)
    │       └── stockout-alerts / surge-alerts → Kafka topics
    │
    ├── Metrics exporter ──► Prometheus ──► Grafana (auto-provisioned dashboard)
    │
    ├── Streamlit (business dashboard — stockout risk, surge alerts, velocity)
    │
    └── Dagster asset graph
            ├── windowed_features_parquet (Parquet export)
            ├── detector_training_run → detector_promotion      (6h schedule)
            ├── forecaster_training_run → forecaster_promotion  (6h+30m schedule)
            ├── drift_report (Evidently, 2h schedule)
            └── drift_retrain_sensor → retrain jobs (PSI > 0.25 threshold + cooldown)
```

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Event streaming | Apache Kafka 3.8 | KRaft mode (no ZooKeeper), dual listeners (EXTERNAL/INTERNAL) |
| Stream processing | PyFlink 1.18 | Event-time tumbling windows, watermarks, dead-letter quarantine |
| Serving store | Postgres 16 | `windowed_features` + `alerts` tables; RDS on AWS |
| Anomaly detection | Isolation Forest (scikit-learn) | Trained on normal windows; scores per event |
| Demand forecasting | XGBoost | Lag features, z-scores, time-of-day; MAE-optimised |
| Experiment tracking | MLflow 3.1 | Model registry with `Production` alias, artifact store |
| Drift detection | Evidently | PSI + KS statistic + Jensen-Shannon divergence |
| Orchestration | Dagster 1.9 | Asset graph, 3 schedules, drift-retrain sensor |
| Metrics | Prometheus + Grafana 10 | Auto-provisioned dashboard from JSON |
| Business dashboard | Streamlit | Reads live Postgres state |
| IaC | Terraform 1.7 | Modules: networking, ec2, rds, s3 |
| CI/CD | GitHub Actions | OIDC (zero stored credentials), SSH deploy on merge |
| Containerisation | Docker Compose | 12 services; single EC2 on AWS |

---

## Data Flow

### Event schemas

**Order event** — emitted by the generator on every synthetic sale:
```json
{
  "event_id": "uuid",
  "event_time": "ISO-8601",
  "store_id": "DS_001",
  "sku_id": "SKU_04213",
  "category": "dairy",
  "quantity": 2,
  "unit_price": 55.0,
  "order_id": "uuid",
  "is_injected_anomaly": false
}
```

**Inventory movement event** — emitted alongside each order:
```json
{
  "event_id": "uuid",
  "event_time": "ISO-8601",
  "store_id": "DS_001",
  "sku_id": "SKU_04213",
  "movement_type": "sale | restock | adjustment",
  "delta_units": -2,
  "on_hand_after": 47,
  "is_injected_anomaly": false
}
```

### Windowed feature record (Flink output → Postgres)
```
store_id, sku_id, category, window_start, window_end,
order_rate, depletion_velocity, demand_momentum,
avg_basket_size, on_hand_est, volume_imbalance
```

### Reference dimensions
- `data/seeds/dim_sku.csv` — sku_id, name, category, unit_price, reorder_point (500 SKUs)
- `data/seeds/dim_store.csv` — store_id, region, capacity (3 dark stores)

---

## AWS Deployment

| Resource | Spec | Purpose |
|---|---|---|
| EC2 m7i-flex.large | ap-south-1 | Runs all 12 Docker containers |
| RDS db.t3.micro | Postgres 16, 20GB gp2 | Durable serving store and alert log |
| S3 veloshelf-features-* | — | Parquet feature exports for training |
| S3 veloshelf-mlflow-* | — | MLflow artifact store |
| VPC | 10.0.0.0/16, 2 public subnets | Networking isolation |

Security groups allow inbound on: 22 (SSH), 9092 (Kafka), 5432 (Postgres), 5000 (MLflow), 3000 (Dagster), 3001 (Grafana), 8080–8081 (Kafka UI / Flink UI), 8000 (metrics), 8501 (Streamlit), 9090 (Prometheus).

### Local service URLs

| Service | URL | Credentials |
|---|---|---|
| Flink UI | http://localhost:8081 | — |
| Kafka UI | http://localhost:8080 | — |
| MLflow | http://localhost:5000 | — |
| Dagster | http://localhost:3000 | — |
| Grafana | http://localhost:3001 | admin / veloshelf |
| Streamlit | http://localhost:8501 | — |
| Prometheus | http://localhost:9090 | — |

---

## Repository Layout

```
veloshelf/
├── generator/              # Synthetic event producer
│   ├── producer.py         # Entry point: python -m generator.producer --mode fast
│   ├── distributions.py    # Poisson arrivals, Zipf SKU popularity, time-of-day surges
│   ├── anomaly_injector.py # Injects + logs ground-truth labels → data/seeds/anomaly_labels.jsonl
│   └── schemas.py          # Pydantic event models (OrderEvent, InventoryMovementEvent)
├── streaming/
│   ├── job.py              # PyFlink pipeline: source → validate → window → score → sink
│   ├── validation.py       # Schema + range checks, dead-letter envelope
│   ├── scoring.py          # Rule-based stockout/surge scorer + ML hot-swap integration
│   └── sinks.py            # PostgresSink (upsert features + insert alerts), KafkaAlertSink
├── ml/
│   ├── export_features.py  # Dumps windowed_features from Postgres → data/features/features.parquet
│   ├── train_detector.py   # Isolation Forest training + MLflow logging + promotion
│   ├── train_forecast.py   # XGBoost forecaster training + MLflow logging + promotion
│   ├── features.py         # Shared feature engineering (lags, z-scores) — online/offline parity
│   ├── evaluate.py         # ForecastEvaluator (MAE/RMSE/MAPE) + DetectorEvaluator (P/R/F1)
│   ├── promote.py          # Validation gate + cooldown + MLflow registry promotion
│   └── model_loader.py     # HotSwapModelLoader — polls MLflow registry every 5 min
├── orchestration/
│   ├── definitions.py      # Dagster Definitions (all assets, jobs, schedules, sensor)
│   ├── assets.py           # 5 assets: features parquet, detector/forecaster train+promote, drift
│   ├── schedules.py        # detector_retrain (6h), forecaster_retrain (6h+30m), drift_check (2h)
│   └── sensors.py          # drift_retrain_sensor — watches drift_report, triggers retrain jobs
├── observability/
│   ├── drift_job.py        # Evidently: PSI/KS/JS per feature, HTML report → data/reports/
│   ├── retrain_trigger.py  # Threshold check + cooldown sentinel (.retrain_cooldowns/)
│   ├── metrics_exporter.py # Prometheus gauges: freshness lag, drift PSI, MAE/F1, alert counts
│   └── prometheus/
│       ├── prometheus.yml  # Scrape config
│       └── alerts.yml      # Alerting rules
├── serving/
│   ├── streamlit_app.py    # Business dashboard: stockout risk, surge alerts, category velocity
│   └── grafana/
│       ├── datasource.yml  # Prometheus datasource (auto-provisioned)
│       ├── dashboard.yml   # Dashboard provider config (auto-provisioned)
│       └── veloshelf_dashboard.json  # 4-section dashboard JSON
├── infra/
│   ├── main.tf             # Root module wiring networking + ec2 + rds + s3
│   ├── variables.tf
│   ├── terraform.tfvars.example
│   └── modules/
│       ├── networking/     # VPC, public subnets, IGW, route tables
│       ├── ec2/            # Instance, IAM role + S3 policy, security group, user_data bootstrap
│       ├── rds/            # Postgres 16, db subnet group, SG (ingress from VPC CIDR)
│       └── s3/             # Features bucket + MLflow artifact bucket
├── .github/workflows/
│   ├── ci.yml              # Lint + test on every PR; docker compose build check
│   └── deploy.yml          # OIDC → terraform apply → SSH docker-compose up
├── data/
│   ├── seeds/              # dim_sku.csv, dim_store.csv, anomaly_labels.jsonl
│   ├── features/           # Local Parquet (training input when not using S3)
│   └── reports/            # Evidently HTML drift reports
├── docker-compose.yml      # Full local stack (12 services)
├── Makefile                # up, down, initdb, topics, flink-submit, train, infra-up, ec2-stop, …
└── pyproject.toml
```

---

## Key Design Decisions

**Serving store: Postgres (not Redis)**
Postgres handles windowed feature upserts well via `ON CONFLICT DO UPDATE`. It also stores alerts with timestamps — needed for Grafana time-series panels and Evidently reference windows. Redis would require a separate time-series store for historical queries.

**Training: scikit-learn + XGBoost (not Spark)**
The training dataset is windowed features from a single dark-store cluster — comfortably under 1M rows. Spark adds overhead (JVM, cluster startup) for no benefit at this scale. scikit-learn and XGBoost load Parquet directly via pandas/pyarrow and are easier to register with MLflow's sklearn/xgboost flavors.

**Online scoring: Flink hot-swap (not a separate serving endpoint)**
The Flink taskmanager holds the model in memory and polls MLflow for promotions every 5 minutes. This means the same process that computes features also scores them — no network hop, sub-millisecond latency per window. The tradeoff is that the model lives in the taskmanager JVM heap, limiting model size.

**Drift detection: all three metrics**
PSI is the industry default (interpretable thresholds). KS is a formal statistical test for continuous features. JS is symmetric and bounded. Using all three in `drift_job.py` gives a complete picture and demonstrates understanding of each metric's limitations.

**Orchestration: Dagster (not Airflow)**
Dagster's asset-oriented model maps directly to the ML pipeline DAG (features → model → eval as typed assets). The conditional retrain becomes a real dependency graph rather than a manual `if` check in a task. Also avoids duplication with UrbanPulse (which already shows Airflow).

**OIDC (not stored AWS keys)**
GitHub Actions assumes an IAM role via short-lived STS tokens. No credentials to rotate, no risk from secret leaks. The trust policy scopes it to `repo:Anand09-in/veloshelf:*`.

---

## Cost (AWS ap-south-1)

| Resource | Running | Stopped |
|---|---|---|
| EC2 m7i-flex.large | ~$73/mo | $0 |
| RDS db.t3.micro | ~$12/mo | ~$12/mo (storage billed) |
| S3 (< 1GB) | < $0.03/mo | — |
| **Total idle** | — | **~$12/mo** |

Stop the EC2 when not demoing: `make ec2-stop`. Destroy everything when done: `make infra-down`.

---

## Interview Q&A

**"Why synthetic data?"**
Full control of distributions lets you demo anomalies on demand. More importantly, injected anomalies are logged as ground-truth labels in `data/seeds/anomaly_labels.jsonl` — so you get real precision/recall/F1 against known positives. Real event streams can't give you that without expensive human labelling.

**"Why PyFlink over Spark Structured Streaming?"**
Event-time watermarks and tumbling/sliding window semantics are first-class in Flink. The Table API gives clean SQL-style window aggregation. Spark Structured Streaming handles late data less elegantly and the PyFlink keyword is differentiated.

**"How does drift detection feed retraining?"**
`drift_job.py` runs every 2h (Dagster schedule), computes PSI/KS/JS per feature using Evidently, and writes a summary as asset metadata. `drift_retrain_sensor` reads that metadata on each tick; if `any_drift=True` (PSI > 0.25), it requests runs of `detector_retrain_job` and `forecaster_retrain_job`. Cooldown sentinel files in `.retrain_cooldowns/` prevent re-triggering within the cooldown window.

**"What's the validation gate?"**
`ml/promote.py` only promotes a new model to the MLflow `Production` alias if its holdout metrics beat the current Production incumbent. Drift does not equal better model — retraining on a drifted distribution can make performance worse.

**"Why not EKS?"**
$72/month for a portfolio piece. The architecture is designed for EKS — Terraform module for EKS committed, k8s manifests written — but runs single-node for cost. "Designed for Kubernetes, ran single-node to stay in free tier" is a stronger answer than silently skipping it.
