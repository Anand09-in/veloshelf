# VeloShelf

**Real-time quick-commerce intelligence pipeline** — a production-grade streaming data + ML platform modelled after Blinkit/Zepto dark-store operations.

Ingests live order and inventory events, computes windowed features in real time, detects **stockouts** and **demand surges**, forecasts short-horizon demand, and runs with full data observability, drift detection, and closed-loop automated retraining — deployed on AWS.

---

## Architecture

```
Synthetic Generator (Poisson arrivals + Zipf SKU popularity + anomaly injection)
        │
        ▼ order & inventory events
   Kafka (KRaft, single broker)
        │
        ▼
   PyFlink (event-time tumbling + sliding windows)
   ├── Validation → dead-letter quarantine
   ├── Windowed features (order_rate, depletion_vel, demand_momentum, on_hand_est)
   └── Online scoring (Isolation Forest hot-swap, rule-based fallback)
        │
        ├──► Postgres RDS     (serving store — live dashboard)
        ├──► S3 Parquet       (training corpus — batch ML layer)
        └──► Kafka alerts     (stockout-alerts, surge-alerts)
                │
                ▼
   Dagster (asset-oriented orchestration)
   ├── drift_check (every 2h) → Evidently PSI + KS + JS → HTML report
   ├── detector_retrain (every 6h + drift-triggered) → Isolation Forest → MLflow
   ├── forecaster_retrain (every 6h + drift-triggered) → XGBoost → MLflow
   └── drift_retrain_sensor → triggers retrain when PSI > 0.25
                │
                ▼
   MLflow Registry (validation gate + cooldown → promote to Production)
                │
                ▼ hot-swap within 5 min (no Flink restart)
   Flink online scorer ← always running

   Observability:
   ├── Prometheus + Grafana  (pipeline health, freshness lag, alert counts, PSI gauges)
   └── Streamlit             (business dashboard — stockout risk, surge alerts, velocity)
```

---

## Stack

| Layer | Tools |
|---|---|
| Ingestion | Python, Kafka (KRaft) |
| Stream processing | PyFlink (event-time windows) |
| Batch ML | Apache Spark, XGBoost, Isolation Forest |
| ML tracking | MLflow (registry + hot-swap) |
| Drift detection | Evidently (PSI + KS + JS divergence) |
| Orchestration | Dagster (assets + schedules + sensor) |
| Serving store | AWS RDS Postgres |
| Object storage | AWS S3 |
| Observability | Prometheus, Grafana, Streamlit |
| IaC | Terraform (S3 backend + DynamoDB lock) |
| CI/CD | GitHub Actions + OIDC (zero stored credentials) |
| Containers | Docker, docker-compose |

---

## Quickstart (local)

```bash
git clone https://github.com/Anand09-in/veloshelf
cd veloshelf

# Python env
conda create -n veloshelf python=3.11 -y
conda activate veloshelf
make setup

# Start local stack
make up           # Kafka + Flink + Postgres + MLflow + Dagster

# One-time setup
make jar          # downloads Flink Kafka connector JAR
make initdb       # creates Postgres tables
make topics       # creates all 5 Kafka topics

# Run the pipeline
make flink-submit                              # submit Flink job
python -m generator.producer --mode fast       # flood Kafka for testing
python -m generator.producer --mode realtime   # simulate live dark store

# Train models (after features accumulate)
python -m ml.train_detector
python -m ml.train_forecast

# Run drift detection
python -m observability.drift_job

# Business dashboard
streamlit run serving/streamlit_app.py
```

**Service URLs (local):**
| Service | URL |
|---|---|
| Flink UI | http://localhost:8081 |
| MLflow | http://localhost:5000 |
| Dagster | http://localhost:3000 |
| Grafana | http://localhost:3001 (admin / veloshelf) |
| Streamlit | http://localhost:8501 |
| Prometheus | http://localhost:9090 |

---

## AWS Deployment

```bash
# One-time: create state bucket + lock table
aws s3 mb s3://veloshelf-tfstate-<suffix> --region ap-south-1
aws dynamodb create-table \
  --table-name veloshelf-tfstate-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1

# Configure and deploy
cp infra/terraform.tfvars.example infra/terraform.tfvars
# edit terraform.tfvars with your values
cd infra && terraform init && terraform apply
```

GitHub Actions deploys automatically on merge to `main` using OIDC (zero stored AWS credentials).

---
## Test coverage

```
88 tests across 6 phases
  Phase 0+1: schemas, seed validation, distributions, inventory, anomaly injector
  Phase 2:   streaming validation, scoring, Parquet sink
  Phase 3:   feature engineering, MAE/RMSE/MAPE, precision/recall/F1, promotion logic
  Phase 4:   PSI, KS, JS divergence, rolling window split, Prometheus push helpers
  Phase 5:   trigger decision, cooldown, multi-feature drift, partial cooldown
```