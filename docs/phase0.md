# Phase 0 — Foundations

Repo scaffold, local Docker stack, seed data, and event schemas. Everything a contributor needs to run the full stack from a cold clone.

---

## What was built

### Repository skeleton

`pyproject.toml` defines the package with optional dependency groups (`[dev]` for ruff + pytest). Entry points are registered so modules run as `python -m generator.producer`, `python -m ml.train_detector`, etc.

`.gitignore` covers: Terraform state and `.tfvars`, SSH key `.pem` files, Python build artefacts, MLflow run directories, local Parquet and report outputs.

### Local stack — `docker-compose.yml`

12 services wired on the default Docker bridge network. Services reference each other by container name (e.g. `kafka:29092`, `postgres:5432`, `mlflow:5000`). Kafka uses the `INTERNAL` listener for intra-container traffic and `EXTERNAL://localhost:9092` for host access.

| Service | Image | Port |
|---|---|---|
| kafka | apache/kafka:3.8.0 | 9092 (host), 29092 (internal) |
| postgres | postgres:16-alpine | 5432 |
| mlflow | ghcr.io/mlflow/mlflow:v3.1.0 | 5000 |
| dagster | python:3.11-slim | 3000 |
| kafka-ui | provectuslabs/kafka-ui | 8080 |
| flink-jobmanager | Dockerfile.flink | 8081 |
| flink-taskmanager | Dockerfile.flink | — |
| prometheus | prom/prometheus:v2.50.1 | 9090 |
| grafana | grafana/grafana:10.3.3 | 3001 |
| metrics-exporter | python:3.11-slim | 8000 |
| streamlit | python:3.11-slim | 8501 |
| redis | redis:7-alpine | 6379 |

Kafka runs in KRaft mode (no ZooKeeper). `CLUSTER_ID` is hard-coded to keep the broker identity stable across restarts. `KAFKA_CONTROLLER_QUORUM_VOTERS` points to `localhost:9093` within the container.

MLflow uses SQLite as its backend store and `/mlflow/artifacts` as the artifact root, both persisted in the `mlflow_data` named volume.

Dagster installs `dagster==1.9.4` at container start, then runs `dagster dev -f orchestration/definitions.py`. It gets `MLFLOW_TRACKING_URI=http://mlflow:5000` and `POSTGRES_DSN=postgresql://veloshelf:veloshelf@postgres:5432/veloshelf` via environment.

### Seed dimensions

`data/seeds/dim_sku.csv` — 500 SKUs across categories (dairy, bakery, snacks, beverages, produce, frozen). Each row has: sku_id, name, category, unit_price, reorder_point.

`data/seeds/dim_store.csv` — 3 dark stores: DS_001 (North), DS_002 (South), DS_003 (East), with region and capacity fields.

`data/seeds/anomaly_labels.jsonl` — ground-truth labels for injected anomalies. Written by `generator/anomaly_injector.py` at producer runtime. Schema: `{event_id, sku_id, store_id, anomaly_type, injected_at}`. Used by `ml/evaluate.py` to compute real precision/recall.

### Event schemas — `generator/schemas.py`

Pydantic v2 models for both event types. `OrderEvent` and `InventoryMovementEvent` share a `BaseEvent` with `event_id` (UUID), `event_time` (ISO-8601 string), `store_id`, and `sku_id`. The `is_injected_anomaly` flag is set by the anomaly injector and propagated to the label file.

### Makefile

Key targets:

| Target | What it does |
|---|---|
| `make up` | `docker-compose up -d` |
| `make down` | `docker-compose down` |
| `make initdb` | Pipes `infra/init_db.sql` into Postgres via psql |
| `make topics` | Creates 5 Kafka topics (raw-orders, raw-inventory, dead-letter, stockout-alerts, surge-alerts) |
| `make flink-submit` | Submits `streaming/job.py` to Flink jobmanager with `--detached` |
| `make export-features` | `python -m ml.export_features` |
| `make train` | export-features + train detector |
| `make forecast` | `python -m ml.train_forecast` |
| `make infra-up` | `terraform apply` from `infra/` using the `veloshelf` conda env |
| `make ec2-stop` | Stops the EC2 instance via AWS CLI |
| `make ec2-start` | Starts the EC2 instance |
| `make ec2-ssh` | SSH into EC2 using `veloshelf-key.pem` |

---

## Environment variables

`.env` (gitignored) sets runtime config. `.env.example` documents all variables:

```
POSTGRES_DSN=postgresql://veloshelf:veloshelf@postgres:5432/veloshelf
MLFLOW_TRACKING_URI=http://mlflow:5000
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
FEATURES_PATH=data/features
```

`FEATURES_PATH` is a local directory path. Using an `s3://` URI here breaks `pathlib.Path` operations in the ML layer — keep it local and point training scripts at the exported Parquet directly.

---

## Postgres schema — `infra/init_db.sql`

Two tables created at `make initdb`:

**`windowed_features`** — one row per (store_id, sku_id, window_start) — upserted by the Flink sink on each window close. Columns: store_id, sku_id, category, window_start, window_end, order_rate, depletion_velocity, demand_momentum, avg_basket_size, on_hand_est, volume_imbalance, created_at.

**`alerts`** — append-only alert log. Columns: id (serial), alert_type (stockout | surge), store_id, sku_id, severity, score, details (JSONB), triggered_at.

Indexes on both tables: (store_id, sku_id), window_start, triggered_at.
