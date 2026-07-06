# VeloShelf — Real-Time Quick-Commerce Intelligence Pipeline

> A production-grade streaming data platform for a Blinkit/Zepto-style dark-store business:
> ingest live order & inventory events, compute windowed features in real time, detect
> **stockouts** and **demand surges**, forecast short-horizon demand, and run the whole thing
> with real data observability, drift detection, and a closed-loop retraining story —
> all inside the AWS Free Tier.

---

## 1. Project Goal

**One-line pitch:** An end-to-end real-time data engineering + streaming-ML platform for quick-commerce dark stores that turns a firehose of order/inventory events into live operational intelligence (stockout risk, demand surges) with production-grade observability and automated model retraining.

**Why this domain:** Quick-commerce (10-minute grocery delivery) is defined by velocity — orders fire every second, inventory depletes in real time, demand spikes without warning (rain, weekends, festivals). The domain's fast-moving nature is expressed *directly in the engineering*: windowed stream processing, event-time handling, freshness SLAs, and real-time alerting. This is not a nightly batch warehouse wearing a quick-commerce label; the pipeline is streaming because the business is streaming.

**What it demonstrates (resume narrative):**
- End-to-end ownership of a **real-time** streaming pipeline (ingestion → processing → serving)
- **Streaming ML** lifecycle: online scoring + offline training + model registry + hot-swap
- **Production observability**: freshness/volume/schema/distribution monitoring
- **Data drift detection** (PSI/KS/JS) and **model evaluation** over time
- **Closed-loop MLOps**: drift-triggered retraining with validation gates
- **Cost engineering**: real streaming on a single EC2 inside the AWS Free Tier
- **IaC + CI/CD**: Terraform-provisioned infra, GitHub Actions with OIDC

**Portfolio positioning:** This is the **streaming + streaming-ML + observability** project. Its sibling, **UrbanPulse** (NYC taxi), is the **batch / dimensional-modeling / dbt** project. Together they form a clean, non-overlapping spread: batch and streaming, both with strong modeling and testing discipline.

---

## 2. Scope Decisions (locked)

These were decided deliberately; each is defensible in an interview.

| Decision | Choice | Rationale |
|---|---|---|
| Domain | Quick-commerce (Blinkit/Zepto grocery) | Velocity is native to the domain; interviewers in India get it instantly |
| Detection targets | **Both** stockout + demand surge | Two business-meaningful outputs from the same windowed features |
| Data source | **Synthetic event generator** | Full control of distributions → can demo anomalies on demand + have ground-truth labels |
| Streaming backbone | **Kafka on a single EC2** | Reuses skills, keeps the Kafka keyword, free-tier friendly |
| Stream processing | **PyFlink** windowed features | Real event-time windowing; carries the Flink keyword |
| Offline training | **Spark + MLflow** | Batch training, model registry, hot-swap |
| Compute scope | **Single-EC2 "done"** | EKS manifests committed but not run → cost-conscious, still shows k8s design |
| Dashboards | **Grafana** (live health) + **Streamlit** (business view) | Grafana for time-series ops; Streamlit for bespoke business tables |
| Observability | **Evidently first**, Great Expectations later | Drift/eval is the differentiator; ship it before table-stakes quality assertions |
| Retraining | **Scheduled** drift-triggered retrain w/ validation gate + cooldown | Closed MLOps loop, but scheduled (not always-on) for cost/simplicity |
| Orchestration | **Dagster** (asset-oriented) | Fresher keyword than Airflow (already on UrbanPulse); asset model fits the ML pipeline; models the conditional retrain as a real dependency graph |
| Serving store | **Redis vs. Postgres — open** | Decide before Phase 2; hinges on how tabular/SQL-driven the dashboard views are |

**On EKS / Kubernetes:** Not a core data-engineering gate — it's a "nice to have" keyword, far more central to ML-platform/MLOps roles. The plan **designs for EKS** (commits k8s manifests + a Terraform EKS module) but **runs on a single EC2** for cost. "Designed for EKS, ran on a single node to stay in free tier" is a strong, cost-conscious interview answer.

---

## 3. Architecture

### 3.1 High-level flow

```
┌──────────────────────┐
│  Synthetic Event Gen │  Poisson arrivals + time-of-day/weekend surges,
│  (Python producer)   │  per-SKU popularity, injected anomalies (ground truth)
└──────────┬───────────┘
           │ order & inventory events
           ▼
┌──────────────────────┐
│   Kafka (single EC2) │  topics: raw-orders, raw-inventory,
│                      │  features-windowed, stockout-alerts, surge-alerts, dead-letter
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   PyFlink job        │  event-time tumbling/sliding windows
│                      │  → order rate, depletion velocity, demand momentum,
│                      │    basket size, per-SKU/-store volume
│                      │  → schema/range validation → dead-letter on failure
└─────┬────────────┬───┘
      │            │
      │ features   │ alerts (stockout + surge)
      ▼            ▼
┌───────────┐  ┌──────────────────┐
│    S3     │  │  Serving store    │  (Redis or Postgres:
│ raw +     │  │  latest window    │   latest per-SKU state,
│ features  │  │  state + alerts)  │   active alerts)
│ (parquet) │  └─────┬────────────┬─┘
└─────┬─────┘        │            │
      │              ▼            ▼
      │        ┌──────────┐  ┌──────────────┐
      │        │ Grafana  │  │  Streamlit    │
      │        │ (live    │  │  (business    │
      │        │  health) │  │   dashboard)  │
      │        └────▲─────┘  └──────────────┘
      │             │ metrics
      │        ┌────┴─────┐
      │        │Prometheus│ ◄── freshness lag, volume, feature stats,
      │        └──────────┘     PSI gauges, alert counts
      │
      ▼
┌────────────────────────────────────────────┐
│  Batch layer (scheduled: Dagster)           │
│  ┌─────────────┐   ┌──────────────────────┐ │
│  │ Spark train │   │ Evidently drift + eval│ │
│  │ forecast +  │   │ PSI/KS/JS, MAE/RMSE,  │ │
│  │ anomaly mdl │   │ precision/recall      │ │
│  └──────┬──────┘   └──────────┬───────────┘ │
│         │                     │             │
│         ▼                     ▼             │
│   ┌──────────┐         drift/error breach?  │
│   │  MLflow  │◄────── retrain trigger ──────┘
│   │ registry │        (validation gate + cooldown)
│   └────┬─────┘
│        │ promote best model
│        ▼
│   hot-swap into online scoring
└────────────────────────────────────────────┘
```

### 3.2 Layer responsibilities

**Ingestion (synthetic generator).** A Python producer emits realistic `order` and `inventory_movement` events. Arrival modeled as a time-varying Poisson process with time-of-day and weekend multipliers; per-SKU demand from a long-tail (Zipf-ish) popularity distribution; per-dark-store partitioning. Anomalies are *injected deliberately* (sudden demand spikes on specific SKUs, accelerated depletion) and **logged as ground-truth labels** — this is the key advantage of synthetic data: you can report real detection precision/recall, which scraped/live data can't give you.

**Streaming backbone (Kafka on EC2).** Single-broker Kafka. Topics carry raw events, windowed features, the two alert streams, and a **dead-letter topic** for malformed/failed-validation events (a maturity signal most demos skip).

**Stream processing (PyFlink).** Event-time tumbling (e.g. 1-min) and sliding (e.g. 5-min/1-min slide) windows compute:
- `order_rate` per SKU / category / store per window
- `depletion_velocity` (units/min) per SKU
- `demand_momentum` (short vs. longer window rate ratio)
- `basket_size` distribution
- per-store `volume_imbalance`

Inline schema + range validation; failures → dead-letter. Outputs windowed features (to S3 + serving store) and evaluates the current online model to emit **stockout** and **surge** alerts.

**Serving store.** Redis (fast, simple) or Postgres holds latest-per-SKU state and active alerts for the dashboards to read.

**Storage (S3).** Raw events and windowed features land as partitioned Parquet (date/hour) — the reference + current windows for drift analysis and the training corpus.

**Batch layer (scheduled).** Two scheduled jobs:
1. **Spark + MLflow training** — trains the demand forecaster and the anomaly/surge model on historical features; logs metrics + registers models.
2. **Evidently drift + evaluation** — PSI/KS/JS on recent vs. reference window; model metrics (MAE/RMSE/MAPE for the forecaster; precision/recall/F1 for the detector against injected labels). Drift report HTML → S3; key metrics → MLflow + mirrored as Prometheus gauges.

**Closed-loop retraining.** Drift threshold breach (e.g. PSI > 0.25) **or** error degradation (e.g. MAE beyond bound) → retrain → **validate against holdout** → promote in MLflow **only if it beats the incumbent** → online scorer hot-swaps. Guardrails: validation gate (drift ≠ better model) and a **cooldown / rate-limit** (no retrain storms).

**Observability.**
- *Data quality:* in-stream dead-letter quarantine now; Great Expectations in the warehouse layer later.
- *Live health:* Prometheus scrapes freshness lag, volume, per-window feature stats, alert counts → Grafana.
- *Drift/eval:* Evidently reports (S3) + metrics in MLflow + PSI gauges surfaced on Grafana.

### 3.3 Observability & evaluation vocabulary (keep these crisp)

- **Data quality testing** — explicit rules (non-null, unique, ranges, referential integrity).
- **Data observability** — the five pillars: freshness, volume, schema, distribution, lineage.
- **Data drift** — distributional change: *feature drift* (PSI, KS, Chi-square, JS, Wasserstein), *concept drift* (feature→target relationship changes), *prediction drift* (output distribution shifts).
- **Model monitoring** — performance over time (MAE/RMSE/MAPE; precision/recall/F1), tracked across versions in MLflow.

*PSI rule of thumb:* < 0.1 stable · 0.1–0.25 moderate shift · > 0.25 significant shift.

---

## 4. Tech Stack

| Concern | Tool |
|---|---|
| Language | Python (+ SQL) |
| Streaming backbone | Apache Kafka (single EC2) |
| Stream processing | PyFlink (event-time windows) |
| Batch training | Apache Spark |
| ML tracking / registry | MLflow |
| Drift / evaluation | Evidently (Great Expectations later) |
| Serving store | Redis or Postgres |
| Object storage | AWS S3 (Parquet) |
| Metrics | Prometheus |
| Dashboards | Grafana (ops) + Streamlit (business) |
| Orchestration | Dagster (asset-oriented; scheduled batch jobs) |
| IaC | Terraform |
| CI/CD | GitHub Actions + OIDC |
| Containerization | Docker (k8s manifests committed, not run) |

---

## 5. Repository Structure

```
veloshelf/
├── README.md
├── docs/
│   ├── architecture.md            # this spec, trimmed
│   ├── observability.md           # drift/eval design + PSI thresholds
│   └── cost.md                    # free-tier cost math
├── generator/                     # synthetic event producer
│   ├── producer.py
│   ├── distributions.py           # Poisson arrivals, Zipf SKU popularity
│   ├── anomaly_injector.py        # injects + logs ground-truth labels
│   └── schemas.py                 # event schemas (order, inventory)
├── streaming/                     # PyFlink job
│   ├── job.py                     # main Flink pipeline
│   ├── windows.py                 # windowed feature definitions
│   ├── validation.py              # schema/range checks → dead-letter
│   ├── scoring.py                 # online model eval → alerts
│   └── sinks.py                   # S3 + serving-store writers
├── ml/
│   ├── train_forecast.py          # Spark demand forecaster + MLflow
│   ├── train_detector.py          # anomaly/surge model + MLflow
│   ├── evaluate.py                # MAE/RMSE, precision/recall vs. labels
│   ├── promote.py                 # validation gate + registry promotion
│   └── features.py                # shared feature logic (online/offline parity)
├── observability/
│   ├── drift_job.py               # Evidently: PSI/KS/JS, report → S3
│   ├── retrain_trigger.py         # threshold check + cooldown → trigger
│   ├── metrics_exporter.py        # push gauges to Prometheus
│   └── prometheus/                # scrape config, rules
├── serving/
│   ├── streamlit_app.py           # business dashboard
│   └── grafana/                   # dashboard JSON, provisioning
├── infra/                         # Terraform
│   ├── main.tf
│   ├── ec2.tf                     # Kafka/Flink host
│   ├── s3.tf
│   ├── iam.tf                     # OIDC roles
│   ├── eks.tf                     # committed, NOT applied (design intent)
│   └── variables.tf
├── k8s/                           # manifests (design intent, not run)
│   ├── kafka.yaml
│   ├── flink.yaml
│   └── streamlit.yaml
├── orchestration/
│   ├── definitions.py             # Dagster Definitions (assets, jobs, schedules)
│   ├── assets.py                  # data/model assets (features → model → eval)
│   └── schedules.py               # scheduled runs for train/drift/retrain
├── tests/
│   ├── test_generator.py
│   ├── test_windows.py
│   ├── test_validation.py
│   └── test_evaluate.py
├── .github/workflows/
│   ├── ci.yml                     # lint, test
│   └── deploy.yml                 # OIDC → Terraform
├── docker-compose.yml             # local full-stack run
├── Makefile
├── pyproject.toml
└── requirements.txt
```

---

## 6. Data Model

### 6.1 Event schemas

**Order event**
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

**Inventory movement event**
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

### 6.2 Windowed feature record (Flink output)
```
store_id, sku_id, category, window_start, window_end,
order_rate, depletion_velocity, demand_momentum,
avg_basket_size, on_hand_estimate, volume_imbalance
```

### 6.3 Reference dimensions (for context / joins)
- `dim_sku` — sku_id, name, category, unit_price, reorder_point
- `dim_store` — store_id, region, capacity

---

## 7. Build Plan (phased)

Sequenced so you always have a runnable slice. Target: **3–4 weekends**.

### Phase 0 — Foundations (½ weekend)
- Repo scaffold, `pyproject.toml`, Makefile, `docker-compose.yml`
- Event schemas + `dim_sku` / `dim_store` seed data
- **Done when:** `docker-compose up` brings up Kafka + Redis + local MLflow.

### Phase 1 — Ingestion (½ weekend)
- Synthetic generator: Poisson arrivals, Zipf SKU popularity, time-of-day/weekend surges
- Anomaly injector writing ground-truth labels
- Producer → `raw-orders` / `raw-inventory`
- **Done when:** events flow into Kafka; labels persisted; `test_generator` passes.

### Phase 2 — Stream processing (1 weekend)
- PyFlink job: event-time windows → the 5 feature families
- Inline validation → dead-letter topic
- Sinks: features → S3 (Parquet) + serving store
- **Done when:** windowed features land in S3 & Redis; bad events quarantined; `test_windows` + `test_validation` pass.

### Phase 3 — ML layer (1 weekend)
- Spark training: demand forecaster + anomaly/surge detector
- MLflow tracking + registry; online scorer reads current model → emits stockout/surge alerts
- Evaluation: MAE/RMSE + precision/recall vs. injected labels
- **Done when:** alerts appear on their topics; models registered; metrics logged.

### Phase 4 — Observability (½–1 weekend)
- Prometheus exporters: freshness lag, volume, feature stats, alert counts
- Grafana dashboards (live health)
- Evidently drift job: PSI/KS/JS → report to S3 + metrics to MLflow + PSI gauges to Prometheus
- Streamlit business dashboard (low-stock/stockout-risk SKUs, active surge alerts, category velocity)
- **Done when:** Grafana shows live health; Evidently report generates; Streamlit reads live state.

### Phase 5 — Closed-loop retraining (½ weekend)
- `retrain_trigger`: threshold check (PSI / MAE) + cooldown
- `promote`: retrain → holdout validation gate → promote only if better → hot-swap
- Scheduled via Dagster (asset-based job with a schedule; conditional retrain step)
- **Done when:** an injected drift event triggers a retrain that promotes only on improvement.

### Phase 6 — IaC, CI/CD, polish (½–1 weekend)
- Terraform: EC2, S3, IAM/OIDC (+ committed-but-unapplied `eks.tf` and `k8s/`)
- GitHub Actions: CI (lint/test) + deploy (OIDC → Terraform)
- README with architecture diagram, screenshots, cost notes, "production evolution" section
- **Done when:** clean clone → documented path to run; CI green.

### Later (only if project succeeds)
- Great Expectations quality suite in the warehouse layer
- Event-driven (always-on) retrain trigger
- Actually run EKS

---

## 8. Free-Tier Cost Math

| Resource | Plan | Est. cost |
|---|---|---|
| EC2 (Kafka + Flink + Streamlit) | 1× t3.small (or t3.micro if tight); stop when idle | Free-tier hours / a few $ |
| S3 | Parquet features + drift reports; lifecycle-expire raw | < $1 |
| MLflow | On the same EC2 (SQLite/local backend) | $0 |
| Prometheus + Grafana | Containers on same EC2 | $0 |
| EKS | **Not run** (manifests only) | $0 |
| Dagster (on same EC2) | Runs alongside stack; stop when idle | ~$0 |
| Data transfer | Minimal (synthetic, self-contained) | ~$0 |

**Guardrails:** stop the EC2 when not demoing; S3 lifecycle rule to expire raw events; no NAT Gateway; no MSK/Kinesis; no always-on EKS. **Keep the $200 credits essentially untouched.**

---

## 9. Resume Bullets (draft — tighten after build)

- Built a real-time quick-commerce intelligence platform ingesting synthetic order/inventory events through **Kafka → PyFlink** event-time windows, detecting **stockouts and demand surges** with sub-minute freshness.
- Implemented a **streaming-ML lifecycle** — online scoring, Spark offline training, MLflow registry, and drift-triggered **hot-swap retraining** with a holdout validation gate.
- Engineered production **observability**: freshness/volume/feature monitoring via **Prometheus + Grafana**, and **data-drift detection** (PSI/KS/JS) with **Evidently**, reporting real detection precision/recall against injected ground-truth anomalies.
- Provisioned all infra with **Terraform** and **GitHub Actions OIDC** CI/CD; ran real streaming inside the **AWS Free Tier** by designing for EKS but deploying single-node for cost.

---

## 10. Interview Talking Points (anticipated Q&A)

- **"Why synthetic data?"** Full control of distributions to demo anomalies on demand, *and* ground-truth labels → real precision/recall, which live data can't give.
- **"Why not MSK/Kinesis?"** Cost. Single-broker Kafka on EC2 gives the same semantics inside free tier; MSK would burn credits for no learning value.
- **"How do you detect drift?"** PSI as primary (thresholds 0.1/0.25), KS for continuous, JS/Chi-square as needed, via Evidently on recent-vs-reference windows.
- **"Isn't auto-retraining dangerous?"** Two guardrails: a holdout **validation gate** (drift ≠ better model — never promote without beating the incumbent) and a **cooldown** to prevent retrain storms.
- **"Why Dagster over Airflow?"** Asset-oriented model maps cleanly to an ML pipeline (features → model → eval as assets), the conditional retrain becomes a real dependency graph, and it's a current keyword I didn't already have (UrbanPulse already shows Airflow) — chosen deliberately, not by default.
- **"Why single-EC2 not EKS?"** Cost-conscious choice; designed for EKS (manifests + Terraform module committed), ran single-node — demonstrates I can right-size infra to the problem.
- **"How is this different from your batch project?"** UrbanPulse is batch/dimensional-modeling; this is streaming + streaming-ML + observability. Complementary, non-overlapping.

---

*Spec complete. Ready to start at Phase 0.*