# VeloShelf

**Real-time quick-commerce intelligence pipeline.** Ingests live order & inventory
events from a Blinkit/Zepto-style dark-store business, computes windowed features in
real time, detects **stockouts** and **demand surges**, forecasts short-horizon demand,
and runs with real data observability, drift detection, and closed-loop retraining.

Streaming sibling to **UrbanPulse** (batch / dimensional modeling).

## Stack

Kafka · PyFlink · Spark · MLflow · Evidently · Dagster · Redis/Postgres ·
Prometheus · Grafana · Streamlit · Terraform · GitHub Actions (OIDC)

## Quickstart (local)

```bash
make setup    # install deps
make up       # start local stack
make seed     # validate seed data
make lint     # ruff
make test     # pytest
```

Services once up:
- Kafka — `localhost:9092`
- MLflow UI — http://localhost:5000
- Dagster UI — http://localhost:3000
- Redis — `localhost:6379`
- Postgres — `localhost:5432`

## Build phases

0. Foundations ← **you are here**
1. Ingestion (synthetic event generator)
2. Stream processing (PyFlink windowed features)
3. ML layer (Spark training + online scoring)
4. Observability (Prometheus/Grafana, Evidently, Streamlit)
5. Closed-loop retraining
6. IaC, CI/CD, polish