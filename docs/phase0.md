# Phase 0 — Foundations

> Goal: a runnable skeleton. `docker-compose up` brings up the local stack;
> `make lint` and `make test` run green; seed dimensions load without error.

**Definition of done:** Kafka + Redis + Postgres + MLflow + Dagster all healthy;
`make seed`, `make lint`, `make test` all pass.

---

## Task list

### Repo & tooling
- [ ] Directory skeleton created
- [ ] `pyproject.toml` with deps + ruff/pytest config
- [ ] `requirements.txt`
- [ ] `Makefile`
- [ ] `.gitignore`
- [ ] `.env.example` + `.env`
- [ ] `README.md`

### Local infrastructure
- [ ] `docker-compose up -d` starts without errors
- [ ] Kafka healthy (check `docker-compose ps`)
- [ ] Redis healthy
- [ ] Postgres healthy
- [ ] MLflow UI reachable at http://localhost:5000
- [ ] Dagster UI reachable at http://localhost:3000

### Data contracts & seeds
- [ ] `generator/schemas.py` — pydantic event models
- [ ] `data/seeds/dim_sku.csv` — 15 SKUs
- [ ] `data/seeds/dim_store.csv` — 3 dark stores
- [ ] `make seed` runs without error

### Sanity checks
- [ ] `make lint` — ruff clean
- [ ] `make test` — 2 tests passing

---

## Deferred to Phase 2
- Serving store final choice (Redis vs. Postgres)

## Notes
- Phase 0 is local-only. No AWS spend yet.
- Both Redis and Postgres are in compose; the app reads `SERVING_STORE` from `.env`.
- Kafka uses KRaft (no ZooKeeper) to keep the stack lean.