# Phase 4 — Observability (Prometheus + Grafana + Evidently + Streamlit)

> Goal: a complete observability + serving layer.
> Grafana shows live pipeline health; Evidently detects rolling data drift;
> Streamlit gives business-facing inventory intelligence.

**Definition of done:**
- `make test` passes all Phase 4 tests.
- `make up` starts Prometheus (9090), Grafana (3001), metrics-exporter (8000), Streamlit (8501).
- Grafana UI shows live metrics from the pipeline.
- `python -m observability.drift_job` generates an HTML report in `data/reports/`.
- Streamlit dashboard shows stockout-risk SKUs and active alerts from Postgres.
- Dagster UI shows 6 assets + 3 schedules.

---

## Task list

### New files
- [x] `observability/__init__.py`
- [x] `observability/drift_job.py`       — rolling drift (PSI+KS+JS), Evidently HTML report
- [x] `observability/metrics_exporter.py` — Prometheus gauges, background pollers
- [x] `observability/prometheus/prometheus.yml` — scrape config
- [x] `observability/prometheus/alerts.yml`     — alerting rules
- [x] `serving/grafana/datasource.yml`   — Grafana Prometheus datasource provisioning
- [x] `serving/streamlit_app.py`         — business dashboard (stockout, surge, velocity, drift)
- [x] `tests/test_observability.py`      — unit tests (PSI, KS, JS, window split, push helpers)

### Updated files
- [x] `orchestration/assets.py`    — added drift_report asset
- [x] `orchestration/schedules.py` — added drift_schedule (every 2h)
- [x] `orchestration/definitions.py` — wires drift_report + drift_schedule
- [x] `docker-compose.yml`         — added prometheus, grafana, metrics-exporter, streamlit
- [x] `pyproject.toml`             — added evidently, scipy, prometheus-client, streamlit
- [x] `requirements.txt`           — same

### Verification steps
- [ ] `make test`  — all tests pass
- [ ] `make lint`  — ruff clean
- [ ] `make setup` — new deps installed
- [ ] `make up`    — all services healthy
- [ ] Prometheus UI reachable: http://localhost:9090
- [ ] Grafana UI reachable: http://localhost:3001 (user: admin / pass: veloshelf)
- [ ] metrics-exporter: http://localhost:8000/metrics shows gauge names
- [ ] Streamlit: http://localhost:8501 loads dashboard
- [ ] `python -m observability.drift_job` generates report in data/reports/
- [ ] Dagster: http://localhost:3000 shows 6 assets + 3 schedules

---

## Step-by-step verification

### 1 — Tests and lint
```bash
make setup     # installs evidently, scipy, prometheus-client, streamlit
make test      # all phases
make lint
```

### 2 — Start the full stack
```bash
make up
docker-compose ps   # all services healthy
```

### 3 — Verify Prometheus scraping metrics
```bash
# Check /metrics endpoint
curl http://localhost:8000/metrics | grep veloshelf

# Open Prometheus UI → Status → Targets
# http://localhost:9090/targets
# veloshelf-pipeline should show UP
```

### 4 — Verify Grafana
Open http://localhost:3001
- Login: admin / veloshelf
- Connections → Data sources → Prometheus should be auto-provisioned
- Explore → query `veloshelf_feature_freshness_lag_seconds`

Import a dashboard:
- Dashboards → Import → Upload the dashboard.json from serving/grafana/
  (if present; can also be built manually in the UI)

### 5 — Run drift job
```bash
python -m observability.drift_job
```
Expected:
```
INFO  Drift | order_rate       psi=0.0xxx ks=0.0xxx js=0.0xxx drifted=False
INFO  Drift job complete | any_drift=False report=data/reports/drift_*.html
```
Open the HTML report in browser.

### 6 — View Streamlit dashboard
```bash
# If running locally (not in Docker):
streamlit run serving/streamlit_app.py
```
Open http://localhost:8501

The dashboard auto-refreshes every 30s and shows:
- Pipeline status badge + freshness lag
- Stockout-risk SKUs sorted by depletion velocity
- Active surge alerts
- Per-store order velocity heatmap
- Store health summary
- Latest drift report link

### 7 — Verify Dagster has 6 assets + 3 schedules
Open http://localhost:3000 → Assets
Expected asset graph:
```
windowed_features_parquet
  ├── detector_training_run  → detector_promotion
  ├── forecaster_training_run → forecaster_promotion
  └── drift_report
```
Schedules: detector_retrain (6h), forecaster_retrain (6h+30m), drift_check (2h)

---

## Design notes (for interviews)

**Why three separate surfaces?**
Grafana / Prometheus: ops + engineering — time-series, pipeline health,
infra metrics. Evidently reports: data-science — deep drift analysis,
feature distributions. Streamlit: business — actionable inventory
intelligence, no raw metrics.

**Rolling vs fixed reference for drift:**
Rolling (last 1h vs prior 24h) is more appropriate for quick-commerce
because demand is intrinsically time-varying (lunch peaks, weekends).
A fixed baseline would permanently "drift" as the business grows.
The tradeoff: rolling reference can miss slow, gradual drift that a
fixed baseline would catch.

**PSI vs KS vs JS — what each catches:**
PSI:  overall shape shift, most common in industry, interpretable thresholds.
      Limitation: not a true statistical test, sensitive to binning.
KS:   formal statistical test for continuous features, p-value tells you
      significance. Limitation: only tests CDF, misses variance changes.
JS:   symmetric, bounded [0,1], works for both continuous and discrete.
      Limitation: less interpretable than PSI for practitioners.
Using all three gives the most complete picture and shows you understand
the tradeoffs, not just cargo-culted one metric.

**Evidently fallback:**
drift_job.py uses Evidently if installed, falls back to a plain HTML
summary otherwise. This means the drift job runs even in minimal
environments. Install evidently properly for rich interactive reports.

---

## Deferred to Phase 5
- Drift sensor in Dagster (auto-trigger retrain when drift_report detects PSI > threshold)
- Retrain cooldown enforcement in the sensor
- Great Expectations quality suite