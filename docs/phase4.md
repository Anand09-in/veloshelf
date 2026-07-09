# Phase 4 — Observability

Three complementary observability surfaces: Prometheus + Grafana for live pipeline health (ops view), Evidently for deep drift analysis (data science view), and Streamlit for actionable inventory intelligence (business view).

---

## Files

| File | Role |
|---|---|
| `observability/drift_job.py` | Evidently: PSI + KS + JS per feature, HTML report → `data/reports/` |
| `observability/metrics_exporter.py` | Prometheus gauges: freshness lag, drift PSI, MAE/F1, alert counts |
| `observability/retrain_trigger.py` | Threshold check + cooldown sentinel logic (used by Phase 5 sensor) |
| `observability/prometheus/prometheus.yml` | Scrape config: targets metrics-exporter at `metrics-exporter:8000` |
| `observability/prometheus/alerts.yml` | Alerting rules: high freshness lag, persistent drift, no alerts in 1h |
| `serving/grafana/datasource.yml` | Prometheus datasource provisioning (auto-loaded at Grafana startup) |
| `serving/grafana/dashboard.yml` | Dashboard provider config (watches provisioning directory for JSON files) |
| `serving/grafana/veloshelf_dashboard.json` | 4-section dashboard with stat panels, time-series, and model performance |
| `serving/streamlit_app.py` | Business dashboard: stockout risk, surge alerts, velocity, drift status |

---

## Prometheus metrics — `observability/metrics_exporter.py`

A long-running Python process exposing metrics at `:8000/metrics` via `prometheus_client`. Background threads poll Postgres and MLflow on configurable intervals.

**Pipeline health gauges:**

| Metric | Description |
|---|---|
| `veloshelf_feature_freshness_lag_seconds` | Seconds since the most recent `window_end` in `windowed_features` |
| `veloshelf_active_alerts_total` | Count of alerts in the last 5 minutes, labelled by `alert_type` |
| `veloshelf_windowed_feature_rows_total` | Total row count in `windowed_features` |

**Drift gauges** (written by `drift_job.py` via `push_to_gateway` or direct gauge set):

| Metric | Description |
|---|---|
| `veloshelf_drift_psi{feature}` | Population Stability Index per feature |
| `veloshelf_drift_ks{feature}` | Kolmogorov-Smirnov statistic per feature |
| `veloshelf_drift_js{feature}` | Jensen-Shannon divergence per feature |
| `veloshelf_drift_detected` | 1 if any feature has PSI > 0.25, else 0 |

**Model performance gauges** (polled from MLflow Production run metrics):

| Metric | Description |
|---|---|
| `veloshelf_forecaster_mae` | MAE of the current Production forecaster |
| `veloshelf_detector_f1` | F1 of the current Production detector |

---

## Grafana dashboard — auto-provisioned

Grafana loads provisioning config at startup from `/etc/grafana/provisioning/`. Two files are mounted read-only from the repo:

- `serving/grafana/datasource.yml` → `/etc/grafana/provisioning/datasources/datasource.yml` — registers the Prometheus datasource pointing at `http://prometheus:9090`
- `serving/grafana/dashboard.yml` → `/etc/grafana/provisioning/dashboards/dashboard.yml` — tells Grafana to watch `/etc/grafana/provisioning/dashboards/` for JSON files, with `updateIntervalSeconds: 30`
- `serving/grafana/veloshelf_dashboard.json` → `/etc/grafana/provisioning/dashboards/veloshelf_dashboard.json` — the dashboard definition

On container start, the dashboard is immediately available. No manual import needed. Changes to the JSON file are picked up within 30 seconds without a restart (`allowUiUpdates: true` lets you also edit in the UI and export back).

### Dashboard sections

**Pipeline Health** (stat panels, top row)
- Feature freshness lag (seconds since last window close)
- Drift detected (yes/no)
- Active alerts (count)
- Max PSI (worst feature drift score)

**Drift Metrics** (time-series)
- PSI, KS, JS per feature over the last 24h

**Pipeline Activity** (time-series)
- Freshness lag over time (SLA line at 120s)
- Alert count over time (stockout vs. surge)

**Model Performance** (time-series)
- Forecaster MAE over model versions
- Detector F1 over model versions

---

## Evidently drift job — `observability/drift_job.py`

Runs as a Dagster asset (`drift_report`) on the 2h schedule and can also be invoked directly:
```bash
python -m observability.drift_job
```

**Reference vs. current window split:**
- Reference: feature rows from the 24h window ending 1h ago
- Current: feature rows from the last 1h

Rolling reference is more appropriate for quick-commerce than a fixed baseline — demand is intrinsically time-varying (lunch peaks, weekends). A fixed baseline would permanently "drift" as the business grows.

**Drift metrics computed per feature** (order_rate, depletion_velocity, demand_momentum, on_hand_est):

| Metric | What it tests | Limitation |
|---|---|---|
| PSI | Overall shape shift; industry standard | Not a statistical test; sensitive to binning |
| KS statistic | Formal test on CDF; gives p-value | Only tests CDF; misses variance changes |
| JS divergence | Symmetric, bounded [0,1]; works for discrete | Less interpretable threshold than PSI |

PSI thresholds: < 0.1 stable · 0.1–0.25 moderate shift · > 0.25 significant shift (triggers retrain sensor).

Output:
- Summary dict `{feature: {psi, ks, js, drifted}, any_drift: bool}` — stored as Dagster asset metadata
- Evidently HTML report → `data/reports/drift_YYYYMMDD_HHMMSS.html`

---

## Streamlit dashboard — `serving/streamlit_app.py`

Business-facing view that reads directly from Postgres. Auto-refreshes every 30 seconds.

**Panels:**
- **Pipeline status** — freshness lag badge (green < 60s, yellow < 120s, red ≥ 120s)
- **Stockout-risk SKUs** — sorted by `depletion_velocity DESC`, showing estimated time to empty
- **Active surge alerts** — alert log from the last 15 minutes, labelled by severity
- **Per-store velocity heatmap** — order_rate per store_id × category, current window
- **Store health summary** — aggregated freshness and alert count per store
- **Latest drift report link** — links to the most recent HTML report in `data/reports/`

Credentials: none. The app reads `POSTGRES_DSN` from environment. On AWS, that DSN points at the RDS endpoint.

---

## Design — why three surfaces

**Grafana (ops / engineering):** time-series, pipeline health, SLAs, model metrics. Prometheus pull model means adding a new metric is a one-line gauge registration — no pipeline changes. Designed for on-call engineers who need to see what's happening right now.

**Evidently (data science):** deep distributional analysis per feature. HTML reports are shareable artefacts, not just dashboards. The interactive distribution comparisons reveal *why* PSI is elevated, not just *that* it is.

**Streamlit (business):** no Prometheus knowledge required. Inventory managers read "SKU_042 — 4 min to empty at current depletion rate" directly. The gap between ops tooling and business intelligence is real; bridging it with Streamlit is a portfolio differentiator.
