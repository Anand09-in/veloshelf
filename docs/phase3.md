# Phase 3 — ML Layer (XGBoost + Isolation Forest + MLflow + Hot-swap)

> Goal: offline training jobs that read windowed features, train two models,
> log to MLflow with a validation gate, and promote to Production.
> The Flink online scorer hot-swaps to the Production model automatically.

**Definition of done:**
- `make test` passes all Phase 3 tests.
- `python -m ml.train_detector` completes and logs to MLflow.
- `python -m ml.train_forecast` completes and logs to MLflow.
- Both models appear in MLflow UI under the Registered Models tab.
- After generator + Flink run, Postgres `windowed_features` has rows.
- `ml/train_detector.py` reads those features and produces a promotion.
- Dagster UI shows the full asset graph (5 assets, 2 schedules).
- `streaming/scoring.py` uses ML model once it's in Production.

---

## Task list

### New files
- [x] `ml/__init__.py`
- [x] `ml/features.py`         — shared feature engineering (lags, z-scores, time feats)
- [x] `ml/evaluate.py`         — ForecastEvaluator (MAE/RMSE/MAPE) + DetectorEvaluator (P/R/F1)
- [x] `ml/promote.py`          — validation gate + cooldown + MLflow registry promotion
- [x] `ml/model_loader.py`     — hot-swap loader polling MLflow registry every 5 min
- [x] `ml/train_detector.py`   — Isolation Forest training job
- [x] `ml/train_forecast.py`   — XGBoost demand forecaster training job
- [x] `orchestration/assets.py`   — Dagster asset graph (5 assets)
- [x] `orchestration/schedules.py` — 2 schedules (6h cadence, offset)
- [x] `tests/test_ml.py`       — unit tests (features, evaluate, promote logic)

### Updated files
- [x] `streaming/scoring.py`   — ML hot-swap + rule-based fallback
- [x] `orchestration/definitions.py` — wires real assets + schedules
- [x] `pyproject.toml`         — added xgboost, scikit-learn, pyarrow
- [x] `requirements.txt`       — same

### Verification steps
- [ ] `make test`  — all tests pass (smoke + phase1 + phase2 + phase3)
- [ ] `make lint`  — ruff clean
- [ ] Install new deps: `make setup`
- [ ] Run generator (fast mode) + Flink job to produce feature Parquet
- [ ] `python -m ml.train_detector` — completes, logs to MLflow
- [ ] `python -m ml.train_forecast` — completes, logs to MLflow
- [ ] MLflow UI (http://localhost:5000) shows both experiments + registered models
- [ ] Check Dagster UI (http://localhost:3000) shows 5 assets in graph
- [ ] Run generator again; confirm Flink scorer uses ML model (check logs for "Hot-swapped")

---

## Step-by-step verification

### 1 — Tests and lint
```bash
make setup      # installs xgboost, scikit-learn, pyarrow
make test       # expect all tests pass
make lint
```

### 2 — Produce feature data (need Flink running)
```bash
make up
make flink-submit
python -m generator.producer --mode fast   # run ~2 min to fill features
```

Check features landed in `data/features/`:
```bash
ls data/features/
```

### 3 — Train detector
```bash
python -m ml.train_detector
```
Expected output:
```
INFO  veloshelf.train_detector | Loaded N feature rows from M files.
INFO  veloshelf.train_detector | Training Isolation Forest ...
INFO  veloshelf.train_detector | Detector metrics: {precision: ..., recall: ..., f1: ...}
INFO  veloshelf.train_detector | Promotion result: PROMOTED
```

### 4 — Train forecaster
```bash
python -m ml.train_forecast
```
Expected output:
```
INFO  veloshelf.train_forecast | Training XGBoost ...
INFO  veloshelf.train_forecast | Forecaster metrics: {mae: ..., rmse: ..., mape: ...}
INFO  veloshelf.train_forecast | Promotion result: PROMOTED
```

### 5 — Verify MLflow registry
Open http://localhost:5000 → Models tab.
You should see:
- `veloshelf-anomaly-detector`  — version 1, stage: Production
- `veloshelf-demand-forecaster` — version 1, stage: Production

### 6 — Verify hot-swap in Flink logs
After promotion, the next poll cycle (within 5 min) should log:
```
INFO veloshelf.ml.model_loader | Hot-swapped veloshelf-anomaly-detector → version 1
```
Check Flink taskmanager logs:
```bash
docker-compose logs flink-taskmanager | grep "Hot-swapped"
```

### 7 — Verify Dagster asset graph
Open http://localhost:3000 → Assets.
You should see the 5 assets connected as a DAG:
```
windowed_features_parquet
  ├── detector_training_run → detector_promotion
  └── forecaster_training_run → forecaster_promotion
```

---

## Design notes (for interviews)

**Why Isolation Forest for anomaly detection?**
Unsupervised, handles multivariate features (order_rate + momentum +
depletion_vel together), no labelled training data required — you train
on "normal" windows only. Standard algorithm interviewers expect for
streaming anomaly detection.

**Why XGBoost over Prophet for forecasting?**
Faster to train, easier to hot-swap (sklearn-compatible API), better
at incorporating non-time features (store_id, SKU popularity). Prophet
is harder to register + serve in real time.

**Training/serving parity — why does it matter?**
features.py is the single source of truth for feature computation,
used by both the offline training jobs AND the online Flink scorer.
If they diverge, the model sees different inputs than it trained on —
the most common source of silent production regressions.

**Validation gate — why two guardrails?**
1. Metric gate: only promote if the new model beats the incumbent.
   Drift ≠ better model; a retrain on drifted data can produce a worse model.
2. Cooldown: prevent retrain storms where a noisy drift signal triggers
   multiple rapid promotions before the system stabilises.

**Ground-truth labels — the synthetic data advantage:**
Because we inject anomalies deliberately and log their ground truth in
`data/anomaly_labels.jsonl`, we can compute real precision/recall/F1
against known positives. Most real streaming projects can't do this
without expensive human labelling. This is a significant portfolio differentiator.

---

## Feature storage note
Features currently written to `data/features/` as local Parquet.
Phase 6 switches `FEATURES_PATH` to `s3://veloshelf-features/` — one
env var change, zero code changes.

## Deferred to Phase 4
- Evidently drift reports
- Prometheus metrics for model health
- Grafana dashboard
- Streamlit business dashboard