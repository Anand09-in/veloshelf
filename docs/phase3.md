# Phase 3 — ML Layer

Two offline training jobs (Isolation Forest for anomaly detection, XGBoost for demand forecasting), an MLflow model registry with a validation gate, and a hot-swap loader that updates the Flink scorer without a job restart. Dagster orchestrates the asset graph with two training schedules.

---

## Files

| File | Role |
|---|---|
| `ml/export_features.py` | Dumps `windowed_features` from Postgres → `data/features/features.parquet` |
| `ml/features.py` | Shared feature engineering: lag features, z-scores, time-of-day encoding |
| `ml/train_detector.py` | Isolation Forest training job: load → engineer → train → evaluate → promote |
| `ml/train_forecast.py` | XGBoost forecaster training job: same pipeline, regression target |
| `ml/evaluate.py` | `ForecastEvaluator` (MAE/RMSE/MAPE) + `DetectorEvaluator` (precision/recall/F1 vs. labels) |
| `ml/promote.py` | Validation gate + MLflow registry promotion to `Production` alias |
| `ml/model_loader.py` | `HotSwapModelLoader` — polls MLflow every 5 min, swaps model in Flink |
| `orchestration/assets.py` | 5 Dagster assets: features parquet, detector train+promote, forecaster train+promote |
| `orchestration/schedules.py` | `detector_retrain` (every 6h), `forecaster_retrain` (every 6h+30m offset) |

---

## Feature export — `ml/export_features.py`

Runs `SELECT * FROM windowed_features ORDER BY window_start` via psycopg, converts to a pandas DataFrame, and writes to `data/features/features.parquet` using pyarrow. Must be run before any training job — this is the training corpus.

In the Dagster asset graph, `windowed_features_parquet` materialises this file. All downstream training assets depend on it.

---

## Feature engineering — `ml/features.py`

`engineer_features(df)` is the **single source of truth** used by both offline training and the Flink online scorer. This guarantees training/serving parity — the model sees the same feature representation it trained on.

Engineered features:
- **Lag features**: `order_rate_lag1`, `order_rate_lag3` (previous 1 and 3 windows for the same sku+store)
- **Z-scores**: `order_rate_zscore` = (current − rolling mean) / rolling std, computed per sku+store
- **Time encoding**: `hour_of_day`, `day_of_week`, `is_weekend` extracted from `window_start`
- **Interaction**: `depletion_momentum` = `depletion_velocity × demand_momentum`

---

## Anomaly detector — `ml/train_detector.py`

**Model**: `sklearn.ensemble.IsolationForest` with `contamination=0.05` (5% expected anomaly rate, matching the generator's injection frequency).

**Training input**: the 6 engineered feature columns — `order_rate`, `depletion_velocity`, `demand_momentum`, `on_hand_est`, `order_rate_zscore`, `depletion_momentum`. IsolationForest is unsupervised, so no labels are used during training — it learns the shape of normal windows.

**Evaluation**: `DetectorEvaluator` in `ml/evaluate.py` joins model predictions (anomaly score < threshold → positive) against `data/seeds/anomaly_labels.jsonl` on (event_time window, sku_id, store_id). Reports precision, recall, and F1 against ground-truth injected anomalies.

**Why IsolationForest?**
- Unsupervised — no labelled training data required
- Naturally handles multivariate features (the combination of high `order_rate` AND low `on_hand_est` is more anomalous than either alone)
- Fast inference (O(log n) per sample) — suitable for per-window online scoring
- Industry-standard choice interviewers recognise

---

## Demand forecaster — `ml/train_forecast.py`

**Model**: `xgboost.XGBRegressor` with `objective='reg:absoluteerror'` (MAE loss), `n_estimators=200`, `learning_rate=0.05`, `max_depth=6`.

**Target**: `order_rate` at the next window — a 1-window-ahead forecast. The model learns to predict demand from recent rate, time features, and lag features.

**Evaluation**: `ForecastEvaluator` in `ml/evaluate.py` computes MAE, RMSE, and MAPE on a held-out 20% time split (last 20% of windows by `window_start` — temporal split to avoid leakage).

**Why XGBoost over Prophet?**
- Prophet is a univariate time-series model — it can't incorporate store_id, SKU category, or the engineered lag/z-score features
- XGBoost handles the tabular feature matrix directly
- sklearn-compatible API makes MLflow registration and serving straightforward
- Faster training — minutes vs. Prophet's per-series fitting overhead

---

## Validation gate and promotion — `ml/promote.py`

`promote_if_better(model, run_id, metrics, model_name)` compares the new run's primary metric against the current `Production` model:

1. Query MLflow for the current `Production` alias version and its logged metrics
2. If no Production model exists → promote unconditionally (first run)
3. If new model beats incumbent (lower MAE for forecaster, higher F1 for detector) → register new version, set `Production` alias
4. If new model is worse → log a "rejected" tag, do not promote

The gate enforces that drift does not equal better model. A distribution shift can cause a retrain that produces a worse model on the new data — the gate prevents silent degradation.

---

## Hot-swap model loader — `ml/model_loader.py`

`HotSwapModelLoader` runs in the Flink taskmanager process. On each call to `get_model()`:

1. Check elapsed time since last poll. If < `MODEL_POLL_INTERVAL_S` (300s), return the cached model.
2. Call `mlflow.sklearn.load_model(f"models:/{model_name}@Production")` (or xgboost flavor).
3. If the version has changed, replace the in-memory model and log "Hot-swapped → version N".
4. Return the current model.

During the 5-minute poll interval, the old model continues scoring. There is no downtime window and no Flink job restart required.

---

## Dagster asset graph

```
windowed_features_parquet
    ├── detector_training_run
    │       └── detector_promotion
    └── forecaster_training_run
            └── forecaster_promotion
```

Each asset is defined in `orchestration/assets.py` using `@asset`. Dependencies are expressed by function signature — `detector_training_run(windowed_features_parquet)` declares that it needs the Parquet export to be fresh before it can run.

Schedules in `orchestration/schedules.py`:
- `detector_retrain_schedule` — every 6 hours, builds `detector_training_run → detector_promotion`
- `forecaster_retrain_schedule` — every 6 hours + 30 minutes offset (avoids competing with detector for Postgres reads)

The 30-minute offset also means the two training jobs don't stack in MLflow simultaneously, keeping experiment UI legible.

---

## MLflow model registry

Both models are logged under named experiments (`veloshelf-detector`, `veloshelf-forecaster`). Each training run logs:
- Parameters: model hyperparameters
- Metrics: evaluation results (MAE/RMSE/MAPE or P/R/F1)
- Artefact: serialised model (sklearn or xgboost MLflow flavor)

On promotion, the run is registered under the model name and the `Production` alias is set on the new version. The Flink loader uses the alias (`@Production`) rather than a version number — promotion is a single alias update, not a code change.
