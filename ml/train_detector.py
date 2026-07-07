"""VeloShelf — Anomaly detector training job (Phase 3).

Trains an Isolation Forest on historical windowed features to detect
stockouts and demand surges. Uses only "normal" (non-anomaly) windows
for training, then scores all windows to generate predictions for
evaluation against ground-truth labels.

Run:
    python -m ml.train_detector

Environment variables:
    FEATURES_PATH         path to Parquet features dir (default: data/features)
    LABEL_PATH            path to ground-truth JSONL (default: data/anomaly_labels.jsonl)
    MLFLOW_TRACKING_URI   (default: http://localhost:5000)
    DETECTOR_MODEL_NAME   (default: veloshelf-anomaly-detector)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import IsolationForest

from ml.evaluate import DetectorEvaluator
from ml.features import build_anomaly_features, get_anomaly_X
from ml.promote import promote_if_better

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
logger = logging.getLogger("veloshelf.train_detector")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FEATURES_PATH = Path(os.getenv("FEATURES_PATH", "data/features"))
LABEL_PATH    = Path(os.getenv("LABEL_PATH",    "data/anomaly_labels.jsonl"))
TRACKING_URI  = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME    = os.getenv("DETECTOR_MODEL_NAME", "veloshelf-anomaly-detector")
EXPERIMENT    = "veloshelf-anomaly-detector"

# Isolation Forest hyperparams
CONTAMINATION     = float(os.getenv("IF_CONTAMINATION",  "0.05"))
N_ESTIMATORS      = int(os.getenv("IF_N_ESTIMATORS",     "100"))
MAX_SAMPLES       = os.getenv("IF_MAX_SAMPLES",           "auto")
RANDOM_STATE      = int(os.getenv("IF_RANDOM_STATE",      "42"))
HOLDOUT_FRACTION  = float(os.getenv("HOLDOUT_FRACTION",   "0.2"))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_features(path: Path) -> pd.DataFrame:
    """Load all Parquet feature files from a directory."""
    files = list(path.glob("**/*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No Parquet files found in {path}. "
            "Run the generator + Flink job to produce features first."
        )
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info("Loaded %d feature rows from %d files.", len(df), len(files))
    return df


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train(df: pd.DataFrame) -> tuple[IsolationForest, dict, list[dict]]:
    """Build features, train Isolation Forest, return (model, metrics, predictions).

    Training set: rows where is_injected_anomaly == False (normal behaviour).
    Evaluation: score ALL rows; compare against ground-truth labels.
    """
    df = build_anomaly_features(df)

    # Split train / holdout by time (last HOLDOUT_FRACTION of windows)
    df = df.sort_values("window_start")
    split_idx = int(len(df) * (1 - HOLDOUT_FRACTION))
    train_df  = df.iloc[:split_idx]
    holdout_df = df.iloc[split_idx:]

    # Train only on normal rows
    normal_mask = ~train_df.get("is_injected_anomaly", pd.Series(False, index=train_df.index))
    X_train = get_anomaly_X(train_df[normal_mask])

    logger.info(
        "Training Isolation Forest | n_train=%d n_estimators=%d contamination=%.2f",
        len(X_train), N_ESTIMATORS, CONTAMINATION,
    )

    max_samples: int | str = MAX_SAMPLES if MAX_SAMPLES == "auto" else int(MAX_SAMPLES)
    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        max_samples=max_samples,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_train)

    # Score holdout → anomaly predictions
    X_holdout = get_anomaly_X(holdout_df)
    model.decision_function(X_holdout)   # negative = more anomalous
    predictions_flag = model.predict(X_holdout)        # -1 = anomaly, 1 = normal

    # Convert model predictions to alert dicts for DetectorEvaluator
    # Heuristic: flag as surge if momentum high, stockout_risk if on_hand low
    alert_preds: list[dict] = []
    for _i, (flag, row) in enumerate(zip(predictions_flag, holdout_df.itertuples(), strict=False)):
        if flag == -1:
            momentum = row.demand_momentum
            alert_type = "surge" if momentum >= 2.0 else "stockout_risk"
            alert_preds.append({
                "alert_type": alert_type,
                "store_id": row.store_id,
                "sku_id": row.sku_id,
                "triggered_at": str(row.window_end),
            })

    evaluator = DetectorEvaluator(LABEL_PATH)
    metrics = evaluator.evaluate(alert_preds)
    logger.info("Detector metrics: %s", metrics)

    return model, metrics, alert_preds


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)

    logger.info("Loading features from %s", FEATURES_PATH)
    df = load_features(FEATURES_PATH)

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        logger.info("MLflow run: %s", run_id)

        # Log hyperparams
        mlflow.log_params({
            "contamination": CONTAMINATION,
            "n_estimators": N_ESTIMATORS,
            "max_samples": MAX_SAMPLES,
            "random_state": RANDOM_STATE,
            "holdout_fraction": HOLDOUT_FRACTION,
            "n_features": len(df),
        })

        model, metrics, _ = train(df)

        # Log metrics
        mlflow.log_metrics(metrics)

        # Log model
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=None,   # promote.py handles registration
            input_example=get_anomaly_X(
                build_anomaly_features(df.head(5))
            ).head(1),
        )

    # Promotion with validation gate
    promoted = promote_if_better(
        model_name=MODEL_NAME,
        run_id=run_id,
        new_metrics=metrics,
        primary_metric="f1",
        higher_is_better=True,
    )
    logger.info("Promotion result: %s", "PROMOTED" if promoted else "SKIPPED")


if __name__ == "__main__":
    main()