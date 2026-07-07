"""Dagster asset definitions for VeloShelf ML pipeline (Phase 3).

Asset graph:
  windowed_features_parquet
    → detector_training_run
    → detector_metrics
    → detector_promotion

  windowed_features_parquet
    → forecaster_training_run
    → forecaster_metrics
    → forecaster_promotion

Assets are materialized by the scheduled jobs defined in schedules.py.
Drift-triggered retraining (Phase 5) will add a sensor that materializes
the training assets when drift exceeds threshold.
"""

import logging
import os
from pathlib import Path

from dagster import AssetExecutionContext, asset

logger = logging.getLogger(__name__)

FEATURES_PATH = Path(os.getenv("FEATURES_PATH", "data/features"))
LABEL_PATH    = Path(os.getenv("LABEL_PATH",    "data/anomaly_labels.jsonl"))


# ---------------------------------------------------------------------------
# Source asset — checks that features exist on disk
# ---------------------------------------------------------------------------

@asset(description="Windowed feature Parquet files written by the Flink job.")
def windowed_features_parquet(context: AssetExecutionContext) -> dict:
    files = list(FEATURES_PATH.glob("**/*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No Parquet feature files found in {FEATURES_PATH}. "
            "Run the generator + Flink job first."
        )
    total_rows = sum(
        __import__("pandas").read_parquet(f).shape[0] for f in files
    )
    context.log.info("Found %d feature files, %d total rows.", len(files), total_rows)
    return {"n_files": len(files), "n_rows": total_rows}


# ---------------------------------------------------------------------------
# Anomaly detector assets
# ---------------------------------------------------------------------------

@asset(
    deps=[windowed_features_parquet],
    description="Train Isolation Forest detector and log to MLflow.",
)
def detector_training_run(context: AssetExecutionContext) -> dict:
    import mlflow
    import mlflow.sklearn

    from ml.train_detector import load_features, train

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    model_name   = os.getenv("DETECTOR_MODEL_NAME", "veloshelf-anomaly-detector")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("veloshelf-anomaly-detector")

    df = load_features(FEATURES_PATH)
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        model, metrics, _ = train(df)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, artifact_path="model",
                                 skops_trusted_types=["sklearn.ensemble._iforest.IsolationForest"])

    context.log.info("Detector run_id=%s metrics=%s", run_id, metrics)
    return {"run_id": run_id, "metrics": metrics, "model_name": model_name}


@asset(
    description="Promote detector to Production if it beats the incumbent.",
)
def detector_promotion(
    context: AssetExecutionContext,
    detector_training_run: dict,
) -> dict:
    from ml.promote import promote_if_better

    promoted = promote_if_better(
        model_name=detector_training_run["model_name"],
        run_id=detector_training_run["run_id"],
        new_metrics=detector_training_run["metrics"],
        primary_metric="f1",
        higher_is_better=True,
    )
    context.log.info("Detector promotion: %s", "PROMOTED" if promoted else "SKIPPED")
    return {"promoted": promoted}


# ---------------------------------------------------------------------------
# Demand forecaster assets
# ---------------------------------------------------------------------------

@asset(
    deps=[windowed_features_parquet],
    description="Train XGBoost demand forecaster and log to MLflow.",
)
def forecaster_training_run(context: AssetExecutionContext) -> dict:
    import mlflow
    import mlflow.xgboost

    from ml.train_forecast import load_features, train

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    model_name   = os.getenv("FORECASTER_MODEL_NAME", "veloshelf-demand-forecaster")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("veloshelf-demand-forecaster")

    df = load_features(FEATURES_PATH)
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        model, store_enc, sku_enc, metrics = train(df)
        mlflow.log_metrics(metrics)
        mlflow.xgboost.log_model(model, artifact_path="model")

    context.log.info("Forecaster run_id=%s metrics=%s", run_id, metrics)
    return {"run_id": run_id, "metrics": metrics, "model_name": model_name}


@asset(
    description="Promote forecaster to Production if it beats the incumbent.",
)
def forecaster_promotion(
    context: AssetExecutionContext,
    forecaster_training_run: dict,
) -> dict:
    from ml.promote import promote_if_better

    promoted = promote_if_better(
        model_name=forecaster_training_run["model_name"],
        run_id=forecaster_training_run["run_id"],
        new_metrics=forecaster_training_run["metrics"],
        primary_metric="mae",
        higher_is_better=False,
    )
    context.log.info("Forecaster promotion: %s", "PROMOTED" if promoted else "SKIPPED")
    return {"promoted": promoted}