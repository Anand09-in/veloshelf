"""VeloShelf — Demand forecaster training job (Phase 3).

Trains a per-SKU XGBoost regressor to predict short-horizon order_rate
(next window). One model is trained across all SKUs using SKU/store ID
label-encoded as features — more practical than one model per SKU at
this scale.

Run:
    python -m ml.train_forecast

Environment variables:
    FEATURES_PATH           path to Parquet features dir (default: data/features)
    MLFLOW_TRACKING_URI     (default: http://localhost:5000)
    FORECASTER_MODEL_NAME   (default: veloshelf-demand-forecaster)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import mlflow
import mlflow.xgboost
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor

from ml.evaluate import ForecastEvaluator
from ml.features import (
    FORECAST_FEATURE_COLS,
    build_forecast_features,
)
from ml.promote import promote_if_better

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
logger = logging.getLogger("veloshelf.train_forecast")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FEATURES_PATH = Path(os.getenv("FEATURES_PATH", "data/features"))
TRACKING_URI  = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME    = os.getenv("FORECASTER_MODEL_NAME", "veloshelf-demand-forecaster")
EXPERIMENT    = "veloshelf-demand-forecaster"

# XGBoost hyperparams
N_ESTIMATORS       = int(os.getenv("XGB_N_ESTIMATORS",   "200"))
MAX_DEPTH          = int(os.getenv("XGB_MAX_DEPTH",       "4"))
LEARNING_RATE      = float(os.getenv("XGB_LR",            "0.05"))
SUBSAMPLE          = float(os.getenv("XGB_SUBSAMPLE",     "0.8"))
COLSAMPLE_BYTREE   = float(os.getenv("XGB_COLSAMPLE",     "0.8"))
RANDOM_STATE       = int(os.getenv("XGB_RANDOM_STATE",    "42"))
HOLDOUT_FRACTION   = float(os.getenv("HOLDOUT_FRACTION",  "0.2"))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_features(path: Path) -> pd.DataFrame:
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
# Feature preparation — adds SKU/store label encoding
# ---------------------------------------------------------------------------

EXTRA_COLS = ["store_enc", "sku_enc"]
ALL_FEATURE_COLS = FORECAST_FEATURE_COLS + EXTRA_COLS


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, LabelEncoder, LabelEncoder]:
    """Build all forecast features for all SKUs + encode store/sku IDs.

    Returns:
        (enriched_df, store_encoder, sku_encoder)
    """
    store_enc = LabelEncoder()
    sku_enc   = LabelEncoder()

    df = df.copy()
    df["store_enc"] = store_enc.fit_transform(df["store_id"].astype(str))
    df["sku_enc"]   = sku_enc.fit_transform(df["sku_id"].astype(str))

    # Build lag features per (store, SKU) group
    groups = []
    for (store_id, sku_id), grp in df.groupby(["store_id", "sku_id"]):
        enriched = build_forecast_features(grp.copy())
        if enriched.empty:
            continue
        # Re-attach encodings (may be lost after feature build)
        enriched["store_enc"] = store_enc.transform([store_id] * len(enriched))
        enriched["sku_enc"]   = sku_enc.transform([sku_id] * len(enriched))
        groups.append(enriched)

    if not groups:
        raise ValueError("No groups survived feature building — need more data.")

    full_df = pd.concat(groups, ignore_index=True)
    logger.info("Feature matrix shape after lag building: %s", full_df.shape)
    return full_df, store_enc, sku_enc


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train(df: pd.DataFrame) -> tuple[XGBRegressor, LabelEncoder, LabelEncoder, dict]:
    """Prepare features, train XGBoost, evaluate on holdout.

    Time-based split: last HOLDOUT_FRACTION of windows = holdout.
    """
    df, store_enc, sku_enc = prepare_features(df)

    # Sort by window_start for time-based split
    df = df.sort_values("window_start")
    split_idx = int(len(df) * (1 - HOLDOUT_FRACTION))
    train_df   = df.iloc[:split_idx]
    holdout_df = df.iloc[split_idx:]

    X_train, y_train = (
        train_df[ALL_FEATURE_COLS],
        train_df["order_rate"],
    )
    X_holdout, y_holdout = (
        holdout_df[ALL_FEATURE_COLS],
        holdout_df["order_rate"],
    )

    logger.info(
        "Training XGBoost | n_train=%d n_holdout=%d n_estimators=%d",
        len(X_train), len(X_holdout), N_ESTIMATORS,
    )

    model = XGBRegressor(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        learning_rate=LEARNING_RATE,
        subsample=SUBSAMPLE,
        colsample_bytree=COLSAMPLE_BYTREE,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbosity=0,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_holdout, y_holdout)],
        verbose=False,
    )

    y_pred = model.predict(X_holdout)
    metrics = ForecastEvaluator.evaluate(y_holdout.values, y_pred)
    logger.info("Forecaster metrics: %s", metrics)

    return model, store_enc, sku_enc, metrics


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
            "n_estimators": N_ESTIMATORS,
            "max_depth": MAX_DEPTH,
            "learning_rate": LEARNING_RATE,
            "subsample": SUBSAMPLE,
            "colsample_bytree": COLSAMPLE_BYTREE,
            "random_state": RANDOM_STATE,
            "holdout_fraction": HOLDOUT_FRACTION,
            "n_rows": len(df),
        })

        model, store_enc, sku_enc, metrics = train(df)

        mlflow.log_metrics(metrics)

        # Log model + encoders as artifacts
        mlflow.xgboost.log_model(
            xgb_model=model,
            artifact_path="model",
            registered_model_name=None,
        )
        import pickle
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            store_path = f"{tmp}/store_enc.pkl"
            sku_path   = f"{tmp}/sku_enc.pkl"
            with open(store_path, "wb") as f:
                pickle.dump(store_enc, f)
            with open(sku_path, "wb") as f:
                pickle.dump(sku_enc, f)
            mlflow.log_artifact(store_path, artifact_path="encoders")
            mlflow.log_artifact(sku_path,   artifact_path="encoders")

    # Promotion with validation gate
    promoted = promote_if_better(
        model_name=MODEL_NAME,
        run_id=run_id,
        new_metrics=metrics,
        primary_metric="mae",
        higher_is_better=False,
    )
    logger.info("Promotion result: %s", "PROMOTED" if promoted else "SKIPPED")


if __name__ == "__main__":
    main()