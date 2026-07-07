"""MLflow model registry promotion with validation gate.

Promotion policy:
  1. Train new model candidate → log to MLflow (stage: None / Staging).
  2. Evaluate candidate on a holdout set.
  3. Compare against the current Production model's logged metrics.
  4. Promote candidate to Production ONLY if it beats the incumbent
     on the primary metric (MAE for forecaster, F1 for detector).
  5. If no incumbent exists, promote unconditionally.
  6. Cooldown: do not promote more than once per COOLDOWN_MINUTES,
     even if metrics improve. Prevents retrain storms.

This module is called by the Dagster retrain asset (orchestration/assets.py).
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
COOLDOWN_MINUTES = int(os.getenv("PROMOTE_COOLDOWN_MINUTES", "30"))

# Sentinel file stores timestamp of last promotion per model name.
_COOLDOWN_DIR = Path(".mlflow_cooldowns")


# ---------------------------------------------------------------------------
# Cooldown helpers
# ---------------------------------------------------------------------------

def _cooldown_path(model_name: str) -> Path:
    _COOLDOWN_DIR.mkdir(exist_ok=True)
    return _COOLDOWN_DIR / f"{model_name}.last_promoted"


def _in_cooldown(model_name: str) -> bool:
    path = _cooldown_path(model_name)
    if not path.exists():
        return False
    last = float(path.read_text().strip())
    elapsed_min = (time.time() - last) / 60.0
    if elapsed_min < COOLDOWN_MINUTES:
        logger.info(
            "Cooldown active for %s: %.1f / %d minutes elapsed.",
            model_name, elapsed_min, COOLDOWN_MINUTES,
        )
        return True
    return False


def _record_promotion(model_name: str) -> None:
    _cooldown_path(model_name).write_text(str(time.time()))


# ---------------------------------------------------------------------------
# Incumbent metric retrieval
# ---------------------------------------------------------------------------

def get_incumbent_metrics(
    model_name: str,
    primary_metric: str,
) -> dict[str, float] | None:
    """Fetch the logged metrics of the current Production model version.

    Returns None if no Production version exists.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    try:
        versions = client.get_latest_versions(model_name, stages=["Production"])
    except Exception:
        return None

    if not versions:
        return None

    run_id = versions[0].run_id
    run = client.get_run(run_id)
    metrics = run.data.metrics
    if primary_metric not in metrics:
        return None
    return dict(metrics)


# ---------------------------------------------------------------------------
# Promotion
# ---------------------------------------------------------------------------

def promote_if_better(
    model_name: str,
    run_id: str,
    new_metrics: dict[str, float],
    primary_metric: str,
    higher_is_better: bool = False,
) -> bool:
    """Promote run_id to Production if it beats the incumbent.

    Args:
        model_name:      MLflow registered model name.
        run_id:          MLflow run ID of the candidate model.
        new_metrics:     Evaluation metrics of the candidate.
        primary_metric:  Metric key to compare (e.g. "mae", "f1").
        higher_is_better: True for F1/recall, False for MAE/RMSE.

    Returns:
        True if the model was promoted, False otherwise.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    # --- Cooldown check ---
    if _in_cooldown(model_name):
        logger.info("Skipping promotion for %s — cooldown active.", model_name)
        return False

    # --- Get incumbent ---
    incumbent = get_incumbent_metrics(model_name, primary_metric)

    if incumbent is None:
        logger.info("No incumbent for %s — promoting unconditionally.", model_name)
        should_promote = True
    else:
        new_val = new_metrics.get(primary_metric)
        inc_val = incumbent.get(primary_metric)
        if new_val is None or inc_val is None:
            logger.warning(
                "Cannot compare: metric '%s' missing from new or incumbent metrics.",
                primary_metric,
            )
            return False

        if higher_is_better:
            should_promote = new_val > inc_val
        else:
            should_promote = new_val < inc_val

        logger.info(
            "Promotion check | model=%s metric=%s new=%.4f incumbent=%.4f promote=%s",
            model_name, primary_metric, new_val, inc_val, should_promote,
        )

    if not should_promote:
        logger.info("Candidate does not beat incumbent. Skipping promotion.")
        return False

    # --- Register and promote ---
    model_uri = f"runs:/{run_id}/model"
    registered = mlflow.register_model(model_uri, model_name)
    version = registered.version

    # Archive any existing Production versions
    try:
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
        for v in prod_versions:
            client.transition_model_version_stage(
                name=model_name,
                version=v.version,
                stage="Archived",
                archive_existing_versions=False,
            )
    except Exception as e:
        logger.warning("Could not archive old Production versions: %s", e)

    # Promote new version
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage="Production",
    )

    _record_promotion(model_name)
    logger.info(
        "Promoted %s v%s to Production | %s=%.4f",
        model_name, version, primary_metric, new_metrics[primary_metric],
    )
    return True