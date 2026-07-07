"""Model evaluation utilities for VeloShelf ML layer.

Two evaluators:
  1. ForecastEvaluator  — MAE, RMSE, MAPE for the XGBoost demand forecaster.
  2. DetectorEvaluator  — precision, recall, F1 for the Isolation Forest,
                          evaluated against the ground-truth JSONL labels
                          written by the anomaly injector (Phase 1).

Both return plain dicts so results can be logged directly to MLflow
with mlflow.log_metrics().
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Forecast evaluation
# ---------------------------------------------------------------------------

class ForecastEvaluator:
    """Evaluates point-forecast predictions against actuals."""

    @staticmethod
    def evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
        """Compute MAE, RMSE, and MAPE.

        Args:
            y_true: actual order_rate values.
            y_pred: predicted order_rate values.

        Returns:
            Dict with keys: mae, rmse, mape.
        """
        y_true = np.asarray(y_true, dtype=float)
        y_pred = np.asarray(y_pred, dtype=float)

        mae = float(np.mean(np.abs(y_true - y_pred)))
        rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

        # MAPE: skip rows where y_true == 0 to avoid division by zero
        mask = y_true != 0
        if mask.sum() == 0:
            mape = float("nan")
        else:
            mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)

        return {"mae": mae, "rmse": rmse, "mape": mape}

    @staticmethod
    def beats_incumbent(
        new_metrics: dict[str, float],
        incumbent_metrics: dict[str, float],
        primary: str = "mae",
    ) -> bool:
        """Return True if new model is strictly better on the primary metric."""
        return new_metrics[primary] < incumbent_metrics[primary]


# ---------------------------------------------------------------------------
# Detector evaluation
# ---------------------------------------------------------------------------

class DetectorEvaluator:
    """Evaluates anomaly detector predictions against ground-truth labels.

    Ground-truth labels are loaded from the JSONL file written by
    AnomalyInjector (Phase 1). Each record has: anomaly_type, store_id,
    sku_id, injected_at.

    Matching strategy: a prediction is a true positive if it fires within
    a configurable time window (default: 2 minutes) of an injected anomaly
    for the same (store_id, sku_id, anomaly_type).
    """

    def __init__(
        self,
        label_path: Path,
        match_window_s: float = 120.0,
    ) -> None:
        self._labels = self._load_labels(label_path)
        self._match_window_s = match_window_s

    @staticmethod
    def _load_labels(path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame(
                columns=["anomaly_type", "store_id", "sku_id", "injected_at"]
            )
        records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        df = pd.DataFrame(records)
        df["injected_at"] = pd.to_datetime(df["injected_at"], utc=True)
        return df

    def evaluate(
        self,
        predictions: list[dict],
    ) -> dict[str, float]:
        """Compute precision, recall, F1 for the detector.

        Args:
            predictions: list of alert dicts, each with:
                alert_type, store_id, sku_id, triggered_at.

        Returns:
            Dict with keys: precision, recall, f1, n_predictions, n_labels.
        """
        if self._labels.empty or not predictions:
            return {
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0,
                "n_predictions": len(predictions),
                "n_labels": len(self._labels),
            }

        pred_df = pd.DataFrame(predictions)
        pred_df["triggered_at"] = pd.to_datetime(pred_df["triggered_at"], utc=True)

        # Normalise alert_type → anomaly_type vocabulary
        type_map = {"stockout_risk": "stockout_risk", "surge": "surge"}
        pred_df["anomaly_type"] = pred_df["alert_type"].map(type_map)

        matched_labels: set[int] = set()
        matched_preds: set[int] = set()

        for pi, pred in pred_df.iterrows():
            for li, label in self._labels.iterrows():
                if label["anomaly_type"] != pred["anomaly_type"]:
                    continue
                if label["store_id"] != pred["store_id"]:
                    continue
                if label["sku_id"] != pred["sku_id"]:
                    continue
                delta = abs(
                    (pred["triggered_at"] - label["injected_at"]).total_seconds()
                )
                if delta <= self._match_window_s:
                    matched_labels.add(li)
                    matched_preds.add(pi)
                    break

        tp = len(matched_preds)
        n_pred = len(pred_df)
        n_labels = len(self._labels)

        precision = tp / n_pred if n_pred > 0 else 0.0
        recall = tp / n_labels if n_labels > 0 else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        return {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "n_predictions": n_pred,
            "n_labels": n_labels,
        }

    @staticmethod
    def beats_incumbent(
        new_metrics: dict[str, float],
        incumbent_metrics: dict[str, float],
        primary: str = "f1",
    ) -> bool:
        """Return True if new model is strictly better on the primary metric."""
        return new_metrics[primary] > incumbent_metrics[primary]