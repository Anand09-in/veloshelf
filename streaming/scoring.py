"""Scorer for VeloShelf streaming pipeline (Phase 3 — ML hot-swap).

Scoring strategy (in priority order):
  1. If the ML detector model is loaded and ready → use it.
  2. Otherwise fall back to the rule-based thresholds from Phase 2.

The fallback ensures the pipeline stays end-to-end runnable before
the first model is trained. Once a Production model exists in the
MLflow registry, the HotSwapModelLoader picks it up within one poll
interval (default 5 min) without a Flink job restart.

make_feature_row() and score_features() preserve their Phase 2
signatures so job.py needs no changes.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rule-based thresholds (Phase 2 fallback)
# ---------------------------------------------------------------------------

STOCKOUT_REORDER_RATIO   = 1.0
SURGE_MOMENTUM_THRESHOLD = 2.5

# ---------------------------------------------------------------------------
# Lazy ML model loader (imported only when available)
# ---------------------------------------------------------------------------

_detector_loader: Any = None


def _get_detector_loader() -> Any:
    global _detector_loader  # noqa: PLW0603
    if _detector_loader is None:
        try:
            from ml.model_loader import DETECTOR_MODEL_NAME, HotSwapModelLoader
            _detector_loader = HotSwapModelLoader(DETECTOR_MODEL_NAME)
            _detector_loader.start_polling()
            logger.info("ML detector loader initialised.")
        except Exception as e:
            logger.warning("Could not initialise ML loader: %s — using rule-based fallback.", e)
            _detector_loader = False   # sentinel: don't retry
    return _detector_loader if _detector_loader else None


# ---------------------------------------------------------------------------
# Feature row builder (unchanged from Phase 2)
# ---------------------------------------------------------------------------

def make_feature_row(
    store_id: str,
    sku_id: str,
    window_start: str,
    window_end: str,
    order_count: int,
    total_units: int,
    depletion_units: int,
    on_hand_latest: int,
    short_rate: float,
    long_rate: float,
) -> dict[str, Any]:
    momentum = short_rate / max(long_rate, 0.01)
    return {
        "store_id":        store_id,
        "sku_id":          sku_id,
        "window_start":    window_start,
        "window_end":      window_end,
        "order_rate":      round(short_rate, 4),
        "depletion_vel":   round(depletion_units / 1.0, 4),
        "demand_momentum": round(momentum, 4),
        "on_hand_est":     on_hand_latest,
        "updated_at":      datetime.now(tz=timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Scorer
# ---------------------------------------------------------------------------

def score_features(
    feature_row: dict[str, Any],
    reorder_point: int,
) -> list[dict[str, Any]]:
    """Score a feature row and return zero or more alert dicts.

    Tries ML model first; falls back to rules if unavailable.
    """
    loader = _get_detector_loader()

    if loader is not None and loader.is_ready():
        return _score_ml(feature_row, reorder_point, loader)
    return _score_rules(feature_row, reorder_point)


# ---------------------------------------------------------------------------
# ML scoring path
# ---------------------------------------------------------------------------

def _score_ml(
    feature_row: dict[str, Any],
    reorder_point: int,
    loader: Any,
) -> list[dict[str, Any]]:
    """Score using the Isolation Forest from the MLflow registry."""
    try:
        from ml.features import build_online_anomaly_features
        model = loader.get_model()
        if model is None:
            return _score_rules(feature_row, reorder_point)

        X = build_online_anomaly_features(feature_row)
        prediction = model.predict(X)[0]   # -1 = anomaly, 1 = normal

        if prediction == 1:
            return []   # model says normal

        # Anomaly detected — classify type by dominant signal
        on_hand  = feature_row.get("on_hand_est", 0)
        momentum = feature_row.get("demand_momentum", 1.0)
        now      = datetime.now(tz=timezone.utc).isoformat()
        store_id = feature_row["store_id"]
        sku_id   = feature_row["sku_id"]

        alerts: list[dict[str, Any]] = []

        if momentum >= SURGE_MOMENTUM_THRESHOLD:
            alerts.append({
                "alert_id":     str(uuid.uuid4()),
                "alert_type":   "surge",
                "store_id":     store_id,
                "sku_id":       sku_id,
                "triggered_at": now,
                "metric_value": float(momentum),
                "threshold":    SURGE_MOMENTUM_THRESHOLD,
                "resolved":     False,
            })

        if on_hand <= reorder_point * STOCKOUT_REORDER_RATIO:
            alerts.append({
                "alert_id":     str(uuid.uuid4()),
                "alert_type":   "stockout_risk",
                "store_id":     store_id,
                "sku_id":       sku_id,
                "triggered_at": now,
                "metric_value": float(on_hand),
                "threshold":    float(reorder_point),
                "resolved":     False,
            })

        return alerts

    except Exception as e:
        logger.warning("ML scoring failed (%s) — falling back to rules.", e)
        return _score_rules(feature_row, reorder_point)


# ---------------------------------------------------------------------------
# Rule-based fallback (Phase 2, unchanged)
# ---------------------------------------------------------------------------

def _score_rules(
    feature_row: dict[str, Any],
    reorder_point: int,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    now      = datetime.now(tz=timezone.utc).isoformat()
    on_hand  = feature_row.get("on_hand_est", 0)
    momentum = feature_row.get("demand_momentum", 1.0)
    store_id = feature_row["store_id"]
    sku_id   = feature_row["sku_id"]

    stockout_threshold = reorder_point * STOCKOUT_REORDER_RATIO
    if on_hand <= stockout_threshold:
        alerts.append({
            "alert_id":     str(uuid.uuid4()),
            "alert_type":   "stockout_risk",
            "store_id":     store_id,
            "sku_id":       sku_id,
            "triggered_at": now,
            "metric_value": float(on_hand),
            "threshold":    float(stockout_threshold),
            "resolved":     False,
        })

    if momentum >= SURGE_MOMENTUM_THRESHOLD:
        alerts.append({
            "alert_id":     str(uuid.uuid4()),
            "alert_type":   "surge",
            "store_id":     store_id,
            "sku_id":       sku_id,
            "triggered_at": now,
            "metric_value": float(momentum),
            "threshold":    SURGE_MOMENTUM_THRESHOLD,
            "resolved":     False,
        })

    return alerts
