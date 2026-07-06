"""Rule-based scorer for VeloShelf streaming pipeline (Phase 2).

Evaluates computed windowed features and emits alert dicts when thresholds
are breached. In Phase 3 this rule-based logic is REPLACED by the ML model's
online scoring — the rules act as a defensible baseline and keep the pipeline
end-to-end runnable before the model exists.

Thresholds are intentionally conservative so alerts fire during development
without needing extreme events.

Returns typed dicts (not dataclasses) so they serialise cleanly to JSON
for Kafka and Postgres without extra conversion.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Thresholds (overridable via env in Phase 3+)
# ---------------------------------------------------------------------------

STOCKOUT_REORDER_RATIO = 1.0   # alert when on_hand_est <= reorder_point * ratio
SURGE_MOMENTUM_THRESHOLD = 2.5 # alert when demand_momentum >= this value


# ---------------------------------------------------------------------------
# Scorer
# ---------------------------------------------------------------------------

def score_features(
    feature_row: dict[str, Any],
    reorder_point: int,
) -> list[dict[str, Any]]:
    """Evaluate a feature row and return zero or more alert dicts.

    Args:
        feature_row:   dict with keys matching windowed_features columns.
        reorder_point: SKU-specific reorder threshold (from dim_sku).

    Returns:
        List of alert dicts ready to insert into the alerts table / Kafka.
        Empty list if no thresholds breached.
    """
    alerts: list[dict[str, Any]] = []
    now = datetime.now(tz=timezone.utc).isoformat()

    on_hand = feature_row.get("on_hand_est", 0)
    momentum = feature_row.get("demand_momentum", 1.0)
    store_id = feature_row["store_id"]
    sku_id = feature_row["sku_id"]

    # --- Stockout risk ---
    stockout_threshold = reorder_point * STOCKOUT_REORDER_RATIO
    if on_hand <= stockout_threshold:
        alerts.append({
            "alert_id": str(uuid.uuid4()),
            "alert_type": "stockout_risk",
            "store_id": store_id,
            "sku_id": sku_id,
            "triggered_at": now,
            "metric_value": float(on_hand),
            "threshold": float(stockout_threshold),
            "resolved": False,
        })

    # --- Demand surge ---
    if momentum >= SURGE_MOMENTUM_THRESHOLD:
        alerts.append({
            "alert_id": str(uuid.uuid4()),
            "alert_type": "surge",
            "store_id": store_id,
            "sku_id": sku_id,
            "triggered_at": now,
            "metric_value": float(momentum),
            "threshold": float(SURGE_MOMENTUM_THRESHOLD),
            "resolved": False,
        })

    return alerts


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
    """Build a feature row dict from raw window aggregates.

    Args:
        store_id, sku_id:       dimension keys.
        window_start/end:       ISO-8601 strings.
        order_count:            number of orders in the 1-min tumbling window.
        total_units:            total units ordered in the window.
        depletion_units:        total units depleted from inventory in the window.
        on_hand_latest:         latest on_hand_after value seen in the window.
        short_rate:             orders/min in the short (1-min) window.
        long_rate:              orders/min in the long (5-min) window.
    """
    # Demand momentum: how much faster orders are arriving vs. recent baseline.
    # Clamp long_rate to avoid division by zero.
    momentum = short_rate / max(long_rate, 0.01)

    return {
        "store_id": store_id,
        "sku_id": sku_id,
        "window_start": window_start,
        "window_end": window_end,
        "order_rate": round(short_rate, 4),
        "depletion_vel": round(depletion_units / 1.0, 4),  # units/min (1-min window)
        "demand_momentum": round(momentum, 4),
        "on_hand_est": on_hand_latest,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }