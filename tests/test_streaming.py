"""Phase 2 unit tests — streaming validation and scoring.

These tests do NOT require a Flink runtime or Kafka connection.
They test the pure-Python logic that the Flink job wraps.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from streaming.scoring import make_feature_row, score_features
from streaming.validation import (
    ReferenceData,
    to_dead_letter,
    validate_inventory_event,
    validate_order_event,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ref() -> ReferenceData:
    return ReferenceData(
        store_ids={"DS_001", "DS_002"},
        sku_ids={"SKU_001", "SKU_002", "SKU_003"},
    )


@pytest.fixture
def valid_order() -> dict:
    return {
        "event_id": "evt-001",
        "event_time": datetime.now(tz=timezone.utc).isoformat(),
        "store_id": "DS_001",
        "sku_id": "SKU_001",
        "category": "dairy",
        "quantity": 2,
        "unit_price": 27.0,
        "order_id": "ord-001",
        "is_injected_anomaly": False,
    }


@pytest.fixture
def valid_inventory() -> dict:
    return {
        "event_id": "evt-inv-001",
        "event_time": datetime.now(tz=timezone.utc).isoformat(),
        "store_id": "DS_001",
        "sku_id": "SKU_001",
        "movement_type": "sale",
        "delta_units": -2,
        "on_hand_after": 48,
        "is_injected_anomaly": False,
    }


# ---------------------------------------------------------------------------
# Validation — order events
# ---------------------------------------------------------------------------

class TestValidateOrderEvent:
    def test_valid_event_passes(self, valid_order, ref):
        result = validate_order_event(valid_order, ref)
        assert result.valid

    def test_missing_field_fails(self, valid_order, ref):
        del valid_order["quantity"]
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "missing" in result.reason

    def test_zero_quantity_fails(self, valid_order, ref):
        valid_order["quantity"] = 0
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "quantity" in result.reason

    def test_negative_price_fails(self, valid_order, ref):
        valid_order["unit_price"] = -5.0
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "unit_price" in result.reason

    def test_unknown_store_fails(self, valid_order, ref):
        valid_order["store_id"] = "DS_999"
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "store_id" in result.reason

    def test_unknown_sku_fails(self, valid_order, ref):
        valid_order["sku_id"] = "SKU_999"
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "sku_id" in result.reason

    def test_future_timestamp_fails(self, valid_order, ref):
        future = (datetime.now(tz=timezone.utc) + timedelta(minutes=5)).isoformat()
        valid_order["event_time"] = future
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "future" in result.reason

    def test_bad_timestamp_fails(self, valid_order, ref):
        valid_order["event_time"] = "not-a-date"
        result = validate_order_event(valid_order, ref)
        assert not result.valid
        assert "unparseable" in result.reason


# ---------------------------------------------------------------------------
# Validation — inventory events
# ---------------------------------------------------------------------------

class TestValidateInventoryEvent:
    def test_valid_event_passes(self, valid_inventory, ref):
        result = validate_inventory_event(valid_inventory, ref)
        assert result.valid

    def test_negative_on_hand_fails(self, valid_inventory, ref):
        valid_inventory["on_hand_after"] = -1
        result = validate_inventory_event(valid_inventory, ref)
        assert not result.valid
        assert "on_hand_after" in result.reason

    def test_invalid_movement_type_fails(self, valid_inventory, ref):
        valid_inventory["movement_type"] = "steal"
        result = validate_inventory_event(valid_inventory, ref)
        assert not result.valid
        assert "movement_type" in result.reason

    def test_all_movement_types_valid(self, valid_inventory, ref):
        for mt in ["sale", "restock", "adjustment"]:
            valid_inventory["movement_type"] = mt
            result = validate_inventory_event(valid_inventory, ref)
            assert result.valid, f"Expected {mt} to be valid"

    def test_unknown_store_fails(self, valid_inventory, ref):
        valid_inventory["store_id"] = "DS_999"
        result = validate_inventory_event(valid_inventory, ref)
        assert not result.valid


# ---------------------------------------------------------------------------
# Dead-letter envelope
# ---------------------------------------------------------------------------

class TestDeadLetter:
    def test_envelope_has_required_fields(self, valid_order, ref):
        valid_order["quantity"] = -1
        result = validate_order_event(valid_order, ref)
        envelope = to_dead_letter(result, "raw-orders")
        assert "source_topic" in envelope
        assert "reason" in envelope
        assert "failed_at" in envelope
        assert "original_event" in envelope
        assert envelope["source_topic"] == "raw-orders"


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

class TestMakeFeatureRow:
    def test_produces_all_expected_keys(self):
        row = make_feature_row(
            store_id="DS_001", sku_id="SKU_001",
            window_start="2024-01-01T12:00:00",
            window_end="2024-01-01T12:01:00",
            order_count=10, total_units=15,
            depletion_units=15, on_hand_latest=45,
            short_rate=10.0, long_rate=4.0,
        )
        for key in ["store_id", "sku_id", "window_start", "window_end",
                    "order_rate", "depletion_vel", "demand_momentum",
                    "on_hand_est", "updated_at"]:
            assert key in row, f"Missing key: {key}"

    def test_momentum_ratio_correct(self):
        row = make_feature_row(
            store_id="DS_001", sku_id="SKU_001",
            window_start="2024-01-01T12:00:00",
            window_end="2024-01-01T12:01:00",
            order_count=10, total_units=10,
            depletion_units=10, on_hand_latest=40,
            short_rate=10.0, long_rate=2.0,
        )
        assert abs(row["demand_momentum"] - 5.0) < 0.01

    def test_zero_long_rate_no_division_error(self):
        row = make_feature_row(
            store_id="DS_001", sku_id="SKU_001",
            window_start="2024-01-01T12:00:00",
            window_end="2024-01-01T12:01:00",
            order_count=5, total_units=5,
            depletion_units=5, on_hand_latest=50,
            short_rate=5.0, long_rate=0.0,
        )
        assert row["demand_momentum"] > 0


class TestScoreFeatures:
    @pytest.fixture(autouse=True)
    def _force_rules(self, monkeypatch):
        # Pin to rules-only path so tests are deterministic regardless of
        # whether an ML model is trained and available in MLflow.
        monkeypatch.setattr("streaming.scoring._detector_loader", False)

    def _row(self, on_hand: int = 100, momentum: float = 1.0) -> dict:
        return make_feature_row(
            store_id="DS_001", sku_id="SKU_001",
            window_start="2024-01-01T12:00:00",
            window_end="2024-01-01T12:01:00",
            order_count=5, total_units=5,
            depletion_units=5, on_hand_latest=on_hand,
            short_rate=momentum * 2.0, long_rate=2.0,
        )

    def test_no_alerts_normal_conditions(self):
        alerts = score_features(self._row(on_hand=100, momentum=1.0), reorder_point=20)
        assert alerts == []

    def test_stockout_alert_fires_below_reorder(self):
        alerts = score_features(self._row(on_hand=5), reorder_point=20)
        types = [a["alert_type"] for a in alerts]
        assert "stockout_risk" in types

    def test_surge_alert_fires_on_high_momentum(self):
        row = self._row(on_hand=100)
        row["demand_momentum"] = 3.0   # above SURGE_MOMENTUM_THRESHOLD=2.5
        alerts = score_features(row, reorder_point=20)
        types = [a["alert_type"] for a in alerts]
        assert "surge" in types

    def test_both_alerts_fire_simultaneously(self):
        row = self._row(on_hand=5)
        row["demand_momentum"] = 3.0
        alerts = score_features(row, reorder_point=20)
        types = {a["alert_type"] for a in alerts}
        assert types == {"stockout_risk", "surge"}

    def test_alert_has_required_fields(self):
        alerts = score_features(self._row(on_hand=5), reorder_point=20)
        assert len(alerts) > 0
        for field in ["alert_id", "alert_type", "store_id", "sku_id",
                      "triggered_at", "metric_value", "threshold", "resolved"]:
            assert field in alerts[0], f"Missing field: {field}"

    def test_alert_ids_unique(self):
        row = self._row(on_hand=5)
        row["demand_momentum"] = 3.0
        alerts = score_features(row, reorder_point=20)
        ids = [a["alert_id"] for a in alerts]
        assert len(ids) == len(set(ids))