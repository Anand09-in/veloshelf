"""Phase 3 unit tests — ML feature engineering, evaluation, and promotion logic.

These tests do NOT require MLflow, Spark, or XGBoost to be running.
They test the pure-Python feature and evaluation logic.
MLflow-dependent tests (train_detector, train_forecast, promote) are
integration tests run manually — see docs/phase3.md.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ml.evaluate import DetectorEvaluator, ForecastEvaluator
from ml.features import (
    ANOMALY_FEATURE_COLS,
    FORECAST_FEATURE_COLS,
    build_anomaly_features,
    build_forecast_features,
    build_online_anomaly_features,
    build_online_forecast_features,
    get_anomaly_X,
    get_forecast_Xy,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_feature_df(
    n: int = 20, store_id: str = "DS_001", sku_id: str = "SKU_001"
) -> pd.DataFrame:
    """Create a synthetic windowed_features DataFrame."""
    now = datetime.now(tz=UTC)
    rows = []
    for i in range(n):
        window_start = (now + timedelta(minutes=i)).isoformat()
        window_end   = (now + timedelta(minutes=i + 1)).isoformat()
        rows.append({
            "store_id":       store_id,
            "sku_id":         sku_id,
            "window_start":   window_start,
            "window_end":     window_end,
            "order_rate":     float(2 + i * 0.1),
            "depletion_vel":  float(1.5 + i * 0.05),
            "demand_momentum": 1.0 + (0.1 * (i % 5)),
            "on_hand_est":    max(0, 80 - i * 2),
            "is_injected_anomaly": (i % 10 == 0),
            "updated_at":     window_start,
        })
    return pd.DataFrame(rows)


@pytest.fixture
def feature_df() -> pd.DataFrame:
    return _make_feature_df(n=20)


@pytest.fixture
def multi_sku_df() -> pd.DataFrame:
    dfs = [
        _make_feature_df(n=20, store_id="DS_001", sku_id="SKU_001"),
        _make_feature_df(n=20, store_id="DS_001", sku_id="SKU_002"),
        _make_feature_df(n=20, store_id="DS_002", sku_id="SKU_001"),
    ]
    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Forecast features
# ---------------------------------------------------------------------------

class TestBuildForecastFeatures:
    def test_produces_expected_columns(self, feature_df):
        result = build_forecast_features(feature_df)
        for col in FORECAST_FEATURE_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_drops_rows_with_insufficient_history(self, feature_df):
        result = build_forecast_features(feature_df)
        # First 5 rows dropped due to lag3 + roll6 requirement
        assert len(result) < len(feature_df)

    def test_lag1_is_previous_order_rate(self, feature_df):
        result = build_forecast_features(feature_df)
        # lag1 should equal the order_rate from the prior window
        assert not result["order_rate_lag1"].isna().any()

    def test_is_weekend_binary(self, feature_df):
        result = build_forecast_features(feature_df)
        assert set(result["is_weekend"].unique()).issubset({0, 1})

    def test_get_forecast_Xy_shapes_match(self, feature_df):
        built = build_forecast_features(feature_df)
        X, y = get_forecast_Xy(built)
        assert len(X) == len(y)
        assert list(X.columns) == FORECAST_FEATURE_COLS


# ---------------------------------------------------------------------------
# Anomaly features
# ---------------------------------------------------------------------------

class TestBuildAnomalyFeatures:
    def test_produces_expected_columns(self, multi_sku_df):
        result = build_anomaly_features(multi_sku_df)
        for col in ANOMALY_FEATURE_COLS:
            assert col in result.columns, f"Missing: {col}"

    def test_no_nan_in_output(self, multi_sku_df):
        result = build_anomaly_features(multi_sku_df)
        X = get_anomaly_X(result)
        assert not X.isna().any().any()

    def test_zscore_mean_near_zero_per_group(self, multi_sku_df):
        result = build_anomaly_features(multi_sku_df)
        for (store, sku), grp in result.groupby(["store_id", "sku_id"]):
            mean_z = grp["order_rate_zscore"].mean()
            assert abs(mean_z) < 1e-6, f"Z-score mean not ~0 for {store}/{sku}"


# ---------------------------------------------------------------------------
# Online feature builders
# ---------------------------------------------------------------------------

class TestOnlineFeatureBuilders:
    def test_online_anomaly_returns_single_row(self):
        row = {
            "order_rate": 2.0, "depletion_vel": 1.5,
            "demand_momentum": 1.2, "on_hand_est": 40,
        }
        df = build_online_anomaly_features(row)
        assert len(df) == 1
        assert list(df.columns) == ANOMALY_FEATURE_COLS

    def test_online_forecast_returns_none_with_insufficient_history(self):
        rows = [{"window_start": "2024-01-01T12:00:00",
                 "order_rate": 1.0, "depletion_vel": 0.5,
                 "demand_momentum": 1.0, "on_hand_est": 50}] * 4
        result = build_online_forecast_features(rows)
        assert result is None

    def test_online_forecast_returns_df_with_enough_history(self, feature_df):
        rows = feature_df.to_dict("records")
        result = build_online_forecast_features(rows)
        assert result is not None
        assert len(result) == 1
        assert list(result.columns) == FORECAST_FEATURE_COLS


# ---------------------------------------------------------------------------
# ForecastEvaluator
# ---------------------------------------------------------------------------

class TestForecastEvaluator:
    def test_perfect_predictions(self):
        y = np.array([1.0, 2.0, 3.0, 4.0])
        metrics = ForecastEvaluator.evaluate(y, y)
        assert metrics["mae"] == 0.0
        assert metrics["rmse"] == 0.0
        assert metrics["mape"] == 0.0

    def test_nonzero_error(self):
        y_true = np.array([2.0, 4.0, 6.0])
        y_pred = np.array([1.0, 3.0, 5.0])
        metrics = ForecastEvaluator.evaluate(y_true, y_pred)
        assert metrics["mae"] == pytest.approx(1.0)
        assert metrics["rmse"] == pytest.approx(1.0)

    def test_mape_skips_zero_actuals(self):
        y_true = np.array([0.0, 2.0, 4.0])
        y_pred = np.array([1.0, 1.0, 3.0])
        metrics = ForecastEvaluator.evaluate(y_true, y_pred)
        assert not math.isnan(metrics["mape"])

    def test_beats_incumbent_lower_mae(self):
        assert ForecastEvaluator.beats_incumbent(
            {"mae": 0.5}, {"mae": 1.0}
        )

    def test_does_not_beat_incumbent_higher_mae(self):
        assert not ForecastEvaluator.beats_incumbent(
            {"mae": 1.5}, {"mae": 1.0}
        )


# ---------------------------------------------------------------------------
# DetectorEvaluator
# ---------------------------------------------------------------------------

class TestDetectorEvaluator:
    def _write_labels(self, path: Path, records: list[dict]) -> None:
        with path.open("w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

    def test_perfect_match(self, tmp_path):
        now = datetime.now(tz=UTC)
        label_path = tmp_path / "labels.jsonl"
        self._write_labels(label_path, [{
            "anomaly_type": "surge",
            "store_id": "DS_001",
            "sku_id": "SKU_001",
            "injected_at": now.isoformat(),
            "surge_extra_events": 20,
            "stockout_target_units": 5,
        }])
        evaluator = DetectorEvaluator(label_path, match_window_s=120)
        preds = [{"alert_type": "surge", "store_id": "DS_001",
                  "sku_id": "SKU_001", "triggered_at": now.isoformat()}]
        metrics = evaluator.evaluate(preds)
        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0
        assert metrics["f1"] == 1.0

    def test_no_match_outside_window(self, tmp_path):
        now = datetime.now(tz=UTC)
        label_path = tmp_path / "labels.jsonl"
        self._write_labels(label_path, [{
            "anomaly_type": "surge",
            "store_id": "DS_001",
            "sku_id": "SKU_001",
            "injected_at": now.isoformat(),
            "surge_extra_events": 20,
            "stockout_target_units": 5,
        }])
        evaluator = DetectorEvaluator(label_path, match_window_s=10)
        late_time = (now + timedelta(minutes=5)).isoformat()
        preds = [{"alert_type": "surge", "store_id": "DS_001",
                  "sku_id": "SKU_001", "triggered_at": late_time}]
        metrics = evaluator.evaluate(preds)
        assert metrics["recall"] == 0.0

    def test_empty_labels(self, tmp_path):
        label_path = tmp_path / "labels.jsonl"
        label_path.write_text("")
        evaluator = DetectorEvaluator(label_path)
        metrics = evaluator.evaluate([{
            "alert_type": "surge",
            "store_id": "DS_001",
            "sku_id": "SKU_001",
            "triggered_at": "2024-01-01T00:00:00+00:00",
        }])
        assert metrics["precision"] == 0.0
        assert metrics["recall"] == 0.0

    def test_empty_predictions(self, tmp_path):
        now = datetime.now(tz=UTC)
        label_path = tmp_path / "labels.jsonl"
        self._write_labels(label_path, [{
            "anomaly_type": "surge", "store_id": "DS_001",
            "sku_id": "SKU_001", "injected_at": now.isoformat(),
            "surge_extra_events": 0, "stockout_target_units": 5,
        }])
        evaluator = DetectorEvaluator(label_path)
        metrics = evaluator.evaluate([])
        assert metrics["recall"] == 0.0

    def test_beats_incumbent_higher_f1(self):
        assert DetectorEvaluator.beats_incumbent({"f1": 0.8}, {"f1": 0.6})

    def test_does_not_beat_incumbent_lower_f1(self):
        assert not DetectorEvaluator.beats_incumbent({"f1": 0.5}, {"f1": 0.6})