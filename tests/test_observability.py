"""Phase 4 unit tests — observability layer.

Tests cover:
  - Drift metric computation (PSI, KS, JS) — pure Python, no external deps
  - Window splitting logic (rolling reference)
  - Metrics exporter push helpers (no Prometheus server needed)

These tests do NOT require Evidently, Prometheus, or Grafana running.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from observability.drift_job import (
    PSI_THRESHOLD,
    _js_divergence,
    _ks_statistic,
    _psi,
    _split_windows,
    compute_drift_metrics,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_df(
    n: int,
    store_id: str = "DS_001",
    sku_id: str = "SKU_001",
    order_rate_mean: float = 2.0,
    order_rate_std: float = 0.3,
    base_time: datetime | None = None,
) -> pd.DataFrame:
    """Synthetic windowed_features DataFrame."""
    rng = np.random.default_rng(42)
    if base_time is None:
        base_time = datetime.now(tz=UTC)
    rows = []
    for i in range(n):
        rows.append({
            "store_id":        store_id,
            "sku_id":          sku_id,
            "window_start":    (base_time + timedelta(minutes=i)).isoformat(),
            "window_end":      (base_time + timedelta(minutes=i + 1)).isoformat(),
            "order_rate":      float(rng.normal(order_rate_mean, order_rate_std)),
            "depletion_vel":   float(rng.normal(1.5, 0.2)),
            "demand_momentum": float(rng.uniform(0.8, 1.4)),
            "on_hand_est":     int(rng.integers(30, 100)),
            "updated_at":      (base_time + timedelta(minutes=i + 1)).isoformat(),
        })
    return pd.DataFrame(rows)


@pytest.fixture
def stable_df() -> pd.DataFrame:
    """25 hours of stable data. Last ~60 rows fall within the 1h current window."""
    now = datetime.now(tz=UTC)
    # Lay down 200 rows: first 140 as reference (>1h ago), last 60 as current (<1h ago)
    ref_base = now - timedelta(hours=25)
    ref_df = _make_df(n=140, base_time=ref_base, order_rate_mean=2.0, order_rate_std=0.3)

    cur_base = now - timedelta(minutes=59)
    cur_df = _make_df(n=60, base_time=cur_base, order_rate_mean=2.0, order_rate_std=0.3)

    df = pd.concat([ref_df, cur_df], ignore_index=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df


@pytest.fixture
def drifted_df() -> pd.DataFrame:
    """Reference data (normal distribution) + current data (heavily shifted)."""
    now = datetime.now(tz=UTC)
    reference_base = now - timedelta(hours=25)
    current_base   = now - timedelta(minutes=59)

    reference_part = _make_df(n=160, base_time=reference_base,
                               order_rate_mean=2.0, order_rate_std=0.2)
    current_part   = _make_df(n=40,  base_time=current_base,
                               order_rate_mean=8.0, order_rate_std=0.5)
    df = pd.concat([reference_part, current_part], ignore_index=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df.sort_values("window_end")


# ---------------------------------------------------------------------------
# PSI tests
# ---------------------------------------------------------------------------

class TestPsi:
    def test_identical_distributions_zero_psi(self):
        x = np.random.default_rng(0).normal(0, 1, 200)
        psi = _psi(x, x)
        assert psi < 0.01

    def test_shifted_distribution_high_psi(self):
        ref = np.random.default_rng(0).normal(0, 1, 200)
        cur = np.random.default_rng(1).normal(5, 1, 200)   # large shift
        psi = _psi(ref, cur)
        assert psi > PSI_THRESHOLD

    def test_psi_non_negative(self):
        ref = np.random.default_rng(0).normal(0, 1, 100)
        cur = np.random.default_rng(1).normal(0.5, 1, 100)
        assert _psi(ref, cur) >= 0

    def test_constant_distribution_returns_zero(self):
        x = np.ones(100)
        assert _psi(x, x) == 0.0


# ---------------------------------------------------------------------------
# KS tests
# ---------------------------------------------------------------------------

class TestKs:
    def test_identical_distributions_low_statistic(self):
        x = np.random.default_rng(0).normal(0, 1, 200)
        ks_stat, ks_pval = _ks_statistic(x, x)
        assert ks_stat == 0.0
        assert ks_pval == 1.0

    def test_different_distributions_high_statistic(self):
        ref = np.random.default_rng(0).normal(0, 1, 200)
        cur = np.random.default_rng(1).normal(5, 1, 200)
        ks_stat, ks_pval = _ks_statistic(ref, cur)
        assert ks_stat > 0.5
        assert ks_pval < 0.05

    def test_returns_two_values(self):
        x = np.random.default_rng(0).normal(0, 1, 50)
        result = _ks_statistic(x, x)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# JS divergence tests
# ---------------------------------------------------------------------------

class TestJsDivergence:
    def test_identical_distributions_near_zero(self):
        x = np.random.default_rng(0).normal(0, 1, 200)
        js = _js_divergence(x, x)
        assert js < 0.05

    def test_very_different_distributions_high_js(self):
        ref = np.random.default_rng(0).normal(0, 0.1, 200)
        cur = np.random.default_rng(1).normal(5, 0.1, 200)
        js = _js_divergence(ref, cur)
        assert js > 0.5

    def test_bounded_zero_to_one(self):
        ref = np.random.default_rng(0).normal(0, 1, 100)
        cur = np.random.default_rng(1).normal(10, 1, 100)
        js = _js_divergence(ref, cur)
        assert 0.0 <= js <= 1.0

    def test_constant_returns_zero(self):
        x = np.ones(100)
        assert _js_divergence(x, x) == 0.0


# ---------------------------------------------------------------------------
# Window splitting
# ---------------------------------------------------------------------------

class TestSplitWindows:
    def test_current_window_is_recent(self, stable_df):
        stable_df["window_end"] = pd.to_datetime(stable_df["window_end"], utc=True)
        now = datetime.now(tz=UTC)
        current, reference = _split_windows(stable_df, now)
        assert len(current) > 0
        # All current rows should be within last 1 hour
        cutoff = now - timedelta(hours=1)
        assert (current["window_end"] >= cutoff).all()

    def test_reference_window_is_older(self, stable_df):
        stable_df["window_end"] = pd.to_datetime(stable_df["window_end"], utc=True)
        now = datetime.now(tz=UTC)
        current, reference = _split_windows(stable_df, now)
        if len(reference) > 0:
            assert (reference["window_end"] < now - timedelta(hours=1)).all()

    def test_no_overlap_between_windows(self, stable_df):
        stable_df["window_end"] = pd.to_datetime(stable_df["window_end"], utc=True)
        now = datetime.now(tz=UTC)
        current, reference = _split_windows(stable_df, now)
        if len(current) > 0 and len(reference) > 0:
            assert current["window_end"].min() > reference["window_end"].max()


# ---------------------------------------------------------------------------
# compute_drift_metrics
# ---------------------------------------------------------------------------

class TestComputeDriftMetrics:
    def test_stable_data_no_drift(self, stable_df):
        stable_df["window_end"] = pd.to_datetime(stable_df["window_end"], utc=True)
        now = datetime.now(tz=UTC)
        current, reference = _split_windows(stable_df, now)
        if len(current) < 5 or len(reference) < 5:
            pytest.skip("Insufficient data for drift test in this environment")
        results = compute_drift_metrics(reference, current)
        drifted = [f for f, m in results.items() if m["drifted"]]
        assert len(drifted) == 0

    def test_drifted_data_detected(self, drifted_df):
        now = datetime.now(tz=UTC)
        current, reference = _split_windows(drifted_df, now)
        if len(current) < 5 or len(reference) < 5:
            pytest.skip("Insufficient data for drift test in this environment")
        results = compute_drift_metrics(reference, current)
        assert "order_rate" in results
        assert results["order_rate"]["drifted"] == 1.0

    def test_result_has_all_metrics(self, drifted_df):
        now = datetime.now(tz=UTC)
        current, reference = _split_windows(drifted_df, now)
        if len(current) < 5 or len(reference) < 5:
            pytest.skip("Insufficient data for drift test in this environment")
        results = compute_drift_metrics(reference, current)
        for feature, metrics in results.items():
            for key in ["psi", "ks_stat", "ks_pval", "js_div", "drifted"]:
                assert key in metrics, f"Missing {key} for {feature}"

    def test_skips_feature_with_insufficient_data(self):
        """compute_drift_metrics should skip features with < 5 rows."""
        tiny_ref = pd.DataFrame({"order_rate": [1.0, 2.0], "depletion_vel": [1.0, 1.1],
                                  "demand_momentum": [1.0, 1.0], "on_hand_est": [50, 50]})
        tiny_cur = pd.DataFrame({"order_rate": [3.0, 4.0], "depletion_vel": [1.5, 1.6],
                                  "demand_momentum": [1.2, 1.3], "on_hand_est": [40, 45]})
        results = compute_drift_metrics(tiny_ref, tiny_cur)
        assert results == {}


# ---------------------------------------------------------------------------
# Metrics exporter push helpers (smoke test — no server needed)
# ---------------------------------------------------------------------------

class TestMetricsExporter:
    def test_push_functions_do_not_raise(self):
        """Push helpers should be no-ops if prometheus_client is unavailable."""
        from observability.metrics_exporter import (
            push_active_alerts,
            push_drift_detected,
            push_drift_metrics,
            push_freshness_lag,
        )
        # These should not raise even if prometheus_client is not installed
        push_freshness_lag(30.0)
        push_active_alerts(2, 1)
        push_drift_metrics("order_rate", psi=0.05, ks=0.1, js=0.03)
        push_drift_detected(False)