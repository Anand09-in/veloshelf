"""Shared feature engineering for VeloShelf ML models.

Used by both the offline training jobs (Spark) and the online scorer
(Flink hot-swap). Keeping feature logic in one place guarantees
training/serving parity — a common production failure mode when
features are computed differently offline vs. online.

Input: a pandas DataFrame with windowed_features columns.
Output: a feature matrix (X) and optional target vector (y).

Forecasting features (XGBoost demand forecaster):
  - Lag features: order_rate at t-1, t-2, t-3 windows
  - Rolling stats: mean/std of order_rate over last 3 and 6 windows
  - Time features: hour_of_day, day_of_week, is_weekend
  - Domain features: depletion_vel, demand_momentum, on_hand_est

Anomaly detection features (Isolation Forest):
  - order_rate, depletion_vel, demand_momentum, on_hand_est
  - Rolling z-score of order_rate (deviation from per-SKU mean)
  - depletion_rate_zscore
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Column names
# ---------------------------------------------------------------------------

FORECAST_FEATURE_COLS = [
    "order_rate_lag1",
    "order_rate_lag2",
    "order_rate_lag3",
    "order_rate_roll3_mean",
    "order_rate_roll3_std",
    "order_rate_roll6_mean",
    "depletion_vel",
    "demand_momentum",
    "on_hand_est",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
]

FORECAST_TARGET_COL = "order_rate"

ANOMALY_FEATURE_COLS = [
    "order_rate",
    "depletion_vel",
    "demand_momentum",
    "on_hand_est",
    "order_rate_zscore",
    "depletion_vel_zscore",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_window_start(df: pd.DataFrame) -> pd.Series:
    """Parse window_start to datetime, handling both ISO strings and ints."""
    ws = df["window_start"]
    if pd.api.types.is_numeric_dtype(ws):
        return pd.to_datetime(ws, unit="ms", utc=True)
    return pd.to_datetime(ws, utc=True)


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std()
    if std == 0 or np.isnan(std):
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - mean) / std


# ---------------------------------------------------------------------------
# Forecast feature builder
# ---------------------------------------------------------------------------

def build_forecast_features(
    df: pd.DataFrame,
    sku_id: str | None = None,
) -> pd.DataFrame:
    """Build XGBoost forecast features for one SKU's time series.

    Args:
        df:      DataFrame sorted by window_start for a single SKU.
                 Must have: window_start, order_rate, depletion_vel,
                            demand_momentum, on_hand_est.
        sku_id:  Optional — used only for error messages.

    Returns:
        DataFrame with FORECAST_FEATURE_COLS columns. Rows with NaN
        lag features (first 3 rows) are dropped.
    """
    df = df.sort_values("window_start").copy()
    dt = _parse_window_start(df)

    # Lag features
    df["order_rate_lag1"] = df["order_rate"].shift(1)
    df["order_rate_lag2"] = df["order_rate"].shift(2)
    df["order_rate_lag3"] = df["order_rate"].shift(3)

    # Rolling stats
    df["order_rate_roll3_mean"] = df["order_rate"].shift(1).rolling(3).mean()
    df["order_rate_roll3_std"]  = df["order_rate"].shift(1).rolling(3).std().fillna(0)
    df["order_rate_roll6_mean"] = df["order_rate"].shift(1).rolling(6).mean()

    # Time features
    df["hour_of_day"]  = dt.dt.hour.values
    df["day_of_week"]  = dt.dt.dayofweek.values
    df["is_weekend"]   = (dt.dt.dayofweek >= 5).astype(int).values

    # Drop rows with NaN lags (first few windows)
    df = df.dropna(subset=["order_rate_lag3", "order_rate_roll6_mean"])

    return df


def get_forecast_Xy(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Return (X, y) for the forecast model from a built feature DataFrame."""
    X = df[FORECAST_FEATURE_COLS].copy()
    y = df[FORECAST_TARGET_COL].copy()
    return X, y


# ---------------------------------------------------------------------------
# Anomaly detection feature builder
# ---------------------------------------------------------------------------

def build_anomaly_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build Isolation Forest features from a windowed_features DataFrame.

    Works across all SKUs / stores together — the z-scores are computed
    per (store_id, sku_id) group so deviation is relative to each SKU's
    own baseline, not the global mean.

    Args:
        df: DataFrame with all windowed_features rows (multiple SKUs/stores OK).

    Returns:
        DataFrame with ANOMALY_FEATURE_COLS columns, index aligned with df.
    """
    df = df.copy()

    # Per-SKU z-scores (deviation from that SKU's normal)
    for col, out_col in [("order_rate", "order_rate_zscore"),
                         ("depletion_vel", "depletion_vel_zscore")]:
        df[out_col] = (
            df.groupby(["store_id", "sku_id"])[col]
            .transform(_zscore)
        )

    df = df.dropna(subset=ANOMALY_FEATURE_COLS)
    return df


def get_anomaly_X(df: pd.DataFrame) -> pd.DataFrame:
    """Return feature matrix X for the anomaly detector."""
    return df[ANOMALY_FEATURE_COLS].copy()


# ---------------------------------------------------------------------------
# Online feature builder (single row, for Flink hot-swap scoring)
# ---------------------------------------------------------------------------

def build_online_forecast_features(
    recent_rows: list[dict],
) -> pd.DataFrame | None:
    """Build forecast features from the last N feature rows for one SKU.

    Args:
        recent_rows: list of windowed_features dicts for a single (store, SKU),
                     ordered oldest → newest. Minimum 6 rows needed.

    Returns:
        Single-row DataFrame with FORECAST_FEATURE_COLS, or None if
        insufficient history.
    """
    if len(recent_rows) < 6:
        return None
    df = pd.DataFrame(recent_rows)
    df = build_forecast_features(df)
    if df.empty:
        return None
    return df.tail(1)[FORECAST_FEATURE_COLS]


def build_online_anomaly_features(row: dict) -> pd.DataFrame:
    """Build anomaly features from a single windowed_features dict.

    Z-scores default to 0 in the online case (no per-SKU history available).
    The offline-trained model is robust to this approximation.
    """
    return pd.DataFrame([{
        "order_rate":          row.get("order_rate", 0.0),
        "depletion_vel":       row.get("depletion_vel", 0.0),
        "demand_momentum":     row.get("demand_momentum", 1.0),
        "on_hand_est":         row.get("on_hand_est", 0),
        "order_rate_zscore":   0.0,   # approximation for online scoring
        "depletion_vel_zscore": 0.0,
    }])