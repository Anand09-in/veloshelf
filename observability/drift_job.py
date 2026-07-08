"""VeloShelf Evidently drift detection job (Phase 4).

Runs on a schedule (every 2h via Dagster). Computes data drift between:
  - Current window:    feature rows from the last 1 hour
  - Reference window:  feature rows from the prior 24 hours

Drift metrics computed per feature:
  - PSI   (Population Stability Index) — detects distribution shift
  - KS    (Kolmogorov-Smirnov test)    — detects shape change in continuous features
  - JS    (Jensen-Shannon divergence)  — symmetric, bounded [0,1] divergence measure

PSI rule of thumb:   < 0.10 stable · 0.10–0.25 moderate · > 0.25 significant
KS p-value rule:     p < 0.05 → distributions likely differ
JS rule of thumb:    > 0.10 worth investigating · > 0.25 significant

Outputs:
  1. HTML drift report → data/reports/drift_YYYYMMDD_HHMMSS.html
  2. Drift metrics    → MLflow (as metrics on a dedicated drift run)
  3. Prometheus gauges → via metrics_exporter.push_drift_metrics()
  4. Return dict      → consumed by Dagster retrain sensor (Phase 5)

Run standalone:
    python -m observability.drift_job
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FEATURES_PATH   = Path(os.getenv("FEATURES_PATH",       "data/features"))
REPORTS_PATH    = Path(os.getenv("REPORTS_PATH",         "data/reports"))
TRACKING_URI    = os.getenv("MLFLOW_TRACKING_URI",        "http://localhost:5000")
EXPERIMENT_NAME = "veloshelf-drift-monitoring"
PSI_THRESHOLD   = float(os.getenv("PSI_DRIFT_THRESHOLD", "0.25"))
CURRENT_WINDOW_H  = int(os.getenv("DRIFT_CURRENT_WINDOW_H",   "1"))
REFERENCE_WINDOW_H = int(os.getenv("DRIFT_REFERENCE_WINDOW_H", "24"))

# Features to monitor for drift
MONITORED_FEATURES = [
    "order_rate",
    "depletion_vel",
    "demand_momentum",
    "on_hand_est",
]


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_all_features() -> pd.DataFrame:
    """Load all Parquet feature files."""
    files = list(FEATURES_PATH.glob("**/*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No feature Parquet files found in {FEATURES_PATH}. "
            "Run the generator + Flink job first."
        )
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df.sort_values("window_end")


def _split_windows(
    df: pd.DataFrame,
    now: datetime,
    current_h: int = CURRENT_WINDOW_H,
    reference_h: int = REFERENCE_WINDOW_H,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split df into current (last current_h hours) and reference (prior reference_h hours)."""
    current_cutoff   = now - timedelta(hours=current_h)
    reference_cutoff = now - timedelta(hours=reference_h + current_h)

    current_df   = df[df["window_end"] >= current_cutoff]
    reference_df = df[
        (df["window_end"] >= reference_cutoff) &
        (df["window_end"] <  current_cutoff)
    ]
    return current_df, reference_df


def _split_windows_percentile(
    df: pd.DataFrame,
    current_frac: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Percentile fallback split: most recent current_frac of rows = current."""
    df_sorted = df.sort_values("window_end")
    split_idx = int(len(df_sorted) * (1 - current_frac))
    return df_sorted.iloc[split_idx:].copy(), df_sorted.iloc[:split_idx].copy()


# ---------------------------------------------------------------------------
# Drift metric computation
# ---------------------------------------------------------------------------

def _psi(reference: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """Compute Population Stability Index between two distributions.

    PSI = sum((current_pct - ref_pct) * ln(current_pct / ref_pct))
    """
    # Build bins from reference distribution
    min_val = min(reference.min(), current.min())
    max_val = max(reference.max(), current.max())
    if min_val == max_val:
        return 0.0

    breakpoints = np.linspace(min_val, max_val, bins + 1)
    ref_counts  = np.histogram(reference, bins=breakpoints)[0]
    cur_counts  = np.histogram(current,   bins=breakpoints)[0]

    # Replace zeros to avoid log(0)
    ref_pct = np.where(ref_counts == 0, 1e-4, ref_counts / len(reference))
    cur_pct = np.where(cur_counts == 0, 1e-4, cur_counts / len(current))

    psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))
    return round(abs(psi), 6)


def _ks_statistic(reference: np.ndarray, current: np.ndarray) -> tuple[float, float]:
    """Compute KS statistic and p-value."""
    from scipy.stats import ks_2samp
    stat, pval = ks_2samp(reference, current)
    return round(float(stat), 6), round(float(pval), 6)


def _js_divergence(reference: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """Compute Jensen-Shannon divergence between two distributions.

    JS is symmetric and bounded [0, 1] (using log base 2 → bits).
    """
    min_val = min(reference.min(), current.min())
    max_val = max(reference.max(), current.max())
    if min_val == max_val:
        return 0.0

    breakpoints = np.linspace(min_val, max_val, bins + 1)
    ref_hist = np.histogram(reference, bins=breakpoints, density=True)[0]
    cur_hist = np.histogram(current,   bins=breakpoints, density=True)[0]

    # Normalise to proper probability distributions
    ref_p = ref_hist / (ref_hist.sum() + 1e-10)
    cur_p = cur_hist / (cur_hist.sum() + 1e-10)
    m = 0.5 * (ref_p + cur_p)

    # KL divergence helper (with zero-protection)
    def _kl(p: np.ndarray, q: np.ndarray) -> float:
        mask = (p > 0) & (q > 0)
        return float(np.sum(p[mask] * np.log2(p[mask] / q[mask])))

    js = 0.5 * _kl(ref_p, m) + 0.5 * _kl(cur_p, m)
    return round(min(abs(js), 1.0), 6)   # clamp to [0, 1]


def compute_drift_metrics(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    """Compute PSI + KS + JS for each monitored feature.

    Returns:
        {feature_name: {psi, ks_stat, ks_pval, js_div, drifted}}
    """
    results: dict[str, dict[str, float]] = {}

    for feature in MONITORED_FEATURES:
        if feature not in reference_df.columns or feature not in current_df.columns:
            logger.warning("Feature %s not found in DataFrames — skipping.", feature)
            continue

        ref_vals = reference_df[feature].dropna().values.astype(float)
        cur_vals = current_df[feature].dropna().values.astype(float)

        if len(ref_vals) < 5 or len(cur_vals) < 5:
            logger.warning(
                "Insufficient data for %s (ref=%d cur=%d) — skipping.",
                feature, len(ref_vals), len(cur_vals),
            )
            continue

        psi              = _psi(ref_vals, cur_vals)
        ks_stat, ks_pval = _ks_statistic(ref_vals, cur_vals)
        js               = _js_divergence(ref_vals, cur_vals)
        drifted          = psi > PSI_THRESHOLD

        results[feature] = {
            "psi":      psi,
            "ks_stat":  ks_stat,
            "ks_pval":  ks_pval,
            "js_div":   js,
            "drifted":  float(drifted),
        }
        logger.info(
            "Drift | %-20s psi=%.4f ks=%.4f js=%.4f drifted=%s",
            feature, psi, ks_stat, js, drifted,
        )

    return results


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def _generate_evidently_report(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    drift_results: dict[str, dict[str, float]],
    report_path: Path,
) -> None:
    """Generate an Evidently HTML drift report.

    Falls back to a plain HTML summary if Evidently is not installed.
    """
    REPORTS_PATH.mkdir(parents=True, exist_ok=True)

    try:
        # Evidently 0.4+ moved ColumnMapping to evidently.pipeline.column_mapping
        try:
            from evidently import ColumnMapping
        except ImportError:
            from evidently.pipeline.column_mapping import ColumnMapping
        from evidently.metric_preset import DataDriftPreset
        from evidently.report import Report

        report = Report(metrics=[DataDriftPreset()])
        col_map = ColumnMapping(numerical_features=MONITORED_FEATURES)
        report.run(
            reference_data=reference_df[MONITORED_FEATURES],
            current_data=current_df[MONITORED_FEATURES],
            column_mapping=col_map,
        )
        report.save_html(str(report_path))
        logger.info("Evidently HTML report saved: %s", report_path)

    except ImportError:
        # Fallback: plain HTML summary (no emoji — safe for all encodings)
        rows = "".join(
            f"<tr><td>{f}</td><td>{m['psi']:.4f}</td>"
            f"<td>{m['ks_stat']:.4f}</td><td>{m['js_div']:.4f}</td>"
            f"<td>{'YES' if m['drifted'] else 'NO'}</td></tr>"
            for f, m in drift_results.items()
        )
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>VeloShelf Drift Report</title>
<style>body{{font-family:sans-serif;padding:2rem}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #ccc;padding:8px;text-align:left}}
th{{background:#f4f4f4}}</style></head><body>
<h1>VeloShelf Drift Report</h1>
<p>Generated: {datetime.now(tz=UTC).isoformat()}</p>
<p>Reference: last {REFERENCE_WINDOW_H}h | Current: last {CURRENT_WINDOW_H}h |
PSI threshold: {PSI_THRESHOLD}</p>
<table><tr><th>Feature</th><th>PSI</th><th>KS</th><th>JS</th><th>Drifted?</th></tr>
{rows}</table></body></html>"""
        report_path.write_text(html, encoding="utf-8")
        logger.info("Fallback HTML report saved: %s", report_path)


# ---------------------------------------------------------------------------
# MLflow logging
# ---------------------------------------------------------------------------

def _log_to_mlflow(
    drift_results: dict[str, dict[str, float]],
    n_current: int,
    n_reference: int,
) -> str:
    """Log drift metrics to MLflow. Returns run_id."""
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    flat_metrics: dict[str, float] = {
        "n_current_rows":   float(n_current),
        "n_reference_rows": float(n_reference),
        "n_features_drifted": float(
            sum(1 for m in drift_results.values() if m["drifted"])
        ),
    }
    for feature, metrics in drift_results.items():
        for metric_name, value in metrics.items():
            flat_metrics[f"{feature}_{metric_name}"] = value

    run_name = f"drift_{datetime.now(tz=UTC).strftime('%Y%m%d_%H%M%S')}"
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_metrics(flat_metrics)
        mlflow.log_param("psi_threshold", PSI_THRESHOLD)
        mlflow.log_param("current_window_h", CURRENT_WINDOW_H)
        mlflow.log_param("reference_window_h", REFERENCE_WINDOW_H)
        return run.info.run_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_drift_job() -> dict[str, Any]:
    """Execute the full drift detection pipeline.

    Returns a summary dict consumed by the Dagster drift asset and
    the Phase 5 retrain sensor.
    """
    logger.info("Starting drift job | current=%dh reference=%dh",
                CURRENT_WINDOW_H, REFERENCE_WINDOW_H)

    df = _load_all_features()
    # Anchor to the latest data timestamp so the job works against historical
    # exports too, not just live data fresh within the last hour.
    now = df["window_end"].max()
    current_df, reference_df = _split_windows(df, now)

    logger.info(
        "Window sizes | current=%d reference=%d rows",
        len(current_df), len(reference_df),
    )

    # Fallback: data may span less than current_h (e.g. fast-mode exports).
    # Use last 20% of rows as current, remaining 80% as reference.
    if len(current_df) < 5 or len(reference_df) < 10:
        logger.info(
            "Time-based split insufficient — falling back to 20/80 percentile split."
        )
        current_df, reference_df = _split_windows_percentile(df, current_frac=0.2)
        logger.info(
            "Percentile split | current=%d reference=%d rows",
            len(current_df), len(reference_df),
        )

    if len(reference_df) < 10:
        logger.warning(
            "Reference window too small (%d rows). "
            "Need more feature history — skipping drift job.",
            len(reference_df),
        )
        return {"skipped": True, "reason": "insufficient_reference_data"}

    if len(current_df) < 5:
        logger.warning(
            "Current window too small (%d rows). "
            "Pipeline may not be running — skipping drift job.",
            len(current_df),
        )
        return {"skipped": True, "reason": "insufficient_current_data"}

    # Compute drift metrics
    drift_results = compute_drift_metrics(reference_df, current_df)

    any_drift = any(m["drifted"] for m in drift_results.values())

    # HTML report
    report_ts   = now.strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_PATH / f"drift_{report_ts}.html"
    _generate_evidently_report(reference_df, current_df, drift_results, report_path)

    # MLflow
    run_id = _log_to_mlflow(drift_results, len(current_df), len(reference_df))

    # Prometheus gauges
    try:
        from observability.metrics_exporter import push_drift_detected, push_drift_metrics
        for feature, metrics in drift_results.items():
            push_drift_metrics(
                feature=feature,
                psi=metrics["psi"],
                ks=metrics["ks_stat"],
                js=metrics["js_div"],
            )
        push_drift_detected(any_drift)
    except Exception as e:
        logger.warning("Could not push to Prometheus: %s", e)

    summary = {
        "skipped":       False,
        "any_drift":     any_drift,
        "drift_results": drift_results,
        "report_path":   str(report_path),
        "mlflow_run_id": run_id,
        "n_current":     len(current_df),
        "n_reference":   len(reference_df),
        "timestamp":     now.isoformat(),
    }

    logger.info(
        "Drift job complete | any_drift=%s n_drifted_features=%d report=%s",
        any_drift,
        sum(1 for m in drift_results.values() if m["drifted"]),
        report_path,
    )
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s")
    result = run_drift_job()
    print(result)