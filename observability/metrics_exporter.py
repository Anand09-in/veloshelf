"""VeloShelf Prometheus metrics exporter (Phase 4).

Exposes pipeline health metrics as Prometheus gauges on a /metrics HTTP
endpoint (port 8000). The Flink job, drift job, and Streamlit app all
push to these gauges at key events.

Metrics exposed:
  veloshelf_feature_freshness_lag_seconds   — seconds since last feature row written
  veloshelf_order_event_count_total         — total order events processed (counter)
  veloshelf_feature_order_rate{store,sku}   — latest order_rate per store/SKU
  veloshelf_active_alerts{alert_type}       — count of unresolved alerts by type
  veloshelf_psi{feature}                    — latest PSI score per feature (from Evidently)
  veloshelf_ks_statistic{feature}           — latest KS statistic per feature
  veloshelf_js_divergence{feature}          — latest JS divergence per feature
  veloshelf_drift_detected                  — 1 if any feature PSI > threshold, else 0
  veloshelf_model_mae{model}                — latest MAE for the forecaster
  veloshelf_model_f1{model}                 — latest F1 for the detector

Run standalone (for local dev):
    python -m observability.metrics_exporter

Or import and call push_* functions from other modules.
"""

from __future__ import annotations

import logging
import os
import threading

logger = logging.getLogger(__name__)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# ---------------------------------------------------------------------------
# Prometheus client setup
# ---------------------------------------------------------------------------

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        start_http_server,
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False
    logger.warning("prometheus_client not installed — metrics export disabled.")

if _PROM_AVAILABLE:
    _REGISTRY = CollectorRegistry(auto_describe=True)

    # Pipeline health
    FEATURE_FRESHNESS = Gauge(
        "veloshelf_feature_freshness_lag_seconds",
        "Seconds since the last windowed feature row was written",
        registry=_REGISTRY,
    )
    ORDER_EVENT_COUNT = Counter(
        "veloshelf_order_event_count_total",
        "Total order events processed by the Flink job",
        registry=_REGISTRY,
    )
    FEATURE_ORDER_RATE = Gauge(
        "veloshelf_feature_order_rate",
        "Latest order_rate per store/SKU from windowed features",
        labelnames=["store_id", "sku_id"],
        registry=_REGISTRY,
    )

    # Alerts
    ACTIVE_ALERTS = Gauge(
        "veloshelf_active_alerts",
        "Count of unresolved alerts by type",
        labelnames=["alert_type"],
        registry=_REGISTRY,
    )

    # Drift metrics (updated by drift_job.py after each Evidently run)
    PSI_GAUGE = Gauge(
        "veloshelf_psi",
        "Population Stability Index per feature (recent vs reference)",
        labelnames=["feature"],
        registry=_REGISTRY,
    )
    KS_GAUGE = Gauge(
        "veloshelf_ks_statistic",
        "Kolmogorov-Smirnov statistic per feature",
        labelnames=["feature"],
        registry=_REGISTRY,
    )
    JS_GAUGE = Gauge(
        "veloshelf_js_divergence",
        "Jensen-Shannon divergence per feature",
        labelnames=["feature"],
        registry=_REGISTRY,
    )
    DRIFT_DETECTED = Gauge(
        "veloshelf_drift_detected",
        "1 if any feature PSI exceeds the drift threshold, else 0",
        registry=_REGISTRY,
    )

    # Model metrics
    MODEL_MAE = Gauge(
        "veloshelf_model_mae",
        "Latest MAE for the demand forecaster",
        labelnames=["model"],
        registry=_REGISTRY,
    )
    MODEL_F1 = Gauge(
        "veloshelf_model_f1",
        "Latest F1 for the anomaly detector",
        labelnames=["model"],
        registry=_REGISTRY,
    )


# ---------------------------------------------------------------------------
# Push helpers — called from other modules
# ---------------------------------------------------------------------------

def push_freshness_lag(lag_seconds: float) -> None:
    if _PROM_AVAILABLE:
        FEATURE_FRESHNESS.set(lag_seconds)


def push_order_event_count(n: int = 1) -> None:
    if _PROM_AVAILABLE:
        ORDER_EVENT_COUNT.inc(n)


def push_feature_order_rate(store_id: str, sku_id: str, rate: float) -> None:
    if _PROM_AVAILABLE:
        FEATURE_ORDER_RATE.labels(store_id=store_id, sku_id=sku_id).set(rate)


def push_active_alerts(stockout_count: int, surge_count: int) -> None:
    if _PROM_AVAILABLE:
        ACTIVE_ALERTS.labels(alert_type="stockout_risk").set(stockout_count)
        ACTIVE_ALERTS.labels(alert_type="surge").set(surge_count)


def push_drift_metrics(
    feature: str,
    psi: float,
    ks: float,
    js: float,
) -> None:
    if _PROM_AVAILABLE:
        PSI_GAUGE.labels(feature=feature).set(psi)
        KS_GAUGE.labels(feature=feature).set(ks)
        JS_GAUGE.labels(feature=feature).set(js)


def push_drift_detected(detected: bool) -> None:
    if _PROM_AVAILABLE:
        DRIFT_DETECTED.set(1 if detected else 0)


def push_model_metrics(
    model_name: str,
    mae: float | None = None,
    f1: float | None = None,
) -> None:
    if _PROM_AVAILABLE:
        if mae is not None:
            MODEL_MAE.labels(model=model_name).set(mae)
        if f1 is not None:
            MODEL_F1.labels(model=model_name).set(f1)


# ---------------------------------------------------------------------------
# Freshness poller — runs as a background thread, checks Postgres
# ---------------------------------------------------------------------------

def _poll_freshness(dsn: str, interval_s: int = 30) -> None:
    """Background thread: polls Postgres for the latest feature updated_at
    and updates the freshness lag gauge."""
    import time
    try:
        from datetime import UTC, datetime

        import psycopg
    except ImportError:
        logger.warning("psycopg not available — freshness poller disabled.")
        return

    while True:
        try:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT MAX(updated_at) FROM windowed_features;"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        lag = (datetime.now(tz=UTC) - row[0]).total_seconds()
                        push_freshness_lag(lag)
                        logger.debug("Freshness lag: %.1fs", lag)
        except Exception as e:
            logger.warning("Freshness poll failed: %s", e)
        time.sleep(interval_s)


def _poll_alerts(dsn: str, interval_s: int = 30) -> None:
    """Background thread: polls Postgres for active alert counts."""
    import time
    try:
        import psycopg
    except ImportError:
        return

    while True:
        try:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT alert_type, COUNT(*)
                        FROM alerts WHERE resolved = FALSE
                        GROUP BY alert_type;
                        """
                    )
                    counts = {"stockout_risk": 0, "surge": 0}
                    for alert_type, count in cur.fetchall():
                        counts[alert_type] = count
                    push_active_alerts(counts["stockout_risk"], counts["surge"])
        except Exception as e:
            logger.warning("Alert poll failed: %s", e)
        time.sleep(interval_s)


# ---------------------------------------------------------------------------
# MLflow poller — pulls model + drift metrics into Prometheus gauges
# ---------------------------------------------------------------------------

def _poll_mlflow_metrics(tracking_uri: str, interval_s: int = 60) -> None:
    """Background thread: polls MLflow for latest model and drift metrics."""
    import time
    try:
        from mlflow.tracking import MlflowClient
    except ImportError:
        logger.warning("mlflow not installed — MLflow metric poller disabled.")
        return

    client = MlflowClient(tracking_uri=tracking_uri)

    while True:
        try:
            # Model metrics
            for exp_name, metric_key, gauge, label in [
                ("veloshelf-demand-forecaster", "mae", MODEL_MAE, "veloshelf-demand-forecaster"),
                ("veloshelf-anomaly-detector",  "f1",  MODEL_F1,  "veloshelf-anomaly-detector"),
            ]:
                exp = client.get_experiment_by_name(exp_name)
                if exp is None:
                    continue
                runs = client.search_runs(
                    experiment_ids=[exp.experiment_id],
                    order_by=["start_time DESC"],
                    max_results=1,
                )
                if runs and metric_key in runs[0].data.metrics:
                    gauge.labels(model=label).set(runs[0].data.metrics[metric_key])

            # Drift metrics
            drift_exp = client.get_experiment_by_name("veloshelf-drift-monitoring")
            if drift_exp:
                runs = client.search_runs(
                    experiment_ids=[drift_exp.experiment_id],
                    order_by=["start_time DESC"],
                    max_results=1,
                )
                if runs:
                    m = runs[0].data.metrics
                    for feat in ["order_rate", "depletion_vel", "demand_momentum", "on_hand_est"]:
                        if f"{feat}_psi"    in m: PSI_GAUGE.labels(feature=feat).set(m[f"{feat}_psi"])
                        if f"{feat}_ks_stat" in m: KS_GAUGE.labels(feature=feat).set(m[f"{feat}_ks_stat"])
                        if f"{feat}_js_div"  in m: JS_GAUGE.labels(feature=feat).set(m[f"{feat}_js_div"])
                    if "n_features_drifted" in m:
                        DRIFT_DETECTED.set(1 if m["n_features_drifted"] > 0 else 0)

        except Exception as e:
            logger.warning("MLflow metric poll failed: %s", e)
        time.sleep(interval_s)


# ---------------------------------------------------------------------------
# Entrypoint — standalone metrics server
# ---------------------------------------------------------------------------

def start_exporter(
    dsn: str | None = None,
    port: int = METRICS_PORT,
) -> None:
    """Start the Prometheus metrics HTTP server + background pollers."""
    if not _PROM_AVAILABLE:
        logger.error("prometheus_client not installed. Run: pip install prometheus-client")
        return

    _dsn = dsn or os.getenv(
        "POSTGRES_DSN",
        "postgresql://veloshelf:veloshelf@localhost:5432/veloshelf",
    )

    # Start Prometheus HTTP server
    start_http_server(port, registry=_REGISTRY)
    logger.info("Prometheus metrics server started on :%d/metrics", port)

    _mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

    # Background pollers
    threading.Thread(target=_poll_freshness,      args=(_dsn,),        daemon=True).start()
    threading.Thread(target=_poll_alerts,         args=(_dsn,),        daemon=True).start()
    threading.Thread(target=_poll_mlflow_metrics, args=(_mlflow_uri,), daemon=True).start()
    logger.info("Background pollers started (freshness + alerts + mlflow).")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s | %(message)s")
    import time
    start_exporter()
    logger.info("Metrics exporter running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Stopped.")