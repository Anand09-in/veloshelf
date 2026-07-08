"""VeloShelf — Streamlit business dashboard (Phase 4).

Business-facing live view reading from Postgres windowed_features + alerts.
Designed for a supply-chain manager or ops lead, not an engineer.

Panels:
  1. Header     — pipeline freshness status + overall health badge
  2. Stockout   — SKUs at risk sorted by depletion velocity (worst first)
  3. Surge      — active surge alerts sorted by momentum
  4. Velocity   — per-category order rate heatmap across stores
  5. Store view — per-store health summary (avg on_hand, alert count)
  6. Drift      — latest PSI / KS / JS per feature (from data/reports/)

Run:
    streamlit run serving/streamlit_app.py
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="VeloShelf — Dark Store Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://veloshelf:veloshelf@localhost:5432/veloshelf",
)
REPORTS_PATH = Path(os.getenv("REPORTS_PATH", "data/reports"))
REFRESH_S    = int(os.getenv("DASHBOARD_REFRESH_S", "30"))


@st.cache_resource
def get_connection():
    import psycopg
    return psycopg.connect(POSTGRES_DSN, autocommit=True)


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_features() -> pd.DataFrame:
    return query("""
        SELECT store_id, sku_id, order_rate, depletion_vel,
               demand_momentum, on_hand_est, updated_at
        FROM windowed_features
        ORDER BY updated_at DESC
    """)


def fetch_active_alerts() -> pd.DataFrame:
    return query("""
        SELECT alert_id, alert_type, store_id, sku_id,
               triggered_at, metric_value, threshold
        FROM alerts
        WHERE resolved = FALSE
        ORDER BY triggered_at DESC
    """)


def fetch_stockout_risks(reorder_threshold: int = 40) -> pd.DataFrame:
    return query("""
        SELECT store_id, sku_id, on_hand_est, depletion_vel,
               order_rate, demand_momentum, updated_at
        FROM windowed_features
        WHERE on_hand_est <= %s
        ORDER BY depletion_vel DESC
    """, (reorder_threshold,))


def fetch_surge_alerts() -> pd.DataFrame:
    return query("""
        SELECT a.store_id, a.sku_id, a.metric_value AS momentum,
               a.triggered_at, f.order_rate, f.on_hand_est
        FROM alerts a
        LEFT JOIN windowed_features f
          ON a.store_id = f.store_id AND a.sku_id = f.sku_id
        WHERE a.alert_type = 'surge' AND a.resolved = FALSE
        ORDER BY a.metric_value DESC
    """)


def fetch_category_velocity() -> pd.DataFrame:
    """Join features with seed SKU data to get per-category rates."""
    return query("""
        SELECT store_id, sku_id,
               order_rate, depletion_vel, on_hand_est
        FROM windowed_features
        ORDER BY order_rate DESC
    """)


def fetch_freshness() -> dict:
    df = query("SELECT MAX(updated_at) AS latest FROM windowed_features")
    if df.empty or df["latest"].isna().all():
        return {"lag_s": None, "status": "no_data"}
    latest = pd.to_datetime(df["latest"].iloc[0], utc=True)
    lag_s  = (datetime.now(tz=UTC) - latest).total_seconds()
    status = "healthy" if lag_s < 120 else "stale" if lag_s < 300 else "down"
    return {"lag_s": lag_s, "latest": latest, "status": status}


def load_latest_drift_report() -> dict | None:
    """Load the most recent drift metrics from MLflow run JSON if available."""
    files = sorted(REPORTS_PATH.glob("drift_*.html"), reverse=True)
    return {"report_path": str(files[0])} if files else None


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def status_badge(status: str) -> str:
    return {"healthy": "🟢 Healthy", "stale": "🟡 Stale", "down": "🔴 Down",
            "no_data": "⚫ No data"}.get(status, "⚫ Unknown")


def _fmt_lag(lag_s: float | None) -> str:
    if lag_s is None:
        return "—"
    if lag_s < 60:
        return f"{lag_s:.0f}s ago"
    return f"{lag_s / 60:.1f}m ago"


# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------

def main() -> None:
    # Auto-refresh
    st.markdown(
        f"<meta http-equiv='refresh' content='{REFRESH_S}'>",
        unsafe_allow_html=True,
    )

    # ---- Header ----
    st.title("🛒 VeloShelf — Dark Store Intelligence")
    freshness = fetch_freshness()
    alerts_df = fetch_active_alerts()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Pipeline Status", status_badge(freshness["status"]))
    col2.metric("Last Feature", _fmt_lag(freshness.get("lag_s")))
    stockout_count = (
        str(len(alerts_df[alerts_df["alert_type"] == "stockout_risk"]))
        if not alerts_df.empty else "0"
    )
    col3.metric("Active Stockout Alerts", stockout_count)
    col4.metric(
        "Active Surge Alerts",
        str(len(alerts_df[alerts_df["alert_type"] == "surge"])) if not alerts_df.empty else "0",
    )

    st.divider()

    # ---- Stockout Risks ----
    st.subheader("⚠️ Stockout Risk — SKUs Below Reorder Point")
    stockout_df = fetch_stockout_risks()
    if stockout_df.empty:
        st.success("No SKUs currently below reorder threshold.")
    else:
        st.dataframe(
            stockout_df.rename(columns={
                "store_id": "Store", "sku_id": "SKU",
                "on_hand_est": "On Hand", "depletion_vel": "Depletion Vel (units/min)",
                "order_rate": "Order Rate (orders/min)", "demand_momentum": "Momentum",
                "updated_at": "Last Updated",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ---- Surge Alerts ----
    st.subheader("🚀 Active Demand Surges")
    surge_df = fetch_surge_alerts()
    if surge_df.empty:
        st.info("No active surge alerts.")
    else:
        st.dataframe(
            surge_df.rename(columns={
                "store_id": "Store", "sku_id": "SKU",
                "momentum": "Demand Momentum", "triggered_at": "Triggered At",
                "order_rate": "Order Rate", "on_hand_est": "On Hand",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ---- Category velocity ----
    st.subheader("📦 Live Order Velocity by Store")
    velocity_df = fetch_category_velocity()
    if not velocity_df.empty:
        pivot = velocity_df.pivot_table(
            index="sku_id", columns="store_id",
            values="order_rate", aggfunc="mean",
        ).fillna(0).round(3)
        st.dataframe(pivot, use_container_width=True)
    else:
        st.info("No feature data available yet.")

    st.divider()

    # ---- Store health summary ----
    st.subheader("🏪 Store Health Summary")
    features_df = fetch_features()
    if not features_df.empty:
        store_summary = (
            features_df
            .groupby("store_id")
            .agg(
                avg_on_hand=("on_hand_est", "mean"),
                avg_order_rate=("order_rate", "mean"),
                avg_momentum=("demand_momentum", "mean"),
            )
            .round(2)
            .reset_index()
        )
        if not alerts_df.empty:
            alert_counts = (
                alerts_df.groupby("store_id")
                .size()
                .reset_index(name="active_alerts")
            )
            store_summary = store_summary.merge(alert_counts, on="store_id", how="left")
            store_summary["active_alerts"] = store_summary["active_alerts"].fillna(0).astype(int)
        st.dataframe(
            store_summary.rename(columns={
                "store_id": "Store",
                "avg_on_hand": "Avg On Hand",
                "avg_order_rate": "Avg Order Rate",
                "avg_momentum": "Avg Momentum",
                "active_alerts": "Active Alerts",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ---- Drift panel ----
    st.subheader("📊 Data Drift — Latest Report")
    drift = load_latest_drift_report()
    if drift:
        st.success(f"Latest report: `{drift['report_path']}`")
        st.info(
            "Open the HTML report in your browser for the full Evidently "
            "drift analysis (PSI / KS / JS per feature)."
        )
        st.caption(
            "PSI rule: < 0.10 stable · 0.10–0.25 moderate · > 0.25 significant drift"
        )
    else:
        st.info("No drift reports yet. Run `python -m observability.drift_job` first.")

    # ---- Footer ----
    st.caption(
        f"VeloShelf · auto-refreshes every {REFRESH_S}s · "
        f"last render: {datetime.now(tz=UTC).strftime('%H:%M:%S UTC')}"
    )


if __name__ == "__main__":
    main()